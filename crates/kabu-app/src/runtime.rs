use crate::coordinator::Coordinator;
use async_channel::{Receiver, Sender, TrySendError};
use kabu_core::{
    AssetClass, CachedMarketState, Candle, ChartHistory, ChartPeriod, ChartRequestKey, DataSource,
    GenerationId, Instrument, InstrumentId, InstrumentSearch, MarketDataError, MarketDataErrorKind,
    PersistedState, PersistenceError, Quote, QuoteSnapshots, QuoteSubscription, SearchResult,
    StateRepository, rank_search_results,
};
use kabu_data::{BinanceClient, JsonRepository, Recovery, YahooClient};
use std::{
    collections::VecDeque,
    fmt, io,
    path::Path,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread::{self, JoinHandle},
};

const COMMAND_CAPACITY: usize = 64;
const EVENT_CAPACITY: usize = 128;
const MAX_CONCURRENT_OTHER_JOBS: usize = 31;

#[derive(Clone, Debug, PartialEq)]
pub enum RuntimeCommand {
    Search {
        generation: GenerationId,
        query: String,
    },
    Snapshot {
        instruments: Vec<Instrument>,
    },
    History {
        generation: GenerationId,
        instrument: Instrument,
        period: ChartPeriod,
    },
    SetVisible(bool),
    SetWatchlist(Vec<Instrument>),
    SetCryptoWatchlist(Vec<Instrument>),
    PersistState(PersistedState),
    PersistMarketCache {
        cache: CachedMarketState,
    },
    Shutdown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RuntimeSendError {
    #[error("the background command queue is full")]
    Full,
    #[error("the background runtime is closed")]
    Closed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderWarning {
    pub source: DataSource,
    pub kind: MarketDataErrorKind,
    pub message: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StreamStatus {
    Connecting,
    Live,
    Reconnecting,
    Paused,
    Stopped,
}

#[derive(Debug)]
pub enum RuntimeEvent {
    SearchCompleted {
        generation: GenerationId,
        results: Vec<SearchResult>,
        warnings: Vec<ProviderWarning>,
    },
    Quotes(Vec<Quote>),
    QuoteError {
        instrument_ids: Vec<InstrumentId>,
        error: MarketDataError,
    },
    HistoryCompleted {
        generation: GenerationId,
        key: ChartRequestKey,
        candles: Vec<Candle>,
    },
    HistoryFailed {
        generation: GenerationId,
        key: ChartRequestKey,
        error: MarketDataError,
    },
    CatalogRefresh {
        result: Result<(), MarketDataErrorKind>,
        catalog_available: bool,
    },
    StreamStatus(StreamStatus),
}

pub trait EquityProvider: InstrumentSearch + QuoteSnapshots + ChartHistory {}
impl<T: InstrumentSearch + QuoteSnapshots + ChartHistory> EquityProvider for T {}

pub trait CryptoProvider:
    InstrumentSearch + QuoteSnapshots + ChartHistory + QuoteSubscription
{
}
impl<T: InstrumentSearch + QuoteSnapshots + ChartHistory + QuoteSubscription> CryptoProvider for T {}

#[derive(Clone)]
pub struct ProviderSet {
    pub equities: Arc<dyn EquityProvider>,
    pub crypto: Arc<dyn CryptoProvider>,
}

pub struct ProductionComponents {
    pub state: Recovery<PersistedState>,
    pub cache: Recovery<CachedMarketState>,
    yahoo: YahooClient,
    binance: BinanceClient,
    repository: Arc<JsonRepository>,
}

#[derive(Debug, thiserror::Error)]
pub enum ProductionStartError {
    #[error(transparent)]
    Persistence(#[from] PersistenceError),
    #[error(transparent)]
    Http(#[from] reqwest::Error),
    #[error(transparent)]
    Io(#[from] io::Error),
}

impl ProductionComponents {
    pub fn production() -> Result<Self, ProductionStartError> {
        Self::from_repository(JsonRepository::default_macos())
    }

    pub fn at(directory: impl AsRef<Path>) -> Result<Self, ProductionStartError> {
        Self::from_repository(JsonRepository::at(directory.as_ref()))
    }

    fn from_repository(repository: JsonRepository) -> Result<Self, ProductionStartError> {
        let state = repository.load_state_with_recovery()?;
        let cache = repository.load_cache_with_recovery()?;
        let yahoo = YahooClient::production()?;
        let binance = BinanceClient::production(cache.value.instrument_catalog.clone())?;
        Ok(Self {
            state,
            cache,
            yahoo,
            binance,
            repository: Arc::new(repository),
        })
    }

    pub fn start(self) -> Result<ProductionLaunch, ProductionStartError> {
        let cache_for_runtime = self.cache.value.clone();
        let binance = self.binance;
        let providers = ProviderSet {
            equities: Arc::new(self.yahoo),
            crypto: Arc::new(binance.clone()),
        };
        let runtime = RuntimeHandle::start_inner(
            providers,
            self.repository,
            cache_for_runtime,
            Some(binance),
        )?;
        Ok(ProductionLaunch {
            state: self.state,
            cache: self.cache,
            runtime,
        })
    }
}

pub struct ProductionLaunch {
    pub state: Recovery<PersistedState>,
    pub cache: Recovery<CachedMarketState>,
    pub runtime: RuntimeHandle,
}

#[derive(Clone)]
pub struct RuntimeHandle {
    mode: RuntimeMode,
}

#[derive(Clone)]
enum RuntimeMode {
    Recording(Arc<RecordingRuntime>),
    Live(Arc<LiveRuntime>),
}

struct RecordingRuntime {
    commands: Mutex<VecDeque<RuntimeCommand>>,
    events: Receiver<RuntimeEvent>,
    event_sender: Sender<RuntimeEvent>,
    shutdown: AtomicBool,
}

struct LiveRuntime {
    commands: Sender<RuntimeCommand>,
    controls: Arc<ControlLane>,
    shutdown_signal: Sender<()>,
    events: Receiver<RuntimeEvent>,
    final_flush: Arc<Mutex<Option<FinalFlush>>>,
    completion: Arc<ShutdownCompletion>,
    join: Mutex<Option<JoinHandle<()>>>,
    shutdown: AtomicBool,
}

#[derive(Clone)]
struct DesiredControlState {
    revision: u64,
    visible: bool,
    watchlist: Vec<Instrument>,
}

struct ControlLane {
    desired: Mutex<DesiredControlState>,
    applied_revision: AtomicU64,
    notify: Sender<()>,
}

struct ShutdownCompletion {
    complete: Mutex<bool>,
    changed: Condvar,
}

struct FinalFlush {
    state: PersistedState,
    cache: CachedMarketState,
}

struct RuntimeStorage {
    repository: Arc<dyn StateRepository>,
    cache: Arc<Mutex<CachedMarketState>>,
    final_flush: Arc<Mutex<Option<FinalFlush>>>,
}

struct NetworkInputs {
    commands: Receiver<RuntimeCommand>,
    controls: Arc<ControlLane>,
    control_notifications: Receiver<()>,
    events: Sender<RuntimeEvent>,
    shutdown: Receiver<()>,
    providers: ProviderSet,
    storage: RuntimeStorage,
    catalog_client: Option<BinanceClient>,
}

#[derive(Default)]
struct DesiredPersistence {
    state: Option<PersistedState>,
    cache: Option<CachedMarketState>,
}

struct PersistenceLane {
    desired: Mutex<DesiredPersistence>,
    notify: Sender<()>,
}

impl Default for RuntimeHandle {
    fn default() -> Self {
        let (event_sender, events) = async_channel::bounded(1);
        Self {
            mode: RuntimeMode::Recording(Arc::new(RecordingRuntime {
                commands: Mutex::new(VecDeque::with_capacity(COMMAND_CAPACITY)),
                events,
                event_sender,
                shutdown: AtomicBool::new(false),
            })),
        }
    }
}

impl fmt::Debug for RuntimeHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RuntimeHandle")
            .field(
                "mode",
                &match self.mode {
                    RuntimeMode::Recording(_) => "recording",
                    RuntimeMode::Live(_) => "live",
                },
            )
            .finish()
    }
}

impl RuntimeHandle {
    pub fn start(providers: ProviderSet, repository: Arc<dyn StateRepository>) -> io::Result<Self> {
        Self::start_inner(providers, repository, CachedMarketState::default(), None)
    }

    fn start_inner(
        providers: ProviderSet,
        repository: Arc<dyn StateRepository>,
        initial_cache: CachedMarketState,
        catalog_client: Option<BinanceClient>,
    ) -> io::Result<Self> {
        let (commands, command_receiver) = async_channel::bounded(COMMAND_CAPACITY);
        let (event_sender, events) = async_channel::bounded(EVENT_CAPACITY);
        let (shutdown_signal, shutdown_receiver) = async_channel::bounded(1);
        let (control_notify, control_notifications) = async_channel::bounded(1);
        let controls = Arc::new(ControlLane {
            desired: Mutex::new(DesiredControlState {
                revision: 0,
                visible: true,
                watchlist: Vec::new(),
            }),
            applied_revision: AtomicU64::new(0),
            notify: control_notify,
        });
        let completion = Arc::new(ShutdownCompletion {
            complete: Mutex::new(false),
            changed: Condvar::new(),
        });
        let final_flush = Arc::new(Mutex::new(None));
        let storage = RuntimeStorage {
            repository,
            cache: Arc::new(Mutex::new(initial_cache)),
            final_flush: final_flush.clone(),
        };
        let network_controls = controls.clone();
        let network_completion = completion.clone();
        let join = thread::Builder::new()
            .name("kabu-network".into())
            .spawn(move || {
                run_network(NetworkInputs {
                    commands: command_receiver,
                    controls: network_controls,
                    control_notifications,
                    events: event_sender,
                    shutdown: shutdown_receiver,
                    providers,
                    storage,
                    catalog_client,
                });
                *network_completion
                    .complete
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = true;
                network_completion.changed.notify_all();
            })?;

        Ok(Self {
            mode: RuntimeMode::Live(Arc::new(LiveRuntime {
                commands,
                controls,
                shutdown_signal,
                events,
                final_flush,
                completion,
                join: Mutex::new(Some(join)),
                shutdown: AtomicBool::new(false),
            })),
        })
    }

    pub fn send(&self, command: RuntimeCommand) -> Result<(), RuntimeSendError> {
        match &self.mode {
            RuntimeMode::Recording(runtime) => {
                if runtime.shutdown.load(Ordering::SeqCst) {
                    return Err(RuntimeSendError::Closed);
                }
                let mut commands = runtime
                    .commands
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if is_control_command(&command) {
                    commands.retain(|existing| !same_control_slot(existing, &command));
                    commands.push_back(command);
                    return Ok(());
                }
                if commands
                    .iter()
                    .filter(|command| !is_control_command(command))
                    .count()
                    == COMMAND_CAPACITY
                {
                    return Err(RuntimeSendError::Full);
                }
                commands.push_back(command);
                Ok(())
            }
            RuntimeMode::Live(runtime) => {
                if matches!(command, RuntimeCommand::Shutdown) {
                    self.shutdown();
                    return Ok(());
                }
                if runtime.shutdown.load(Ordering::SeqCst) {
                    return Err(RuntimeSendError::Closed);
                }
                match command {
                    RuntimeCommand::SetVisible(visible) => {
                        runtime.controls.update_visibility(visible);
                        Ok(())
                    }
                    RuntimeCommand::SetWatchlist(watchlist) => {
                        runtime.controls.update_watchlist(watchlist);
                        Ok(())
                    }
                    RuntimeCommand::SetCryptoWatchlist(crypto) => {
                        runtime.controls.update_crypto_watchlist(crypto);
                        Ok(())
                    }
                    command => runtime
                        .commands
                        .try_send(command)
                        .map_err(|error| match error {
                            TrySendError::Full(_) => RuntimeSendError::Full,
                            TrySendError::Closed(_) => RuntimeSendError::Closed,
                        }),
                }
            }
        }
    }

    pub fn events(&self) -> Receiver<RuntimeEvent> {
        match &self.mode {
            RuntimeMode::Recording(runtime) => runtime.events.clone(),
            RuntimeMode::Live(runtime) => runtime.events.clone(),
        }
    }

    pub fn recorded_commands(&self) -> Vec<RuntimeCommand> {
        match &self.mode {
            RuntimeMode::Recording(runtime) => runtime
                .commands
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .iter()
                .cloned()
                .collect(),
            RuntimeMode::Live(_) => Vec::new(),
        }
    }

    #[doc(hidden)]
    pub fn take_recorded_commands(&self) -> Vec<RuntimeCommand> {
        match &self.mode {
            RuntimeMode::Recording(runtime) => runtime
                .commands
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .drain(..)
                .collect(),
            RuntimeMode::Live(_) => Vec::new(),
        }
    }

    pub fn shutdown(&self) {
        let runtime = match &self.mode {
            RuntimeMode::Recording(runtime) => {
                runtime.shutdown.store(true, Ordering::SeqCst);
                runtime.event_sender.close();
                return;
            }
            RuntimeMode::Live(runtime) => runtime,
        };
        initiate_shutdown(runtime);
        _ = wait_for_live_shutdown(runtime, std::time::Duration::from_secs(2));
    }

    pub fn shutdown_with_flush(&self, state: PersistedState, cache: CachedMarketState) {
        self.begin_shutdown_with_flush(state, cache);
        _ = self.wait_for_shutdown(std::time::Duration::from_secs(2));
    }

    pub fn begin_shutdown_with_flush(&self, state: PersistedState, cache: CachedMarketState) {
        match &self.mode {
            RuntimeMode::Recording(_) => {
                _ = self.send(RuntimeCommand::PersistState(state));
                _ = self.send(RuntimeCommand::PersistMarketCache { cache });
            }
            RuntimeMode::Live(runtime) => {
                *runtime
                    .final_flush
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) =
                    Some(FinalFlush { state, cache });
            }
        }
        self.begin_shutdown();
    }

    pub fn begin_shutdown(&self) {
        match &self.mode {
            RuntimeMode::Recording(runtime) => {
                let mut commands = runtime
                    .commands
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if commands.len() < COMMAND_CAPACITY {
                    commands.push_back(RuntimeCommand::Shutdown);
                }
                runtime.shutdown.store(true, Ordering::SeqCst);
                runtime.event_sender.close();
            }
            RuntimeMode::Live(runtime) => initiate_shutdown(runtime),
        }
    }

    pub fn wait_for_shutdown(&self, timeout: std::time::Duration) -> bool {
        match &self.mode {
            RuntimeMode::Recording(runtime) => runtime.shutdown.load(Ordering::SeqCst),
            RuntimeMode::Live(runtime) => wait_for_live_shutdown(runtime, timeout),
        }
    }

    pub fn is_shutdown_complete(&self) -> bool {
        match &self.mode {
            RuntimeMode::Recording(runtime) => runtime.shutdown.load(Ordering::SeqCst),
            RuntimeMode::Live(runtime) => *runtime
                .completion
                .complete
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        }
    }

    pub fn control_state_is_synchronized(&self) -> bool {
        match &self.mode {
            RuntimeMode::Recording(_) => true,
            RuntimeMode::Live(runtime) => runtime.controls.is_synchronized(),
        }
    }

    pub fn retry_control_state(&self) {
        if let RuntimeMode::Live(runtime) = &self.mode {
            runtime.controls.notify();
        }
    }

    pub fn wait_for_control_state(&self, timeout: std::time::Duration) -> bool {
        let started = std::time::Instant::now();
        while !self.control_state_is_synchronized() && started.elapsed() < timeout {
            self.retry_control_state();
            thread::sleep(std::time::Duration::from_millis(1));
        }
        self.control_state_is_synchronized()
    }

    pub fn is_shutdown(&self) -> bool {
        match &self.mode {
            RuntimeMode::Recording(runtime) => runtime.shutdown.load(Ordering::SeqCst),
            RuntimeMode::Live(runtime) => runtime.shutdown.load(Ordering::SeqCst),
        }
    }

    pub fn is_live(&self) -> bool {
        matches!(self.mode, RuntimeMode::Live(_))
    }
}

fn is_control_command(command: &RuntimeCommand) -> bool {
    matches!(
        command,
        RuntimeCommand::SetVisible(_)
            | RuntimeCommand::SetWatchlist(_)
            | RuntimeCommand::SetCryptoWatchlist(_)
    )
}

fn same_control_slot(left: &RuntimeCommand, right: &RuntimeCommand) -> bool {
    matches!(
        (left, right),
        (RuntimeCommand::SetVisible(_), RuntimeCommand::SetVisible(_))
            | (
                RuntimeCommand::SetWatchlist(_),
                RuntimeCommand::SetWatchlist(_)
            )
            | (
                RuntimeCommand::SetCryptoWatchlist(_),
                RuntimeCommand::SetCryptoWatchlist(_)
            )
    )
}

impl Drop for LiveRuntime {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        _ = self.shutdown_signal.try_send(());
        _ = self.commands.try_send(RuntimeCommand::Shutdown);
        self.commands.close();
        self.controls.notify.close();
        self.shutdown_signal.close();
        let mut complete = self
            .completion
            .complete
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while !*complete {
            complete = self
                .completion
                .changed
                .wait_timeout(complete, std::time::Duration::from_secs(2))
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .0;
        }
        if let Some(join) = self
            .join
            .get_mut()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
        {
            _ = join.join();
        }
    }
}

impl ControlLane {
    fn update_visibility(&self, visible: bool) {
        self.update(|state| state.visible = visible);
    }

    fn update_watchlist(&self, watchlist: Vec<Instrument>) {
        self.update(|state| state.watchlist = watchlist);
    }

    fn update_crypto_watchlist(&self, crypto: Vec<Instrument>) {
        self.update(|state| {
            state
                .watchlist
                .retain(|instrument| instrument.id.asset_class() == AssetClass::Equity);
            state.watchlist.extend(crypto);
        });
    }

    fn update(&self, mutate: impl FnOnce(&mut DesiredControlState)) {
        let mut desired = self
            .desired
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        mutate(&mut desired);
        desired.revision = desired.revision.wrapping_add(1);
        drop(desired);
        self.notify();
    }

    fn notify(&self) {
        _ = self.notify.try_send(());
    }

    fn is_synchronized(&self) -> bool {
        let desired_revision = self
            .desired
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .revision;
        self.applied_revision.load(Ordering::SeqCst) == desired_revision
    }
}

fn initiate_shutdown(runtime: &LiveRuntime) {
    if runtime.shutdown.swap(true, Ordering::SeqCst) {
        return;
    }
    // Cancellation is a dedicated reliable edge and is always signalled before
    // asking the bounded ordinary command queue to record Shutdown.
    _ = runtime.shutdown_signal.try_send(());
    _ = runtime.commands.try_send(RuntimeCommand::Shutdown);
    runtime.commands.close();
    runtime.controls.notify.close();
}

fn wait_for_live_shutdown(runtime: &LiveRuntime, timeout: std::time::Duration) -> bool {
    let complete = runtime
        .completion
        .complete
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let (complete, _) = runtime
        .completion
        .changed
        .wait_timeout_while(complete, timeout, |complete| !*complete)
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if !*complete {
        return false;
    }
    drop(complete);
    if let Some(join) = runtime
        .join
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .take()
    {
        _ = join.join();
    }
    true
}

fn run_network(inputs: NetworkInputs) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("kabu-network")
        .enable_all()
        .build();
    let Ok(runtime) = runtime else {
        return;
    };
    runtime.block_on(run_dispatcher(inputs));
}

async fn run_dispatcher(inputs: NetworkInputs) {
    let NetworkInputs {
        commands,
        controls,
        control_notifications,
        events,
        shutdown,
        providers,
        storage,
        catalog_client,
    } = inputs;
    let RuntimeStorage {
        repository,
        cache: cache_state,
        final_flush,
    } = storage;
    let mut jobs = tokio::task::JoinSet::new();
    let mut search_job: Option<tokio::task::JoinHandle<()>> = None;
    let mut pending = VecDeque::with_capacity(COMMAND_CAPACITY);
    let mut other_in_flight = 0usize;
    let mut visible = true;
    let mut coordinator = Coordinator::new(providers.clone(), events.clone(), Vec::new());
    let (persistence_notify, persistence_notifications) = async_channel::bounded(1);
    let persistence = Arc::new(PersistenceLane {
        desired: Mutex::new(DesiredPersistence::default()),
        notify: persistence_notify,
    });
    let persistence_job = tokio::spawn(run_persistence(
        persistence.clone(),
        persistence_notifications,
        repository.clone(),
        cache_state.clone(),
    ));
    let mut catalog_job = catalog_client.map(|client| {
        let events = events.clone();
        tokio::spawn(async move {
            let result =
                refresh_catalog_cache(client.clone(), repository.clone(), cache_state.clone())
                    .await
                    .map_err(|error| error.kind);
            let catalog_available = client.catalog().ok().flatten().is_some();
            _ = events
                .send(RuntimeEvent::CatalogRefresh {
                    result,
                    catalog_available,
                })
                .await;
        })
    });

    loop {
        if !controls.is_synchronized() {
            _ = control_notifications.try_recv();
            let desired = controls
                .desired
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone();
            if desired.visible {
                coordinator.set_watchlist(desired.watchlist).await;
                visible = true;
                coordinator.set_visible(true).await;
            } else {
                visible = false;
                if let Some(job) = search_job.take() {
                    job.abort();
                    _ = job.await;
                }
                pending.clear();
                jobs.abort_all();
                while jobs.join_next().await.is_some() {}
                other_in_flight = 0;
                coordinator.set_visible(false).await;
                coordinator.set_watchlist(desired.watchlist).await;
            }
            controls
                .applied_revision
                .store(desired.revision, Ordering::SeqCst);
            continue;
        }
        while other_in_flight < MAX_CONCURRENT_OTHER_JOBS {
            let Some(command) = pending.pop_front() else {
                break;
            };
            spawn_other_job(command, &mut jobs, &events, &providers);
            other_in_flight += 1;
        }

        tokio::select! {
            biased;
            _ = shutdown.recv() => break,
            control = control_notifications.recv() => {
                if control.is_err() {
                    continue;
                }
            }
            completed = jobs.join_next(), if !jobs.is_empty() => {
                if completed.is_some() {
                    other_in_flight = other_in_flight.saturating_sub(1);
                }
            }
            command = commands.recv(), if pending.len() < COMMAND_CAPACITY => {
                let Ok(command) = command else {
                    break;
                };
                match command {
                    RuntimeCommand::Search { generation, query } => {
                        if !visible {
                            continue;
                        }
                        if let Some(job) = search_job.take() {
                            job.abort();
                            _ = job.await;
                        }
                        let events = events.clone();
                        let providers = providers.clone();
                        search_job = Some(tokio::spawn(async move {
                            send_search(generation, query, &providers, &events).await;
                        }));
                    }
                    RuntimeCommand::Snapshot { .. } | RuntimeCommand::History { .. } => {
                        if !visible {
                            continue;
                        } else if other_in_flight < MAX_CONCURRENT_OTHER_JOBS {
                            spawn_other_job(command, &mut jobs, &events, &providers);
                            other_in_flight += 1;
                        } else {
                            pending.push_back(command);
                        }
                    }
                    RuntimeCommand::SetVisible(next_visible) => {
                        visible = next_visible;
                        coordinator.set_visible(next_visible).await;
                        if !next_visible {
                            if let Some(job) = search_job.take() {
                                job.abort();
                                _ = job.await;
                            }
                            pending.clear();
                            jobs.abort_all();
                            while jobs.join_next().await.is_some() {}
                            other_in_flight = 0;
                        }
                    }
                    RuntimeCommand::SetWatchlist(watchlist) => {
                        coordinator.set_watchlist(watchlist).await;
                    }
                    RuntimeCommand::SetCryptoWatchlist(watchlist) => {
                        coordinator.set_crypto_watchlist(watchlist).await;
                    }
                    RuntimeCommand::PersistState(state) => {
                        persistence.update_state(state);
                    }
                    RuntimeCommand::PersistMarketCache { cache } => {
                        persistence.update_cache(cache);
                    }
                    RuntimeCommand::Shutdown => break,
                }
            }
        }
    }

    if let Some(job) = search_job {
        job.abort();
        _ = job.await;
    }
    jobs.abort_all();
    while jobs.join_next().await.is_some() {}
    coordinator.shutdown().await;
    if let Some(job) = catalog_job.take() {
        job.abort();
        _ = job.await;
    }
    while let Ok(command) = commands.try_recv() {
        match command {
            RuntimeCommand::PersistState(state) => persistence.update_state(state),
            RuntimeCommand::PersistMarketCache { cache } => persistence.update_cache(cache),
            _ => {}
        }
    }
    if let Some(final_flush) = final_flush
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .take()
    {
        persistence.update_state(final_flush.state);
        persistence.update_cache(final_flush.cache);
    }
    persistence.notify.close();
    _ = persistence_job.await;
}

impl PersistenceLane {
    fn update_state(&self, state: PersistedState) {
        self.desired
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .state = Some(state);
        _ = self.notify.try_send(());
    }

    fn update_cache(&self, cache: CachedMarketState) {
        self.desired
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .cache = Some(cache);
        _ = self.notify.try_send(());
    }

    fn take(&self) -> DesiredPersistence {
        std::mem::take(
            &mut *self
                .desired
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        )
    }
}

async fn run_persistence(
    persistence: Arc<PersistenceLane>,
    notifications: Receiver<()>,
    repository: Arc<dyn StateRepository>,
    cache_state: Arc<Mutex<CachedMarketState>>,
) {
    while notifications.recv().await.is_ok() {
        loop {
            let desired = persistence.take();
            if desired.state.is_none() && desired.cache.is_none() {
                break;
            }
            let repository = repository.clone();
            let cache_state = cache_state.clone();
            _ = tokio::task::spawn_blocking(move || {
                if let Some(state) = desired.state {
                    _ = repository.save_state(&state);
                }
                if let Some(cache) = desired.cache {
                    let mut current = cache_state
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    current.quotes = cache.quotes;
                    current.charts = cache.charts;
                    _ = repository.save_cache(&current);
                }
            })
            .await;
        }
    }
}

fn spawn_other_job(
    command: RuntimeCommand,
    jobs: &mut tokio::task::JoinSet<()>,
    events: &Sender<RuntimeEvent>,
    providers: &ProviderSet,
) {
    let events = events.clone();
    let providers = providers.clone();
    jobs.spawn(async move {
        match command {
            RuntimeCommand::Snapshot { instruments } => {
                send_snapshots(instruments, &providers, &events).await;
            }
            RuntimeCommand::History {
                generation,
                instrument,
                period,
            } => {
                send_history(generation, instrument, period, &providers, &events).await;
            }
            RuntimeCommand::Search { .. }
            | RuntimeCommand::SetVisible(_)
            | RuntimeCommand::SetWatchlist(_)
            | RuntimeCommand::SetCryptoWatchlist(_)
            | RuntimeCommand::PersistState(_)
            | RuntimeCommand::PersistMarketCache { .. }
            | RuntimeCommand::Shutdown => {}
        }
    });
}

async fn send_search(
    generation: GenerationId,
    query: String,
    providers: &ProviderSet,
    events: &Sender<RuntimeEvent>,
) {
    let (equities, crypto) = tokio::join!(
        providers.equities.search(&query),
        providers.crypto.search(&query)
    );
    let mut results = Vec::new();
    let mut warnings = Vec::new();
    collect_search_outcome(DataSource::Yahoo, equities, &mut results, &mut warnings);
    collect_search_outcome(DataSource::Binance, crypto, &mut results, &mut warnings);
    _ = events
        .send(RuntimeEvent::SearchCompleted {
            generation,
            results: rank_search_results(&query, results),
            warnings,
        })
        .await;
}

async fn send_history(
    generation: GenerationId,
    instrument: Instrument,
    period: ChartPeriod,
    providers: &ProviderSet,
    events: &Sender<RuntimeEvent>,
) {
    let key = ChartRequestKey {
        instrument_id: instrument.id.clone(),
        period,
    };
    let outcome = match instrument.id.asset_class() {
        AssetClass::Equity => providers.equities.history(&instrument, period).await,
        AssetClass::Crypto => providers.crypto.history(&instrument, period).await,
    };
    let event = match outcome {
        Ok(candles) => RuntimeEvent::HistoryCompleted {
            generation,
            key,
            candles,
        },
        Err(error) => RuntimeEvent::HistoryFailed {
            generation,
            key,
            error,
        },
    };
    _ = events.send(event).await;
}

fn collect_search_outcome(
    source: DataSource,
    outcome: Result<Vec<SearchResult>, MarketDataError>,
    results: &mut Vec<SearchResult>,
    warnings: &mut Vec<ProviderWarning>,
) {
    match outcome {
        Ok(provider_results) => results.extend(provider_results),
        Err(error) => warnings.push(ProviderWarning {
            source,
            kind: error.kind,
            message: match source {
                DataSource::Yahoo => "Yahoo search is temporarily unavailable.",
                DataSource::Binance => "Binance search is temporarily unavailable.",
            }
            .into(),
        }),
    }
}

async fn send_snapshots(
    instruments: Vec<Instrument>,
    providers: &ProviderSet,
    events: &Sender<RuntimeEvent>,
) {
    let (equities, crypto): (Vec<_>, Vec<_>) = instruments
        .into_iter()
        .partition(|instrument| instrument.id.asset_class() == AssetClass::Equity);
    let (equity_result, crypto_result) = tokio::join!(
        providers.equities.snapshots(&equities),
        providers.crypto.snapshots(&crypto)
    );
    let mut quotes = Vec::new();
    collect_snapshot_outcome(equities, equity_result, &mut quotes, events).await;
    collect_snapshot_outcome(crypto, crypto_result, &mut quotes, events).await;
    if !quotes.is_empty() {
        _ = events.send(RuntimeEvent::Quotes(quotes)).await;
    }
}

async fn collect_snapshot_outcome(
    instruments: Vec<Instrument>,
    outcome: Result<Vec<Quote>, MarketDataError>,
    quotes: &mut Vec<Quote>,
    events: &Sender<RuntimeEvent>,
) {
    match outcome {
        Ok(provider_quotes) => quotes.extend(provider_quotes),
        Err(error) => {
            _ = events
                .send(RuntimeEvent::QuoteError {
                    instrument_ids: instruments
                        .into_iter()
                        .map(|instrument| instrument.id)
                        .collect(),
                    error,
                })
                .await;
        }
    }
}

async fn refresh_catalog_cache(
    client: BinanceClient,
    repository: Arc<dyn StateRepository>,
    cache: Arc<Mutex<CachedMarketState>>,
) -> Result<(), MarketDataError> {
    client.refresh_catalog().await?;
    let catalog = client.catalog()?;
    let mut cache = cache
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    cache.instrument_catalog = catalog;
    repository.save_cache(&cache).map_err(|_| MarketDataError {
        kind: MarketDataErrorKind::Internal,
        message: "Kabu could not save the Binance catalog cache".into(),
        retry_after: None,
    })
}

#[cfg(test)]
mod tests {
    use super::{
        ProductionComponents, ProviderSet, RuntimeCommand, RuntimeEvent, RuntimeHandle,
        StreamStatus, refresh_catalog_cache,
    };
    use async_trait::async_trait;
    use kabu_core::{
        CachedMarketState, Candle, ChartHistory, ChartPeriod, DataSource, Instrument, InstrumentId,
        InstrumentSearch, MarketDataError, MarketDataErrorKind, MarketState, PersistedState,
        PersistenceError, Quote, QuoteSnapshots, QuoteStream, QuoteSubscription, SearchResult,
        StateRepository,
    };
    use std::{
        sync::{
            Arc, Condvar, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        thread,
        time::{Duration, Instant},
    };
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    #[test]
    fn recording_runtime_coalesces_controls_without_admitting_extra_ordinary_work() {
        let runtime = RuntimeHandle::default();
        for _ in 0..super::COMMAND_CAPACITY {
            runtime
                .send(RuntimeCommand::Snapshot {
                    instruments: Vec::new(),
                })
                .expect("within bounded capacity");
        }

        assert_eq!(
            runtime.send(RuntimeCommand::Snapshot {
                instruments: Vec::new(),
            }),
            Err(super::RuntimeSendError::Full)
        );

        let aapl = aapl_instrument();
        let btc = btc_instrument();
        for _ in 0..300 {
            runtime
                .send(RuntimeCommand::SetWatchlist(vec![aapl.clone()]))
                .expect("watchlist control remains reliable");
            runtime
                .send(RuntimeCommand::SetCryptoWatchlist(vec![btc.clone()]))
                .expect("crypto watchlist control remains reliable");
            runtime
                .send(RuntimeCommand::SetVisible(false))
                .expect("visibility control remains reliable");
        }
        runtime
            .send(RuntimeCommand::SetVisible(true))
            .expect("latest visibility control remains reliable");

        let commands = runtime.recorded_commands();
        assert_eq!(
            commands
                .iter()
                .filter(|command| !super::is_control_command(command))
                .count(),
            super::COMMAND_CAPACITY
        );
        let controls: Vec<_> = commands
            .iter()
            .filter(|command| super::is_control_command(command))
            .collect();
        assert!(matches!(
            controls.as_slice(),
            [
                RuntimeCommand::SetWatchlist(instruments),
                RuntimeCommand::SetCryptoWatchlist(crypto),
                RuntimeCommand::SetVisible(true),
            ] if instruments == &vec![aapl] && crypto == &vec![btc]
        ));
    }

    #[test]
    fn production_components_construct_concrete_adapters_without_network_access() {
        let directory = tempfile::tempdir().expect("tempdir");

        let components = ProductionComponents::at(directory.path()).expect("components");

        assert!(!components.state.recovered_from_corruption);
        assert!(!components.cache.recovered_from_corruption);
        assert!(components.cache.value.instrument_catalog.is_none());
        assert_eq!(components.state.value.watchlist.len(), 4);
    }

    #[tokio::test]
    async fn missing_catalog_refresh_updates_active_catalog_and_persists_cache() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v3/exchangeInfo"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(
                include_str!("../../kabu-data/tests/fixtures/binance_exchange_info.json"),
                "application/json",
            ))
            .mount(&server)
            .await;
        let directory = tempfile::tempdir().expect("tempdir");
        let repository = Arc::new(kabu_data::JsonRepository::at(directory.path()));
        let initial = CachedMarketState::default();
        let client = kabu_data::BinanceClient::new(
            reqwest::Client::new(),
            server.uri().parse().expect("REST URL"),
            server.uri().parse().expect("stream URL"),
            None,
        );
        let cache = Arc::new(Mutex::new(initial));

        refresh_catalog_cache(client, repository.clone(), cache)
            .await
            .expect("refresh");

        let persisted = repository.load_cache().expect("persisted cache");
        let catalog = persisted.instrument_catalog.expect("catalog");
        assert!(
            catalog
                .instruments
                .iter()
                .any(|instrument| { instrument.id.canonical_symbol() == "BTC/USDC" })
        );
    }

    #[tokio::test]
    async fn stale_catalog_refresh_updates_active_catalog_and_persists_cache() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v3/exchangeInfo"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(
                include_str!("../../kabu-data/tests/fixtures/binance_exchange_info.json"),
                "application/json",
            ))
            .mount(&server)
            .await;
        let directory = tempfile::tempdir().expect("tempdir");
        let repository = Arc::new(kabu_data::JsonRepository::at(directory.path()));
        let initial = CachedMarketState {
            instrument_catalog: Some(kabu_core::CachedInstrumentCatalog {
                source: DataSource::Binance,
                fetched_at: chrono::Utc::now() - chrono::Duration::hours(25),
                instruments: vec![btc_instrument()],
            }),
            ..CachedMarketState::default()
        };
        repository.save_cache(&initial).expect("initial cache");
        let client = kabu_data::BinanceClient::new(
            reqwest::Client::new(),
            server.uri().parse().expect("REST URL"),
            server.uri().parse().expect("stream URL"),
            initial.instrument_catalog.clone(),
        );
        let cache = Arc::new(Mutex::new(initial));

        refresh_catalog_cache(client, repository.clone(), cache)
            .await
            .expect("refresh");

        let persisted = repository.load_cache().expect("persisted cache");
        let catalog = persisted.instrument_catalog.expect("catalog");
        assert!(
            catalog
                .instruments
                .iter()
                .any(|instrument| { instrument.id.canonical_symbol() == "BTC/USDC" })
        );
    }

    #[tokio::test]
    async fn failed_catalog_refresh_preserves_the_last_good_catalog() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/v3/exchangeInfo"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;
        let directory = tempfile::tempdir().expect("tempdir");
        let repository = Arc::new(kabu_data::JsonRepository::at(directory.path()));
        let initial = CachedMarketState {
            instrument_catalog: Some(kabu_core::CachedInstrumentCatalog {
                source: DataSource::Binance,
                fetched_at: chrono::Utc::now() - chrono::Duration::hours(25),
                instruments: vec![btc_instrument()],
            }),
            ..CachedMarketState::default()
        };
        repository.save_cache(&initial).expect("initial cache");
        let client = kabu_data::BinanceClient::new(
            reqwest::Client::new(),
            server.uri().parse().expect("REST URL"),
            server.uri().parse().expect("stream URL"),
            initial.instrument_catalog.clone(),
        );
        let cache = Arc::new(Mutex::new(initial));

        assert!(
            refresh_catalog_cache(client, repository.clone(), cache)
                .await
                .is_err()
        );

        let persisted = repository.load_cache().expect("persisted cache");
        let catalog = persisted.instrument_catalog.expect("last good catalog");
        assert_eq!(catalog.instruments, vec![btc_instrument()]);
    }

    #[tokio::test]
    async fn startup_catalog_refresh_emits_sanitized_typed_failures() {
        for (status, body, expected) in [
            (429, "{}", MarketDataErrorKind::RateLimited),
            (503, "{}", MarketDataErrorKind::Unavailable),
            (200, "not-json", MarketDataErrorKind::MalformedResponse),
        ] {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path("/api/v3/exchangeInfo"))
                .respond_with(ResponseTemplate::new(status).set_body_raw(body, "application/json"))
                .mount(&server)
                .await;
            let client = kabu_data::BinanceClient::new(
                reqwest::Client::new(),
                server.uri().parse().expect("REST URL"),
                server.uri().parse().expect("stream URL"),
                None,
            );
            let providers = ProviderSet {
                equities: Arc::new(FakeProvider::immediate(DataSource::Yahoo, Vec::new())),
                crypto: Arc::new(FakeProvider::immediate(DataSource::Binance, Vec::new())),
            };
            let runtime = RuntimeHandle::start_inner(
                providers,
                Arc::new(NoopRepository),
                CachedMarketState::default(),
                Some(client),
            )
            .expect("runtime");

            let event = tokio::time::timeout(Duration::from_secs(1), runtime.events().recv())
                .await
                .expect("catalog event deadline")
                .expect("catalog event");
            assert!(matches!(
                event,
                RuntimeEvent::CatalogRefresh {
                    result: Err(kind),
                    catalog_available: false,
                } if kind == expected
            ));
            runtime.shutdown();
        }
    }

    #[test]
    fn shutdown_command_joins_the_network_thread() {
        let providers = ProviderSet {
            equities: Arc::new(FakeProvider::immediate(DataSource::Yahoo, Vec::new())),
            crypto: Arc::new(FakeProvider::immediate(DataSource::Binance, Vec::new())),
        };
        let runtime = RuntimeHandle::start(providers, Arc::new(NoopRepository)).expect("runtime");

        runtime
            .send(RuntimeCommand::Shutdown)
            .expect("shutdown command");

        assert!(runtime.is_shutdown());
    }

    #[test]
    fn shutdown_keeps_stopped_observable_until_coordinator_cleanup_finishes() {
        let providers = ProviderSet {
            equities: Arc::new(FakeProvider::immediate(DataSource::Yahoo, Vec::new())),
            crypto: Arc::new(FakeProvider::immediate(DataSource::Binance, Vec::new())),
        };
        let runtime = RuntimeHandle::start(providers, Arc::new(NoopRepository)).expect("runtime");
        let events = runtime.events();

        runtime.shutdown();

        let mut stopped = false;
        while let Ok(event) = events.try_recv() {
            stopped |= matches!(event, RuntimeEvent::StreamStatus(StreamStatus::Stopped));
        }
        assert!(stopped);
    }

    #[test]
    fn orderly_shutdown_flushes_queued_state_and_cache_before_bounded_acknowledgement() {
        let directory = tempfile::tempdir().expect("tempdir");
        let repository = Arc::new(kabu_data::JsonRepository::at(directory.path()));
        let providers = ProviderSet {
            equities: Arc::new(FakeProvider::immediate(DataSource::Yahoo, Vec::new())),
            crypto: Arc::new(FakeProvider::immediate(DataSource::Binance, Vec::new())),
        };
        let runtime = RuntimeHandle::start(providers, repository.clone()).expect("runtime");
        let mut state = PersistedState::starter();
        state.theme = kabu_core::ThemePreference::Signal;
        let mut cache = CachedMarketState::default();
        let instrument = aapl_instrument();
        cache.quotes.insert(
            instrument.id.clone(),
            test_quote(instrument.id, DataSource::Yahoo),
        );
        runtime
            .send(RuntimeCommand::PersistState(state))
            .expect("state command");
        runtime
            .send(RuntimeCommand::PersistMarketCache { cache })
            .expect("cache command");

        let started = Instant::now();
        runtime
            .send(RuntimeCommand::Shutdown)
            .expect("shutdown command");

        assert!(started.elapsed() < Duration::from_secs(2));
        assert_eq!(
            repository.load_state().expect("saved state").theme,
            kabu_core::ThemePreference::Signal
        );
        assert_eq!(
            repository.load_cache().expect("saved cache").quotes.len(),
            1
        );
        assert!(runtime.is_shutdown());
    }

    #[test]
    fn shutdown_flush_payload_is_persisted_before_bounded_acknowledgement() {
        let directory = tempfile::tempdir().expect("tempdir");
        let repository = Arc::new(kabu_data::JsonRepository::at(directory.path()));
        let providers = ProviderSet {
            equities: Arc::new(FakeProvider::immediate(DataSource::Yahoo, Vec::new())),
            crypto: Arc::new(FakeProvider::immediate(DataSource::Binance, Vec::new())),
        };
        let runtime = RuntimeHandle::start(providers, repository.clone()).expect("runtime");
        let mut state = PersistedState::starter();
        state.theme = kabu_core::ThemePreference::Midnight;
        let mut cache = CachedMarketState::default();
        let instrument = aapl_instrument();
        cache.quotes.insert(
            instrument.id.clone(),
            test_quote(instrument.id, DataSource::Yahoo),
        );

        runtime.shutdown_with_flush(state, cache);

        assert_eq!(
            repository.load_state().expect("saved state").theme,
            kabu_core::ThemePreference::Midnight
        );
        assert_eq!(
            repository.load_cache().expect("saved cache").quotes.len(),
            1
        );
        assert!(runtime.is_shutdown());
    }

    #[test]
    fn visibility_and_watchlist_commands_drive_the_coordinator() {
        let probe = Arc::new(OperationProbe::default());
        let providers = ProviderSet {
            equities: Arc::new(ControlledProvider::new(
                DataSource::Yahoo,
                probe.clone(),
                Duration::ZERO,
            )),
            crypto: Arc::new(ControlledProvider::new(
                DataSource::Binance,
                Arc::new(OperationProbe::default()),
                Duration::ZERO,
            )),
        };
        let runtime = RuntimeHandle::start(providers, Arc::new(NoopRepository)).expect("runtime");
        runtime
            .send(RuntimeCommand::SetWatchlist(vec![aapl_instrument()]))
            .expect("watchlist");
        runtime
            .send(RuntimeCommand::SetVisible(true))
            .expect("visible");

        wait_for_counter(&probe.started, 1);
        runtime
            .send(RuntimeCommand::SetVisible(false))
            .expect("hidden");
        let calls_after_hide = probe.started.load(Ordering::SeqCst);
        thread::sleep(Duration::from_millis(30));

        assert_eq!(probe.started.load(Ordering::SeqCst), calls_after_hide);
        runtime.shutdown();
    }

    #[test]
    fn hiding_cancels_and_drains_in_flight_history() {
        let probe = Arc::new(OperationProbe::default());
        let providers = ProviderSet {
            equities: Arc::new(ControlledProvider::new(
                DataSource::Yahoo,
                probe.clone(),
                Duration::from_secs(2),
            )),
            crypto: Arc::new(ControlledProvider::new(
                DataSource::Binance,
                Arc::new(OperationProbe::default()),
                Duration::ZERO,
            )),
        };
        let runtime = RuntimeHandle::start(providers, Arc::new(NoopRepository)).expect("runtime");
        runtime
            .send(RuntimeCommand::SetVisible(true))
            .expect("visible");
        runtime
            .send(RuntimeCommand::History {
                generation: kabu_core::Generation::default().next(),
                instrument: aapl_instrument(),
                period: ChartPeriod::OneDay,
            })
            .expect("history");
        wait_for_counter(&probe.started, 1);

        runtime
            .send(RuntimeCommand::SetVisible(false))
            .expect("hidden");
        let started = Instant::now();
        while probe.active.load(Ordering::SeqCst) != 0 && started.elapsed() < Duration::from_secs(1)
        {
            thread::sleep(Duration::from_millis(1));
        }

        assert_eq!(probe.active.load(Ordering::SeqCst), 0);
        assert_eq!(probe.cancelled.load(Ordering::SeqCst), 1);
        runtime.shutdown();
    }

    #[test]
    fn reliable_hide_preempts_a_full_ordinary_queue_and_restore_reconciles_first() {
        let probe = Arc::new(OperationProbe::default());
        let yahoo =
            ControlledProvider::new(DataSource::Yahoo, probe.clone(), Duration::from_secs(30));
        let providers = ProviderSet {
            equities: Arc::new(yahoo),
            crypto: Arc::new(ControlledProvider::new(
                DataSource::Binance,
                Arc::new(OperationProbe::default()),
                Duration::ZERO,
            )),
        };
        let runtime = RuntimeHandle::start(providers, Arc::new(NoopRepository)).expect("runtime");
        runtime
            .send(RuntimeCommand::SetWatchlist(vec![aapl_instrument()]))
            .expect("watchlist control");
        runtime
            .send(RuntimeCommand::SetVisible(true))
            .expect("visible control");
        wait_for_counter(&probe.started, 1);

        fill_ordinary_queue(&runtime, &aapl_instrument());
        runtime
            .send(RuntimeCommand::SetVisible(false))
            .expect("reliable hide control");
        assert!(runtime.wait_for_control_state(Duration::from_secs(1)));
        assert_eq!(probe.active.load(Ordering::SeqCst), 0);
        let calls_while_hidden = probe.started.load(Ordering::SeqCst);
        thread::sleep(Duration::from_millis(30));
        assert_eq!(probe.started.load(Ordering::SeqCst), calls_while_hidden);

        runtime
            .send(RuntimeCommand::SetVisible(true))
            .expect("reliable restore control");
        assert!(runtime.wait_for_control_state(Duration::from_secs(1)));
        wait_for_counter(&probe.started, calls_while_hidden + 1);
        runtime.shutdown();
    }

    #[test]
    fn reliable_watchlist_control_applies_only_the_latest_add_remove_state() {
        let probe = Arc::new(OperationProbe::default());
        let yahoo = ControlledProvider::new(DataSource::Yahoo, probe, Duration::ZERO);
        let observed = yahoo.snapshot_instruments.clone();
        let providers = ProviderSet {
            equities: Arc::new(yahoo),
            crypto: Arc::new(ControlledProvider::new(
                DataSource::Binance,
                Arc::new(OperationProbe::default()),
                Duration::ZERO,
            )),
        };
        let runtime = RuntimeHandle::start(providers, Arc::new(NoopRepository)).expect("runtime");
        runtime
            .send(RuntimeCommand::SetVisible(false))
            .expect("hidden");
        runtime
            .send(RuntimeCommand::SetWatchlist(vec![aapl_instrument()]))
            .expect("intermediate add");
        runtime
            .send(RuntimeCommand::SetWatchlist(Vec::new()))
            .expect("intermediate remove");
        let nvda = Instrument::equity("NVDA", Some("NASDAQ"), "NVIDIA", "USD").expect("NVDA");
        runtime
            .send(RuntimeCommand::SetWatchlist(vec![nvda.clone()]))
            .expect("latest add");
        runtime
            .send(RuntimeCommand::SetVisible(true))
            .expect("restore");

        assert!(runtime.wait_for_control_state(Duration::from_secs(1)));
        let started = Instant::now();
        while observed
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_empty()
            && started.elapsed() < Duration::from_secs(1)
        {
            thread::sleep(Duration::from_millis(1));
        }
        let batches = observed
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert_eq!(batches.last(), Some(&vec![nvda.id]));
        drop(batches);
        runtime.shutdown();
    }

    #[test]
    fn shutdown_timeout_keeps_owned_cleanup_until_final_persistence_ack() {
        let probe = Arc::new(OperationProbe::default());
        let providers = ProviderSet {
            equities: Arc::new(ControlledProvider::new(
                DataSource::Yahoo,
                probe.clone(),
                Duration::from_secs(30),
            )),
            crypto: Arc::new(ControlledProvider::new(
                DataSource::Binance,
                Arc::new(OperationProbe::default()),
                Duration::ZERO,
            )),
        };
        let repository = Arc::new(DelayedRepository::default());
        let runtime = RuntimeHandle::start(providers, repository.clone()).expect("runtime");
        runtime
            .send(RuntimeCommand::History {
                generation: kabu_core::Generation::default().next(),
                instrument: aapl_instrument(),
                period: ChartPeriod::OneDay,
            })
            .expect("history");
        wait_for_counter(&probe.started, 1);
        fill_ordinary_queue(&runtime, &aapl_instrument());
        let mut state = PersistedState::starter();
        state.theme = kabu_core::ThemePreference::Signal;

        runtime.begin_shutdown_with_flush(state, CachedMarketState::default());
        let cancellation_started = Instant::now();
        while probe.active.load(Ordering::SeqCst) != 0
            && cancellation_started.elapsed() < Duration::from_secs(1)
        {
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(probe.active.load(Ordering::SeqCst), 0);
        repository.wait_until_save_started();

        let wait_started = Instant::now();
        assert!(!runtime.wait_for_shutdown(Duration::from_millis(50)));
        assert!(wait_started.elapsed() < Duration::from_secs(2));
        assert!(!runtime.is_shutdown_complete());

        repository.release();
        assert!(runtime.wait_for_shutdown(Duration::from_secs(2)));
        assert!(runtime.is_shutdown_complete());
        assert_eq!(
            repository.saved_theme(),
            Some(kabu_core::ThemePreference::Signal)
        );
        assert_eq!(probe.active.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn searches_equity_and_crypto_providers_concurrently_and_ranks_results() {
        let active = Arc::new(AtomicUsize::new(0));
        let maximum = Arc::new(AtomicUsize::new(0));
        let providers = ProviderSet {
            equities: Arc::new(FakeProvider::successful(
                DataSource::Yahoo,
                vec![nvda_result()],
                active.clone(),
                maximum.clone(),
            )),
            crypto: Arc::new(FakeProvider::successful(
                DataSource::Binance,
                vec![btc_result()],
                active,
                maximum.clone(),
            )),
        };
        let runtime = RuntimeHandle::start(providers, Arc::new(NoopRepository)).expect("runtime");

        runtime
            .send(RuntimeCommand::Search {
                generation: kabu_core::Generation::default().next(),
                query: "btc".into(),
            })
            .expect("queue search");

        let event = runtime.events().recv_blocking().expect("search event");
        let RuntimeEvent::SearchCompleted {
            results, warnings, ..
        } = event
        else {
            panic!("expected search completion");
        };
        assert_eq!(maximum.load(Ordering::SeqCst), 2);
        assert_eq!(
            results
                .iter()
                .map(|result| result.instrument.id.canonical_symbol())
                .collect::<Vec<_>>(),
            ["BTC/USDT", "NVDA"]
        );
        assert!(warnings.is_empty());
        runtime.shutdown();
        assert!(runtime.is_shutdown());
    }

    #[test]
    fn one_provider_failure_keeps_successes_and_sanitizes_the_warning() {
        let query = "private-query-419";
        let providers = ProviderSet {
            equities: Arc::new(FakeProvider::failed(
                DataSource::Yahoo,
                MarketDataErrorKind::RateLimited,
                query,
            )),
            crypto: Arc::new(FakeProvider::immediate(
                DataSource::Binance,
                vec![btc_result()],
            )),
        };
        let runtime = RuntimeHandle::start(providers, Arc::new(NoopRepository)).expect("runtime");

        runtime
            .send(RuntimeCommand::Search {
                generation: kabu_core::Generation::default().next(),
                query: query.into(),
            })
            .expect("queue search");

        let RuntimeEvent::SearchCompleted {
            results, warnings, ..
        } = runtime.events().recv_blocking().expect("search event")
        else {
            panic!("expected search completion");
        };
        assert_eq!(results, vec![btc_result()]);
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].source, DataSource::Yahoo);
        assert_eq!(warnings[0].kind, MarketDataErrorKind::RateLimited);
        assert!(!warnings[0].message.contains(query));
        assert!(!format!("{:?}", warnings).contains(query));
        runtime.shutdown();
    }

    #[test]
    fn both_provider_failures_return_a_terminal_empty_completion() {
        let providers = ProviderSet {
            equities: Arc::new(FakeProvider::failed(
                DataSource::Yahoo,
                MarketDataErrorKind::Unavailable,
                "equity secret",
            )),
            crypto: Arc::new(FakeProvider::failed(
                DataSource::Binance,
                MarketDataErrorKind::Timeout,
                "crypto secret",
            )),
        };
        let runtime = RuntimeHandle::start(providers, Arc::new(NoopRepository)).expect("runtime");

        runtime
            .send(RuntimeCommand::Search {
                generation: kabu_core::Generation::default().next(),
                query: "nothing".into(),
            })
            .expect("queue search");

        let RuntimeEvent::SearchCompleted {
            results, warnings, ..
        } = runtime.events().recv_blocking().expect("search event")
        else {
            panic!("expected search completion");
        };
        assert!(results.is_empty());
        assert_eq!(warnings.len(), 2);
        runtime.shutdown();
    }

    #[test]
    fn shutdown_cancels_an_in_flight_provider_search() {
        let active = Arc::new(AtomicUsize::new(0));
        let providers = ProviderSet {
            equities: Arc::new(FakeProvider::slow(active.clone())),
            crypto: Arc::new(FakeProvider::slow(active.clone())),
        };
        let runtime = RuntimeHandle::start(providers, Arc::new(NoopRepository)).expect("runtime");
        runtime
            .send(RuntimeCommand::Search {
                generation: kabu_core::Generation::default().next(),
                query: "cancel-me".into(),
            })
            .expect("queue search");
        let wait_started = Instant::now();
        while active.load(Ordering::SeqCst) == 0 && wait_started.elapsed() < Duration::from_secs(1)
        {
            thread::sleep(Duration::from_millis(1));
        }
        assert!(active.load(Ordering::SeqCst) > 0, "provider search started");

        let shutdown_started = Instant::now();
        runtime.shutdown();

        assert!(shutdown_started.elapsed() < Duration::from_millis(250));
        assert!(runtime.is_shutdown());
    }

    #[test]
    fn snapshot_and_history_commands_overlap_and_emit_success_events() {
        let probe = Arc::new(OperationProbe::default());
        let providers = ProviderSet {
            equities: Arc::new(ControlledProvider::new(
                DataSource::Yahoo,
                probe.clone(),
                Duration::from_millis(80),
            )),
            crypto: Arc::new(ControlledProvider::new(
                DataSource::Binance,
                Arc::new(OperationProbe::default()),
                Duration::ZERO,
            )),
        };
        let runtime = RuntimeHandle::start(providers, Arc::new(NoopRepository)).expect("runtime");
        let instrument = Instrument::equity("AAPL", Some("NASDAQ"), "Apple", "USD").expect("AAPL");
        let generation = kabu_core::Generation::default().next();

        runtime
            .send(RuntimeCommand::Snapshot {
                instruments: vec![instrument.clone()],
            })
            .expect("snapshot");
        runtime
            .send(RuntimeCommand::History {
                generation,
                instrument: instrument.clone(),
                period: ChartPeriod::OneDay,
            })
            .expect("history");

        let events = [
            runtime.events().recv_blocking().expect("first event"),
            runtime.events().recv_blocking().expect("second event"),
        ];
        assert!(
            events
                .iter()
                .any(|event| matches!(event, RuntimeEvent::Quotes(quotes) if quotes.len() == 1 && quotes[0].instrument_id == instrument.id))
        );
        assert!(events.iter().any(|event| matches!(
            event,
            RuntimeEvent::HistoryCompleted { generation: event_generation, key, candles }
                if *event_generation == generation
                    && key.instrument_id == instrument.id
                    && key.period == ChartPeriod::OneDay
                    && candles.len() == 1
        )));
        assert!(
            runtime.events().try_recv().is_err(),
            "exactly one snapshot and one history completion"
        );
        assert_eq!(probe.maximum.load(Ordering::SeqCst), 2);
        runtime.shutdown();
    }

    #[test]
    fn newer_search_cancels_the_prior_generation_before_it_can_emit() {
        let probe = Arc::new(OperationProbe::default());
        let providers = ProviderSet {
            equities: Arc::new(
                ControlledProvider::new(DataSource::Yahoo, probe.clone(), Duration::ZERO)
                    .with_slow_search(Duration::from_millis(400)),
            ),
            crypto: Arc::new(
                ControlledProvider::new(DataSource::Binance, probe.clone(), Duration::ZERO)
                    .with_slow_search(Duration::from_millis(400)),
            ),
        };
        let runtime = RuntimeHandle::start(providers, Arc::new(NoopRepository)).expect("runtime");
        let mut generations = kabu_core::Generation::default();
        let old = generations.next();
        let current = generations.next();
        runtime
            .send(RuntimeCommand::Search {
                generation: old,
                query: "old".into(),
            })
            .expect("old search");
        wait_for_counter(&probe.started, 2);

        runtime
            .send(RuntimeCommand::Search {
                generation: current,
                query: "new".into(),
            })
            .expect("new search");

        let RuntimeEvent::SearchCompleted { generation, .. } =
            runtime.events().recv_blocking().expect("current result")
        else {
            panic!("expected search completion");
        };
        assert_eq!(generation, current);
        assert_eq!(probe.cancelled.load(Ordering::SeqCst), 2);
        assert!(runtime.events().try_recv().is_err());
        runtime.shutdown();
    }

    #[test]
    fn shutdown_cancels_and_drains_every_tracked_job() {
        let probe = Arc::new(OperationProbe::default());
        let providers = ProviderSet {
            equities: Arc::new(
                ControlledProvider::new(DataSource::Yahoo, probe.clone(), Duration::from_secs(2))
                    .with_slow_search(Duration::from_secs(2)),
            ),
            crypto: Arc::new(
                ControlledProvider::new(DataSource::Binance, probe.clone(), Duration::ZERO)
                    .with_slow_search(Duration::from_secs(2)),
            ),
        };
        let runtime = RuntimeHandle::start(providers, Arc::new(NoopRepository)).expect("runtime");
        let instrument = Instrument::equity("AAPL", Some("NASDAQ"), "Apple", "USD").expect("AAPL");
        runtime
            .send(RuntimeCommand::Search {
                generation: kabu_core::Generation::default().next(),
                query: "old".into(),
            })
            .expect("search");
        runtime
            .send(RuntimeCommand::Snapshot {
                instruments: vec![instrument.clone()],
            })
            .expect("snapshot");
        runtime
            .send(RuntimeCommand::History {
                generation: kabu_core::Generation::default().next(),
                instrument,
                period: ChartPeriod::OneDay,
            })
            .expect("history");
        wait_for_counter(&probe.started, 4);

        runtime.shutdown();

        assert_eq!(probe.active.load(Ordering::SeqCst), 0);
        assert_eq!(probe.cancelled.load(Ordering::SeqCst), 4);
        assert!(runtime.is_shutdown());
    }

    #[test]
    fn snapshot_and_history_failures_preserve_typed_event_shapes() {
        let provider = ControlledProvider::new(
            DataSource::Yahoo,
            Arc::new(OperationProbe::default()),
            Duration::ZERO,
        )
        .with_snapshot_failure()
        .with_history_failure();
        let providers = ProviderSet {
            equities: Arc::new(provider),
            crypto: Arc::new(ControlledProvider::new(
                DataSource::Binance,
                Arc::new(OperationProbe::default()),
                Duration::ZERO,
            )),
        };
        let runtime = RuntimeHandle::start(providers, Arc::new(NoopRepository)).expect("runtime");
        let instrument = Instrument::equity("AAPL", Some("NASDAQ"), "Apple", "USD").expect("AAPL");
        let generation = kabu_core::Generation::default().next();
        runtime
            .send(RuntimeCommand::Snapshot {
                instruments: vec![instrument.clone()],
            })
            .expect("snapshot");
        runtime
            .send(RuntimeCommand::History {
                generation,
                instrument: instrument.clone(),
                period: ChartPeriod::OneDay,
            })
            .expect("history");

        let events = [
            runtime.events().recv_blocking().expect("first failure"),
            runtime.events().recv_blocking().expect("second failure"),
        ];
        assert!(events.iter().any(|event| matches!(
            event,
            RuntimeEvent::QuoteError { instrument_ids, error }
                if instrument_ids == std::slice::from_ref(&instrument.id)
                    && error.kind == MarketDataErrorKind::Unavailable
        )));
        assert!(events.iter().any(|event| matches!(
            event,
            RuntimeEvent::HistoryFailed { generation: event_generation, key, error }
                if *event_generation == generation
                    && key.instrument_id == instrument.id
                    && key.period == ChartPeriod::OneDay
                    && error.kind == MarketDataErrorKind::Timeout
        )));
        runtime.shutdown();
    }

    #[derive(Clone)]
    struct FakeProvider {
        source: DataSource,
        outcome: Result<Vec<SearchResult>, MarketDataError>,
        active: Option<Arc<AtomicUsize>>,
        maximum: Option<Arc<AtomicUsize>>,
        delay: Option<Duration>,
    }

    impl FakeProvider {
        fn immediate(source: DataSource, results: Vec<SearchResult>) -> Self {
            Self {
                source,
                outcome: Ok(results),
                active: None,
                maximum: None,
                delay: None,
            }
        }

        fn successful(
            source: DataSource,
            results: Vec<SearchResult>,
            active: Arc<AtomicUsize>,
            maximum: Arc<AtomicUsize>,
        ) -> Self {
            Self {
                source,
                outcome: Ok(results),
                active: Some(active),
                maximum: Some(maximum),
                delay: Some(Duration::from_millis(30)),
            }
        }

        fn failed(source: DataSource, kind: MarketDataErrorKind, message: &str) -> Self {
            Self {
                source,
                outcome: Err(MarketDataError {
                    kind,
                    message: message.into(),
                    retry_after: None,
                }),
                active: None,
                maximum: None,
                delay: None,
            }
        }

        fn slow(active: Arc<AtomicUsize>) -> Self {
            Self {
                source: DataSource::Yahoo,
                outcome: Ok(Vec::new()),
                active: Some(active),
                maximum: Some(Arc::new(AtomicUsize::new(0))),
                delay: Some(Duration::from_secs(2)),
            }
        }
    }

    #[async_trait]
    impl InstrumentSearch for FakeProvider {
        async fn search(&self, _: &str) -> Result<Vec<SearchResult>, MarketDataError> {
            if let (Some(active), Some(maximum)) = (&self.active, &self.maximum) {
                let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                maximum.fetch_max(current, Ordering::SeqCst);
                tokio::time::sleep(self.delay.unwrap_or_default()).await;
                active.fetch_sub(1, Ordering::SeqCst);
            }
            self.outcome.clone()
        }
    }

    #[async_trait]
    impl QuoteSnapshots for FakeProvider {
        async fn snapshots(&self, _: &[Instrument]) -> Result<Vec<Quote>, MarketDataError> {
            Ok(Vec::new())
        }
    }

    #[async_trait]
    impl ChartHistory for FakeProvider {
        async fn history(
            &self,
            _: &Instrument,
            _: ChartPeriod,
        ) -> Result<Vec<Candle>, MarketDataError> {
            Ok(Vec::new())
        }
    }

    #[async_trait]
    impl QuoteSubscription for FakeProvider {
        async fn subscribe(&self, _: &[Instrument]) -> Result<QuoteStream, MarketDataError> {
            Err(MarketDataError {
                kind: MarketDataErrorKind::Unavailable,
                message: format!("{:?} stream unavailable in test", self.source),
                retry_after: None,
            })
        }
    }

    #[derive(Default)]
    struct OperationProbe {
        active: AtomicUsize,
        maximum: AtomicUsize,
        started: AtomicUsize,
        cancelled: AtomicUsize,
    }

    struct OperationGuard {
        probe: Arc<OperationProbe>,
        completed: bool,
    }

    impl OperationGuard {
        fn begin(probe: Arc<OperationProbe>) -> Self {
            let active = probe.active.fetch_add(1, Ordering::SeqCst) + 1;
            probe.maximum.fetch_max(active, Ordering::SeqCst);
            probe.started.fetch_add(1, Ordering::SeqCst);
            Self {
                probe,
                completed: false,
            }
        }

        fn complete(mut self) {
            self.completed = true;
        }
    }

    impl Drop for OperationGuard {
        fn drop(&mut self) {
            self.probe.active.fetch_sub(1, Ordering::SeqCst);
            if !self.completed {
                self.probe.cancelled.fetch_add(1, Ordering::SeqCst);
            }
        }
    }

    #[derive(Clone)]
    struct ControlledProvider {
        source: DataSource,
        probe: Arc<OperationProbe>,
        operation_delay: Duration,
        slow_search_delay: Duration,
        fail_snapshot: bool,
        fail_history: bool,
        snapshot_instruments: Arc<Mutex<Vec<Vec<InstrumentId>>>>,
    }

    impl ControlledProvider {
        fn new(source: DataSource, probe: Arc<OperationProbe>, operation_delay: Duration) -> Self {
            Self {
                source,
                probe,
                operation_delay,
                slow_search_delay: Duration::ZERO,
                fail_snapshot: false,
                fail_history: false,
                snapshot_instruments: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn with_slow_search(mut self, delay: Duration) -> Self {
            self.slow_search_delay = delay;
            self
        }

        fn with_snapshot_failure(mut self) -> Self {
            self.fail_snapshot = true;
            self
        }

        fn with_history_failure(mut self) -> Self {
            self.fail_history = true;
            self
        }

        async fn controlled_delay(&self, delay: Duration) -> OperationGuard {
            let guard = OperationGuard::begin(self.probe.clone());
            tokio::time::sleep(delay).await;
            guard
        }
    }

    #[async_trait]
    impl InstrumentSearch for ControlledProvider {
        async fn search(&self, query: &str) -> Result<Vec<SearchResult>, MarketDataError> {
            let delay = if query == "old" {
                self.slow_search_delay
            } else {
                Duration::ZERO
            };
            let guard = self.controlled_delay(delay).await;
            guard.complete();
            Ok(match self.source {
                DataSource::Yahoo => vec![nvda_result()],
                DataSource::Binance => vec![btc_result()],
            })
        }
    }

    #[async_trait]
    impl QuoteSnapshots for ControlledProvider {
        async fn snapshots(
            &self,
            instruments: &[Instrument],
        ) -> Result<Vec<Quote>, MarketDataError> {
            if instruments.is_empty() {
                return Ok(Vec::new());
            }
            self.snapshot_instruments
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(
                    instruments
                        .iter()
                        .map(|instrument| instrument.id.clone())
                        .collect(),
                );
            let guard = self.controlled_delay(self.operation_delay).await;
            guard.complete();
            if self.fail_snapshot {
                return Err(test_error(MarketDataErrorKind::Unavailable));
            }
            Ok(instruments
                .iter()
                .map(|instrument| test_quote(instrument.id.clone(), self.source))
                .collect())
        }
    }

    #[async_trait]
    impl ChartHistory for ControlledProvider {
        async fn history(
            &self,
            _: &Instrument,
            _: ChartPeriod,
        ) -> Result<Vec<Candle>, MarketDataError> {
            let guard = self.controlled_delay(self.operation_delay).await;
            guard.complete();
            if self.fail_history {
                Err(test_error(MarketDataErrorKind::Timeout))
            } else {
                Ok(vec![test_candle()])
            }
        }
    }

    #[async_trait]
    impl QuoteSubscription for ControlledProvider {
        async fn subscribe(&self, _: &[Instrument]) -> Result<QuoteStream, MarketDataError> {
            Err(test_error(MarketDataErrorKind::Unavailable))
        }
    }

    fn wait_for_counter(counter: &AtomicUsize, expected: usize) {
        let started = Instant::now();
        while counter.load(Ordering::SeqCst) < expected
            && started.elapsed() < Duration::from_secs(1)
        {
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(counter.load(Ordering::SeqCst), expected);
    }

    fn test_error(kind: MarketDataErrorKind) -> MarketDataError {
        MarketDataError {
            kind,
            message: "controlled provider failure".into(),
            retry_after: None,
        }
    }

    fn test_quote(instrument_id: InstrumentId, source: DataSource) -> Quote {
        let timestamp = chrono::DateTime::from_timestamp(1_700_000_000, 0).expect("timestamp");
        Quote {
            instrument_id,
            last: 100.0,
            previous_close: Some(99.0),
            open: None,
            high: None,
            low: None,
            volume: None,
            currency: "USD".into(),
            source,
            market_state: MarketState::Regular,
            provider_timestamp: timestamp,
            received_at: timestamp,
            provider_delayed: false,
        }
    }

    fn test_candle() -> Candle {
        Candle {
            starts_at: chrono::DateTime::from_timestamp(1_700_000_000, 0).expect("timestamp"),
            open: 99.0,
            high: 102.0,
            low: 98.0,
            close: 100.0,
            volume: 1_000.0,
        }
    }

    struct NoopRepository;

    impl StateRepository for NoopRepository {
        fn load_state(&self) -> Result<PersistedState, PersistenceError> {
            Ok(PersistedState::starter())
        }

        fn save_state(&self, _: &PersistedState) -> Result<(), PersistenceError> {
            Ok(())
        }

        fn load_cache(&self) -> Result<CachedMarketState, PersistenceError> {
            Ok(CachedMarketState::default())
        }

        fn save_cache(&self, _: &CachedMarketState) -> Result<(), PersistenceError> {
            Ok(())
        }
    }

    #[derive(Default)]
    struct DelayedRepository {
        gate: Mutex<bool>,
        ready: Condvar,
        save_started: AtomicUsize,
        saved_state: Mutex<Option<PersistedState>>,
    }

    impl DelayedRepository {
        fn wait_until_save_started(&self) {
            wait_for_counter(&self.save_started, 1);
        }

        fn release(&self) {
            *self
                .gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = true;
            self.ready.notify_all();
        }

        fn saved_theme(&self) -> Option<kabu_core::ThemePreference> {
            self.saved_state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .as_ref()
                .map(|state| state.theme)
        }

        fn wait_for_release(&self) {
            self.save_started.fetch_add(1, Ordering::SeqCst);
            let mut released = self
                .gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            while !*released {
                released = self
                    .ready
                    .wait(released)
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
            }
        }
    }

    impl StateRepository for DelayedRepository {
        fn load_state(&self) -> Result<PersistedState, PersistenceError> {
            Ok(PersistedState::starter())
        }

        fn save_state(&self, state: &PersistedState) -> Result<(), PersistenceError> {
            self.wait_for_release();
            *self
                .saved_state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(state.clone());
            Ok(())
        }

        fn load_cache(&self) -> Result<CachedMarketState, PersistenceError> {
            Ok(CachedMarketState::default())
        }

        fn save_cache(&self, _: &CachedMarketState) -> Result<(), PersistenceError> {
            Ok(())
        }
    }

    fn fill_ordinary_queue(runtime: &RuntimeHandle, instrument: &Instrument) {
        let mut generations = kabu_core::Generation::default();
        for _ in 0..10_000 {
            let result = runtime.send(RuntimeCommand::History {
                generation: generations.next(),
                instrument: instrument.clone(),
                period: ChartPeriod::OneDay,
            });
            if result == Err(super::RuntimeSendError::Full) {
                return;
            }
            result.expect("ordinary command accepted before saturation");
        }
        panic!("ordinary command queue never saturated");
    }

    fn nvda_result() -> SearchResult {
        SearchResult {
            instrument: Instrument::equity("NVDA", Some("NASDAQ"), "NVIDIA", "USD").expect("NVDA"),
            source: DataSource::Yahoo,
            venue_label: "NASDAQ".into(),
        }
    }

    fn aapl_instrument() -> Instrument {
        Instrument::equity("AAPL", Some("NASDAQ"), "Apple", "USD").expect("AAPL")
    }

    fn btc_instrument() -> Instrument {
        Instrument::crypto("BTC", "USDT", "Bitcoin").expect("BTC")
    }

    fn btc_result() -> SearchResult {
        SearchResult {
            instrument: Instrument::crypto("BTC", "USDT", "Bitcoin").expect("BTC"),
            source: DataSource::Binance,
            venue_label: "Binance".into(),
        }
    }
}
