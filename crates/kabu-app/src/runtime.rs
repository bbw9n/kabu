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
    collections::{BTreeMap, HashMap, VecDeque},
    fmt, io,
    path::Path,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread::{self, JoinHandle},
};

const COMMAND_CAPACITY: usize = 64;
const MAX_CONCURRENT_OTHER_JOBS: usize = 31;

#[derive(Clone, Debug, PartialEq)]
pub enum RuntimeCommand {
    Search {
        generation: GenerationId,
        query: String,
    },
    RetrySearchProvider {
        generation: GenerationId,
        query: String,
        source: DataSource,
    },
    CancelSearch,
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

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum StreamStatus {
    Connecting,
    Live,
    Reconnecting,
    Paused,
    Stopped,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum PersistenceCategory {
    State,
    MarketCache,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PersistenceResult {
    Saved,
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShutdownOutcome {
    pub state: Option<PersistenceResult>,
    pub market_cache: Option<PersistenceResult>,
    runtime_failed: bool,
}

impl ShutdownOutcome {
    const fn success_without_flush() -> Self {
        Self {
            state: None,
            market_cache: None,
            runtime_failed: false,
        }
    }

    const fn runtime_failed() -> Self {
        Self {
            state: None,
            market_cache: None,
            runtime_failed: true,
        }
    }

    pub const fn is_success(self) -> bool {
        !self.runtime_failed
            && !matches!(self.state, Some(PersistenceResult::Failed))
            && !matches!(self.market_cache, Some(PersistenceResult::Failed))
    }
}

#[derive(Debug)]
pub enum RuntimeEvent {
    SearchCompleted {
        generation: GenerationId,
        results: Vec<SearchResult>,
        warnings: Vec<ProviderWarning>,
    },
    SearchProviderCompleted {
        generation: GenerationId,
        source: DataSource,
        results: Vec<SearchResult>,
        warning: Option<ProviderWarning>,
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
    PersistenceCompleted {
        category: PersistenceCategory,
        result: PersistenceResult,
    },
    StreamStatus(StreamStatus),
}

/// A coalescing runtime event mailbox backed by a bounded-one notification
/// channel. Repeated data-plane updates replace the pending value in their
/// semantic slot (quote per instrument, error per instrument/error class,
/// status, persistence category, or request key). Dynamic data families have
/// fixed cardinality caps and evict their oldest same-family value. Control,
/// persistence, and terminal slots are never evicted by data floods.
#[derive(Clone)]
pub struct RuntimeEvents {
    state: Arc<Mutex<RuntimeEventState>>,
    notifications: Receiver<()>,
}

#[derive(Clone)]
pub(crate) struct RuntimeEventSender {
    state: Arc<Mutex<RuntimeEventState>>,
    notify: Sender<()>,
}

#[derive(Default)]
struct RuntimeEventState {
    next_sequence: u64,
    slots: HashMap<RuntimeEventSlot, u64>,
    pending: BTreeMap<u64, RuntimeEvent>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum RuntimeEventSlot {
    Search,
    SearchProvider(DataSource),
    Quote(InstrumentId),
    QuoteError(InstrumentId, MarketDataErrorKind),
    History(ChartRequestKey),
    Catalog,
    Persistence(PersistenceCategory),
    StreamStatus,
}

const QUOTE_EVENT_SLOT_CAPACITY: usize = 64;
const QUOTE_ERROR_EVENT_SLOT_CAPACITY: usize = 64;
const HISTORY_EVENT_SLOT_CAPACITY: usize = 32;

pub(crate) fn runtime_event_channel() -> (RuntimeEventSender, RuntimeEvents) {
    let (notify, notifications) = async_channel::bounded(1);
    let state = Arc::new(Mutex::new(RuntimeEventState::default()));
    (
        RuntimeEventSender {
            state: state.clone(),
            notify,
        },
        RuntimeEvents {
            state,
            notifications,
        },
    )
}

impl RuntimeEventSender {
    fn publish(&self, event: RuntimeEvent) -> Result<(), RuntimeEvent> {
        if self.notify.is_closed() {
            return Err(event);
        }
        let events = normalize_runtime_event(event);
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for (slot, event) in events {
            if state
                .slots
                .get(&slot)
                .and_then(|sequence| state.pending.get(sequence))
                .is_some_and(|pending| event_is_older_generation(&event, pending))
            {
                continue;
            }
            if !state.slots.contains_key(&slot) {
                evict_oldest_dynamic_slot_at_capacity(&mut state, &slot);
            }
            state.next_sequence = state
                .next_sequence
                .checked_add(1)
                .expect("runtime event sequence exhausted");
            let sequence = state.next_sequence;
            if let Some(previous) = state.slots.insert(slot, sequence) {
                state.pending.remove(&previous);
            }
            state.pending.insert(sequence, event);
        }
        drop(state);
        _ = self.notify.try_send(());
        Ok(())
    }

    async fn send(&self, event: RuntimeEvent) -> Result<(), RuntimeEvent> {
        self.publish(event)
    }

    pub(crate) fn try_send(&self, event: RuntimeEvent) -> Result<(), RuntimeEvent> {
        self.publish(event)
    }

    fn close(&self) {
        self.notify.close();
    }

    fn clear_search(&self) {
        self.remove_slots(|slot| {
            matches!(
                slot,
                RuntimeEventSlot::Search | RuntimeEventSlot::SearchProvider(_)
            )
        });
    }

    fn clear_search_provider(&self, source: DataSource) {
        self.remove_slots(
            |slot| matches!(slot, RuntimeEventSlot::SearchProvider(candidate) if *candidate == source),
        );
    }

    fn retain_watchlist_events(&self, watchlist: &[Instrument]) {
        self.remove_slots(|slot| {
            let instrument_id = match slot {
                RuntimeEventSlot::Quote(instrument_id)
                | RuntimeEventSlot::QuoteError(instrument_id, _) => Some(instrument_id),
                RuntimeEventSlot::History(key) => Some(&key.instrument_id),
                RuntimeEventSlot::Search
                | RuntimeEventSlot::SearchProvider(_)
                | RuntimeEventSlot::Catalog
                | RuntimeEventSlot::Persistence(_)
                | RuntimeEventSlot::StreamStatus => None,
            };
            instrument_id.is_some_and(|instrument_id| {
                !watchlist
                    .iter()
                    .any(|instrument| &instrument.id == instrument_id)
            })
        });
    }

    fn remove_slots(&self, remove: impl Fn(&RuntimeEventSlot) -> bool) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let removed: Vec<_> = state
            .slots
            .iter()
            .filter_map(|(slot, sequence)| remove(slot).then_some((slot.clone(), *sequence)))
            .collect();
        for (slot, sequence) in removed {
            state.slots.remove(&slot);
            state.pending.remove(&sequence);
        }
    }
}

fn event_is_older_generation(incoming: &RuntimeEvent, pending: &RuntimeEvent) -> bool {
    match (
        runtime_event_generation(incoming),
        runtime_event_generation(pending),
    ) {
        (Some(incoming), Some(pending)) => incoming < pending,
        _ => false,
    }
}

fn runtime_event_generation(event: &RuntimeEvent) -> Option<GenerationId> {
    match event {
        RuntimeEvent::SearchCompleted { generation, .. }
        | RuntimeEvent::SearchProviderCompleted { generation, .. }
        | RuntimeEvent::HistoryCompleted { generation, .. }
        | RuntimeEvent::HistoryFailed { generation, .. } => Some(*generation),
        RuntimeEvent::Quotes(_)
        | RuntimeEvent::QuoteError { .. }
        | RuntimeEvent::CatalogRefresh { .. }
        | RuntimeEvent::PersistenceCompleted { .. }
        | RuntimeEvent::StreamStatus(_) => None,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DynamicEventFamily {
    Quote,
    QuoteError,
    History,
}

fn dynamic_event_family(slot: &RuntimeEventSlot) -> Option<(DynamicEventFamily, usize)> {
    match slot {
        RuntimeEventSlot::Quote(_) => Some((DynamicEventFamily::Quote, QUOTE_EVENT_SLOT_CAPACITY)),
        RuntimeEventSlot::QuoteError(_, _) => Some((
            DynamicEventFamily::QuoteError,
            QUOTE_ERROR_EVENT_SLOT_CAPACITY,
        )),
        RuntimeEventSlot::History(_) => {
            Some((DynamicEventFamily::History, HISTORY_EVENT_SLOT_CAPACITY))
        }
        RuntimeEventSlot::Search
        | RuntimeEventSlot::SearchProvider(_)
        | RuntimeEventSlot::Catalog
        | RuntimeEventSlot::Persistence(_)
        | RuntimeEventSlot::StreamStatus => None,
    }
}

fn evict_oldest_dynamic_slot_at_capacity(
    state: &mut RuntimeEventState,
    incoming: &RuntimeEventSlot,
) {
    let Some((family, capacity)) = dynamic_event_family(incoming) else {
        return;
    };
    let family_slots = state.slots.iter().filter_map(|(slot, sequence)| {
        (dynamic_event_family(slot).map(|(candidate, _)| candidate) == Some(family))
            .then_some((slot.clone(), *sequence))
    });
    let mut family_len = 0;
    let mut oldest = None;
    for candidate in family_slots {
        family_len += 1;
        if oldest
            .as_ref()
            .is_none_or(|(_, sequence)| candidate.1 < *sequence)
        {
            oldest = Some(candidate);
        }
    }
    if family_len < capacity {
        return;
    }
    let Some((oldest_slot, oldest_sequence)) = oldest else {
        return;
    };
    state.slots.remove(&oldest_slot);
    state.pending.remove(&oldest_sequence);
}

impl RuntimeEvents {
    pub async fn recv(&self) -> Result<RuntimeEvent, async_channel::RecvError> {
        loop {
            if let Some(event) = self.take_next() {
                return Ok(event);
            }
            self.notifications.recv().await?;
        }
    }

    pub fn recv_blocking(&self) -> Result<RuntimeEvent, async_channel::RecvError> {
        loop {
            if let Some(event) = self.take_next() {
                return Ok(event);
            }
            self.notifications.recv_blocking()?;
        }
    }

    pub fn try_recv(&self) -> Result<RuntimeEvent, async_channel::TryRecvError> {
        if let Some(event) = self.take_next() {
            return Ok(event);
        }
        if self.notifications.is_closed() {
            Err(async_channel::TryRecvError::Closed)
        } else {
            Err(async_channel::TryRecvError::Empty)
        }
    }

    fn take_next(&self) -> Option<RuntimeEvent> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let (&first_sequence, _) = state.pending.first_key_value()?;
        let priority_sequence = state
            .pending
            .iter()
            .find_map(|(sequence, event)| event_is_priority(event).then_some(*sequence));
        let sequence = priority_sequence
            .and_then(|priority_sequence| {
                quote_before_error(&state, priority_sequence).or(Some(priority_sequence))
            })
            .unwrap_or(first_sequence);
        let event = state.pending.remove(&sequence)?;
        let slot = runtime_event_slot(&event);
        if state.slots.get(&slot) == Some(&sequence) {
            state.slots.remove(&slot);
        }
        Some(event)
    }
}

fn normalize_runtime_event(event: RuntimeEvent) -> Vec<(RuntimeEventSlot, RuntimeEvent)> {
    match event {
        RuntimeEvent::Quotes(quotes) => {
            let skip = quotes.len().saturating_sub(QUOTE_EVENT_SLOT_CAPACITY);
            quotes
                .into_iter()
                .skip(skip)
                .map(|quote| {
                    let slot = RuntimeEventSlot::Quote(quote.instrument_id.clone());
                    (slot, RuntimeEvent::Quotes(vec![quote]))
                })
                .collect()
        }
        RuntimeEvent::QuoteError {
            instrument_ids,
            error,
        } => {
            let skip = instrument_ids
                .len()
                .saturating_sub(QUOTE_ERROR_EVENT_SLOT_CAPACITY);
            instrument_ids
                .into_iter()
                .skip(skip)
                .map(|instrument_id| {
                    let slot = RuntimeEventSlot::QuoteError(instrument_id.clone(), error.kind);
                    (
                        slot,
                        RuntimeEvent::QuoteError {
                            instrument_ids: vec![instrument_id],
                            error: error.clone(),
                        },
                    )
                })
                .collect()
        }
        event => vec![(runtime_event_slot(&event), event)],
    }
}

fn runtime_event_slot(event: &RuntimeEvent) -> RuntimeEventSlot {
    match event {
        RuntimeEvent::SearchCompleted { .. } => RuntimeEventSlot::Search,
        RuntimeEvent::SearchProviderCompleted { source, .. } => {
            RuntimeEventSlot::SearchProvider(*source)
        }
        RuntimeEvent::Quotes(quotes) => RuntimeEventSlot::Quote(
            quotes
                .first()
                .expect("normalized quote event is non-empty")
                .instrument_id
                .clone(),
        ),
        RuntimeEvent::QuoteError {
            instrument_ids,
            error,
        } => RuntimeEventSlot::QuoteError(
            instrument_ids
                .first()
                .expect("normalized quote error has an identity")
                .clone(),
            error.kind,
        ),
        RuntimeEvent::HistoryCompleted { key, .. } | RuntimeEvent::HistoryFailed { key, .. } => {
            RuntimeEventSlot::History(key.clone())
        }
        RuntimeEvent::CatalogRefresh { .. } => RuntimeEventSlot::Catalog,
        RuntimeEvent::PersistenceCompleted { category, .. } => {
            RuntimeEventSlot::Persistence(*category)
        }
        RuntimeEvent::StreamStatus(_) => RuntimeEventSlot::StreamStatus,
    }
}

fn event_is_priority(event: &RuntimeEvent) -> bool {
    !matches!(event, RuntimeEvent::Quotes(_))
}

fn quote_before_error(state: &RuntimeEventState, priority_sequence: u64) -> Option<u64> {
    let RuntimeEvent::QuoteError { instrument_ids, .. } = state.pending.get(&priority_sequence)?
    else {
        return None;
    };
    let quote_slot = RuntimeEventSlot::Quote(instrument_ids.first()?.clone());
    state
        .slots
        .get(&quote_slot)
        .copied()
        .filter(|quote_sequence| *quote_sequence < priority_sequence)
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
    #[error("persisted state could not be loaded: {0}")]
    PersistedState(PersistenceError),
    #[error("market cache could not be loaded: {0}")]
    MarketCache(PersistenceError),
    #[error("market-data clients could not be initialized: {0}")]
    ProviderSetup(reqwest::Error),
    #[error("background runtime could not be started: {0}")]
    BackgroundRuntime(io::Error),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StartupFailureCategory {
    PersistedState,
    MarketCache,
    ProviderSetup,
    BackgroundRuntime,
}

impl ProductionStartError {
    pub const fn category(&self) -> StartupFailureCategory {
        match self {
            Self::PersistedState(_) => StartupFailureCategory::PersistedState,
            Self::MarketCache(_) => StartupFailureCategory::MarketCache,
            Self::ProviderSetup(_) => StartupFailureCategory::ProviderSetup,
            Self::BackgroundRuntime(_) => StartupFailureCategory::BackgroundRuntime,
        }
    }
}

impl ProductionComponents {
    pub fn production() -> Result<Self, ProductionStartError> {
        Self::from_repository(JsonRepository::default_macos())
    }

    pub fn at(directory: impl AsRef<Path>) -> Result<Self, ProductionStartError> {
        Self::from_repository(JsonRepository::at(directory.as_ref()))
    }

    fn from_repository(repository: JsonRepository) -> Result<Self, ProductionStartError> {
        let state = repository
            .load_state_with_recovery()
            .map_err(ProductionStartError::PersistedState)?;
        let cache = repository
            .load_cache_with_recovery()
            .map_err(ProductionStartError::MarketCache)?;
        let yahoo = YahooClient::production().map_err(ProductionStartError::ProviderSetup)?;
        let binance = BinanceClient::production(cache.value.instrument_catalog.clone())
            .map_err(ProductionStartError::ProviderSetup)?;
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
        )
        .map_err(ProductionStartError::BackgroundRuntime)?;
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
    events: RuntimeEvents,
    event_sender: RuntimeEventSender,
    shutdown: AtomicBool,
}

struct LiveRuntime {
    commands: Sender<RuntimeCommand>,
    controls: Arc<ControlLane>,
    shutdown_signal: Sender<()>,
    events: RuntimeEvents,
    repository: Arc<dyn StateRepository>,
    cache_state: Arc<Mutex<CachedMarketState>>,
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
    search: DesiredSearchState,
}

#[derive(Clone, Default)]
struct DesiredSearchState {
    full: DesiredSearchSlot,
    yahoo: DesiredSearchSlot,
    binance: DesiredSearchSlot,
}

#[derive(Clone, Default)]
struct DesiredSearchSlot {
    revision: u64,
    request: Option<SearchRequest>,
}

#[derive(Clone)]
struct SearchRequest {
    generation: GenerationId,
    query: String,
}

struct ControlLane {
    desired: Mutex<DesiredControlState>,
    applied_revision: AtomicU64,
    notify: Sender<()>,
}

struct ShutdownCompletion {
    outcome: Mutex<Option<ShutdownOutcome>>,
    changed: Condvar,
}

#[derive(Clone)]
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
    events: RuntimeEventSender,
    shutdown: Receiver<()>,
    providers: ProviderSet,
    storage: RuntimeStorage,
    catalog_client: Option<BinanceClient>,
}

#[derive(Default)]
struct SearchJobs {
    full: Option<tokio::task::JoinHandle<()>>,
    yahoo: Option<tokio::task::JoinHandle<()>>,
    binance: Option<tokio::task::JoinHandle<()>>,
}

#[derive(Default)]
struct AppliedSearchRevisions {
    full: u64,
    yahoo: u64,
    binance: u64,
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
        let (event_sender, events) = runtime_event_channel();
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
        let (event_sender, events) = runtime_event_channel();
        let (shutdown_signal, shutdown_receiver) = async_channel::bounded(1);
        let (control_notify, control_notifications) = async_channel::bounded(1);
        let controls = Arc::new(ControlLane {
            desired: Mutex::new(DesiredControlState {
                revision: 0,
                visible: true,
                watchlist: Vec::new(),
                search: DesiredSearchState::default(),
            }),
            applied_revision: AtomicU64::new(0),
            notify: control_notify,
        });
        let completion = Arc::new(ShutdownCompletion {
            outcome: Mutex::new(None),
            changed: Condvar::new(),
        });
        let final_flush = Arc::new(Mutex::new(None));
        let cache_state = Arc::new(Mutex::new(initial_cache));
        let storage = RuntimeStorage {
            repository: repository.clone(),
            cache: cache_state.clone(),
            final_flush: final_flush.clone(),
        };
        let network_controls = controls.clone();
        let network_completion = completion.clone();
        let join = thread::Builder::new()
            .name("kabu-network".into())
            .spawn(move || {
                let outcome = run_network(NetworkInputs {
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
                    .outcome
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(outcome);
                network_completion.changed.notify_all();
            })?;

        Ok(Self {
            mode: RuntimeMode::Live(Arc::new(LiveRuntime {
                commands,
                controls,
                shutdown_signal,
                events,
                repository,
                cache_state,
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
                    coalesce_recording_control(&mut commands, &command);
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
                    RuntimeCommand::Search { generation, query } => {
                        runtime.controls.update_search(generation, query);
                        Ok(())
                    }
                    RuntimeCommand::RetrySearchProvider {
                        generation,
                        query,
                        source,
                    } => {
                        runtime
                            .controls
                            .update_search_retry(generation, query, source);
                        Ok(())
                    }
                    RuntimeCommand::CancelSearch => {
                        runtime.controls.cancel_search();
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

    pub fn events(&self) -> RuntimeEvents {
        match &self.mode {
            RuntimeMode::Recording(runtime) => runtime.events.clone(),
            RuntimeMode::Live(runtime) => runtime.events.clone(),
        }
    }

    #[doc(hidden)]
    pub fn publish_recorded_event(&self, event: RuntimeEvent) {
        if let RuntimeMode::Recording(runtime) = &self.mode {
            _ = runtime.event_sender.try_send(event);
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

    pub fn wait_for_shutdown(&self, timeout: std::time::Duration) -> Option<ShutdownOutcome> {
        match &self.mode {
            RuntimeMode::Recording(runtime) => runtime
                .shutdown
                .load(Ordering::SeqCst)
                .then(ShutdownOutcome::success_without_flush),
            RuntimeMode::Live(runtime) => wait_for_live_shutdown(runtime, timeout),
        }
    }

    pub fn shutdown_outcome(&self) -> Option<ShutdownOutcome> {
        match &self.mode {
            RuntimeMode::Recording(runtime) => runtime
                .shutdown
                .load(Ordering::SeqCst)
                .then(ShutdownOutcome::success_without_flush),
            RuntimeMode::Live(runtime) => *runtime
                .completion
                .outcome
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        }
    }

    pub fn retry_shutdown_persistence(&self) -> ShutdownOutcome {
        let RuntimeMode::Live(runtime) = &self.mode else {
            return ShutdownOutcome::success_without_flush();
        };
        let previous = runtime
            .completion
            .outcome
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .unwrap_or_else(ShutdownOutcome::runtime_failed);
        if previous.runtime_failed {
            return previous;
        }
        let Some(flush) = runtime
            .final_flush
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
        else {
            return previous;
        };
        *runtime
            .completion
            .outcome
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
        let outcome = retry_final_flush(
            runtime.repository.as_ref(),
            &runtime.cache_state,
            &flush,
            previous,
        );
        *runtime
            .completion
            .outcome
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(outcome);
        runtime.completion.changed.notify_all();
        outcome
    }

    pub fn is_shutdown_complete(&self) -> bool {
        match &self.mode {
            RuntimeMode::Recording(runtime) => runtime.shutdown.load(Ordering::SeqCst),
            RuntimeMode::Live(runtime) => runtime
                .completion
                .outcome
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .is_some(),
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
        RuntimeCommand::Search { .. }
            | RuntimeCommand::RetrySearchProvider { .. }
            | RuntimeCommand::CancelSearch
            | RuntimeCommand::SetVisible(_)
            | RuntimeCommand::SetWatchlist(_)
            | RuntimeCommand::SetCryptoWatchlist(_)
    )
}

fn coalesce_recording_control(commands: &mut VecDeque<RuntimeCommand>, incoming: &RuntimeCommand) {
    commands.retain(|existing| match incoming {
        RuntimeCommand::Search { .. } | RuntimeCommand::CancelSearch => !matches!(
            existing,
            RuntimeCommand::Search { .. }
                | RuntimeCommand::RetrySearchProvider { .. }
                | RuntimeCommand::CancelSearch
        ),
        RuntimeCommand::RetrySearchProvider { source, .. } => !matches!(
            existing,
            RuntimeCommand::RetrySearchProvider {
                source: existing_source,
                ..
            } if existing_source == source
        ),
        RuntimeCommand::SetVisible(_) => !matches!(existing, RuntimeCommand::SetVisible(_)),
        RuntimeCommand::SetWatchlist(_) => !matches!(existing, RuntimeCommand::SetWatchlist(_)),
        RuntimeCommand::SetCryptoWatchlist(_) => {
            !matches!(existing, RuntimeCommand::SetCryptoWatchlist(_))
        }
        RuntimeCommand::Snapshot { .. }
        | RuntimeCommand::History { .. }
        | RuntimeCommand::PersistState(_)
        | RuntimeCommand::PersistMarketCache { .. }
        | RuntimeCommand::Shutdown => true,
    });
}

impl Drop for LiveRuntime {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        _ = self.shutdown_signal.try_send(());
        _ = self.commands.try_send(RuntimeCommand::Shutdown);
        self.commands.close();
        self.controls.notify.close();
        self.shutdown_signal.close();
        let mut outcome = self
            .completion
            .outcome
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while outcome.is_none() {
            outcome = self
                .completion
                .changed
                .wait_timeout(outcome, std::time::Duration::from_secs(2))
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
        self.update(|state| {
            state.visible = visible;
            if !visible {
                state.search.cancel_all();
            }
        });
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

    fn update_search(&self, generation: GenerationId, query: String) {
        self.update(|state| {
            state
                .search
                .replace_full(SearchRequest { generation, query });
        });
    }

    fn update_search_retry(&self, generation: GenerationId, query: String, source: DataSource) {
        self.update(|state| {
            state
                .search
                .replace_retry(source, SearchRequest { generation, query });
        });
    }

    fn cancel_search(&self) {
        self.update(|state| state.search.cancel_all());
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

impl DesiredSearchState {
    fn replace_full(&mut self, request: SearchRequest) {
        self.full.replace(Some(request));
        self.yahoo.replace(None);
        self.binance.replace(None);
    }

    fn replace_retry(&mut self, source: DataSource, request: SearchRequest) {
        self.provider_mut(source).replace(Some(request));
    }

    fn cancel_all(&mut self) {
        self.full.replace(None);
        self.yahoo.replace(None);
        self.binance.replace(None);
    }

    fn provider(&self, source: DataSource) -> &DesiredSearchSlot {
        match source {
            DataSource::Yahoo => &self.yahoo,
            DataSource::Binance => &self.binance,
        }
    }

    fn provider_mut(&mut self, source: DataSource) -> &mut DesiredSearchSlot {
        match source {
            DataSource::Yahoo => &mut self.yahoo,
            DataSource::Binance => &mut self.binance,
        }
    }
}

impl DesiredSearchSlot {
    fn replace(&mut self, request: Option<SearchRequest>) {
        self.revision = self.revision.wrapping_add(1);
        self.request = request;
    }
}

impl SearchJobs {
    async fn cancel_all(&mut self) {
        abort_and_join(&mut self.full).await;
        abort_and_join(&mut self.yahoo).await;
        abort_and_join(&mut self.binance).await;
    }

    async fn cancel_provider(&mut self, source: DataSource) {
        abort_and_join(self.provider_mut(source)).await;
    }

    fn provider_mut(&mut self, source: DataSource) -> &mut Option<tokio::task::JoinHandle<()>> {
        match source {
            DataSource::Yahoo => &mut self.yahoo,
            DataSource::Binance => &mut self.binance,
        }
    }
}

async fn abort_and_join(job: &mut Option<tokio::task::JoinHandle<()>>) {
    if let Some(job) = job.take() {
        job.abort();
        _ = job.await;
    }
}

async fn apply_desired_search(
    desired: &DesiredSearchState,
    applied: &mut AppliedSearchRevisions,
    jobs: &mut SearchJobs,
    visible: bool,
    providers: &ProviderSet,
    events: &RuntimeEventSender,
) {
    if applied.full != desired.full.revision {
        jobs.cancel_all().await;
        events.clear_search();
        if visible && let Some(request) = desired.full.request.clone() {
            let providers = providers.clone();
            let events = events.clone();
            jobs.full = Some(tokio::spawn(async move {
                send_search(request.generation, request.query, &providers, &events).await;
            }));
        }
        applied.full = desired.full.revision;
    }

    for source in [DataSource::Yahoo, DataSource::Binance] {
        let desired_slot = desired.provider(source);
        let applied_revision = match source {
            DataSource::Yahoo => &mut applied.yahoo,
            DataSource::Binance => &mut applied.binance,
        };
        if *applied_revision == desired_slot.revision {
            continue;
        }
        jobs.cancel_provider(source).await;
        events.clear_search_provider(source);
        if visible && let Some(request) = desired_slot.request.clone() {
            let providers = providers.clone();
            let events = events.clone();
            *jobs.provider_mut(source) = Some(tokio::spawn(async move {
                send_provider_search(
                    request.generation,
                    request.query,
                    source,
                    &providers,
                    &events,
                )
                .await;
            }));
        }
        *applied_revision = desired_slot.revision;
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

fn wait_for_live_shutdown(
    runtime: &LiveRuntime,
    timeout: std::time::Duration,
) -> Option<ShutdownOutcome> {
    let outcome = runtime
        .completion
        .outcome
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let (outcome, _) = runtime
        .completion
        .changed
        .wait_timeout_while(outcome, timeout, |outcome| outcome.is_none())
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let result = *outcome;
    drop(outcome);
    result?;
    if let Some(join) = runtime
        .join
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .take()
    {
        _ = join.join();
    }
    result
}

fn run_network(inputs: NetworkInputs) -> ShutdownOutcome {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("kabu-network")
        .enable_all()
        .build();
    let Ok(runtime) = runtime else {
        return ShutdownOutcome::runtime_failed();
    };
    runtime.block_on(run_dispatcher(inputs))
}

async fn run_dispatcher(inputs: NetworkInputs) -> ShutdownOutcome {
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
    let mut search_jobs = SearchJobs::default();
    let mut applied_search = AppliedSearchRevisions::default();
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
        events.clone(),
    ));
    let catalog_repository = repository.clone();
    let catalog_cache_state = cache_state.clone();
    let mut catalog_job = catalog_client.map(|client| {
        let events = events.clone();
        let repository = catalog_repository.clone();
        let cache_state = catalog_cache_state.clone();
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
                coordinator.set_watchlist(desired.watchlist.clone()).await;
                visible = true;
                coordinator.set_visible(true).await;
            } else {
                visible = false;
                pending.clear();
                jobs.abort_all();
                while jobs.join_next().await.is_some() {}
                other_in_flight = 0;
                coordinator.set_visible(false).await;
                coordinator.set_watchlist(desired.watchlist.clone()).await;
            }
            events.retain_watchlist_events(&desired.watchlist);
            apply_desired_search(
                &desired.search,
                &mut applied_search,
                &mut search_jobs,
                visible,
                &providers,
                &events,
            )
            .await;
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
                    RuntimeCommand::Search { .. }
                    | RuntimeCommand::RetrySearchProvider { .. }
                    | RuntimeCommand::CancelSearch => {}
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
                            search_jobs.cancel_all().await;
                            events.clear_search();
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

    search_jobs.cancel_all().await;
    events.clear_search();
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
    let final_flush = final_flush
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    persistence.notify.close();
    _ = persistence_job.await;
    let Some(final_flush) = final_flush else {
        return ShutdownOutcome::success_without_flush();
    };
    let repository_for_flush = repository.clone();
    let cache_for_flush = cache_state.clone();
    let outcome = tokio::task::spawn_blocking(move || {
        persist_final_flush(
            repository_for_flush.as_ref(),
            &cache_for_flush,
            &final_flush,
        )
    })
    .await
    .unwrap_or_else(|_| ShutdownOutcome::runtime_failed());
    publish_persistence_outcome(&events, PersistenceCategory::State, outcome.state).await;
    publish_persistence_outcome(
        &events,
        PersistenceCategory::MarketCache,
        outcome.market_cache,
    )
    .await;
    outcome
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
    events: RuntimeEventSender,
) {
    while notifications.recv().await.is_ok() {
        loop {
            let desired = persistence.take();
            if desired.state.is_none() && desired.cache.is_none() {
                break;
            }
            let repository = repository.clone();
            let cache_state = cache_state.clone();
            let attempted_state = desired.state.is_some();
            let attempted_cache = desired.cache.is_some();
            let results = tokio::task::spawn_blocking(move || {
                let state = desired
                    .state
                    .map(|state| persistence_result(repository.save_state(&state)));
                let cache = desired.cache.map(|cache| {
                    let mut current = cache_state
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    current.quotes = cache.quotes;
                    current.charts = cache.charts;
                    persistence_result(repository.save_cache(&current))
                });
                (state, cache)
            })
            .await;
            let (state, cache) = results.unwrap_or((
                attempted_state.then_some(PersistenceResult::Failed),
                attempted_cache.then_some(PersistenceResult::Failed),
            ));
            publish_persistence_outcome(&events, PersistenceCategory::State, state).await;
            publish_persistence_outcome(&events, PersistenceCategory::MarketCache, cache).await;
        }
    }
}

fn persistence_result(result: Result<(), PersistenceError>) -> PersistenceResult {
    if result.is_ok() {
        PersistenceResult::Saved
    } else {
        PersistenceResult::Failed
    }
}

async fn publish_persistence_outcome(
    events: &RuntimeEventSender,
    category: PersistenceCategory,
    result: Option<PersistenceResult>,
) {
    if let Some(result) = result {
        _ = events
            .send(RuntimeEvent::PersistenceCompleted { category, result })
            .await;
    }
}

fn persist_final_flush(
    repository: &dyn StateRepository,
    cache_state: &Arc<Mutex<CachedMarketState>>,
    flush: &FinalFlush,
) -> ShutdownOutcome {
    ShutdownOutcome {
        state: Some(persistence_result(repository.save_state(&flush.state))),
        market_cache: Some(persist_final_cache(repository, cache_state, &flush.cache)),
        runtime_failed: false,
    }
}

fn retry_final_flush(
    repository: &dyn StateRepository,
    cache_state: &Arc<Mutex<CachedMarketState>>,
    flush: &FinalFlush,
    previous: ShutdownOutcome,
) -> ShutdownOutcome {
    let state = match previous.state {
        Some(PersistenceResult::Failed) => {
            Some(persistence_result(repository.save_state(&flush.state)))
        }
        result => result,
    };
    let market_cache = match previous.market_cache {
        Some(PersistenceResult::Failed) => {
            Some(persist_final_cache(repository, cache_state, &flush.cache))
        }
        result => result,
    };
    ShutdownOutcome {
        state,
        market_cache,
        runtime_failed: false,
    }
}

fn persist_final_cache(
    repository: &dyn StateRepository,
    cache_state: &Arc<Mutex<CachedMarketState>>,
    cache: &CachedMarketState,
) -> PersistenceResult {
    let mut current = cache_state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    current.quotes = cache.quotes.clone();
    current.charts = cache.charts.clone();
    persistence_result(repository.save_cache(&current))
}

fn spawn_other_job(
    command: RuntimeCommand,
    jobs: &mut tokio::task::JoinSet<()>,
    events: &RuntimeEventSender,
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
            | RuntimeCommand::RetrySearchProvider { .. }
            | RuntimeCommand::CancelSearch
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
    events: &RuntimeEventSender,
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

async fn send_provider_search(
    generation: GenerationId,
    query: String,
    source: DataSource,
    providers: &ProviderSet,
    events: &RuntimeEventSender,
) {
    let outcome = match source {
        DataSource::Yahoo => providers.equities.search(&query).await,
        DataSource::Binance => providers.crypto.search(&query).await,
    };
    let (results, warning) = match outcome {
        Ok(results) => (rank_search_results(&query, results), None),
        Err(error) => (
            Vec::new(),
            Some(ProviderWarning {
                source,
                kind: error.kind,
                message: search_warning_message(source).into(),
            }),
        ),
    };
    _ = events
        .send(RuntimeEvent::SearchProviderCompleted {
            generation,
            source,
            results,
            warning,
        })
        .await;
}

async fn send_history(
    generation: GenerationId,
    instrument: Instrument,
    period: ChartPeriod,
    providers: &ProviderSet,
    events: &RuntimeEventSender,
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
            message: search_warning_message(source).into(),
        }),
    }
}

fn search_warning_message(source: DataSource) -> &'static str {
    match source {
        DataSource::Yahoo => "Yahoo search is temporarily unavailable.",
        DataSource::Binance => "Binance search is temporarily unavailable.",
    }
}

async fn send_snapshots(
    instruments: Vec<Instrument>,
    providers: &ProviderSet,
    events: &RuntimeEventSender,
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
    events: &RuntimeEventSender,
) {
    match outcome {
        Ok(provider_quotes) => quotes.extend(provider_quotes.into_iter().filter(|quote| {
            instruments
                .iter()
                .any(|instrument| instrument.id == quote.instrument_id)
        })),
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
        PersistenceCategory, PersistenceResult, ProductionComponents, ProviderSet, RuntimeCommand,
        RuntimeEvent, RuntimeHandle, StreamStatus, refresh_catalog_cache,
    };
    use crate::app_model::{AppModel, ChartAvailability};
    use async_trait::async_trait;
    use kabu_core::{
        CachedMarketState, Candle, ChartHistory, ChartPeriod, ChartRequestKey, DataSource,
        Generation, Instrument, InstrumentId, InstrumentSearch, MarketDataError,
        MarketDataErrorKind, MarketState, PersistedState, PersistenceError, Quote, QuoteSnapshots,
        QuoteStream, QuoteSubscription, SearchResult, StateRepository,
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
    fn distinct_event_flood_has_fixed_cardinality_and_retains_terminal_state() {
        let (sender, events) = super::runtime_event_channel();
        let mut generations = Generation::default();
        let final_id = distinct_equity_id(511);

        sender
            .try_send(RuntimeEvent::SearchCompleted {
                generation: generations.next(),
                results: vec![nvda_result()],
                warnings: Vec::new(),
            })
            .expect("terminal search event");
        sender
            .try_send(RuntimeEvent::CatalogRefresh {
                result: Err(MarketDataErrorKind::Unavailable),
                catalog_available: false,
            })
            .expect("catalog control event");
        sender
            .try_send(RuntimeEvent::PersistenceCompleted {
                category: PersistenceCategory::State,
                result: PersistenceResult::Failed,
            })
            .expect("state persistence event");
        sender
            .try_send(RuntimeEvent::PersistenceCompleted {
                category: PersistenceCategory::MarketCache,
                result: PersistenceResult::Failed,
            })
            .expect("cache persistence event");
        sender
            .try_send(RuntimeEvent::StreamStatus(StreamStatus::Connecting))
            .expect("initial status");

        for index in 0..512 {
            let instrument_id = distinct_equity_id(index);
            let mut quote = test_quote(instrument_id.clone(), DataSource::Yahoo);
            quote.last = index as f64;
            sender
                .try_send(RuntimeEvent::Quotes(vec![quote]))
                .expect("quote flood remains nonblocking");
            sender
                .try_send(RuntimeEvent::QuoteError {
                    instrument_ids: vec![instrument_id.clone()],
                    error: test_error(MarketDataErrorKind::Disconnected),
                })
                .expect("error flood remains nonblocking");
            sender
                .try_send(RuntimeEvent::HistoryCompleted {
                    generation: generations.next(),
                    key: ChartRequestKey {
                        instrument_id,
                        period: ChartPeriod::OneDay,
                    },
                    candles: vec![test_candle()],
                })
                .expect("history flood remains nonblocking");
        }
        sender
            .try_send(RuntimeEvent::StreamStatus(StreamStatus::Paused))
            .expect("final status");

        {
            let state = sender
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let quote_slots = state
                .slots
                .keys()
                .filter(|slot| matches!(slot, super::RuntimeEventSlot::Quote(_)))
                .count();
            let error_slots = state
                .slots
                .keys()
                .filter(|slot| matches!(slot, super::RuntimeEventSlot::QuoteError(_, _)))
                .count();
            let history_slots = state
                .slots
                .keys()
                .filter(|slot| matches!(slot, super::RuntimeEventSlot::History(_)))
                .count();
            assert!(quote_slots <= 64, "quote slots must have a fixed cap");
            assert!(error_slots <= 64, "error slots must have a fixed cap");
            assert!(history_slots <= 32, "history slots must have a fixed cap");
            assert!(state.pending.len() <= 167, "all semantic slots are bounded");
        }

        let mut saw_final_quote = false;
        let mut saw_final_error = false;
        let mut saw_search = false;
        let mut saw_catalog = false;
        let mut saw_state_persistence = false;
        let mut saw_cache_persistence = false;
        let mut status = None;
        while let Ok(event) = events.try_recv() {
            match event {
                RuntimeEvent::Quotes(quotes) => {
                    saw_final_quote |= quotes.first().is_some_and(|quote| {
                        quote.instrument_id == final_id && quote.last == 511.0
                    });
                }
                RuntimeEvent::QuoteError {
                    instrument_ids,
                    error,
                } => {
                    saw_final_error |= instrument_ids == [final_id.clone()]
                        && error.kind == MarketDataErrorKind::Disconnected;
                }
                RuntimeEvent::SearchCompleted { .. } => saw_search = true,
                RuntimeEvent::CatalogRefresh { .. } => saw_catalog = true,
                RuntimeEvent::PersistenceCompleted { category, .. } => match category {
                    PersistenceCategory::State => saw_state_persistence = true,
                    PersistenceCategory::MarketCache => saw_cache_persistence = true,
                },
                RuntimeEvent::StreamStatus(next) => status = Some(next),
                RuntimeEvent::SearchProviderCompleted { .. }
                | RuntimeEvent::HistoryCompleted { .. }
                | RuntimeEvent::HistoryFailed { .. } => {}
            }
        }
        assert!(saw_final_quote);
        assert!(saw_final_error);
        assert!(saw_search);
        assert!(saw_catalog);
        assert!(saw_state_persistence);
        assert!(saw_cache_persistence);
        assert_eq!(status, Some(StreamStatus::Paused));
    }

    #[test]
    fn one_oversized_provider_event_normalizes_to_a_fixed_number_of_slots() {
        let quotes: Vec<_> = (0..512)
            .map(|index| test_quote(distinct_equity_id(index), DataSource::Yahoo))
            .collect();
        let quote_events = super::normalize_runtime_event(RuntimeEvent::Quotes(quotes));
        assert_eq!(quote_events.len(), super::QUOTE_EVENT_SLOT_CAPACITY);
        assert!(matches!(
            quote_events.last(),
            Some((super::RuntimeEventSlot::Quote(id), _)) if id == &distinct_equity_id(511)
        ));

        let instrument_ids: Vec<_> = (0..512).map(distinct_equity_id).collect();
        let error_events = super::normalize_runtime_event(RuntimeEvent::QuoteError {
            instrument_ids,
            error: test_error(MarketDataErrorKind::Timeout),
        });
        assert_eq!(error_events.len(), super::QUOTE_ERROR_EVENT_SLOT_CAPACITY);
        assert!(matches!(
            error_events.last(),
            Some((super::RuntimeEventSlot::QuoteError(id, _), _))
                if id == &distinct_equity_id(511)
        ));
    }

    #[test]
    fn mailbox_never_replaces_newer_generation_with_late_stale_history_or_search() {
        let (sender, events) = super::runtime_event_channel();
        let mut generations = Generation::default();
        let stale = generations.next();
        let current = generations.next();
        let key = ChartRequestKey {
            instrument_id: aapl_instrument().id,
            period: ChartPeriod::OneMonth,
        };

        sender
            .try_send(RuntimeEvent::HistoryCompleted {
                generation: current,
                key: key.clone(),
                candles: vec![test_candle()],
            })
            .expect("current history");
        sender
            .try_send(RuntimeEvent::HistoryFailed {
                generation: stale,
                key,
                error: test_error(MarketDataErrorKind::Timeout),
            })
            .expect("late stale history");
        sender
            .try_send(RuntimeEvent::SearchCompleted {
                generation: current,
                results: vec![btc_result()],
                warnings: Vec::new(),
            })
            .expect("current search");
        sender
            .try_send(RuntimeEvent::SearchCompleted {
                generation: stale,
                results: vec![nvda_result()],
                warnings: Vec::new(),
            })
            .expect("late stale search");
        sender
            .try_send(RuntimeEvent::SearchProviderCompleted {
                generation: current,
                source: DataSource::Binance,
                results: vec![btc_result()],
                warning: None,
            })
            .expect("current provider search");
        sender
            .try_send(RuntimeEvent::SearchProviderCompleted {
                generation: stale,
                source: DataSource::Binance,
                results: Vec::new(),
                warning: Some(super::ProviderWarning {
                    source: DataSource::Binance,
                    kind: MarketDataErrorKind::Unavailable,
                    message: "fixed warning".into(),
                }),
            })
            .expect("late stale provider search");

        let mut history_generation = None;
        let mut search_generation = None;
        let mut provider_generation = None;
        while let Ok(event) = events.try_recv() {
            match event {
                RuntimeEvent::HistoryCompleted { generation, .. }
                | RuntimeEvent::HistoryFailed { generation, .. } => {
                    history_generation = Some(generation);
                }
                RuntimeEvent::SearchCompleted { generation, .. } => {
                    search_generation = Some(generation);
                }
                RuntimeEvent::SearchProviderCompleted { generation, .. } => {
                    provider_generation = Some(generation);
                }
                RuntimeEvent::Quotes(_)
                | RuntimeEvent::QuoteError { .. }
                | RuntimeEvent::CatalogRefresh { .. }
                | RuntimeEvent::PersistenceCompleted { .. }
                | RuntimeEvent::StreamStatus(_) => {}
            }
        }
        assert_eq!(history_generation, Some(current));
        assert_eq!(search_generation, Some(current));
        assert_eq!(provider_generation, Some(current));
    }

    #[test]
    fn reverse_history_completion_order_reaches_current_terminal_app_state() {
        let cases = [true, false];
        for current_succeeds in cases {
            let (sender, events) = super::runtime_event_channel();
            let runtime = RuntimeHandle::default();
            let mut model = AppModel::new(
                PersistedState::starter(),
                CachedMarketState::default(),
                runtime,
            );
            let instrument = model.selected().expect("starter selection").clone();
            let stale = model.request_history(instrument.clone(), ChartPeriod::OneMonth);
            let current = model.request_history(instrument.clone(), ChartPeriod::OneMonth);
            let key = ChartRequestKey {
                instrument_id: instrument.id,
                period: ChartPeriod::OneMonth,
            };

            if current_succeeds {
                sender
                    .try_send(RuntimeEvent::HistoryCompleted {
                        generation: current,
                        key: key.clone(),
                        candles: vec![test_candle()],
                    })
                    .expect("current completion");
                sender
                    .try_send(RuntimeEvent::HistoryFailed {
                        generation: stale,
                        key,
                        error: test_error(MarketDataErrorKind::Timeout),
                    })
                    .expect("late stale failure");
            } else {
                sender
                    .try_send(RuntimeEvent::HistoryFailed {
                        generation: current,
                        key: key.clone(),
                        error: test_error(MarketDataErrorKind::Timeout),
                    })
                    .expect("current failure");
                sender
                    .try_send(RuntimeEvent::HistoryCompleted {
                        generation: stale,
                        key,
                        candles: vec![test_candle()],
                    })
                    .expect("late stale completion");
            }

            while let Ok(event) = events.try_recv() {
                model.apply_runtime_event(event);
            }
            assert_eq!(
                model.selected_chart_availability(),
                if current_succeeds {
                    ChartAvailability::Ready
                } else {
                    ChartAvailability::Unavailable
                }
            );
        }
    }

    #[test]
    fn watchlist_and_search_cleanup_remove_dead_mailbox_keys() {
        let (sender, events) = super::runtime_event_channel();
        let keep = aapl_instrument();
        let remove =
            Instrument::equity("DROP", Some("NYSE"), "Removed", "USD").expect("removed instrument");
        let generation = kabu_core::Generation::default().next();
        for instrument in [&keep, &remove] {
            sender
                .try_send(RuntimeEvent::Quotes(vec![test_quote(
                    instrument.id.clone(),
                    DataSource::Yahoo,
                )]))
                .expect("quote");
            sender
                .try_send(RuntimeEvent::QuoteError {
                    instrument_ids: vec![instrument.id.clone()],
                    error: test_error(MarketDataErrorKind::Timeout),
                })
                .expect("quote error");
            sender
                .try_send(RuntimeEvent::HistoryCompleted {
                    generation,
                    key: ChartRequestKey {
                        instrument_id: instrument.id.clone(),
                        period: ChartPeriod::OneDay,
                    },
                    candles: vec![test_candle()],
                })
                .expect("history");
        }
        sender
            .try_send(RuntimeEvent::SearchCompleted {
                generation,
                results: vec![nvda_result()],
                warnings: Vec::new(),
            })
            .expect("full search");
        sender
            .try_send(RuntimeEvent::SearchProviderCompleted {
                generation,
                source: DataSource::Yahoo,
                results: vec![nvda_result()],
                warning: None,
            })
            .expect("provider search");

        sender.retain_watchlist_events(std::slice::from_ref(&keep));
        sender.clear_search();

        {
            let state = sender
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            assert!(state.slots.keys().all(|slot| match slot {
                super::RuntimeEventSlot::Quote(id) | super::RuntimeEventSlot::QuoteError(id, _) =>
                    id == &keep.id,
                super::RuntimeEventSlot::History(key) => key.instrument_id == keep.id,
                super::RuntimeEventSlot::Search | super::RuntimeEventSlot::SearchProvider(_) => {
                    false
                }
                _ => true,
            }));
        }
        while events.try_recv().is_ok() {}
        let state = sender
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert!(state.slots.is_empty());
        assert!(state.pending.is_empty());
    }

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
    fn search_cancel_is_reliable_when_the_ordinary_queue_is_saturated() {
        let runtime = RuntimeHandle::default();
        for _ in 0..super::COMMAND_CAPACITY {
            runtime
                .send(RuntimeCommand::Snapshot {
                    instruments: Vec::new(),
                })
                .expect("fill ordinary queue");
        }

        runtime
            .send(RuntimeCommand::CancelSearch)
            .expect("search cancellation uses the reliable control lane");

        assert!(matches!(
            runtime.recorded_commands().last(),
            Some(RuntimeCommand::CancelSearch)
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
    fn live_persistence_reports_state_and_cache_failure_then_same_category_recovery() {
        let repository = Arc::new(FlakyRepository::failing());
        let providers = ProviderSet {
            equities: Arc::new(FakeProvider::immediate(DataSource::Yahoo, Vec::new())),
            crypto: Arc::new(FakeProvider::immediate(DataSource::Binance, Vec::new())),
        };
        let runtime = RuntimeHandle::start(providers, repository.clone()).expect("runtime");
        let events = runtime.events();

        runtime
            .send(RuntimeCommand::PersistState(PersistedState::starter()))
            .expect("state command");
        runtime
            .send(RuntimeCommand::PersistMarketCache {
                cache: CachedMarketState::default(),
            })
            .expect("cache command");

        assert_persistence_event(
            &events,
            PersistenceCategory::State,
            PersistenceResult::Failed,
        );
        assert_persistence_event(
            &events,
            PersistenceCategory::MarketCache,
            PersistenceResult::Failed,
        );

        repository.recover_state();
        runtime
            .send(RuntimeCommand::PersistState(PersistedState::starter()))
            .expect("state retry");
        assert_persistence_event(
            &events,
            PersistenceCategory::State,
            PersistenceResult::Saved,
        );

        repository.recover_cache();
        runtime
            .send(RuntimeCommand::PersistMarketCache {
                cache: CachedMarketState::default(),
            })
            .expect("cache retry");
        assert_persistence_event(
            &events,
            PersistenceCategory::MarketCache,
            PersistenceResult::Saved,
        );
        runtime.shutdown();
    }

    #[test]
    fn final_persistence_failure_is_a_typed_shutdown_outcome_and_can_be_retried() {
        let repository = Arc::new(FlakyRepository::failing());
        let providers = ProviderSet {
            equities: Arc::new(FakeProvider::immediate(DataSource::Yahoo, Vec::new())),
            crypto: Arc::new(FakeProvider::immediate(DataSource::Binance, Vec::new())),
        };
        let runtime = RuntimeHandle::start(providers, repository.clone()).expect("runtime");

        runtime.begin_shutdown_with_flush(PersistedState::starter(), CachedMarketState::default());
        let failed = runtime
            .wait_for_shutdown(Duration::from_secs(2))
            .expect("finished shutdown");
        assert!(!failed.is_success());
        assert_eq!(failed.state, Some(PersistenceResult::Failed));
        assert_eq!(failed.market_cache, Some(PersistenceResult::Failed));

        repository.recover_state();
        repository.recover_cache();
        let recovered = runtime.retry_shutdown_persistence();
        assert!(recovered.is_success());
        assert_eq!(recovered.state, Some(PersistenceResult::Saved));
        assert_eq!(recovered.market_cache, Some(PersistenceResult::Saved));
        assert_eq!(repository.state_saves.load(Ordering::SeqCst), 2);
        assert_eq!(repository.cache_saves.load(Ordering::SeqCst), 2);
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
        assert!(
            runtime
                .wait_for_shutdown(Duration::from_millis(50))
                .is_none()
        );
        assert!(wait_started.elapsed() < Duration::from_secs(2));
        assert!(!runtime.is_shutdown_complete());

        repository.release();
        assert!(runtime.wait_for_shutdown(Duration::from_secs(2)).is_some());
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
    fn retry_search_calls_only_the_failed_provider_and_returns_a_typed_replacement() {
        let yahoo = SequencedSearchProvider::new(DataSource::Yahoo, vec![Ok(vec![nvda_result()])]);
        let yahoo_calls = yahoo.calls.clone();
        let crypto = SequencedSearchProvider::new(
            DataSource::Binance,
            vec![
                Err(MarketDataError {
                    kind: MarketDataErrorKind::Unavailable,
                    message: "private-query-provider-body".into(),
                    retry_after: None,
                }),
                Ok(vec![btc_result()]),
            ],
        );
        let crypto_calls = crypto.calls.clone();
        let providers = ProviderSet {
            equities: Arc::new(yahoo),
            crypto: Arc::new(crypto),
        };
        let runtime = RuntimeHandle::start(providers, Arc::new(NoopRepository)).expect("runtime");
        let generation = kabu_core::Generation::default().next();

        runtime
            .send(RuntimeCommand::Search {
                generation,
                query: "btc".into(),
            })
            .expect("initial search");
        let RuntimeEvent::SearchCompleted {
            results, warnings, ..
        } = runtime
            .events()
            .recv_blocking()
            .expect("initial completion")
        else {
            panic!("expected full search completion");
        };
        assert_eq!(results, vec![nvda_result()]);
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].source, DataSource::Binance);

        runtime
            .send(RuntimeCommand::RetrySearchProvider {
                generation,
                query: "btc".into(),
                source: DataSource::Binance,
            })
            .expect("provider retry");
        let RuntimeEvent::SearchProviderCompleted {
            source,
            results,
            warning,
            ..
        } = runtime.events().recv_blocking().expect("retry completion")
        else {
            panic!("expected provider completion");
        };

        assert_eq!(source, DataSource::Binance);
        assert_eq!(results, vec![btc_result()]);
        assert!(warning.is_none());
        assert_eq!(yahoo_calls.load(Ordering::SeqCst), 1);
        assert_eq!(crypto_calls.load(Ordering::SeqCst), 2);
        runtime.shutdown();
    }

    #[test]
    fn merged_runtime_search_preserves_common_crypto_quote_priority() {
        let providers = ProviderSet {
            equities: Arc::new(FakeProvider::immediate(DataSource::Yahoo, Vec::new())),
            crypto: Arc::new(FakeProvider::immediate(
                DataSource::Binance,
                vec![
                    crypto_result("BTC", "TRY"),
                    crypto_result("BTC", "ETH"),
                    crypto_result("BTC", "USDC"),
                    crypto_result("BTC", "USDT"),
                ],
            )),
        };
        let runtime = RuntimeHandle::start(providers, Arc::new(NoopRepository)).expect("runtime");
        runtime
            .send(RuntimeCommand::Search {
                generation: kabu_core::Generation::default().next(),
                query: "BTC".into(),
            })
            .expect("search");

        let RuntimeEvent::SearchCompleted { results, .. } =
            runtime.events().recv_blocking().expect("search result")
        else {
            panic!("expected search completion");
        };
        assert_eq!(
            results
                .iter()
                .map(|result| result.instrument.id.canonical_symbol())
                .collect::<Vec<_>>(),
            vec!["BTC/USDT", "BTC/USDC", "BTC/ETH", "BTC/TRY"]
        );
        runtime.shutdown();
    }

    #[test]
    fn live_event_pressure_keeps_latest_quote_reconnecting_status_and_typed_error() {
        let flood = FloodStreamProvider::new(300);
        let subscriptions = flood.subscriptions.clone();
        let providers = ProviderSet {
            equities: Arc::new(FakeProvider::immediate(DataSource::Yahoo, Vec::new())),
            crypto: Arc::new(flood),
        };
        let runtime = RuntimeHandle::start(providers, Arc::new(NoopRepository)).expect("runtime");
        runtime
            .send(RuntimeCommand::SetWatchlist(vec![btc_instrument()]))
            .expect("watchlist");

        wait_for_counter(&subscriptions, 1);
        thread::sleep(Duration::from_millis(50));
        let mut latest_quote = None;
        let mut quote_events = 0;
        let mut statuses = Vec::new();
        let mut errors = Vec::new();
        while let Ok(event) = runtime.events().try_recv() {
            match event {
                RuntimeEvent::Quotes(quotes) => {
                    quote_events += 1;
                    latest_quote = quotes.into_iter().next();
                }
                RuntimeEvent::StreamStatus(status) => statuses.push(status),
                RuntimeEvent::QuoteError { error, .. } => errors.push(error.kind),
                _ => {}
            }
        }

        assert_eq!(quote_events, 1, "one pending quote slot per instrument");
        assert_eq!(latest_quote.expect("latest quote").last, 399.0);
        assert_eq!(statuses.last(), Some(&StreamStatus::Reconnecting));
        assert!(errors.contains(&MarketDataErrorKind::Disconnected));
        runtime.shutdown();
        assert!(runtime.is_shutdown());
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
    fn explicit_snapshot_filters_unrequested_provider_output() {
        let unexpected = distinct_equity_id(9_999);
        let provider = ControlledProvider::new(
            DataSource::Yahoo,
            Arc::new(OperationProbe::default()),
            Duration::ZERO,
        )
        .with_snapshot_extra(test_quote(unexpected.clone(), DataSource::Yahoo));
        let providers = ProviderSet {
            equities: Arc::new(provider),
            crypto: Arc::new(ControlledProvider::new(
                DataSource::Binance,
                Arc::new(OperationProbe::default()),
                Duration::ZERO,
            )),
        };
        let runtime = RuntimeHandle::start(providers, Arc::new(NoopRepository)).expect("runtime");
        let requested = aapl_instrument();

        runtime
            .send(RuntimeCommand::Snapshot {
                instruments: vec![requested.clone()],
            })
            .expect("snapshot");
        let first = runtime.events().recv_blocking().expect("requested quote");
        thread::sleep(Duration::from_millis(10));
        let mut ids = match first {
            RuntimeEvent::Quotes(quotes) => quotes
                .into_iter()
                .map(|quote| quote.instrument_id)
                .collect::<Vec<_>>(),
            event => panic!("unexpected event: {event:?}"),
        };
        while let Ok(RuntimeEvent::Quotes(quotes)) = runtime.events().try_recv() {
            ids.extend(quotes.into_iter().map(|quote| quote.instrument_id));
        }

        assert_eq!(ids, vec![requested.id]);
        assert!(!ids.contains(&unexpected));
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
    fn newer_query_cancels_both_in_flight_provider_retries_before_they_can_emit() {
        let probe = Arc::new(OperationProbe::default());
        let providers = ProviderSet {
            equities: Arc::new(
                ControlledProvider::new(DataSource::Yahoo, probe.clone(), Duration::ZERO)
                    .with_slow_search(Duration::from_millis(200)),
            ),
            crypto: Arc::new(
                ControlledProvider::new(DataSource::Binance, probe.clone(), Duration::ZERO)
                    .with_slow_search(Duration::from_millis(200)),
            ),
        };
        let runtime = RuntimeHandle::start(providers, Arc::new(NoopRepository)).expect("runtime");
        let mut generations = kabu_core::Generation::default();
        let retry_generation = generations.next();
        let current = generations.next();
        runtime
            .send(RuntimeCommand::RetrySearchProvider {
                generation: retry_generation,
                query: "old".into(),
                source: DataSource::Yahoo,
            })
            .expect("old Yahoo provider retry");
        runtime
            .send(RuntimeCommand::RetrySearchProvider {
                generation: retry_generation,
                query: "old".into(),
                source: DataSource::Binance,
            })
            .expect("old Binance provider retry");
        wait_for_counter(&probe.started, 2);

        runtime
            .send(RuntimeCommand::Search {
                generation: current,
                query: "new".into(),
            })
            .expect("new query");

        let RuntimeEvent::SearchCompleted { generation, .. } =
            runtime.events().recv_blocking().expect("new query result")
        else {
            panic!("expected current full search completion");
        };
        assert_eq!(generation, current);
        assert_eq!(probe.cancelled.load(Ordering::SeqCst), 2);
        assert!(runtime.events().try_recv().is_err());
        runtime.shutdown();
    }

    #[test]
    fn provider_retries_have_independent_owned_jobs_and_same_source_replacement() {
        let yahoo_probe = Arc::new(OperationProbe::default());
        let binance_probe = Arc::new(OperationProbe::default());
        let providers = ProviderSet {
            equities: Arc::new(
                ControlledProvider::new(DataSource::Yahoo, yahoo_probe.clone(), Duration::ZERO)
                    .with_slow_search(Duration::from_secs(30)),
            ),
            crypto: Arc::new(
                ControlledProvider::new(DataSource::Binance, binance_probe.clone(), Duration::ZERO)
                    .with_slow_search(Duration::from_secs(30)),
            ),
        };
        let runtime = RuntimeHandle::start(providers, Arc::new(NoopRepository)).expect("runtime");
        let generation = kabu_core::Generation::default().next();

        runtime
            .send(RuntimeCommand::RetrySearchProvider {
                generation,
                query: "old".into(),
                source: DataSource::Yahoo,
            })
            .expect("Yahoo retry");
        wait_for_counter(&yahoo_probe.started, 1);
        runtime
            .send(RuntimeCommand::RetrySearchProvider {
                generation,
                query: "old".into(),
                source: DataSource::Binance,
            })
            .expect("Binance retry");
        wait_for_counter(&binance_probe.started, 1);

        assert_eq!(yahoo_probe.active.load(Ordering::SeqCst), 1);
        assert_eq!(binance_probe.active.load(Ordering::SeqCst), 1);

        runtime
            .send(RuntimeCommand::RetrySearchProvider {
                generation,
                query: "old".into(),
                source: DataSource::Yahoo,
            })
            .expect("replacement Yahoo retry");
        wait_for_counter(&yahoo_probe.started, 2);
        assert_eq!(yahoo_probe.cancelled.load(Ordering::SeqCst), 1);
        assert_eq!(yahoo_probe.active.load(Ordering::SeqCst), 1);
        assert_eq!(binance_probe.cancelled.load(Ordering::SeqCst), 0);
        assert_eq!(binance_probe.active.load(Ordering::SeqCst), 1);

        runtime
            .send(RuntimeCommand::CancelSearch)
            .expect("reliable cancellation");
        assert!(runtime.wait_for_control_state(Duration::from_secs(1)));
        assert_eq!(yahoo_probe.active.load(Ordering::SeqCst), 0);
        assert_eq!(binance_probe.active.load(Ordering::SeqCst), 0);
        assert!(runtime.events().try_recv().is_err());
        runtime.shutdown();
    }

    #[test]
    fn model_search_close_awaits_actual_slow_provider_cancellation() {
        let yahoo_probe = Arc::new(OperationProbe::default());
        let binance_probe = Arc::new(OperationProbe::default());
        let providers = ProviderSet {
            equities: Arc::new(
                ControlledProvider::new(DataSource::Yahoo, yahoo_probe.clone(), Duration::ZERO)
                    .with_slow_search(Duration::from_secs(30)),
            ),
            crypto: Arc::new(
                ControlledProvider::new(DataSource::Binance, binance_probe.clone(), Duration::ZERO)
                    .with_slow_search(Duration::from_secs(30)),
            ),
        };
        let runtime = RuntimeHandle::start(providers, Arc::new(NoopRepository)).expect("runtime");
        let mut model = AppModel::new(
            PersistedState::starter(),
            CachedMarketState::default(),
            runtime.clone(),
        );
        model.set_search_query("old");
        model.advance_search_clock(Duration::from_millis(250));
        wait_for_counter(&yahoo_probe.started, 1);
        wait_for_counter(&binance_probe.started, 1);

        model.cancel_pending_search();

        assert!(runtime.wait_for_control_state(Duration::from_secs(1)));
        assert_eq!(yahoo_probe.active.load(Ordering::SeqCst), 0);
        assert_eq!(binance_probe.active.load(Ordering::SeqCst), 0);
        assert_eq!(yahoo_probe.cancelled.load(Ordering::SeqCst), 1);
        assert_eq!(binance_probe.cancelled.load(Ordering::SeqCst), 1);
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
        let retry_generation = kabu_core::Generation::default().next();
        runtime
            .send(RuntimeCommand::RetrySearchProvider {
                generation: retry_generation,
                query: "old".into(),
                source: DataSource::Yahoo,
            })
            .expect("Yahoo retry");
        runtime
            .send(RuntimeCommand::RetrySearchProvider {
                generation: retry_generation,
                query: "old".into(),
                source: DataSource::Binance,
            })
            .expect("Binance retry");
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
        wait_for_counter(&probe.started, 6);

        runtime.shutdown();

        assert_eq!(probe.active.load(Ordering::SeqCst), 0);
        assert_eq!(probe.cancelled.load(Ordering::SeqCst), 6);
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

    struct SequencedSearchProvider {
        source: DataSource,
        outcomes: Mutex<std::collections::VecDeque<Result<Vec<SearchResult>, MarketDataError>>>,
        calls: Arc<AtomicUsize>,
    }

    struct FloodStreamProvider {
        count: usize,
        subscriptions: Arc<AtomicUsize>,
    }

    impl FloodStreamProvider {
        fn new(count: usize) -> Self {
            Self {
                count,
                subscriptions: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    #[async_trait]
    impl InstrumentSearch for FloodStreamProvider {
        async fn search(&self, _: &str) -> Result<Vec<SearchResult>, MarketDataError> {
            Ok(Vec::new())
        }
    }

    #[async_trait]
    impl QuoteSnapshots for FloodStreamProvider {
        async fn snapshots(&self, _: &[Instrument]) -> Result<Vec<Quote>, MarketDataError> {
            Ok(Vec::new())
        }
    }

    #[async_trait]
    impl ChartHistory for FloodStreamProvider {
        async fn history(
            &self,
            _: &Instrument,
            _: ChartPeriod,
        ) -> Result<Vec<Candle>, MarketDataError> {
            Ok(Vec::new())
        }
    }

    #[async_trait]
    impl QuoteSubscription for FloodStreamProvider {
        async fn subscribe(&self, _: &[Instrument]) -> Result<QuoteStream, MarketDataError> {
            let attempt = self.subscriptions.fetch_add(1, Ordering::SeqCst);
            if attempt > 0 {
                return Ok(QuoteStream::new(futures_util::stream::pending()));
            }
            let base_timestamp =
                chrono::DateTime::from_timestamp(1_700_000_000, 0).expect("timestamp");
            let mut events = (0..self.count)
                .map(|index| {
                    let mut quote = test_quote(btc_instrument().id, DataSource::Binance);
                    quote.last = 100.0 + index as f64;
                    quote.provider_timestamp =
                        base_timestamp + chrono::Duration::milliseconds(index as i64);
                    Ok(quote)
                })
                .collect::<Vec<_>>();
            events.push(Err(MarketDataError {
                kind: MarketDataErrorKind::Disconnected,
                message: "private stream frame".into(),
                retry_after: None,
            }));
            Ok(QuoteStream::new(futures_util::stream::iter(events)))
        }
    }

    impl SequencedSearchProvider {
        fn new(
            source: DataSource,
            outcomes: Vec<Result<Vec<SearchResult>, MarketDataError>>,
        ) -> Self {
            Self {
                source,
                outcomes: Mutex::new(outcomes.into()),
                calls: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    #[async_trait]
    impl InstrumentSearch for SequencedSearchProvider {
        async fn search(&self, _: &str) -> Result<Vec<SearchResult>, MarketDataError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.outcomes
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .pop_front()
                .expect("unexpected extra search call")
        }
    }

    #[async_trait]
    impl QuoteSnapshots for SequencedSearchProvider {
        async fn snapshots(&self, _: &[Instrument]) -> Result<Vec<Quote>, MarketDataError> {
            Ok(Vec::new())
        }
    }

    #[async_trait]
    impl ChartHistory for SequencedSearchProvider {
        async fn history(
            &self,
            _: &Instrument,
            _: ChartPeriod,
        ) -> Result<Vec<Candle>, MarketDataError> {
            Ok(Vec::new())
        }
    }

    #[async_trait]
    impl QuoteSubscription for SequencedSearchProvider {
        async fn subscribe(&self, _: &[Instrument]) -> Result<QuoteStream, MarketDataError> {
            Err(MarketDataError {
                kind: MarketDataErrorKind::Unavailable,
                message: format!("{:?} stream unavailable in test", self.source),
                retry_after: None,
            })
        }
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
        snapshot_extras: Vec<Quote>,
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
                snapshot_extras: Vec::new(),
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

        fn with_snapshot_extra(mut self, quote: Quote) -> Self {
            self.snapshot_extras.push(quote);
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
            let mut quotes: Vec<_> = instruments
                .iter()
                .map(|instrument| test_quote(instrument.id.clone(), self.source))
                .collect();
            quotes.extend(self.snapshot_extras.clone());
            Ok(quotes)
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

    fn distinct_equity_id(index: usize) -> InstrumentId {
        InstrumentId::equity(&format!("X{index}"), Some("NASDAQ")).expect("distinct identity")
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

    struct FlakyRepository {
        fail_state: std::sync::atomic::AtomicBool,
        fail_cache: std::sync::atomic::AtomicBool,
        state_saves: AtomicUsize,
        cache_saves: AtomicUsize,
    }

    impl FlakyRepository {
        fn failing() -> Self {
            Self {
                fail_state: std::sync::atomic::AtomicBool::new(true),
                fail_cache: std::sync::atomic::AtomicBool::new(true),
                state_saves: AtomicUsize::new(0),
                cache_saves: AtomicUsize::new(0),
            }
        }

        fn recover_state(&self) {
            self.fail_state.store(false, Ordering::SeqCst);
        }

        fn recover_cache(&self) {
            self.fail_cache.store(false, Ordering::SeqCst);
        }
    }

    impl StateRepository for FlakyRepository {
        fn load_state(&self) -> Result<PersistedState, PersistenceError> {
            Ok(PersistedState::starter())
        }

        fn save_state(&self, _: &PersistedState) -> Result<(), PersistenceError> {
            self.state_saves.fetch_add(1, Ordering::SeqCst);
            if self.fail_state.load(Ordering::SeqCst) {
                Err(PersistenceError::Io(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "/private/state.json must never reach the UI",
                )))
            } else {
                Ok(())
            }
        }

        fn load_cache(&self) -> Result<CachedMarketState, PersistenceError> {
            Ok(CachedMarketState::default())
        }

        fn save_cache(&self, _: &CachedMarketState) -> Result<(), PersistenceError> {
            self.cache_saves.fetch_add(1, Ordering::SeqCst);
            if self.fail_cache.load(Ordering::SeqCst) {
                Err(PersistenceError::Io(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "/private/cache.json must never reach the UI",
                )))
            } else {
                Ok(())
            }
        }
    }

    fn assert_persistence_event(
        events: &super::RuntimeEvents,
        expected_category: PersistenceCategory,
        expected_result: PersistenceResult,
    ) {
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            match events.try_recv() {
                Ok(RuntimeEvent::PersistenceCompleted { category, result }) => {
                    assert_eq!(category, expected_category);
                    assert_eq!(result, expected_result);
                    return;
                }
                Ok(_) | Err(async_channel::TryRecvError::Empty) => {}
                Err(async_channel::TryRecvError::Closed) => {
                    panic!("persistence event channel closed before acknowledgement")
                }
            }
            assert!(Instant::now() < deadline, "persistence event deadline");
            thread::sleep(Duration::from_millis(1));
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

    fn crypto_result(base: &str, quote: &str) -> SearchResult {
        SearchResult {
            instrument: Instrument::crypto(base, quote, "Crypto").expect("crypto"),
            source: DataSource::Binance,
            venue_label: "Binance Spot".into(),
        }
    }
}
