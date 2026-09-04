use crate::coordinator::Coordinator;
use async_channel::{Receiver, Sender, TrySendError};
use kabu_core::{
    AssetClass, Brokerage, BrokerageConnectionState, BrokerageError, BrokerageErrorKind,
    CachedMarketState, Candle, ChartHistory, ChartPeriod, ChartRequestKey, DataSource,
    GenerationId, Instrument, InstrumentId, InstrumentSearch, MarketDataError, MarketDataErrorKind,
    PersistedState, PersistenceError, PortfolioSnapshot, Quote, QuoteSnapshots, QuoteSubscription,
    SearchResult, StateRepository, rank_search_results,
};
use kabu_data::{BinanceClient, BrokerageClient, JsonRepository, Recovery, YahooClient};
use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    fmt, io,
    path::Path,
    process::{Command, Stdio},
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread::{self, JoinHandle},
};
use tokio_util::sync::CancellationToken;

const COMMAND_CAPACITY: usize = 64;
const MAX_CONCURRENT_OTHER_JOBS: usize = 31;
const PORTFOLIO_REFRESH_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

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
    ConnectBroker(Brokerage),
    CancelBrokerConnection(Brokerage),
    RefreshPortfolio(Brokerage),
    DisconnectBroker(Brokerage),
    SetPortfolioVisible(bool),
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
    brokerage_shutdown_failed: bool,
}

impl ShutdownOutcome {
    const fn success_without_flush() -> Self {
        Self {
            state: None,
            market_cache: None,
            runtime_failed: false,
            brokerage_shutdown_failed: false,
        }
    }

    const fn runtime_failed() -> Self {
        Self {
            state: None,
            market_cache: None,
            runtime_failed: true,
            brokerage_shutdown_failed: false,
        }
    }

    pub const fn is_success(self) -> bool {
        !self.runtime_failed
            && !self.brokerage_shutdown_failed
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
    BrokerConnectionChanged {
        brokerage: Brokerage,
        state: BrokerageConnectionState,
    },
    PortfolioUpdated(PortfolioSnapshot),
}

/// A coalescing runtime event mailbox backed by a bounded-one notification
/// channel. Repeated data-plane updates replace the pending value in their
/// semantic slot (quote per instrument, error per instrument/error class,
/// status, persistence category, or request key). Dynamic data families have
/// fixed cardinality caps. Quote/error overflow evicts its oldest same-family
/// value; history retains the highest generations and newest equal-generation
/// terminals. Fixed search, catalog, persistence, and status slots are never
/// evicted by dynamic-family floods.
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
    BrokerConnection(Brokerage),
    Portfolio(Brokerage),
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
            if !state.slots.contains_key(&slot) && !admit_dynamic_slot(&mut state, &slot, &event) {
                continue;
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

    fn clear_brokerage(&self, brokerage: Brokerage) {
        self.remove_slots(|slot| {
            matches!(
                slot,
                RuntimeEventSlot::BrokerConnection(candidate)
                    | RuntimeEventSlot::Portfolio(candidate)
                    if *candidate == brokerage
            )
        });
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
                | RuntimeEventSlot::StreamStatus
                | RuntimeEventSlot::BrokerConnection(_)
                | RuntimeEventSlot::Portfolio(_) => None,
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
        | RuntimeEvent::StreamStatus(_)
        | RuntimeEvent::BrokerConnectionChanged { .. }
        | RuntimeEvent::PortfolioUpdated(_) => None,
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
        | RuntimeEventSlot::StreamStatus
        | RuntimeEventSlot::BrokerConnection(_)
        | RuntimeEventSlot::Portfolio(_) => None,
    }
}

fn admit_dynamic_slot(
    state: &mut RuntimeEventState,
    incoming: &RuntimeEventSlot,
    incoming_event: &RuntimeEvent,
) -> bool {
    let Some((family, capacity)) = dynamic_event_family(incoming) else {
        return true;
    };
    let family_slots = state.slots.iter().filter_map(|(slot, sequence)| {
        (dynamic_event_family(slot).map(|(candidate, _)| candidate) == Some(family))
            .then_some((slot.clone(), *sequence))
    });
    let mut family_len = 0;
    let mut lowest = None;
    for candidate in family_slots {
        family_len += 1;
        let generation = (family == DynamicEventFamily::History).then(|| {
            runtime_event_generation(
                state
                    .pending
                    .get(&candidate.1)
                    .expect("runtime event slot points to pending state"),
            )
            .expect("history event has a generation")
        });
        if lowest
            .as_ref()
            .is_none_or(|(_, sequence, retained_generation)| {
                (generation, candidate.1) < (*retained_generation, *sequence)
            })
        {
            lowest = Some((candidate.0, candidate.1, generation));
        }
    }
    if family_len < capacity {
        return true;
    }
    let Some((lowest_slot, lowest_sequence, lowest_generation)) = lowest else {
        return true;
    };
    if family == DynamicEventFamily::History
        && runtime_event_generation(incoming_event).expect("history event has a generation")
            < lowest_generation.expect("retained history event has a generation")
    {
        return false;
    }
    state.slots.remove(&lowest_slot);
    state.pending.remove(&lowest_sequence);
    true
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
        RuntimeEvent::BrokerConnectionChanged { brokerage, .. } => {
            RuntimeEventSlot::BrokerConnection(*brokerage)
        }
        RuntimeEvent::PortfolioUpdated(snapshot) => RuntimeEventSlot::Portfolio(snapshot.brokerage),
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

pub type BrokerageProviders = HashMap<Brokerage, Arc<dyn BrokerageClient>>;

pub trait AuthorizationUrlOpener: Send + Sync {
    fn open(&self, url: &url::Url) -> Result<(), BrokerageError>;
}

struct ProductionAuthorizationUrlOpener;

impl AuthorizationUrlOpener for ProductionAuthorizationUrlOpener {
    fn open(&self, url: &url::Url) -> Result<(), BrokerageError> {
        let mut command = Command::new("/usr/bin/open");
        command.arg(url.as_str());
        run_authorization_browser_command(command)
    }
}

fn run_authorization_browser_command(mut command: Command) -> Result<(), BrokerageError> {
    let status = command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map_err(|_| BrokerageError::new(BrokerageErrorKind::Internal))?;
    status
        .success()
        .then_some(())
        .ok_or_else(|| BrokerageError::new(BrokerageErrorKind::Internal))
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
            BrokerageProviders::new(),
            Arc::new(ProductionAuthorizationUrlOpener),
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
    brokerage_commands: Arc<BrokerageCommandLane>,
    brokerage_providers: BrokerageProviders,
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

struct BrokerageCommandLane {
    pending: Mutex<VecDeque<RuntimeCommand>>,
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
    brokerage_providers: BrokerageProviders,
    authorization_url_opener: Arc<dyn AuthorizationUrlOpener>,
    brokerage_commands: Arc<BrokerageCommandLane>,
    brokerage_notifications: Receiver<()>,
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

struct BrokerageRuntime {
    providers: BrokerageProviders,
    authorization_url_opener: Arc<dyn AuthorizationUrlOpener>,
    authorization_jobs: HashMap<Brokerage, AuthorizationJob>,
    refresh_jobs: HashMap<Brokerage, tokio::task::JoinHandle<()>>,
    refresh_locks: HashMap<Brokerage, Arc<tokio::sync::Mutex<()>>>,
    states: Arc<Mutex<HashMap<Brokerage, BrokerageConnectionState>>>,
    snapshots: Arc<Mutex<HashMap<Brokerage, chrono::DateTime<chrono::Utc>>>>,
    live_sessions: Arc<Mutex<HashSet<Brokerage>>>,
    refresh_deadlines: Arc<Mutex<HashMap<Brokerage, tokio::time::Instant>>>,
    refresh_schedule_changed: Arc<tokio::sync::Notify>,
    visible: bool,
}

struct AuthorizationJob {
    cancellation: CancellationToken,
    task: tokio::task::JoinHandle<()>,
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
        Self::start_inner(
            providers,
            BrokerageProviders::new(),
            Arc::new(ProductionAuthorizationUrlOpener),
            repository,
            CachedMarketState::default(),
            None,
        )
    }

    fn start_inner(
        providers: ProviderSet,
        brokerage_providers: BrokerageProviders,
        authorization_url_opener: Arc<dyn AuthorizationUrlOpener>,
        repository: Arc<dyn StateRepository>,
        initial_cache: CachedMarketState,
        catalog_client: Option<BinanceClient>,
    ) -> io::Result<Self> {
        let (commands, command_receiver) = async_channel::bounded(COMMAND_CAPACITY);
        let (event_sender, events) = runtime_event_channel();
        let (shutdown_signal, shutdown_receiver) = async_channel::bounded(1);
        let (control_notify, control_notifications) = async_channel::bounded(1);
        let (brokerage_notify, brokerage_notifications) = async_channel::bounded(1);
        let brokerage_commands = Arc::new(BrokerageCommandLane {
            pending: Mutex::new(VecDeque::new()),
            notify: brokerage_notify,
        });
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
        let network_brokerage_commands = brokerage_commands.clone();
        let network_brokerage_providers = brokerage_providers.clone();
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
                    brokerage_providers: network_brokerage_providers,
                    authorization_url_opener,
                    brokerage_commands: network_brokerage_commands,
                    brokerage_notifications,
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
                brokerage_commands,
                brokerage_providers,
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
                if is_brokerage_command(&command) {
                    coalesce_recording_brokerage(&mut commands, &command);
                    commands.push_back(command);
                    return Ok(());
                }
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
                if is_brokerage_command(&command) {
                    runtime.brokerage_commands.push(command);
                    return Ok(());
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
        let mut previous = runtime
            .completion
            .outcome
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .unwrap_or_else(ShutdownOutcome::runtime_failed);
        if previous.runtime_failed {
            return previous;
        }
        if previous.brokerage_shutdown_failed {
            previous.brokerage_shutdown_failed =
                !close_broker_sessions(&runtime.brokerage_providers);
        }
        let Some(flush) = runtime
            .final_flush
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
        else {
            *runtime
                .completion
                .outcome
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(previous);
            runtime.completion.changed.notify_all();
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
            | RuntimeCommand::ConnectBroker(_)
            | RuntimeCommand::CancelBrokerConnection(_)
            | RuntimeCommand::RefreshPortfolio(_)
            | RuntimeCommand::DisconnectBroker(_)
            | RuntimeCommand::SetPortfolioVisible(_)
    )
}

fn is_brokerage_command(command: &RuntimeCommand) -> bool {
    matches!(
        command,
        RuntimeCommand::ConnectBroker(_)
            | RuntimeCommand::CancelBrokerConnection(_)
            | RuntimeCommand::RefreshPortfolio(_)
            | RuntimeCommand::DisconnectBroker(_)
            | RuntimeCommand::SetPortfolioVisible(_)
    )
}

fn coalesce_recording_brokerage(
    commands: &mut VecDeque<RuntimeCommand>,
    incoming: &RuntimeCommand,
) {
    commands.retain(|existing| match incoming {
        RuntimeCommand::RefreshPortfolio(brokerage) => !matches!(
            existing,
            RuntimeCommand::RefreshPortfolio(existing_brokerage)
                if existing_brokerage == brokerage
        ),
        _ => true,
    });
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
        RuntimeCommand::ConnectBroker(_)
        | RuntimeCommand::CancelBrokerConnection(_)
        | RuntimeCommand::RefreshPortfolio(_)
        | RuntimeCommand::DisconnectBroker(_)
        | RuntimeCommand::SetPortfolioVisible(_) => true,
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
        self.brokerage_commands.notify.close();
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

impl BrokerageCommandLane {
    fn push(&self, command: RuntimeCommand) {
        let mut pending = self
            .pending
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let RuntimeCommand::RefreshPortfolio(brokerage) = &command {
            pending.retain(|candidate| {
                !matches!(
                    candidate,
                    RuntimeCommand::RefreshPortfolio(candidate_brokerage)
                        if candidate_brokerage == brokerage
                )
            });
        }
        pending.push_back(command);
        drop(pending);
        _ = self.notify.try_send(());
    }

    fn take(&self) -> VecDeque<RuntimeCommand> {
        std::mem::take(
            &mut *self
                .pending
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        )
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

impl BrokerageRuntime {
    fn new(
        providers: BrokerageProviders,
        authorization_url_opener: Arc<dyn AuthorizationUrlOpener>,
    ) -> Self {
        let states = providers
            .keys()
            .copied()
            .map(|brokerage| (brokerage, BrokerageConnectionState::Disconnected))
            .collect();
        let refresh_locks = providers
            .keys()
            .copied()
            .map(|brokerage| (brokerage, Arc::new(tokio::sync::Mutex::new(()))))
            .collect();
        Self {
            providers,
            authorization_url_opener,
            authorization_jobs: HashMap::new(),
            refresh_jobs: HashMap::new(),
            refresh_locks,
            states: Arc::new(Mutex::new(states)),
            snapshots: Arc::new(Mutex::new(HashMap::new())),
            live_sessions: Arc::new(Mutex::new(HashSet::new())),
            refresh_deadlines: Arc::new(Mutex::new(HashMap::new())),
            refresh_schedule_changed: Arc::new(tokio::sync::Notify::new()),
            visible: true,
        }
    }

    async fn apply(&mut self, command: RuntimeCommand, events: &RuntimeEventSender) {
        match command {
            RuntimeCommand::ConnectBroker(brokerage) => self.connect(brokerage, events).await,
            RuntimeCommand::CancelBrokerConnection(brokerage) => {
                self.cancel_connection(brokerage, events).await;
            }
            RuntimeCommand::RefreshPortfolio(brokerage) => {
                if self.visible {
                    self.refresh(brokerage, events).await;
                }
            }
            RuntimeCommand::DisconnectBroker(brokerage) => {
                self.disconnect(brokerage, events).await;
            }
            RuntimeCommand::SetPortfolioVisible(visible) => {
                self.set_visible(visible, events).await;
            }
            _ => {}
        }
    }

    async fn connect(&mut self, brokerage: Brokerage, events: &RuntimeEventSender) {
        cancel_authorization_job(&mut self.authorization_jobs, brokerage).await;
        abort_brokerage_job(&mut self.refresh_jobs, brokerage).await;
        let Some(provider) = self.providers.get(&brokerage).cloned() else {
            publish_brokerage_error(
                &self.states,
                events,
                brokerage,
                &BrokerageError::new(BrokerageErrorKind::Unavailable),
            );
            return;
        };
        let refresh_lock = self
            .refresh_locks
            .get(&brokerage)
            .cloned()
            .expect("brokerage provider has a refresh lock");
        publish_brokerage_state(
            &self.states,
            events,
            brokerage,
            BrokerageConnectionState::Connecting,
        );
        let opener = self.authorization_url_opener.clone();
        let states = self.states.clone();
        let snapshots = self.snapshots.clone();
        let live_sessions = self.live_sessions.clone();
        let refresh_deadlines = self.refresh_deadlines.clone();
        let refresh_schedule_changed = self.refresh_schedule_changed.clone();
        let events = events.clone();
        let cancellation = CancellationToken::new();
        let task_cancellation = cancellation.clone();
        self.authorization_jobs.insert(
            brokerage,
            AuthorizationJob {
                cancellation,
                task: tokio::spawn(async move {
                    let open_authorization_url = |url: &url::Url| {
                        publish_brokerage_state(
                            &states,
                            &events,
                            brokerage,
                            BrokerageConnectionState::Authorizing,
                        );
                        opener.open(url)
                    };
                    let result = async {
                        provider
                            .connect(&open_authorization_url, task_cancellation.clone())
                            .await?;
                        live_sessions
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner())
                            .insert(brokerage);
                        tokio::select! {
                            refresh = async {
                                let _refresh_guard = refresh_lock.lock().await;
                                provider.refresh_portfolio().await
                            } => refresh,
                            _ = task_cancellation.cancelled() => {
                                Err(BrokerageError::new(BrokerageErrorKind::AuthorizationDenied))
                            }
                        }
                    }
                    .await;
                    match result {
                        Ok(snapshot) if snapshot.brokerage == brokerage => {
                            snapshots
                                .lock()
                                .unwrap_or_else(|poisoned| poisoned.into_inner())
                                .insert(brokerage, snapshot.received_at);
                            refresh_deadlines
                                .lock()
                                .unwrap_or_else(|poisoned| poisoned.into_inner())
                                .insert(
                                    brokerage,
                                    tokio::time::Instant::now() + PORTFOLIO_REFRESH_INTERVAL,
                                );
                            refresh_schedule_changed.notify_one();
                            let last_synced_at = snapshot.received_at;
                            _ = events.try_send(RuntimeEvent::PortfolioUpdated(snapshot));
                            publish_brokerage_state(
                                &states,
                                &events,
                                brokerage,
                                BrokerageConnectionState::Connected {
                                    last_synced_at: Some(last_synced_at),
                                },
                            );
                        }
                        Ok(_) => {
                            refresh_deadlines
                                .lock()
                                .unwrap_or_else(|poisoned| poisoned.into_inner())
                                .insert(
                                    brokerage,
                                    tokio::time::Instant::now() + PORTFOLIO_REFRESH_INTERVAL,
                                );
                            refresh_schedule_changed.notify_one();
                            publish_brokerage_error(
                                &states,
                                &events,
                                brokerage,
                                &BrokerageError::new(BrokerageErrorKind::MalformedResponse),
                            );
                        }
                        Err(error) => {
                            if error.kind == BrokerageErrorKind::Disconnected {
                                live_sessions
                                    .lock()
                                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                                    .remove(&brokerage);
                            } else {
                                refresh_deadlines
                                    .lock()
                                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                                    .insert(
                                        brokerage,
                                        tokio::time::Instant::now() + PORTFOLIO_REFRESH_INTERVAL,
                                    );
                                refresh_schedule_changed.notify_one();
                            }
                            publish_brokerage_error(&states, &events, brokerage, &error);
                        }
                    }
                }),
            },
        );
    }

    async fn refresh(&mut self, brokerage: Brokerage, events: &RuntimeEventSender) {
        abort_brokerage_job(&mut self.refresh_jobs, brokerage).await;
        self.refresh_deadlines
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&brokerage);
        let Some(provider) = self.providers.get(&brokerage).cloned() else {
            publish_brokerage_error(
                &self.states,
                events,
                brokerage,
                &BrokerageError::new(BrokerageErrorKind::Unavailable),
            );
            return;
        };
        let refresh_lock = self
            .refresh_locks
            .get(&brokerage)
            .cloned()
            .expect("brokerage provider has a refresh lock");
        let last_synced_at = brokerage_last_synced(&self.states, brokerage);
        publish_brokerage_state(
            &self.states,
            events,
            brokerage,
            BrokerageConnectionState::Refreshing { last_synced_at },
        );
        let states = self.states.clone();
        let snapshots = self.snapshots.clone();
        let live_sessions = self.live_sessions.clone();
        let refresh_deadlines = self.refresh_deadlines.clone();
        let refresh_schedule_changed = self.refresh_schedule_changed.clone();
        let events = events.clone();
        self.refresh_jobs.insert(
            brokerage,
            tokio::spawn(async move {
                let _refresh_guard = refresh_lock.lock().await;
                match provider.refresh_portfolio().await {
                    Ok(snapshot) if snapshot.brokerage == brokerage => {
                        snapshots
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner())
                            .insert(brokerage, snapshot.received_at);
                        refresh_deadlines
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner())
                            .insert(
                                brokerage,
                                tokio::time::Instant::now() + PORTFOLIO_REFRESH_INTERVAL,
                            );
                        refresh_schedule_changed.notify_one();
                        let last_synced_at = snapshot.received_at;
                        _ = events.try_send(RuntimeEvent::PortfolioUpdated(snapshot));
                        publish_brokerage_state(
                            &states,
                            &events,
                            brokerage,
                            BrokerageConnectionState::Connected {
                                last_synced_at: Some(last_synced_at),
                            },
                        );
                    }
                    Ok(_) => {
                        refresh_deadlines
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner())
                            .insert(
                                brokerage,
                                tokio::time::Instant::now() + PORTFOLIO_REFRESH_INTERVAL,
                            );
                        refresh_schedule_changed.notify_one();
                        publish_brokerage_error(
                            &states,
                            &events,
                            brokerage,
                            &BrokerageError::new(BrokerageErrorKind::MalformedResponse),
                        );
                    }
                    Err(error) => {
                        if error.kind == BrokerageErrorKind::Disconnected {
                            live_sessions
                                .lock()
                                .unwrap_or_else(|poisoned| poisoned.into_inner())
                                .remove(&brokerage);
                        } else {
                            refresh_deadlines
                                .lock()
                                .unwrap_or_else(|poisoned| poisoned.into_inner())
                                .insert(
                                    brokerage,
                                    tokio::time::Instant::now() + PORTFOLIO_REFRESH_INTERVAL,
                                );
                            refresh_schedule_changed.notify_one();
                        }
                        publish_brokerage_error(&states, &events, brokerage, &error);
                    }
                }
            }),
        );
    }

    async fn cancel_connection(&mut self, brokerage: Brokerage, events: &RuntimeEventSender) {
        cancel_authorization_job(&mut self.authorization_jobs, brokerage).await;
        abort_brokerage_job(&mut self.refresh_jobs, brokerage).await;
        let result = match self.providers.get(&brokerage) {
            Some(provider) => provider.close_session().await,
            None => Err(BrokerageError::new(BrokerageErrorKind::Unavailable)),
        };
        self.mark_session_closed(brokerage);
        events.clear_brokerage(brokerage);
        match result {
            Ok(()) => publish_brokerage_state(
                &self.states,
                events,
                brokerage,
                BrokerageConnectionState::Disconnected,
            ),
            Err(error) => publish_brokerage_error(&self.states, events, brokerage, &error),
        }
    }

    async fn disconnect(&mut self, brokerage: Brokerage, events: &RuntimeEventSender) {
        cancel_authorization_job(&mut self.authorization_jobs, brokerage).await;
        abort_brokerage_job(&mut self.refresh_jobs, brokerage).await;
        let result = match self.providers.get(&brokerage) {
            Some(provider) => provider.disconnect().await,
            None => Err(BrokerageError::new(BrokerageErrorKind::Unavailable)),
        };
        self.mark_session_closed(brokerage);
        events.clear_brokerage(brokerage);
        self.snapshots
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&brokerage);
        match result {
            Ok(()) => publish_brokerage_state(
                &self.states,
                events,
                brokerage,
                BrokerageConnectionState::Disconnected,
            ),
            Err(error) => publish_brokerage_error(&self.states, events, brokerage, &error),
        }
    }

    async fn set_visible(&mut self, visible: bool, events: &RuntimeEventSender) {
        self.visible = visible;
        if !visible {
            let authorizing = cancel_all_authorization_jobs(&mut self.authorization_jobs).await;
            let refreshing = abort_all_brokerage_jobs(&mut self.refresh_jobs).await;
            for brokerage in &authorizing {
                let result = match self.providers.get(brokerage) {
                    Some(provider) => provider.close_session().await,
                    None => Err(BrokerageError::new(BrokerageErrorKind::Unavailable)),
                };
                self.mark_session_closed(*brokerage);
                match result {
                    Ok(()) => publish_brokerage_state(
                        &self.states,
                        events,
                        *brokerage,
                        BrokerageConnectionState::Disconnected,
                    ),
                    Err(error) => {
                        publish_brokerage_error(&self.states, events, *brokerage, &error);
                    }
                }
            }
            for brokerage in refreshing {
                if authorizing.contains(&brokerage) {
                    continue;
                }
                publish_brokerage_state(
                    &self.states,
                    events,
                    brokerage,
                    BrokerageConnectionState::Connected {
                        last_synced_at: brokerage_last_synced(&self.states, brokerage),
                    },
                );
                self.reschedule_from_snapshot(brokerage);
            }
            return;
        }

        let stale = self.connected_brokers_needing_refresh();
        for brokerage in stale {
            self.refresh(brokerage, events).await;
        }
    }

    fn connected_brokers_needing_refresh(&self) -> Vec<Brokerage> {
        let live_sessions = self
            .live_sessions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let snapshots = self
            .snapshots
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let now = chrono::Utc::now();
        live_sessions
            .iter()
            .copied()
            .filter(|brokerage| {
                snapshots.get(brokerage).is_none_or(|received_at| {
                    now.signed_duration_since(*received_at) >= chrono::Duration::seconds(60)
                })
            })
            .collect()
    }

    #[cfg(test)]
    fn mark_live_session(&self, brokerage: Brokerage) {
        self.live_sessions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(brokerage);
    }

    fn mark_session_closed(&self, brokerage: Brokerage) {
        self.live_sessions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&brokerage);
        self.refresh_deadlines
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&brokerage);
        self.refresh_schedule_changed.notify_one();
    }

    #[cfg(test)]
    fn record_snapshot_completion(&self, brokerage: Brokerage, completed_at: tokio::time::Instant) {
        self.refresh_deadlines
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(brokerage, completed_at + PORTFOLIO_REFRESH_INTERVAL);
        self.refresh_schedule_changed.notify_one();
    }

    fn record_retry_deadline(&self, brokerage: Brokerage, deadline: tokio::time::Instant) {
        self.refresh_deadlines
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(brokerage, deadline);
        self.refresh_schedule_changed.notify_one();
    }

    fn reschedule_from_snapshot(&self, brokerage: Brokerage) {
        let now = chrono::Utc::now();
        let remaining = self
            .snapshots
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&brokerage)
            .map(|received_at| {
                PORTFOLIO_REFRESH_INTERVAL.saturating_sub(
                    now.signed_duration_since(*received_at)
                        .to_std()
                        .unwrap_or_default(),
                )
            })
            .unwrap_or_default();
        self.record_retry_deadline(brokerage, tokio::time::Instant::now() + remaining);
    }

    fn brokers_due_for_refresh(&self, now: tokio::time::Instant) -> Vec<Brokerage> {
        let live_sessions = self
            .live_sessions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let deadlines = self
            .refresh_deadlines
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        live_sessions
            .iter()
            .filter_map(|brokerage| {
                deadlines
                    .get(brokerage)
                    .is_some_and(|deadline| *deadline <= now)
                    .then_some(*brokerage)
            })
            .collect()
    }

    fn next_refresh_deadline(&self) -> Option<tokio::time::Instant> {
        if !self.visible {
            return None;
        }
        let live_sessions = self
            .live_sessions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let deadlines = self
            .refresh_deadlines
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        live_sessions
            .iter()
            .filter_map(|brokerage| deadlines.get(brokerage).copied())
            .min()
    }

    async fn scheduled_refresh(&mut self, events: &RuntimeEventSender) {
        if !self.visible {
            return;
        }
        let due = self.brokers_due_for_refresh(tokio::time::Instant::now());
        for brokerage in due {
            self.refresh(brokerage, events).await;
        }
    }

    async fn shutdown(&mut self, events: &RuntimeEventSender) -> bool {
        cancel_all_authorization_jobs(&mut self.authorization_jobs).await;
        abort_all_brokerage_jobs(&mut self.refresh_jobs).await;
        let mut succeeded = true;
        for (brokerage, provider) in &self.providers {
            if let Err(error) = provider.close_session().await {
                succeeded = false;
                publish_brokerage_error(&self.states, events, *brokerage, &error);
            }
        }
        succeeded
    }
}

async fn cancel_authorization_job(
    jobs: &mut HashMap<Brokerage, AuthorizationJob>,
    brokerage: Brokerage,
) -> bool {
    let Some(job) = jobs.remove(&brokerage) else {
        return false;
    };
    let was_running = !job.task.is_finished();
    job.cancellation.cancel();
    _ = job.task.await;
    was_running
}

async fn cancel_all_authorization_jobs(
    jobs: &mut HashMap<Brokerage, AuthorizationJob>,
) -> Vec<Brokerage> {
    let jobs = std::mem::take(jobs).into_iter().collect::<Vec<_>>();
    let cancelled = jobs
        .iter()
        .filter_map(|(brokerage, job)| (!job.task.is_finished()).then_some(*brokerage))
        .collect::<Vec<_>>();
    for (_, job) in &jobs {
        job.cancellation.cancel();
    }
    for (_, job) in jobs {
        _ = job.task.await;
    }
    cancelled
}

async fn abort_brokerage_job(
    jobs: &mut HashMap<Brokerage, tokio::task::JoinHandle<()>>,
    brokerage: Brokerage,
) -> bool {
    let Some(job) = jobs.remove(&brokerage) else {
        return false;
    };
    let was_running = !job.is_finished();
    job.abort();
    _ = job.await;
    was_running
}

async fn abort_all_brokerage_jobs(
    jobs: &mut HashMap<Brokerage, tokio::task::JoinHandle<()>>,
) -> Vec<Brokerage> {
    let jobs = std::mem::take(jobs).into_iter().collect::<Vec<_>>();
    let cancelled = jobs
        .iter()
        .filter_map(|(brokerage, job)| (!job.is_finished()).then_some(*brokerage))
        .collect::<Vec<_>>();
    for (_, job) in &jobs {
        job.abort();
    }
    for (_, job) in jobs {
        _ = job.await;
    }
    cancelled
}

fn brokerage_last_synced(
    states: &Arc<Mutex<HashMap<Brokerage, BrokerageConnectionState>>>,
    brokerage: Brokerage,
) -> Option<chrono::DateTime<chrono::Utc>> {
    match states
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(&brokerage)
    {
        Some(BrokerageConnectionState::Connected { last_synced_at })
        | Some(BrokerageConnectionState::Refreshing { last_synced_at }) => *last_synced_at,
        _ => None,
    }
}

fn publish_brokerage_state(
    states: &Arc<Mutex<HashMap<Brokerage, BrokerageConnectionState>>>,
    events: &RuntimeEventSender,
    brokerage: Brokerage,
    state: BrokerageConnectionState,
) {
    states
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert(brokerage, state.clone());
    _ = events.try_send(RuntimeEvent::BrokerConnectionChanged { brokerage, state });
}

fn publish_brokerage_error(
    states: &Arc<Mutex<HashMap<Brokerage, BrokerageConnectionState>>>,
    events: &RuntimeEventSender,
    brokerage: Brokerage,
    error: &BrokerageError,
) {
    publish_brokerage_state(
        states,
        events,
        brokerage,
        BrokerageConnectionState::Error {
            message: error.user_message().to_owned(),
        },
    );
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
    runtime.brokerage_commands.notify.close();
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

fn close_broker_sessions(providers: &BrokerageProviders) -> bool {
    let providers = providers.clone();
    let Ok(join) = thread::Builder::new()
        .name("kabu-broker-shutdown-retry".into())
        .spawn(move || {
            let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            else {
                return false;
            };
            runtime.block_on(async {
                let mut succeeded = true;
                for provider in providers.values() {
                    succeeded &= provider.close_session().await.is_ok();
                }
                succeeded
            })
        })
    else {
        return false;
    };
    join.join().unwrap_or(false)
}

async fn run_dispatcher(inputs: NetworkInputs) -> ShutdownOutcome {
    let NetworkInputs {
        commands,
        controls,
        control_notifications,
        events,
        shutdown,
        providers,
        brokerage_providers,
        authorization_url_opener,
        brokerage_commands,
        brokerage_notifications,
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
    let mut brokerage = BrokerageRuntime::new(brokerage_providers, authorization_url_opener);
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
        let refresh_deadline = brokerage.next_refresh_deadline();
        let refresh_schedule_changed = brokerage.refresh_schedule_changed.clone();

        tokio::select! {
            biased;
            _ = shutdown.recv() => break,
            control = control_notifications.recv() => {
                if control.is_err() {
                    continue;
                }
            }
            brokerage_ready = brokerage_notifications.recv() => {
                if brokerage_ready.is_err() {
                    continue;
                }
                for command in brokerage_commands.take() {
                    brokerage.apply(command, &events).await;
                }
            }
            _ = async {
                match refresh_deadline {
                    Some(deadline) => tokio::time::sleep_until(deadline).await,
                    None => std::future::pending::<()>().await,
                }
            } => {
                brokerage.scheduled_refresh(&events).await;
            }
            _ = refresh_schedule_changed.notified() => {}
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
                    RuntimeCommand::ConnectBroker(_)
                    | RuntimeCommand::CancelBrokerConnection(_)
                    | RuntimeCommand::RefreshPortfolio(_)
                    | RuntimeCommand::DisconnectBroker(_)
                    | RuntimeCommand::SetPortfolioVisible(_) => {}
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
    let brokerage_shutdown_failed = !brokerage.shutdown(&events).await;
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
        let mut outcome = ShutdownOutcome::success_without_flush();
        outcome.brokerage_shutdown_failed = brokerage_shutdown_failed;
        return outcome;
    };
    let repository_for_flush = repository.clone();
    let cache_for_flush = cache_state.clone();
    let mut outcome = tokio::task::spawn_blocking(move || {
        persist_final_flush(
            repository_for_flush.as_ref(),
            &cache_for_flush,
            &final_flush,
        )
    })
    .await
    .unwrap_or_else(|_| ShutdownOutcome::runtime_failed());
    outcome.brokerage_shutdown_failed = brokerage_shutdown_failed;
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
        brokerage_shutdown_failed: false,
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
        brokerage_shutdown_failed: previous.brokerage_shutdown_failed,
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
            | RuntimeCommand::ConnectBroker(_)
            | RuntimeCommand::CancelBrokerConnection(_)
            | RuntimeCommand::RefreshPortfolio(_)
            | RuntimeCommand::DisconnectBroker(_)
            | RuntimeCommand::SetPortfolioVisible(_)
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
        AuthorizationUrlOpener, BrokerageProviders, PersistenceCategory, PersistenceResult,
        ProductionAuthorizationUrlOpener, ProductionComponents, ProviderSet, RuntimeCommand,
        RuntimeEvent, RuntimeHandle, StreamStatus, refresh_catalog_cache,
    };
    use crate::app_model::{AppModel, ChartAvailability};
    use async_trait::async_trait;
    use kabu_core::{
        Brokerage, BrokerageError, BrokerageErrorKind, CachedMarketState, Candle, ChartHistory,
        ChartPeriod, ChartRequestKey, DataSource, Generation, Instrument, InstrumentId,
        InstrumentSearch, MarketDataError, MarketDataErrorKind, MarketState, PersistedState,
        PersistenceError, PortfolioProvider, PortfolioSnapshot, Quote, QuoteSnapshots, QuoteStream,
        QuoteSubscription, SearchResult, StateRepository,
    };
    use kabu_data::BrokerageClient;
    use std::{
        collections::VecDeque,
        process::Command,
        sync::{
            Arc, Condvar, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        thread,
        time::{Duration, Instant},
    };
    use tokio_util::sync::CancellationToken;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    #[test]
    fn failed_authorization_browser_command_is_a_sanitized_typed_error() {
        let error = super::run_authorization_browser_command(Command::new("/usr/bin/false"))
            .expect_err("a failed browser command is not authorization success");

        assert_eq!(error.kind, BrokerageErrorKind::Internal);
        assert_eq!(
            error.user_message(),
            "Unable to load this broker portfolio. Please try again."
        );
    }

    #[test]
    fn newer_portfolio_event_replaces_pending_event_for_same_broker() {
        let (sender, events) = super::runtime_event_channel();
        sender
            .publish(RuntimeEvent::PortfolioUpdated(portfolio_snapshot(
                Brokerage::Robinhood,
                1,
            )))
            .expect("first");
        sender
            .publish(RuntimeEvent::PortfolioUpdated(portfolio_snapshot(
                Brokerage::Robinhood,
                2,
            )))
            .expect("second");

        let RuntimeEvent::PortfolioUpdated(actual) = events.try_recv().expect("latest") else {
            panic!("expected portfolio event");
        };
        assert_eq!(actual, portfolio_snapshot(Brokerage::Robinhood, 2));
        assert!(events.try_recv().is_err());
    }

    #[test]
    fn repeated_refresh_keeps_one_job_per_broker() {
        let provider = Arc::new(BlockingPortfolioProvider::new(Brokerage::Robinhood));
        let runtime = live_runtime_with_broker(provider.clone());

        runtime
            .send(RuntimeCommand::RefreshPortfolio(Brokerage::Robinhood))
            .expect("first");
        wait_for_counter(&provider.refresh.started, 1);
        runtime
            .send(RuntimeCommand::RefreshPortfolio(Brokerage::Robinhood))
            .expect("second");
        wait_for_counter(&provider.refresh.started, 2);

        assert_eq!(provider.refresh.maximum.load(Ordering::SeqCst), 1);
        assert_eq!(provider.refresh.active.load(Ordering::SeqCst), 1);
        assert_eq!(provider.refresh.cancelled.load(Ordering::SeqCst), 1);
        runtime.shutdown();
    }

    #[test]
    fn connection_refresh_and_manual_refresh_never_call_broker_concurrently() {
        let provider = Arc::new(
            TestBrokerageProvider::new(Brokerage::Robinhood)
                .with_refreshes([PortfolioBehavior::Block, PortfolioBehavior::Block]),
        );
        let runtime = live_runtime_with_broker_client(provider.clone());
        runtime
            .send(RuntimeCommand::ConnectBroker(Brokerage::Robinhood))
            .expect("connect");
        wait_for_counter(&provider.refresh.started, 1);

        runtime
            .send(RuntimeCommand::RefreshPortfolio(Brokerage::Robinhood))
            .expect("manual refresh");
        thread::sleep(Duration::from_millis(20));

        assert_eq!(provider.refresh.started.load(Ordering::SeqCst), 1);
        assert_eq!(provider.refresh.maximum.load(Ordering::SeqCst), 1);
        provider.release_one_refresh();
        wait_for_counter(&provider.refresh.started, 2);
        assert_eq!(provider.refresh.maximum.load(Ordering::SeqCst), 1);
        runtime.shutdown();
    }

    #[test]
    fn hiding_overlapping_connection_and_refresh_finishes_disconnected() {
        let provider = Arc::new(
            TestBrokerageProvider::new(Brokerage::Robinhood)
                .with_refreshes([PortfolioBehavior::Block, PortfolioBehavior::Block]),
        );
        let runtime = live_runtime_with_broker_client(provider.clone());
        runtime
            .send(RuntimeCommand::ConnectBroker(Brokerage::Robinhood))
            .expect("connect");
        wait_for_counter(&provider.refresh.started, 1);
        runtime
            .send(RuntimeCommand::RefreshPortfolio(Brokerage::Robinhood))
            .expect("queued manual refresh");

        runtime
            .send(RuntimeCommand::SetPortfolioVisible(false))
            .expect("hide");
        wait_for_counter(&provider.closes, 1);
        wait_for_broker_state(&runtime, Brokerage::Robinhood, |state| {
            matches!(state, kabu_core::BrokerageConnectionState::Disconnected)
        });
        assert_eq!(provider.refresh.active.load(Ordering::SeqCst), 0);
        runtime.shutdown();
    }

    #[test]
    fn newer_connection_state_replaces_pending_state_for_same_broker() {
        let (sender, events) = super::runtime_event_channel();
        sender
            .publish(RuntimeEvent::BrokerConnectionChanged {
                brokerage: Brokerage::Robinhood,
                state: kabu_core::BrokerageConnectionState::Connecting,
            })
            .expect("connecting");
        sender
            .publish(RuntimeEvent::BrokerConnectionChanged {
                brokerage: Brokerage::Robinhood,
                state: kabu_core::BrokerageConnectionState::Connected {
                    last_synced_at: None,
                },
            })
            .expect("connected");

        assert!(matches!(
            events.try_recv().expect("latest state"),
            RuntimeEvent::BrokerConnectionChanged {
                brokerage: Brokerage::Robinhood,
                state: kabu_core::BrokerageConnectionState::Connected { .. },
            }
        ));
        assert!(events.try_recv().is_err());
    }

    #[test]
    fn brokerage_fixed_slots_do_not_consume_quote_dynamic_capacity() {
        let (sender, _events) = super::runtime_event_channel();
        for index in 0..super::QUOTE_EVENT_SLOT_CAPACITY {
            sender
                .publish(RuntimeEvent::Quotes(vec![test_quote(
                    distinct_equity_id(index),
                    DataSource::Yahoo,
                )]))
                .expect("quote");
        }
        for brokerage in [Brokerage::Robinhood, Brokerage::Longbridge] {
            sender
                .publish(RuntimeEvent::BrokerConnectionChanged {
                    brokerage,
                    state: kabu_core::BrokerageConnectionState::Disconnected,
                })
                .expect("broker state");
            sender
                .publish(RuntimeEvent::PortfolioUpdated(portfolio_snapshot(
                    brokerage, 1,
                )))
                .expect("portfolio");
        }

        let state = sender
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert_eq!(
            state
                .slots
                .keys()
                .filter(|slot| matches!(slot, super::RuntimeEventSlot::Quote(_)))
                .count(),
            super::QUOTE_EVENT_SLOT_CAPACITY
        );
        assert_eq!(
            state
                .slots
                .keys()
                .filter(|slot| matches!(slot, super::RuntimeEventSlot::BrokerConnection(_)))
                .count(),
            2
        );
        assert_eq!(
            state
                .slots
                .keys()
                .filter(|slot| matches!(slot, super::RuntimeEventSlot::Portfolio(_)))
                .count(),
            2
        );
    }

    #[test]
    fn portfolio_hide_and_restore_are_both_reliable_controls() {
        let runtime = RuntimeHandle::default();

        runtime
            .send(RuntimeCommand::SetPortfolioVisible(false))
            .expect("hide");
        runtime
            .send(RuntimeCommand::SetPortfolioVisible(true))
            .expect("restore");

        assert_eq!(
            runtime.recorded_commands(),
            vec![
                RuntimeCommand::SetPortfolioVisible(false),
                RuntimeCommand::SetPortfolioVisible(true),
            ]
        );
    }

    #[test]
    fn connecting_uses_the_injected_opener_then_publishes_portfolio_and_connected() {
        let provider = Arc::new(
            TestBrokerageProvider::new(Brokerage::Robinhood)
                .opening_authorization_url()
                .with_refreshes([PortfolioBehavior::Complete(portfolio_snapshot(
                    Brokerage::Robinhood,
                    10,
                ))]),
        );
        let opener = Arc::new(RecordingAuthorizationUrlOpener::new());
        let runtime = live_runtime_with_broker_and_opener(provider.clone(), opener.clone());

        runtime
            .send(RuntimeCommand::ConnectBroker(Brokerage::Robinhood))
            .expect("connect");
        wait_for_counter(&provider.refresh.started, 1);
        wait_for_broker_state(&runtime, Brokerage::Robinhood, |state| {
            matches!(state, kabu_core::BrokerageConnectionState::Connected { .. })
        });

        let urls = opener
            .urls
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert_eq!(urls.len(), 1);
        assert_eq!(urls[0].scheme(), "https");
        assert_eq!(urls[0].host_str(), Some("broker.example"));
        drop(urls);
        assert_eq!(provider.connect.started.load(Ordering::SeqCst), 1);
        runtime.shutdown();
    }

    #[test]
    fn cancelling_connection_awaits_authorization_and_closes_without_disconnect() {
        let provider = Arc::new(
            TestBrokerageProvider::new(Brokerage::Robinhood).with_connect(ConnectBehavior::Block),
        );
        let runtime = live_runtime_with_broker_client(provider.clone());
        runtime
            .send(RuntimeCommand::ConnectBroker(Brokerage::Robinhood))
            .expect("connect");
        wait_for_counter(&provider.connect.started, 1);

        runtime
            .send(RuntimeCommand::CancelBrokerConnection(Brokerage::Robinhood))
            .expect("cancel");
        wait_for_counter(&provider.closes, 1);

        assert_eq!(provider.connect.active.load(Ordering::SeqCst), 0);
        assert_eq!(provider.connect.cancelled.load(Ordering::SeqCst), 1);
        assert_eq!(provider.disconnects.load(Ordering::SeqCst), 0);
        wait_for_broker_state(&runtime, Brokerage::Robinhood, |state| {
            matches!(state, kabu_core::BrokerageConnectionState::Disconnected)
        });
        runtime.shutdown();
    }

    #[test]
    fn cancelling_connection_signals_the_authorization_before_awaiting_it() {
        let provider = Arc::new(
            TestBrokerageProvider::new(Brokerage::Robinhood).with_connect(ConnectBehavior::Block),
        );
        let runtime = live_runtime_with_broker_client(provider.clone());
        runtime
            .send(RuntimeCommand::ConnectBroker(Brokerage::Robinhood))
            .expect("connect");
        wait_for_counter(&provider.connect.started, 1);

        runtime
            .send(RuntimeCommand::CancelBrokerConnection(Brokerage::Robinhood))
            .expect("cancel");
        wait_for_counter(&provider.closes, 1);

        assert_eq!(
            provider.connect.cancellation_signals.load(Ordering::SeqCst),
            1,
            "the provider must observe cancellation rather than relying on task drop"
        );
        runtime.shutdown();
    }

    #[test]
    fn hiding_signals_all_authorizations_before_waiting_and_closes_each_broker() {
        let slow = Arc::new(CoordinatedAuthorizationProvider::new(
            Brokerage::Robinhood,
            false,
        ));
        let completing = Arc::new(CoordinatedAuthorizationProvider::new(
            Brokerage::Longbridge,
            true,
        ));
        let runtime = live_runtime_with_brokerages([
            slow.clone() as Arc<dyn BrokerageClient>,
            completing.clone() as Arc<dyn BrokerageClient>,
        ]);

        runtime
            .send(RuntimeCommand::ConnectBroker(Brokerage::Robinhood))
            .expect("start slow authorization");
        runtime
            .send(RuntimeCommand::ConnectBroker(Brokerage::Longbridge))
            .expect("start completing authorization");
        wait_for_counter(&slow.connect_started, 1);
        wait_for_counter(&completing.connect_started, 1);

        runtime
            .send(RuntimeCommand::SetPortfolioVisible(false))
            .expect("hide");

        let cleanup_slow = slow.clone();
        let cleanup_completing = completing.clone();
        let cleanup = thread::spawn(move || {
            thread::sleep(Duration::from_millis(300));
            cleanup_slow.ensure_connect_released();
            cleanup_completing.ensure_connect_released();
        });
        let deadline = Instant::now() + Duration::from_millis(100);
        while (slow.cancellation_signals.load(Ordering::SeqCst) != 1
            || completing.cancellation_signals.load(Ordering::SeqCst) != 1)
            && Instant::now() < deadline
        {
            thread::sleep(Duration::from_millis(1));
        }
        let all_authorizations_signalled = slow.cancellation_signals.load(Ordering::SeqCst) == 1
            && completing.cancellation_signals.load(Ordering::SeqCst) == 1;

        if all_authorizations_signalled {
            completing.release_connect();
            wait_for_counter(&completing.connect_completed, 1);
            assert_eq!(
                completing.closes.load(Ordering::SeqCst),
                0,
                "hide waits for the slow job before closing the completed broker"
            );
            slow.release_connect();
        }
        cleanup.join().expect("cleanup authorization releases");

        wait_for_counter(&slow.closes, 1);
        wait_for_counter(&completing.closes, 1);
        runtime.shutdown();
        assert!(
            all_authorizations_signalled,
            "hide must signal every authorization before awaiting any one of them"
        );
    }

    #[test]
    fn repeated_connect_keeps_one_authorization_job_per_broker() {
        let provider = Arc::new(
            TestBrokerageProvider::new(Brokerage::Robinhood).with_connect(ConnectBehavior::Block),
        );
        let runtime = live_runtime_with_broker_client(provider.clone());
        runtime
            .send(RuntimeCommand::ConnectBroker(Brokerage::Robinhood))
            .expect("first connect");
        wait_for_counter(&provider.connect.started, 1);

        runtime
            .send(RuntimeCommand::ConnectBroker(Brokerage::Robinhood))
            .expect("replacement connect");
        wait_for_counter(&provider.connect.started, 2);

        assert_eq!(provider.connect.maximum.load(Ordering::SeqCst), 1);
        assert_eq!(provider.connect.active.load(Ordering::SeqCst), 1);
        assert_eq!(provider.connect.cancelled.load(Ordering::SeqCst), 1);
        runtime.shutdown();
    }

    #[test]
    fn restore_refreshes_stale_connected_broker_and_hide_cancels_that_refresh() {
        let provider = Arc::new(
            TestBrokerageProvider::new(Brokerage::Robinhood).with_refreshes([
                PortfolioBehavior::Complete(portfolio_snapshot(Brokerage::Robinhood, 10)),
                PortfolioBehavior::Block,
            ]),
        );
        let runtime = live_runtime_with_broker_client(provider.clone());
        runtime
            .send(RuntimeCommand::ConnectBroker(Brokerage::Robinhood))
            .expect("connect");
        wait_for_counter(&provider.refresh.started, 1);
        wait_for_broker_state(&runtime, Brokerage::Robinhood, |state| {
            matches!(state, kabu_core::BrokerageConnectionState::Connected { .. })
        });

        runtime
            .send(RuntimeCommand::SetPortfolioVisible(false))
            .expect("hide");
        runtime
            .send(RuntimeCommand::SetPortfolioVisible(true))
            .expect("restore");
        wait_for_counter(&provider.refresh.started, 2);
        assert_eq!(provider.refresh.active.load(Ordering::SeqCst), 1);

        runtime
            .send(RuntimeCommand::SetPortfolioVisible(false))
            .expect("hide during refresh");
        wait_for_counter(&provider.refresh.cancelled, 1);
        assert_eq!(provider.refresh.active.load(Ordering::SeqCst), 0);
        assert_eq!(provider.disconnects.load(Ordering::SeqCst), 0);
        runtime.shutdown();
    }

    #[test]
    fn restore_refresh_selection_uses_snapshot_age_per_broker() {
        let robinhood: Arc<dyn BrokerageClient> =
            Arc::new(TestBrokerageProvider::new(Brokerage::Robinhood));
        let longbridge: Arc<dyn BrokerageClient> =
            Arc::new(TestBrokerageProvider::new(Brokerage::Longbridge));
        let mut providers = BrokerageProviders::new();
        providers.insert(Brokerage::Robinhood, robinhood);
        providers.insert(Brokerage::Longbridge, longbridge);
        let brokerage = super::BrokerageRuntime::new(
            providers,
            Arc::new(RecordingAuthorizationUrlOpener::new()),
        );
        let now = chrono::Utc::now();
        {
            let mut states = brokerage
                .states
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            states.insert(
                Brokerage::Robinhood,
                kabu_core::BrokerageConnectionState::Connected {
                    last_synced_at: Some(now - chrono::Duration::seconds(61)),
                },
            );
            states.insert(
                Brokerage::Longbridge,
                kabu_core::BrokerageConnectionState::Connected {
                    last_synced_at: Some(now - chrono::Duration::seconds(30)),
                },
            );
        }
        {
            let mut snapshots = brokerage
                .snapshots
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            snapshots.insert(Brokerage::Robinhood, now - chrono::Duration::seconds(61));
            snapshots.insert(Brokerage::Longbridge, now - chrono::Duration::seconds(30));
        }
        brokerage.mark_live_session(Brokerage::Robinhood);
        brokerage.mark_live_session(Brokerage::Longbridge);

        let stale = brokerage.connected_brokers_needing_refresh();
        assert_eq!(stale.len(), 1);
        assert!(stale.contains(&Brokerage::Robinhood));
    }

    #[test]
    fn broker_refresh_deadline_is_measured_from_its_snapshot_completion() {
        let brokerage = super::BrokerageRuntime::new(
            BrokerageProviders::new(),
            Arc::new(RecordingAuthorizationUrlOpener::new()),
        );
        let completed_at = tokio::time::Instant::now();

        brokerage.mark_live_session(Brokerage::Robinhood);
        brokerage.record_snapshot_completion(Brokerage::Robinhood, completed_at);

        assert_eq!(
            brokerage.next_refresh_deadline(),
            Some(completed_at + Duration::from_secs(60))
        );
    }

    #[test]
    fn transient_refresh_error_keeps_a_live_session_eligible_for_periodic_refresh() {
        let provider: Arc<dyn BrokerageClient> =
            Arc::new(TestBrokerageProvider::new(Brokerage::Robinhood));
        let mut providers = BrokerageProviders::new();
        providers.insert(Brokerage::Robinhood, provider);
        let brokerage = super::BrokerageRuntime::new(
            providers,
            Arc::new(RecordingAuthorizationUrlOpener::new()),
        );
        let due_at = tokio::time::Instant::now() - Duration::from_secs(1);

        brokerage.mark_live_session(Brokerage::Robinhood);
        brokerage
            .states
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(
                Brokerage::Robinhood,
                kabu_core::BrokerageConnectionState::Error {
                    message: "Unable to load this broker portfolio. Please try again.".into(),
                },
            );
        brokerage.record_retry_deadline(Brokerage::Robinhood, due_at);

        assert_eq!(
            brokerage.brokers_due_for_refresh(tokio::time::Instant::now()),
            vec![Brokerage::Robinhood]
        );
    }

    #[tokio::test]
    async fn scheduled_tick_does_not_refresh_a_snapshot_younger_than_sixty_seconds() {
        let provider = Arc::new(
            TestBrokerageProvider::new(Brokerage::Robinhood)
                .with_refreshes([PortfolioBehavior::Block]),
        );
        let mut providers = BrokerageProviders::new();
        providers.insert(
            Brokerage::Robinhood,
            provider.clone() as Arc<dyn BrokerageClient>,
        );
        let mut brokerage = super::BrokerageRuntime::new(
            providers,
            Arc::new(RecordingAuthorizationUrlOpener::new()),
        );
        let now = chrono::Utc::now();
        brokerage
            .states
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(
                Brokerage::Robinhood,
                kabu_core::BrokerageConnectionState::Connected {
                    last_synced_at: Some(now),
                },
            );
        brokerage
            .snapshots
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(Brokerage::Robinhood, now);
        let (events, _receiver) = super::runtime_event_channel();

        brokerage.scheduled_refresh(&events).await;
        tokio::task::yield_now().await;

        assert_eq!(provider.refresh.started.load(Ordering::SeqCst), 0);
        brokerage.shutdown(&events).await;
    }

    #[test]
    fn hide_publishes_sanitized_error_when_authorization_session_close_fails() {
        let provider = Arc::new(
            TestBrokerageProvider::new(Brokerage::Robinhood)
                .with_connect(ConnectBehavior::Block)
                .with_close_error(),
        );
        let runtime = live_runtime_with_broker_client(provider.clone());
        runtime
            .send(RuntimeCommand::ConnectBroker(Brokerage::Robinhood))
            .expect("connect");
        wait_for_counter(&provider.connect.started, 1);

        runtime
            .send(RuntimeCommand::SetPortfolioVisible(false))
            .expect("hide");
        wait_for_counter(&provider.closes, 1);
        let state = wait_for_broker_state(&runtime, Brokerage::Robinhood, |state| {
            matches!(state, kabu_core::BrokerageConnectionState::Error { .. })
        });

        let kabu_core::BrokerageConnectionState::Error { message } = state else {
            panic!("expected error state");
        };
        assert_eq!(
            message,
            "This broker is temporarily unavailable. Please try again later."
        );
        assert!(!message.contains("private"));
        runtime.shutdown();
    }

    #[test]
    fn refresh_failure_publishes_only_the_fixed_safe_message() {
        let provider = Arc::new(
            TestBrokerageProvider::new(Brokerage::Robinhood)
                .with_refreshes([PortfolioBehavior::Fail(BrokerageErrorKind::RateLimited)]),
        );
        let runtime = live_runtime_with_broker_client(provider.clone());

        runtime
            .send(RuntimeCommand::RefreshPortfolio(Brokerage::Robinhood))
            .expect("refresh");
        let state = wait_for_broker_state(&runtime, Brokerage::Robinhood, |state| {
            matches!(state, kabu_core::BrokerageConnectionState::Error { .. })
        });

        let kabu_core::BrokerageConnectionState::Error { message } = state else {
            panic!("expected error state");
        };
        assert_eq!(
            message,
            "The broker is receiving too many requests. Please try again shortly."
        );
        runtime.shutdown();
    }

    #[test]
    fn explicit_disconnect_cancels_work_and_uses_credential_clearing_operation() {
        let provider = Arc::new(
            TestBrokerageProvider::new(Brokerage::Robinhood).with_connect(ConnectBehavior::Block),
        );
        let runtime = live_runtime_with_broker_client(provider.clone());
        runtime
            .send(RuntimeCommand::ConnectBroker(Brokerage::Robinhood))
            .expect("connect");
        wait_for_counter(&provider.connect.started, 1);

        runtime
            .send(RuntimeCommand::DisconnectBroker(Brokerage::Robinhood))
            .expect("disconnect");
        wait_for_counter(&provider.disconnects, 1);

        assert_eq!(provider.connect.active.load(Ordering::SeqCst), 0);
        assert_eq!(provider.connect.cancelled.load(Ordering::SeqCst), 1);
        assert_eq!(provider.closes.load(Ordering::SeqCst), 0);
        wait_for_broker_state(&runtime, Brokerage::Robinhood, |state| {
            matches!(state, kabu_core::BrokerageConnectionState::Disconnected)
        });
        runtime.shutdown();
    }

    #[test]
    fn brokerage_work_starts_while_all_ordinary_job_slots_are_in_use() {
        let market_probe = Arc::new(OperationProbe::default());
        let providers = ProviderSet {
            equities: Arc::new(ControlledProvider::new(
                DataSource::Yahoo,
                market_probe.clone(),
                Duration::from_secs(30),
            )),
            crypto: Arc::new(FakeProvider::immediate(DataSource::Binance, Vec::new())),
        };
        let broker = Arc::new(
            TestBrokerageProvider::new(Brokerage::Robinhood).with_connect(ConnectBehavior::Block),
        );
        let runtime = live_runtime_with_providers_and_broker(providers, broker.clone());
        let instrument = aapl_instrument();
        for _ in 0..super::MAX_CONCURRENT_OTHER_JOBS {
            runtime
                .send(RuntimeCommand::History {
                    generation: kabu_core::Generation::default().next(),
                    instrument: instrument.clone(),
                    period: ChartPeriod::OneDay,
                })
                .expect("ordinary job");
        }
        wait_for_counter(&market_probe.started, super::MAX_CONCURRENT_OTHER_JOBS);

        runtime
            .send(RuntimeCommand::ConnectBroker(Brokerage::Robinhood))
            .expect("broker control remains available");
        wait_for_counter(&broker.connect.started, 1);

        assert_eq!(market_probe.active.load(Ordering::SeqCst), 31);
        runtime.shutdown();
    }

    #[test]
    fn shutdown_closes_broker_session_before_final_flush_without_clearing_credentials() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let provider =
            Arc::new(TestBrokerageProvider::new(Brokerage::Robinhood).with_order(order.clone()));
        let repository = Arc::new(OrderingRepository {
            order: order.clone(),
        });
        let runtime = live_runtime_with_components(
            default_provider_set(),
            provider.clone(),
            Arc::new(RecordingAuthorizationUrlOpener::new()),
            repository,
        );

        runtime.begin_shutdown_with_flush(PersistedState::starter(), CachedMarketState::default());
        let outcome = runtime
            .wait_for_shutdown(Duration::from_secs(1))
            .expect("shutdown");

        assert!(outcome.is_success());
        assert_eq!(provider.closes.load(Ordering::SeqCst), 1);
        assert_eq!(provider.disconnects.load(Ordering::SeqCst), 0);
        let order = order
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let close = order
            .iter()
            .position(|step| *step == "close")
            .expect("close");
        let save = order
            .iter()
            .position(|step| *step == "save-state")
            .expect("save state");
        assert!(
            close < save,
            "broker sessions close before final persistence"
        );
        assert_eq!(
            runtime.send(RuntimeCommand::ConnectBroker(Brokerage::Robinhood)),
            Err(super::RuntimeSendError::Closed)
        );
    }

    #[test]
    fn failed_broker_session_close_makes_shutdown_outcome_unsuccessful() {
        let provider =
            Arc::new(TestBrokerageProvider::new(Brokerage::Robinhood).with_close_error());
        let runtime = live_runtime_with_broker_client(provider.clone());

        runtime.begin_shutdown();
        let outcome = runtime
            .wait_for_shutdown(Duration::from_secs(1))
            .expect("shutdown acknowledgement");

        assert!(!outcome.is_success());
        assert!(runtime.retry_shutdown_persistence().is_success());
        assert_eq!(provider.closes.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn broker_close_retry_does_not_skip_failed_state_and_cache_retries() {
        let provider =
            Arc::new(TestBrokerageProvider::new(Brokerage::Robinhood).with_close_failures(2));
        let repository = Arc::new(FlakyRepository::failing());
        let runtime = live_runtime_with_components(
            default_provider_set(),
            provider.clone(),
            Arc::new(RecordingAuthorizationUrlOpener::new()),
            repository.clone(),
        );

        runtime.begin_shutdown_with_flush(PersistedState::starter(), CachedMarketState::default());
        let failed = runtime
            .wait_for_shutdown(Duration::from_secs(1))
            .expect("initial shutdown acknowledgement");
        assert!(!failed.is_success());
        assert_eq!(failed.state, Some(PersistenceResult::Failed));
        assert_eq!(failed.market_cache, Some(PersistenceResult::Failed));

        repository.recover_state();
        repository.recover_cache();
        let retried = runtime.retry_shutdown_persistence();

        assert!(
            !retried.is_success(),
            "a still-failing broker close remains a shutdown failure"
        );
        assert_eq!(retried.state, Some(PersistenceResult::Saved));
        assert_eq!(retried.market_cache, Some(PersistenceResult::Saved));
        assert_eq!(provider.closes.load(Ordering::SeqCst), 2);
        assert_eq!(repository.state_saves.load(Ordering::SeqCst), 2);
        assert_eq!(repository.cache_saves.load(Ordering::SeqCst), 2);
    }

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
                | RuntimeEvent::HistoryFailed { .. }
                | RuntimeEvent::BrokerConnectionChanged { .. }
                | RuntimeEvent::PortfolioUpdated(_) => {}
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
                | RuntimeEvent::StreamStatus(_)
                | RuntimeEvent::BrokerConnectionChanged { .. }
                | RuntimeEvent::PortfolioUpdated(_) => {}
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
    fn current_selected_history_survives_lower_generation_distinct_key_pressure() {
        for current_succeeds in [true, false] {
            let (sender, events) = super::runtime_event_channel();
            let runtime = RuntimeHandle::default();
            let mut model = AppModel::new(
                PersistedState::starter(),
                CachedMarketState::default(),
                runtime,
            );
            let selected = model.selected().expect("starter selection").clone();
            let mut stale = Vec::new();
            for index in 0..40 {
                let instrument = Instrument::equity(
                    &format!("OLD{index}"),
                    Some("NASDAQ"),
                    "Stale history",
                    "USD",
                )
                .expect("stale history instrument");
                let generation = model.request_history(instrument.clone(), ChartPeriod::OneDay);
                stale.push((
                    generation,
                    ChartRequestKey {
                        instrument_id: instrument.id,
                        period: ChartPeriod::OneDay,
                    },
                ));
            }
            let current = model.request_history(selected.clone(), ChartPeriod::OneMonth);
            let current_key = ChartRequestKey {
                instrument_id: selected.id,
                period: ChartPeriod::OneMonth,
            };

            if current_succeeds {
                sender
                    .try_send(RuntimeEvent::HistoryCompleted {
                        generation: current,
                        key: current_key,
                        candles: vec![test_candle()],
                    })
                    .expect("current success is published first");
            } else {
                sender
                    .try_send(RuntimeEvent::HistoryFailed {
                        generation: current,
                        key: current_key,
                        error: test_error(MarketDataErrorKind::Timeout),
                    })
                    .expect("current failure is published first");
            }
            for (generation, key) in stale {
                sender
                    .try_send(RuntimeEvent::HistoryCompleted {
                        generation,
                        key,
                        candles: vec![test_candle()],
                    })
                    .expect("lower-generation distinct completion");
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
            let state = sender
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            assert!(state.slots.is_empty());
            assert!(state.pending.is_empty());
        }
    }

    #[test]
    fn history_capacity_retains_highest_generations_and_newest_equal_arrival() {
        let (sender, events) = super::runtime_event_channel();
        let mut generations = Generation::default();
        let lower = generations.next();
        let middle = generations.next();
        let higher = generations.next();
        let lowest_key = history_key(0);

        sender
            .try_send(history_completion(lower, lowest_key.clone()))
            .expect("one lower generation");
        for index in 1..32 {
            sender
                .try_send(history_completion(middle, history_key(index)))
                .expect("middle generation fills history capacity");
        }

        let discarded_key = history_key(100);
        sender
            .try_send(history_completion(
                kabu_core::GenerationId::default(),
                discarded_key.clone(),
            ))
            .expect("lower candidate is handled without blocking");
        assert_history_slots(&sender, 32, &[&lowest_key], &[&discarded_key]);

        let newest_equal_key = history_key(32);
        sender
            .try_send(history_completion(lower, newest_equal_key.clone()))
            .expect("newest equal-generation terminal");
        assert_history_slots(&sender, 32, &[&newest_equal_key], &[&lowest_key]);

        let higher_key = history_key(33);
        sender
            .try_send(history_completion(higher, higher_key.clone()))
            .expect("higher-generation terminal");
        assert_history_slots(
            &sender,
            32,
            &[&higher_key, &history_key(1)],
            &[&newest_equal_key],
        );

        while events.try_recv().is_ok() {}
        let state = sender
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert!(state.slots.is_empty());
        assert!(state.pending.is_empty());
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
                BrokerageProviders::new(),
                Arc::new(ProductionAuthorizationUrlOpener),
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

    struct RecordingAuthorizationUrlOpener {
        urls: Mutex<Vec<url::Url>>,
    }

    impl RecordingAuthorizationUrlOpener {
        fn new() -> Self {
            Self {
                urls: Mutex::new(Vec::new()),
            }
        }
    }

    #[derive(Clone, Copy)]
    enum ConnectBehavior {
        Complete,
        Block,
    }

    enum PortfolioBehavior {
        Complete(PortfolioSnapshot),
        Block,
        Fail(BrokerageErrorKind),
    }

    struct TestBrokerageProvider {
        brokerage: Brokerage,
        connect_behavior: ConnectBehavior,
        opens_authorization_url: bool,
        refresh_behaviors: Mutex<VecDeque<PortfolioBehavior>>,
        refresh_release: async_channel::Receiver<()>,
        release_refresh: async_channel::Sender<()>,
        connect: Arc<OperationProbe>,
        refresh: Arc<OperationProbe>,
        closes: AtomicUsize,
        disconnects: AtomicUsize,
        close_failures: AtomicUsize,
        order: Option<Arc<Mutex<Vec<&'static str>>>>,
    }

    impl TestBrokerageProvider {
        fn new(brokerage: Brokerage) -> Self {
            let (release_refresh, refresh_release) = async_channel::unbounded();
            Self {
                brokerage,
                connect_behavior: ConnectBehavior::Complete,
                opens_authorization_url: false,
                refresh_behaviors: Mutex::new(VecDeque::new()),
                refresh_release,
                release_refresh,
                connect: Arc::new(OperationProbe::default()),
                refresh: Arc::new(OperationProbe::default()),
                closes: AtomicUsize::new(0),
                disconnects: AtomicUsize::new(0),
                close_failures: AtomicUsize::new(0),
                order: None,
            }
        }

        fn with_connect(mut self, behavior: ConnectBehavior) -> Self {
            self.connect_behavior = behavior;
            self
        }

        fn opening_authorization_url(mut self) -> Self {
            self.opens_authorization_url = true;
            self
        }

        fn with_refreshes(self, behaviors: impl IntoIterator<Item = PortfolioBehavior>) -> Self {
            *self
                .refresh_behaviors
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = behaviors.into_iter().collect();
            self
        }

        fn with_close_error(mut self) -> Self {
            self.close_failures = AtomicUsize::new(1);
            self
        }

        fn with_close_failures(mut self, failures: usize) -> Self {
            self.close_failures = AtomicUsize::new(failures);
            self
        }

        fn with_order(mut self, order: Arc<Mutex<Vec<&'static str>>>) -> Self {
            self.order = Some(order);
            self
        }

        fn release_one_refresh(&self) {
            self.release_refresh.try_send(()).expect("release refresh");
        }

        fn record(&self, step: &'static str) {
            if let Some(order) = &self.order {
                order
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .push(step);
            }
        }
    }

    #[async_trait]
    impl BrokerageClient for TestBrokerageProvider {
        async fn connect(
            &self,
            open_authorization_url: &(
                 dyn for<'url> Fn(&'url url::Url) -> Result<(), BrokerageError> + Send + Sync
             ),
            cancellation: CancellationToken,
        ) -> Result<(), BrokerageError> {
            let guard = OperationGuard::begin(self.connect.clone());
            if self.opens_authorization_url {
                let url = url::Url::parse("https://broker.example/authorize?private=secret")
                    .expect("authorization URL");
                open_authorization_url(&url)?;
            }
            match self.connect_behavior {
                ConnectBehavior::Complete => {
                    guard.complete();
                    Ok(())
                }
                ConnectBehavior::Block => {
                    cancellation.cancelled().await;
                    self.connect
                        .cancellation_signals
                        .fetch_add(1, Ordering::SeqCst);
                    Err(BrokerageError::new(BrokerageErrorKind::AuthorizationDenied))
                }
            }
        }

        async fn close_session(&self) -> Result<(), BrokerageError> {
            self.closes.fetch_add(1, Ordering::SeqCst);
            self.record("close");
            if self
                .close_failures
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                Err(BrokerageError::new(BrokerageErrorKind::Unavailable))
            } else {
                Ok(())
            }
        }
    }

    #[async_trait]
    impl PortfolioProvider for TestBrokerageProvider {
        fn brokerage(&self) -> Brokerage {
            self.brokerage
        }

        async fn refresh_portfolio(&self) -> Result<PortfolioSnapshot, BrokerageError> {
            let guard = OperationGuard::begin(self.refresh.clone());
            let behavior = self
                .refresh_behaviors
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .pop_front()
                .unwrap_or_else(|| {
                    PortfolioBehavior::Complete(portfolio_snapshot(
                        self.brokerage,
                        chrono::Utc::now().timestamp(),
                    ))
                });
            match behavior {
                PortfolioBehavior::Complete(snapshot) => {
                    guard.complete();
                    Ok(snapshot)
                }
                PortfolioBehavior::Block => {
                    self.refresh_release
                        .recv()
                        .await
                        .map_err(|_| BrokerageError::new(BrokerageErrorKind::Internal))?;
                    guard.complete();
                    Ok(portfolio_snapshot(
                        self.brokerage,
                        chrono::Utc::now().timestamp(),
                    ))
                }
                PortfolioBehavior::Fail(kind) => {
                    guard.complete();
                    Err(BrokerageError::new(kind))
                }
            }
        }

        async fn disconnect(&self) -> Result<(), BrokerageError> {
            self.disconnects.fetch_add(1, Ordering::SeqCst);
            self.record("disconnect");
            Ok(())
        }
    }

    struct CoordinatedAuthorizationProvider {
        brokerage: Brokerage,
        completes_after_cancellation: bool,
        connect_started: AtomicUsize,
        cancellation_signals: AtomicUsize,
        connect_completed: AtomicUsize,
        closes: AtomicUsize,
        release_connect: async_channel::Sender<()>,
        wait_for_release: async_channel::Receiver<()>,
    }

    impl CoordinatedAuthorizationProvider {
        fn new(brokerage: Brokerage, completes_after_cancellation: bool) -> Self {
            let (release_connect, wait_for_release) = async_channel::bounded(1);
            Self {
                brokerage,
                completes_after_cancellation,
                connect_started: AtomicUsize::new(0),
                cancellation_signals: AtomicUsize::new(0),
                connect_completed: AtomicUsize::new(0),
                closes: AtomicUsize::new(0),
                release_connect,
                wait_for_release,
            }
        }

        fn release_connect(&self) {
            self.release_connect
                .try_send(())
                .expect("release authorization");
        }

        fn ensure_connect_released(&self) {
            _ = self.release_connect.try_send(());
        }
    }

    #[async_trait]
    impl BrokerageClient for CoordinatedAuthorizationProvider {
        async fn connect(
            &self,
            _open_authorization_url: &(
                 dyn for<'url> Fn(&'url url::Url) -> Result<(), BrokerageError> + Send + Sync
             ),
            cancellation: CancellationToken,
        ) -> Result<(), BrokerageError> {
            self.connect_started.fetch_add(1, Ordering::SeqCst);
            cancellation.cancelled().await;
            self.cancellation_signals.fetch_add(1, Ordering::SeqCst);
            self.wait_for_release
                .recv()
                .await
                .expect("release authorization connection");
            self.connect_completed.fetch_add(1, Ordering::SeqCst);
            if self.completes_after_cancellation {
                Ok(())
            } else {
                Err(BrokerageError::new(BrokerageErrorKind::AuthorizationDenied))
            }
        }

        async fn close_session(&self) -> Result<(), BrokerageError> {
            self.closes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[async_trait]
    impl PortfolioProvider for CoordinatedAuthorizationProvider {
        fn brokerage(&self) -> Brokerage {
            self.brokerage
        }

        async fn refresh_portfolio(&self) -> Result<PortfolioSnapshot, BrokerageError> {
            Ok(portfolio_snapshot(
                self.brokerage,
                chrono::Utc::now().timestamp(),
            ))
        }

        async fn disconnect(&self) -> Result<(), BrokerageError> {
            Ok(())
        }
    }

    impl AuthorizationUrlOpener for RecordingAuthorizationUrlOpener {
        fn open(&self, url: &url::Url) -> Result<(), BrokerageError> {
            self.urls
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(url.clone());
            Ok(())
        }
    }

    struct BlockingPortfolioProvider {
        brokerage: Brokerage,
        refresh: Arc<OperationProbe>,
        connects: AtomicUsize,
        closes: AtomicUsize,
        disconnects: AtomicUsize,
    }

    impl BlockingPortfolioProvider {
        fn new(brokerage: Brokerage) -> Self {
            Self {
                brokerage,
                refresh: Arc::new(OperationProbe::default()),
                connects: AtomicUsize::new(0),
                closes: AtomicUsize::new(0),
                disconnects: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl BrokerageClient for BlockingPortfolioProvider {
        async fn connect(
            &self,
            _open_authorization_url: &(
                 dyn for<'url> Fn(&'url url::Url) -> Result<(), BrokerageError> + Send + Sync
             ),
            _cancellation: CancellationToken,
        ) -> Result<(), BrokerageError> {
            self.connects.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn close_session(&self) -> Result<(), BrokerageError> {
            self.closes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[async_trait]
    impl PortfolioProvider for BlockingPortfolioProvider {
        fn brokerage(&self) -> Brokerage {
            self.brokerage
        }

        async fn refresh_portfolio(&self) -> Result<PortfolioSnapshot, BrokerageError> {
            let _guard = OperationGuard::begin(self.refresh.clone());
            std::future::pending().await
        }

        async fn disconnect(&self) -> Result<(), BrokerageError> {
            self.disconnects.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn live_runtime_with_broker(provider: Arc<BlockingPortfolioProvider>) -> RuntimeHandle {
        live_runtime_with_components(
            default_provider_set(),
            provider,
            Arc::new(RecordingAuthorizationUrlOpener::new()),
            Arc::new(NoopRepository),
        )
    }

    fn live_runtime_with_broker_client(provider: Arc<TestBrokerageProvider>) -> RuntimeHandle {
        live_runtime_with_broker_and_opener(
            provider,
            Arc::new(RecordingAuthorizationUrlOpener::new()),
        )
    }

    fn live_runtime_with_broker_and_opener(
        provider: Arc<TestBrokerageProvider>,
        opener: Arc<RecordingAuthorizationUrlOpener>,
    ) -> RuntimeHandle {
        live_runtime_with_components(
            default_provider_set(),
            provider,
            opener,
            Arc::new(NoopRepository),
        )
    }

    fn live_runtime_with_providers_and_broker(
        providers: ProviderSet,
        broker: Arc<TestBrokerageProvider>,
    ) -> RuntimeHandle {
        live_runtime_with_components(
            providers,
            broker,
            Arc::new(RecordingAuthorizationUrlOpener::new()),
            Arc::new(NoopRepository),
        )
    }

    fn live_runtime_with_components(
        providers: ProviderSet,
        provider: Arc<dyn BrokerageClient>,
        opener: Arc<dyn AuthorizationUrlOpener>,
        repository: Arc<dyn StateRepository>,
    ) -> RuntimeHandle {
        let mut brokerages = BrokerageProviders::new();
        brokerages.insert(provider.brokerage(), provider);
        RuntimeHandle::start_inner(
            providers,
            brokerages,
            opener,
            repository,
            CachedMarketState::default(),
            None,
        )
        .expect("runtime")
    }

    fn live_runtime_with_brokerages(
        brokerages: impl IntoIterator<Item = Arc<dyn BrokerageClient>>,
    ) -> RuntimeHandle {
        let mut providers = BrokerageProviders::new();
        for provider in brokerages {
            providers.insert(provider.brokerage(), provider);
        }
        RuntimeHandle::start_inner(
            default_provider_set(),
            providers,
            Arc::new(RecordingAuthorizationUrlOpener::new()),
            Arc::new(NoopRepository),
            CachedMarketState::default(),
            None,
        )
        .expect("runtime")
    }

    fn wait_for_broker_state(
        runtime: &RuntimeHandle,
        brokerage: Brokerage,
        predicate: impl Fn(&kabu_core::BrokerageConnectionState) -> bool,
    ) -> kabu_core::BrokerageConnectionState {
        let started = Instant::now();
        while started.elapsed() < Duration::from_secs(1) {
            while let Ok(event) = runtime.events().try_recv() {
                if let RuntimeEvent::BrokerConnectionChanged {
                    brokerage: candidate,
                    state,
                } = event
                    && candidate == brokerage
                    && predicate(&state)
                {
                    return state;
                }
            }
            thread::sleep(Duration::from_millis(1));
        }
        panic!("timed out waiting for broker state");
    }

    fn default_provider_set() -> ProviderSet {
        ProviderSet {
            equities: Arc::new(FakeProvider::immediate(DataSource::Yahoo, Vec::new())),
            crypto: Arc::new(FakeProvider::immediate(DataSource::Binance, Vec::new())),
        }
    }

    fn portfolio_snapshot(brokerage: Brokerage, second: i64) -> PortfolioSnapshot {
        PortfolioSnapshot {
            brokerage,
            accounts: Vec::new(),
            positions: Vec::new(),
            received_at: chrono::DateTime::from_timestamp(second, 0).expect("timestamp"),
        }
    }

    struct OrderingRepository {
        order: Arc<Mutex<Vec<&'static str>>>,
    }

    impl StateRepository for OrderingRepository {
        fn load_state(&self) -> Result<PersistedState, PersistenceError> {
            Ok(PersistedState::starter())
        }

        fn save_state(&self, _: &PersistedState) -> Result<(), PersistenceError> {
            self.order
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push("save-state");
            Ok(())
        }

        fn load_cache(&self) -> Result<CachedMarketState, PersistenceError> {
            Ok(CachedMarketState::default())
        }

        fn save_cache(&self, _: &CachedMarketState) -> Result<(), PersistenceError> {
            self.order
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push("save-cache");
            Ok(())
        }
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
        cancellation_signals: AtomicUsize,
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

    fn history_key(index: usize) -> ChartRequestKey {
        ChartRequestKey {
            instrument_id: distinct_equity_id(index),
            period: ChartPeriod::OneMonth,
        }
    }

    fn history_completion(
        generation: kabu_core::GenerationId,
        key: ChartRequestKey,
    ) -> RuntimeEvent {
        RuntimeEvent::HistoryCompleted {
            generation,
            key,
            candles: vec![test_candle()],
        }
    }

    fn assert_history_slots(
        sender: &super::RuntimeEventSender,
        expected_len: usize,
        present: &[&ChartRequestKey],
        absent: &[&ChartRequestKey],
    ) {
        let state = sender
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let history_keys: Vec<_> = state
            .slots
            .keys()
            .filter_map(|slot| match slot {
                super::RuntimeEventSlot::History(key) => Some(key),
                _ => None,
            })
            .collect();
        assert_eq!(history_keys.len(), expected_len);
        for key in present {
            assert!(history_keys.contains(key), "expected retained history key");
        }
        for key in absent {
            assert!(
                !history_keys.contains(key),
                "expected discarded history key"
            );
        }
        assert_eq!(state.pending.len(), expected_len);
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
