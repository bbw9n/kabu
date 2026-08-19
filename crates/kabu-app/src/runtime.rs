use async_channel::{Receiver, Sender, TrySendError};
use kabu_core::{
    AssetClass, Candle, ChartHistory, ChartPeriod, ChartRequestKey, DataSource, GenerationId,
    Instrument, InstrumentId, InstrumentSearch, MarketDataError, MarketDataErrorKind, Quote,
    QuoteSnapshots, QuoteSubscription, SearchResult, StateRepository, rank_search_results,
};
use std::{
    collections::VecDeque,
    fmt, io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
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
    SetCryptoWatchlist(Vec<Instrument>),
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
    shutdown_signal: Sender<()>,
    events: Receiver<RuntimeEvent>,
    join: Mutex<Option<JoinHandle<()>>>,
    shutdown: AtomicBool,
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
        let (commands, command_receiver) = async_channel::bounded(COMMAND_CAPACITY);
        let (event_sender, events) = async_channel::bounded(EVENT_CAPACITY);
        let (shutdown_signal, shutdown_receiver) = async_channel::bounded(1);
        let join = thread::Builder::new()
            .name("kabu-network".into())
            .spawn(move || {
                run_network(
                    command_receiver,
                    event_sender,
                    shutdown_receiver,
                    providers,
                    repository,
                );
            })?;

        Ok(Self {
            mode: RuntimeMode::Live(Arc::new(LiveRuntime {
                commands,
                shutdown_signal,
                events,
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
                if commands.len() == COMMAND_CAPACITY {
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
                runtime
                    .commands
                    .try_send(command)
                    .map_err(|error| match error {
                        TrySendError::Full(_) => RuntimeSendError::Full,
                        TrySendError::Closed(_) => RuntimeSendError::Closed,
                    })
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

    pub fn shutdown(&self) {
        let runtime = match &self.mode {
            RuntimeMode::Recording(runtime) => {
                runtime.shutdown.store(true, Ordering::SeqCst);
                runtime.event_sender.close();
                return;
            }
            RuntimeMode::Live(runtime) => runtime,
        };
        if runtime.shutdown.swap(true, Ordering::SeqCst) {
            return;
        }

        _ = runtime.shutdown_signal.try_send(());
        runtime.shutdown_signal.close();
        runtime.commands.close();
        runtime.events.close();
        if let Some(join) = runtime
            .join
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
        {
            _ = join.join();
        }
    }

    pub fn is_shutdown(&self) -> bool {
        match &self.mode {
            RuntimeMode::Recording(runtime) => runtime.shutdown.load(Ordering::SeqCst),
            RuntimeMode::Live(runtime) => runtime.shutdown.load(Ordering::SeqCst),
        }
    }
}

impl Drop for LiveRuntime {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        _ = self.shutdown_signal.try_send(());
        self.shutdown_signal.close();
        self.commands.close();
        self.events.close();
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

fn run_network(
    commands: Receiver<RuntimeCommand>,
    events: Sender<RuntimeEvent>,
    shutdown: Receiver<()>,
    providers: ProviderSet,
    _repository: Arc<dyn StateRepository>,
) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("kabu-network")
        .enable_all()
        .build();
    let Ok(runtime) = runtime else {
        return;
    };
    runtime.block_on(run_dispatcher(commands, events, shutdown, providers));
}

async fn run_dispatcher(
    commands: Receiver<RuntimeCommand>,
    events: Sender<RuntimeEvent>,
    shutdown: Receiver<()>,
    providers: ProviderSet,
) {
    let mut jobs = tokio::task::JoinSet::new();
    let mut search_job: Option<tokio::task::JoinHandle<()>> = None;
    let mut pending = VecDeque::with_capacity(COMMAND_CAPACITY);
    let mut other_in_flight = 0usize;

    loop {
        while other_in_flight < MAX_CONCURRENT_OTHER_JOBS {
            let Some(command) = pending.pop_front() else {
                break;
            };
            spawn_other_job(command, &mut jobs, &events, &providers);
            other_in_flight += 1;
        }

        tokio::select! {
            _ = shutdown.recv() => break,
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
                        if other_in_flight < MAX_CONCURRENT_OTHER_JOBS {
                            spawn_other_job(command, &mut jobs, &events, &providers);
                            other_in_flight += 1;
                        } else {
                            pending.push_back(command);
                        }
                    }
                    RuntimeCommand::SetVisible(_) | RuntimeCommand::SetCryptoWatchlist(_) => {}
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
            | RuntimeCommand::SetCryptoWatchlist(_)
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

#[cfg(test)]
mod tests {
    use super::{ProviderSet, RuntimeCommand, RuntimeEvent, RuntimeHandle};
    use async_trait::async_trait;
    use kabu_core::{
        CachedMarketState, Candle, ChartHistory, ChartPeriod, DataSource, Instrument, InstrumentId,
        InstrumentSearch, MarketDataError, MarketDataErrorKind, MarketState, PersistedState,
        PersistenceError, Quote, QuoteSnapshots, QuoteStream, QuoteSubscription, SearchResult,
        StateRepository,
    };
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        thread,
        time::{Duration, Instant},
    };

    #[test]
    fn recording_runtime_rejects_commands_after_its_bounded_capacity() {
        let runtime = RuntimeHandle::default();
        for _ in 0..super::COMMAND_CAPACITY {
            runtime
                .send(RuntimeCommand::SetVisible(true))
                .expect("within bounded capacity");
        }

        assert_eq!(
            runtime.send(RuntimeCommand::SetVisible(false)),
            Err(super::RuntimeSendError::Full)
        );
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

    fn nvda_result() -> SearchResult {
        SearchResult {
            instrument: Instrument::equity("NVDA", Some("NASDAQ"), "NVIDIA", "USD").expect("NVDA"),
            source: DataSource::Yahoo,
            venue_label: "NASDAQ".into(),
        }
    }

    fn btc_result() -> SearchResult {
        SearchResult {
            instrument: Instrument::crypto("BTC", "USDT", "Bitcoin").expect("BTC"),
            source: DataSource::Binance,
            venue_label: "Binance".into(),
        }
    }
}
