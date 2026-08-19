use crate::runtime::{ProviderSet, RuntimeEvent, StreamStatus};
use async_channel::Sender;
use futures_util::StreamExt as _;
use kabu_core::{
    AssetClass, Instrument, InstrumentId, MarketDataError, MarketDataErrorKind, RetryPolicy,
    jittered_delay,
};
use std::{collections::HashSet, sync::Arc};
use tokio::task::JoinHandle;

type Jitter = Arc<dyn Fn() -> f64 + Send + Sync>;

pub struct Coordinator {
    providers: ProviderSet,
    events: Sender<RuntimeEvent>,
    jitter: Jitter,
    visible: bool,
    equities: Vec<Instrument>,
    crypto: Vec<Instrument>,
    stock_job: Option<JoinHandle<()>>,
    crypto_job: Option<JoinHandle<()>>,
}

impl Coordinator {
    pub fn new(
        providers: ProviderSet,
        events: Sender<RuntimeEvent>,
        watchlist: Vec<Instrument>,
    ) -> Self {
        Self::with_jitter(providers, events, watchlist, Arc::new(rand::random))
    }

    fn with_jitter(
        providers: ProviderSet,
        events: Sender<RuntimeEvent>,
        watchlist: Vec<Instrument>,
        jitter: Jitter,
    ) -> Self {
        let (equities, crypto) = partition(watchlist);
        Self {
            providers,
            events,
            jitter,
            visible: false,
            equities,
            crypto,
            stock_job: None,
            crypto_job: None,
        }
    }

    pub async fn set_visible(&mut self, visible: bool) {
        if self.visible == visible {
            return;
        }
        self.visible = visible;
        if visible {
            self.start_stock_job();
            self.start_crypto_job();
        } else {
            stop_job(&mut self.stock_job).await;
            stop_job(&mut self.crypto_job).await;
            send_event(
                &self.events,
                RuntimeEvent::StreamStatus(StreamStatus::Paused),
            )
            .await;
        }
    }

    pub async fn set_watchlist(&mut self, watchlist: Vec<Instrument>) {
        let (equities, crypto) = partition(watchlist);
        let equities_changed = identities(&self.equities) != identities(&equities);
        let crypto_changed = identities(&self.crypto) != identities(&crypto);
        self.equities = equities;
        self.crypto = crypto;

        if self.visible && equities_changed {
            stop_job(&mut self.stock_job).await;
            self.start_stock_job();
        }
        if self.visible && crypto_changed {
            stop_job(&mut self.crypto_job).await;
            self.start_crypto_job();
        }
    }

    pub async fn set_crypto_watchlist(&mut self, crypto: Vec<Instrument>) {
        let mut watchlist = self.equities.clone();
        watchlist.extend(crypto);
        self.set_watchlist(watchlist).await;
    }

    pub async fn shutdown(&mut self) {
        self.visible = false;
        stop_job(&mut self.stock_job).await;
        stop_job(&mut self.crypto_job).await;
        send_event(
            &self.events,
            RuntimeEvent::StreamStatus(StreamStatus::Stopped),
        )
        .await;
    }

    fn start_stock_job(&mut self) {
        if self.equities.is_empty() {
            return;
        }
        let instruments = self.equities.clone();
        let provider = self.providers.equities.clone();
        let events = self.events.clone();
        let jitter = self.jitter.clone();
        self.stock_job = Some(tokio::spawn(async move {
            run_stock_polls(instruments, provider, events, jitter).await;
        }));
    }

    fn start_crypto_job(&mut self) {
        if self.crypto.is_empty() {
            return;
        }
        let instruments = self.crypto.clone();
        let provider = self.providers.crypto.clone();
        let events = self.events.clone();
        let jitter = self.jitter.clone();
        self.crypto_job = Some(tokio::spawn(async move {
            run_crypto_stream(instruments, provider, events, jitter).await;
        }));
    }
}

async fn run_stock_polls(
    instruments: Vec<Instrument>,
    provider: Arc<dyn crate::runtime::EquityProvider>,
    events: Sender<RuntimeEvent>,
    jitter: Jitter,
) {
    let retry = RetryPolicy::stock_poll();
    let mut failures = 0usize;
    loop {
        match provider.snapshots(&instruments).await {
            Ok(quotes) => {
                failures = 0;
                if !quotes.is_empty() {
                    send_event(&events, RuntimeEvent::Quotes(quotes)).await;
                }
            }
            Err(error) => {
                send_quote_error(&events, &instruments, error).await;
                failures = failures.saturating_add(1);
            }
        }
        let attempt = failures.saturating_sub(1);
        let delay = jittered_delay(retry.base_delay(attempt), jitter());
        tokio::time::sleep(delay).await;
    }
}

async fn run_crypto_stream(
    instruments: Vec<Instrument>,
    provider: Arc<dyn crate::runtime::CryptoProvider>,
    events: Sender<RuntimeEvent>,
    jitter: Jitter,
) {
    send_event(
        &events,
        RuntimeEvent::StreamStatus(StreamStatus::Connecting),
    )
    .await;
    reconcile_crypto(&instruments, provider.as_ref(), &events).await;

    let retry = RetryPolicy::crypto_stream();
    let mut subscribe_attempt = 0usize;
    loop {
        let stream = provider.subscribe(&instruments).await;
        let mut stream = match stream {
            Ok(stream) => {
                subscribe_attempt = 0;
                send_event(&events, RuntimeEvent::StreamStatus(StreamStatus::Live)).await;
                stream
            }
            Err(error) => {
                send_quote_error(&events, &instruments, error).await;
                send_event(
                    &events,
                    RuntimeEvent::StreamStatus(StreamStatus::Reconnecting),
                )
                .await;
                let delay = jittered_delay(retry.base_delay(subscribe_attempt), jitter());
                subscribe_attempt = subscribe_attempt.saturating_add(1);
                tokio::time::sleep(delay).await;
                continue;
            }
        };

        let mut reconcile_after_disconnect = false;
        while let Some(item) = stream.next().await {
            match item {
                Ok(quote) => {
                    if reconcile_after_disconnect {
                        reconcile_crypto(&instruments, provider.as_ref(), &events).await;
                        send_event(&events, RuntimeEvent::StreamStatus(StreamStatus::Live)).await;
                        reconcile_after_disconnect = false;
                    }
                    send_event(&events, RuntimeEvent::Quotes(vec![quote])).await;
                }
                Err(error) => {
                    if error.kind == MarketDataErrorKind::Disconnected {
                        reconcile_after_disconnect = true;
                        send_event(
                            &events,
                            RuntimeEvent::StreamStatus(StreamStatus::Reconnecting),
                        )
                        .await;
                    }
                    send_quote_error(&events, &instruments, error).await;
                }
            }
        }

        send_event(
            &events,
            RuntimeEvent::StreamStatus(StreamStatus::Reconnecting),
        )
        .await;
        let delay = jittered_delay(retry.base_delay(subscribe_attempt), jitter());
        subscribe_attempt = subscribe_attempt.saturating_add(1);
        tokio::time::sleep(delay).await;
        reconcile_crypto(&instruments, provider.as_ref(), &events).await;
    }
}

async fn reconcile_crypto(
    instruments: &[Instrument],
    provider: &dyn crate::runtime::CryptoProvider,
    events: &Sender<RuntimeEvent>,
) {
    match provider.snapshots(instruments).await {
        Ok(quotes) if !quotes.is_empty() => send_event(events, RuntimeEvent::Quotes(quotes)).await,
        Ok(_) => {}
        Err(error) => send_quote_error(events, instruments, error).await,
    }
}

async fn send_quote_error(
    events: &Sender<RuntimeEvent>,
    instruments: &[Instrument],
    error: MarketDataError,
) {
    send_event(
        events,
        RuntimeEvent::QuoteError {
            instrument_ids: instruments
                .iter()
                .map(|instrument| instrument.id.clone())
                .collect(),
            error,
        },
    )
    .await;
}

async fn send_event(events: &Sender<RuntimeEvent>, event: RuntimeEvent) {
    _ = events.send(event).await;
}

async fn stop_job(job: &mut Option<JoinHandle<()>>) {
    if let Some(job) = job.take() {
        job.abort();
        _ = job.await;
    }
}

fn partition(watchlist: Vec<Instrument>) -> (Vec<Instrument>, Vec<Instrument>) {
    watchlist
        .into_iter()
        .partition(|instrument| instrument.id.asset_class() == AssetClass::Equity)
}

fn identities(instruments: &[Instrument]) -> HashSet<InstrumentId> {
    instruments
        .iter()
        .map(|instrument| instrument.id.clone())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::Coordinator;
    use crate::runtime::{ProviderSet, RuntimeEvent, StreamStatus};
    use async_trait::async_trait;
    use futures_util::stream;
    use kabu_core::{
        Candle, ChartHistory, ChartPeriod, DataSource, Instrument, InstrumentSearch,
        MarketDataError, MarketDataErrorKind, MarketState, Quote, QuoteSnapshots, QuoteStream,
        QuoteSubscription, SearchResult,
    };
    use std::{
        collections::VecDeque,
        sync::{
            Arc, Mutex as StdMutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };
    use tokio::sync::Mutex;

    #[tokio::test(start_paused = true)]
    async fn visible_stocks_poll_at_zero_fifteen_thirty_and_forty_five_seconds() {
        let harness = CoordinatorHarness::with_watchlist(vec![aapl_instrument()]);

        harness.set_visible(true).await;
        harness.settle().await;
        assert_eq!(harness.yahoo_snapshot_calls(), 1);
        for expected in 2..=4 {
            harness.advance(Duration::from_secs(15)).await;
            assert_eq!(harness.yahoo_snapshot_calls(), expected);
        }

        harness.shutdown().await;
    }

    #[tokio::test(start_paused = true)]
    async fn hidden_window_cancels_calls_then_restore_reconciles_before_steady_state() {
        let harness = CoordinatorHarness::with_watchlist(vec![aapl_instrument(), btc_instrument()]);
        harness.set_visible(true).await;
        harness.settle().await;
        assert_eq!(harness.yahoo_snapshot_calls(), 1);
        assert_eq!(harness.binance_snapshot_calls(), 1);
        assert_eq!(harness.binance_subscribe_calls(), 1);

        harness.set_visible(false).await;
        harness.advance(Duration::from_secs(120)).await;
        assert_eq!(harness.yahoo_snapshot_calls(), 1);
        assert_eq!(harness.binance_snapshot_calls(), 1);
        assert_eq!(harness.binance_subscribe_calls(), 1);

        harness.set_visible(true).await;
        harness.settle().await;
        assert_eq!(harness.yahoo_snapshot_calls(), 2);
        assert_eq!(harness.binance_snapshot_calls(), 2);
        assert_eq!(harness.binance_subscribe_calls(), 2);
        harness.advance(Duration::from_secs(14)).await;
        assert_eq!(harness.yahoo_snapshot_calls(), 2);
        harness.advance(Duration::from_secs(1)).await;
        assert_eq!(harness.yahoo_snapshot_calls(), 3);

        harness.shutdown().await;
    }

    #[tokio::test(start_paused = true)]
    async fn stock_failures_back_off_fifteen_thirty_sixty_then_success_resets() {
        let timeout = || Err(error(MarketDataErrorKind::Timeout));
        let harness = CoordinatorHarness::with_yahoo_results(
            vec![aapl_instrument()],
            [timeout(), timeout(), timeout(), Ok(vec![aapl_quote()])],
        );

        harness.set_visible(true).await;
        harness.settle().await;
        assert_eq!(harness.yahoo_snapshot_calls(), 1);
        for (delay, expected) in [(15, 2), (30, 3), (60, 4), (15, 5)] {
            harness.advance(Duration::from_secs(delay)).await;
            assert_eq!(harness.yahoo_snapshot_calls(), expected);
        }

        harness.shutdown().await;
    }

    #[tokio::test(start_paused = true)]
    async fn equivalent_crypto_identity_set_does_not_restart_but_a_changed_set_does() {
        let btc = btc_instrument();
        let eth = eth_instrument();
        let harness = CoordinatorHarness::with_watchlist(vec![btc.clone(), eth.clone()]);
        harness.set_visible(true).await;
        harness.settle().await;
        assert_eq!(harness.binance_subscribe_calls(), 1);

        harness.set_watchlist(vec![eth, btc.clone()]).await;
        harness.settle().await;
        assert_eq!(harness.binance_subscribe_calls(), 1);

        harness.set_watchlist(vec![btc]).await;
        harness.settle().await;
        assert_eq!(harness.binance_subscribe_calls(), 2);
        harness.shutdown().await;
    }

    #[tokio::test(start_paused = true)]
    async fn crypto_stream_reconciles_initially_and_before_first_quote_after_disconnect() {
        let (stream_sender, stream_receiver) = async_channel::unbounded();
        let yahoo = Arc::new(FakeProvider::new(DataSource::Yahoo, std::iter::empty()));
        let binance = Arc::new(FakeProvider::with_stream(
            DataSource::Binance,
            Ok(Box::pin(stream_receiver)),
        ));
        let harness = CoordinatorHarness::with_providers(vec![btc_instrument()], yahoo, binance);

        harness.set_visible(true).await;
        harness.settle().await;
        assert_eq!(harness.binance_snapshot_calls(), 1);
        stream_sender
            .send(Err(error(MarketDataErrorKind::Disconnected)))
            .await
            .expect("disconnect event");
        harness.settle().await;
        assert_eq!(harness.binance_snapshot_calls(), 1);
        stream_sender
            .send(Ok(quote(&btc_instrument(), DataSource::Binance)))
            .await
            .expect("reconnected quote");
        harness.settle().await;
        assert_eq!(harness.binance_snapshot_calls(), 2);
        let statuses = harness.stream_statuses();
        assert!(statuses.contains(&StreamStatus::Reconnecting));
        assert_eq!(statuses.last(), Some(&StreamStatus::Live));

        harness.shutdown().await;
    }

    #[tokio::test(start_paused = true)]
    async fn hiding_cancels_failed_stream_retry_without_more_provider_calls() {
        let yahoo = Arc::new(FakeProvider::new(DataSource::Yahoo, std::iter::empty()));
        let binance = Arc::new(FakeProvider::with_stream(
            DataSource::Binance,
            Err(error(MarketDataErrorKind::Unavailable)),
        ));
        let harness = CoordinatorHarness::with_providers(vec![btc_instrument()], yahoo, binance);
        harness.set_visible(true).await;
        harness.settle().await;
        assert_eq!(harness.binance_subscribe_calls(), 1);

        harness.set_visible(false).await;
        harness.advance(Duration::from_secs(60)).await;

        assert_eq!(harness.binance_subscribe_calls(), 1);
        assert_eq!(
            harness.stream_statuses().last(),
            Some(&StreamStatus::Paused)
        );
        harness.shutdown().await;
    }

    struct CoordinatorHarness {
        coordinator: Mutex<Coordinator>,
        yahoo: Arc<FakeProvider>,
        binance: Arc<FakeProvider>,
        events: async_channel::Receiver<RuntimeEvent>,
    }

    impl CoordinatorHarness {
        fn with_watchlist(watchlist: Vec<Instrument>) -> Self {
            Self::with_yahoo_results(watchlist, std::iter::empty())
        }

        fn with_yahoo_results(
            watchlist: Vec<Instrument>,
            results: impl IntoIterator<Item = Result<Vec<Quote>, MarketDataError>>,
        ) -> Self {
            let yahoo = Arc::new(FakeProvider::new(DataSource::Yahoo, results));
            let binance = Arc::new(FakeProvider::new(DataSource::Binance, std::iter::empty()));
            Self::with_providers(watchlist, yahoo, binance)
        }

        fn with_providers(
            watchlist: Vec<Instrument>,
            yahoo: Arc<FakeProvider>,
            binance: Arc<FakeProvider>,
        ) -> Self {
            let (events, event_receiver) = async_channel::bounded(128);
            let coordinator = Coordinator::with_jitter(
                ProviderSet {
                    equities: yahoo.clone(),
                    crypto: binance.clone(),
                },
                events,
                watchlist,
                Arc::new(|| 0.5),
            );
            Self {
                coordinator: Mutex::new(coordinator),
                yahoo,
                binance,
                events: event_receiver,
            }
        }

        async fn set_visible(&self, visible: bool) {
            let mut coordinator = self.coordinator.lock().await;
            coordinator.set_visible(visible).await;
        }

        async fn set_watchlist(&self, watchlist: Vec<Instrument>) {
            let mut coordinator = self.coordinator.lock().await;
            coordinator.set_watchlist(watchlist).await;
        }

        async fn advance(&self, duration: Duration) {
            tokio::time::advance(duration).await;
            self.settle().await;
        }

        async fn settle(&self) {
            for _ in 0..8 {
                tokio::task::yield_now().await;
            }
        }

        fn yahoo_snapshot_calls(&self) -> usize {
            self.yahoo.snapshot_calls.load(Ordering::SeqCst)
        }

        fn binance_snapshot_calls(&self) -> usize {
            self.binance.snapshot_calls.load(Ordering::SeqCst)
        }

        fn binance_subscribe_calls(&self) -> usize {
            self.binance.subscribe_calls.load(Ordering::SeqCst)
        }

        fn stream_statuses(&self) -> Vec<StreamStatus> {
            let mut statuses = Vec::new();
            while let Ok(event) = self.events.try_recv() {
                if let RuntimeEvent::StreamStatus(status) = event {
                    statuses.push(status);
                }
            }
            statuses
        }

        async fn shutdown(&self) {
            let mut coordinator = self.coordinator.lock().await;
            coordinator.shutdown().await;
        }
    }

    #[derive(Clone)]
    struct FakeProvider {
        source: DataSource,
        snapshot_calls: Arc<AtomicUsize>,
        snapshot_results: SharedSnapshotResults,
        subscribe_calls: Arc<AtomicUsize>,
        stream_results: Arc<StdMutex<VecDeque<Result<QuoteStream, MarketDataError>>>>,
    }

    type SharedSnapshotResults = Arc<StdMutex<VecDeque<Result<Vec<Quote>, MarketDataError>>>>;

    impl FakeProvider {
        fn new(
            source: DataSource,
            results: impl IntoIterator<Item = Result<Vec<Quote>, MarketDataError>>,
        ) -> Self {
            Self {
                source,
                snapshot_calls: Arc::new(AtomicUsize::new(0)),
                snapshot_results: Arc::new(StdMutex::new(results.into_iter().collect())),
                subscribe_calls: Arc::new(AtomicUsize::new(0)),
                stream_results: Arc::new(StdMutex::new(VecDeque::new())),
            }
        }

        fn with_stream(source: DataSource, stream: Result<QuoteStream, MarketDataError>) -> Self {
            let mut provider = Self::new(source, std::iter::empty());
            provider.stream_results = Arc::new(StdMutex::new(VecDeque::from([stream])));
            provider
        }
    }

    #[async_trait]
    impl InstrumentSearch for FakeProvider {
        async fn search(&self, _: &str) -> Result<Vec<SearchResult>, MarketDataError> {
            Ok(Vec::new())
        }
    }

    #[async_trait]
    impl QuoteSnapshots for FakeProvider {
        async fn snapshots(
            &self,
            instruments: &[Instrument],
        ) -> Result<Vec<Quote>, MarketDataError> {
            if instruments.is_empty() {
                return Ok(Vec::new());
            }
            self.snapshot_calls.fetch_add(1, Ordering::SeqCst);
            self.snapshot_results
                .lock()
                .expect("results lock")
                .pop_front()
                .unwrap_or_else(|| {
                    Ok(instruments
                        .iter()
                        .map(|instrument| quote(instrument, self.source))
                        .collect())
                })
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
            self.subscribe_calls.fetch_add(1, Ordering::SeqCst);
            self.stream_results
                .lock()
                .expect("stream results lock")
                .pop_front()
                .unwrap_or_else(|| Ok(Box::pin(stream::pending())))
        }
    }

    fn aapl_instrument() -> Instrument {
        Instrument::equity("AAPL", Some("NASDAQ"), "Apple", "USD").expect("AAPL")
    }

    fn aapl_quote() -> Quote {
        quote(&aapl_instrument(), DataSource::Yahoo)
    }

    fn btc_instrument() -> Instrument {
        Instrument::crypto("BTC", "USDT", "Bitcoin").expect("BTC")
    }

    fn eth_instrument() -> Instrument {
        Instrument::crypto("ETH", "USDT", "Ethereum").expect("ETH")
    }

    fn quote(instrument: &Instrument, source: DataSource) -> Quote {
        let timestamp = chrono::DateTime::from_timestamp(1_700_000_000, 0).expect("timestamp");
        Quote {
            instrument_id: instrument.id.clone(),
            last: 100.0,
            previous_close: Some(99.0),
            open: Some(99.0),
            high: Some(101.0),
            low: Some(98.0),
            volume: Some(1_000.0),
            currency: instrument.currency.clone(),
            source,
            market_state: match source {
                DataSource::Yahoo => MarketState::Regular,
                DataSource::Binance => MarketState::Continuous,
            },
            provider_timestamp: timestamp,
            received_at: timestamp,
            provider_delayed: false,
        }
    }

    fn error(kind: MarketDataErrorKind) -> MarketDataError {
        MarketDataError {
            kind,
            message: "sensitive provider detail".into(),
            retry_after: None,
        }
    }
}
