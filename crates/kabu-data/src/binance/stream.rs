use std::{collections::BTreeMap, sync::Arc, time::Duration};

use async_channel::Sender;
use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use kabu_core::{
    DataSource, Instrument, MarketDataError, MarketDataErrorKind, MarketState, Quote, QuoteStream,
    QuoteSubscription, RetryPolicy, jittered_delay,
};
use serde::Deserialize;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use super::{BinanceClient, catalog::wire_symbol};

const STREAM_ENDPOINT: &str = "Binance stream";

#[async_trait::async_trait]
trait Sleeper: Send + Sync {
    async fn sleep(&self, duration: Duration);
}

trait JitterSource: Send + Sync {
    fn sample(&self) -> f64;
}

struct TokioSleeper;

#[async_trait::async_trait]
impl Sleeper for TokioSleeper {
    async fn sleep(&self, duration: Duration) {
        tokio::time::sleep(duration).await;
    }
}

struct RandomJitter;

impl JitterSource for RandomJitter {
    fn sample(&self) -> f64 {
        rand::random()
    }
}

#[async_trait::async_trait]
impl QuoteSubscription for BinanceClient {
    async fn subscribe(&self, instruments: &[Instrument]) -> Result<QuoteStream, MarketDataError> {
        subscribe_with_runtime(
            self,
            instruments,
            Arc::new(TokioSleeper),
            Arc::new(RandomJitter),
        )
    }
}

fn subscribe_with_runtime(
    client: &BinanceClient,
    instruments: &[Instrument],
    sleeper: Arc<dyn Sleeper>,
    jitter: Arc<dyn JitterSource>,
) -> Result<QuoteStream, MarketDataError> {
    let subscriptions = subscriptions(instruments)?;
    let url = stream_url(client, &subscriptions)?;
    let (sender, receiver) = async_channel::unbounded();
    tokio::spawn(run_stream(url, subscriptions, sender, sleeper, jitter));
    Ok(Box::pin(receiver))
}

fn subscriptions(
    instruments: &[Instrument],
) -> Result<BTreeMap<String, Instrument>, MarketDataError> {
    let mut subscriptions: BTreeMap<String, Instrument> = BTreeMap::new();
    for instrument in instruments {
        let symbol = wire_symbol(instrument)?.to_ascii_lowercase();
        if let Some(existing) = subscriptions.get(&symbol) {
            if existing.id != instrument.id {
                return Err(subscription_error(
                    "Binance stream subscription contains ambiguous instruments",
                ));
            }
        } else {
            subscriptions.insert(symbol, instrument.clone());
        }
    }
    if subscriptions.is_empty() {
        return Err(subscription_error(
            "Binance stream requires at least one crypto instrument",
        ));
    }
    Ok(subscriptions)
}

fn stream_url(
    client: &BinanceClient,
    subscriptions: &BTreeMap<String, Instrument>,
) -> Result<String, MarketDataError> {
    if !matches!(client.stream_base.scheme(), "ws" | "wss") {
        return Err(internal_error("Binance stream URL is invalid"));
    }
    let streams = subscriptions
        .keys()
        .map(|symbol| format!("{symbol}@ticker"))
        .collect::<Vec<_>>()
        .join("/");
    let mut url = client.stream_base.clone();
    url.set_path("/stream");
    url.set_query(Some(&format!("streams={streams}")));
    Ok(url.into())
}

async fn run_stream(
    url: String,
    subscriptions: BTreeMap<String, Instrument>,
    sender: Sender<Result<Quote, MarketDataError>>,
    sleeper: Arc<dyn Sleeper>,
    jitter: Arc<dyn JitterSource>,
) {
    let retry = RetryPolicy::crypto_stream();
    let mut attempt = 0usize;
    loop {
        let connection = tokio::select! {
            _ = sender.closed() => return,
            connection = connect_async(&url) => connection,
        };
        if let Ok((mut websocket, _)) = connection {
            loop {
                let message = tokio::select! {
                    _ = sender.closed() => return,
                    message = websocket.next() => message,
                };
                match message {
                    Some(Ok(Message::Text(text))) => match decode_ticker(&text, &subscriptions) {
                        Ok(Some(quote)) => {
                            attempt = 0;
                            if sender.send(Ok(quote)).await.is_err() {
                                return;
                            }
                        }
                        Ok(None) => {}
                        Err(error) => {
                            if sender.send(Err(error)).await.is_err() {
                                return;
                            }
                        }
                    },
                    Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
                    Some(Ok(Message::Binary(_))) => {
                        if sender.send(Err(malformed_error())).await.is_err() {
                            return;
                        }
                    }
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                    Some(Ok(_)) => {}
                }
            }
        }

        if wait_to_reconnect(&sender, &retry, attempt, sleeper.as_ref(), jitter.as_ref())
            .await
            .is_none()
        {
            return;
        }
        attempt = attempt.saturating_add(1);
    }
}

async fn wait_to_reconnect(
    sender: &Sender<Result<Quote, MarketDataError>>,
    retry: &RetryPolicy,
    attempt: usize,
    sleeper: &dyn Sleeper,
    jitter: &dyn JitterSource,
) -> Option<Duration> {
    let delay = jittered_delay(retry.base_delay(attempt), jitter.sample());
    if sender.send(Err(disconnected_error(delay))).await.is_err() {
        return None;
    }
    tokio::select! {
        _ = sender.closed() => None,
        _ = sleeper.sleep(delay) => Some(delay),
    }
}

fn decode_ticker(
    message: &str,
    subscriptions: &BTreeMap<String, Instrument>,
) -> Result<Option<Quote>, MarketDataError> {
    let routing: RoutingEnvelope = serde_json::from_str(message).map_err(|_| malformed_error())?;
    let Some(stream_symbol) = routing.stream.strip_suffix("@ticker") else {
        return Ok(None);
    };
    let stream_symbol = stream_symbol.to_ascii_lowercase();
    let Some(instrument) = subscriptions.get(&stream_symbol) else {
        return Ok(None);
    };
    let payload_symbol = routing.data.symbol.to_ascii_lowercase();
    if !subscriptions.contains_key(&payload_symbol) {
        return Ok(None);
    }
    if payload_symbol != stream_symbol {
        return Err(malformed_error());
    }
    let envelope: TickerEnvelope = serde_json::from_str(message).map_err(|_| malformed_error())?;
    let _quote_volume = finite_number(&envelope.data.quote_volume)?;
    Ok(Some(Quote {
        instrument_id: instrument.id.clone(),
        last: finite_number(&envelope.data.last_price)?,
        previous_close: Some(finite_number(&envelope.data.previous_close)?),
        open: Some(finite_number(&envelope.data.open_price)?),
        high: Some(finite_number(&envelope.data.high_price)?),
        low: Some(finite_number(&envelope.data.low_price)?),
        volume: Some(finite_number(&envelope.data.volume)?),
        currency: instrument.currency.clone(),
        source: DataSource::Binance,
        market_state: MarketState::Continuous,
        provider_timestamp: timestamp(envelope.data.event_time)?,
        received_at: Utc::now(),
        provider_delayed: false,
    }))
}

fn finite_number(value: &str) -> Result<f64, MarketDataError> {
    value
        .parse::<f64>()
        .ok()
        .filter(|number| number.is_finite())
        .ok_or_else(malformed_error)
}

fn timestamp(value: i64) -> Result<DateTime<Utc>, MarketDataError> {
    DateTime::from_timestamp_millis(value).ok_or_else(malformed_error)
}

fn disconnected_error(retry_after: Duration) -> MarketDataError {
    MarketDataError {
        kind: MarketDataErrorKind::Disconnected,
        message: "Binance stream disconnected".into(),
        retry_after: Some(retry_after),
    }
}

fn malformed_error() -> MarketDataError {
    MarketDataError {
        kind: MarketDataErrorKind::MalformedResponse,
        message: format!("{STREAM_ENDPOINT} returned malformed data"),
        retry_after: None,
    }
}

fn subscription_error(message: &str) -> MarketDataError {
    MarketDataError {
        kind: MarketDataErrorKind::UnsupportedInstrument,
        message: message.into(),
        retry_after: None,
    }
}

fn internal_error(message: &str) -> MarketDataError {
    MarketDataError {
        kind: MarketDataErrorKind::Internal,
        message: message.into(),
        retry_after: None,
    }
}

#[derive(Deserialize)]
struct TickerEnvelope {
    data: StreamTicker,
}

#[derive(Deserialize)]
struct RoutingEnvelope {
    stream: String,
    data: RoutingTicker,
}

#[derive(Deserialize)]
struct RoutingTicker {
    #[serde(rename = "s")]
    symbol: String,
}

#[derive(Deserialize)]
struct StreamTicker {
    #[serde(rename = "E")]
    event_time: i64,
    #[serde(rename = "c")]
    last_price: String,
    #[serde(rename = "o")]
    open_price: String,
    #[serde(rename = "h")]
    high_price: String,
    #[serde(rename = "l")]
    low_price: String,
    #[serde(rename = "v")]
    volume: String,
    #[serde(rename = "q")]
    quote_volume: String,
    #[serde(rename = "x")]
    previous_close: String,
}

#[cfg(test)]
mod tests {
    use std::{
        future::pending,
        sync::{
            Mutex,
            atomic::{AtomicBool, Ordering},
        },
    };

    use tokio::sync::Notify;

    use super::*;

    #[tokio::test]
    async fn reconnect_wait_uses_exact_crypto_policy_jitter_and_cancellation() {
        let sleeper = RecordingSleeper::default();
        let jitter = FixedJitter(0.0);
        let policy = RetryPolicy::crypto_stream();
        let (sender, receiver) = async_channel::unbounded();

        assert_eq!(
            wait_to_reconnect(&sender, &policy, 0, &sleeper, &jitter).await,
            Some(Duration::from_millis(800))
        );
        assert_eq!(
            wait_to_reconnect(&sender, &policy, 1, &sleeper, &jitter).await,
            Some(Duration::from_millis(1_600))
        );
        drop(receiver);
        assert_eq!(
            wait_to_reconnect(&sender, &policy, 2, &sleeper, &jitter).await,
            None
        );
        assert_eq!(
            sleeper.delays(),
            vec![Duration::from_millis(800), Duration::from_millis(1_600)]
        );
    }

    #[tokio::test]
    async fn dropping_receiver_cancels_a_pending_reconnect_sleep() {
        let sleeper = Arc::new(PendingSleeper::default());
        let policy = RetryPolicy::crypto_stream();
        let (sender, receiver) = async_channel::unbounded();
        let task_sleeper = Arc::clone(&sleeper);
        let task = tokio::spawn(async move {
            wait_to_reconnect(
                &sender,
                &policy,
                0,
                task_sleeper.as_ref(),
                &FixedJitter(0.5),
            )
            .await
        });
        let error = receiver
            .recv()
            .await
            .expect("disconnect event")
            .expect_err("typed disconnect");
        assert_eq!(error.kind, MarketDataErrorKind::Disconnected);
        sleeper.started.notified().await;

        drop(receiver);

        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), task)
                .await
                .expect("reconnect task stopped")
                .expect("reconnect task joined"),
            None
        );
        assert!(sleeper.future_dropped.load(Ordering::Acquire));
    }

    #[derive(Default)]
    struct RecordingSleeper {
        delays: Mutex<Vec<Duration>>,
    }

    impl RecordingSleeper {
        fn delays(&self) -> Vec<Duration> {
            self.delays.lock().expect("delay lock").clone()
        }
    }

    #[async_trait::async_trait]
    impl Sleeper for RecordingSleeper {
        async fn sleep(&self, duration: Duration) {
            self.delays.lock().expect("delay lock").push(duration);
        }
    }

    struct FixedJitter(f64);

    impl JitterSource for FixedJitter {
        fn sample(&self) -> f64 {
            self.0
        }
    }

    #[derive(Default)]
    struct PendingSleeper {
        started: Notify,
        future_dropped: Arc<AtomicBool>,
    }

    #[async_trait::async_trait]
    impl Sleeper for PendingSleeper {
        async fn sleep(&self, _duration: Duration) {
            let _drop_signal = DropSignal(Arc::clone(&self.future_dropped));
            self.started.notify_one();
            pending::<()>().await;
        }
    }

    struct DropSignal(Arc<AtomicBool>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }
}
