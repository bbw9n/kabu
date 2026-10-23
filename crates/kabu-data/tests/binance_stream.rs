use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use futures_util::{SinkExt, StreamExt};
use kabu_core::{Instrument, QuoteSubscription};
use kabu_data::BinanceClient;
use tokio::{
    io::AsyncReadExt,
    net::TcpListener,
    sync::{Mutex, Notify},
    task::JoinHandle,
};
use tokio_tungstenite::{
    accept_hdr_async,
    tungstenite::{
        Message,
        handshake::server::{ErrorResponse, Request, Response},
    },
};

#[tokio::test]
async fn stream_maps_ticker_and_preserves_pair() {
    let server = TestWebSocketServer::start(vec![vec![binance_ticker_message()]]).await;
    let client = binance_with_stream_url(server.url());

    let mut stream = client
        .subscribe(&[btc_instrument()])
        .await
        .expect("subscribe");
    let quote = stream.next().await.expect("event").expect("quote");

    assert_eq!(quote.instrument_id.canonical_symbol(), "BTC/USDT");
    assert_eq!(quote.last, 93_742.18);
}

#[tokio::test]
async fn reconnect_rebuilds_subscription_from_current_symbols() {
    let server = TestWebSocketServer::start(vec![Vec::new(), Vec::new()]).await;
    let client = binance_with_stream_url(server.url());

    let mut stream = client
        .subscribe(&[
            eth_instrument(),
            btc_instrument(),
            Instrument::crypto("BTC", "USDT", "Bitcoin duplicate").expect("instrument"),
        ])
        .await
        .expect("subscribe");
    server.wait_for_connections(1).await;
    let error = stream
        .next()
        .await
        .expect("disconnect event")
        .expect_err("typed disconnect");
    assert_eq!(error.kind, kabu_core::MarketDataErrorKind::Disconnected);
    tokio::time::pause();
    tokio::time::advance(std::time::Duration::from_secs(2)).await;
    tokio::time::resume();
    server.wait_for_connections(2).await;
    drop(stream);

    assert_eq!(
        server.connection_paths().await,
        vec!["/stream?streams=btcusdt@ticker/ethusdt@ticker"; 2]
    );
}

#[tokio::test]
async fn stream_maps_all_supported_ticker_fields() {
    let server = TestWebSocketServer::start(vec![vec![binance_ticker_message()]]).await;
    let client = binance_with_stream_url(server.url());

    let mut stream = client
        .subscribe(&[btc_instrument()])
        .await
        .expect("subscribe");
    let quote = stream.next().await.expect("event").expect("quote");

    assert_eq!(quote.previous_close, Some(91_000.0));
    assert_eq!(quote.open, Some(91_100.0));
    assert_eq!(quote.high, Some(94_500.0));
    assert_eq!(quote.low, Some(90_200.0));
    assert_eq!(quote.volume, Some(1_234.567));
    assert_eq!(quote.currency, "USDT");
    assert_eq!(
        quote.provider_timestamp.timestamp_millis(),
        1_787_112_345_678
    );
}

#[tokio::test]
async fn malformed_ticker_is_typed_and_does_not_close_stream() {
    let malformed = binance_ticker_message().replace("93742.18", "not-a-number");
    let server = TestWebSocketServer::start(vec![vec![malformed, binance_ticker_message()]]).await;
    let client = binance_with_stream_url(server.url());

    let mut stream = client
        .subscribe(&[btc_instrument()])
        .await
        .expect("subscribe");
    let error = stream
        .next()
        .await
        .expect("malformed event")
        .expect_err("typed error");
    let quote = stream.next().await.expect("next event").expect("quote");

    assert_eq!(
        error.kind,
        kabu_core::MarketDataErrorKind::MalformedResponse
    );
    assert_eq!(error.message, "Binance stream returned malformed data");
    assert_eq!(quote.instrument_id.canonical_symbol(), "BTC/USDT");
}

#[tokio::test]
async fn ticker_for_unsubscribed_symbol_is_ignored() {
    let unknown = r#"{"stream":"dogeusdt@ticker","data":{"s":"DOGEUSDT"}}"#.into();
    let server = TestWebSocketServer::start(vec![vec![unknown, binance_ticker_message()]]).await;
    let client = binance_with_stream_url(server.url());

    let mut stream = client
        .subscribe(&[btc_instrument()])
        .await
        .expect("subscribe");
    let quote = stream.next().await.expect("event").expect("quote");

    assert_eq!(quote.instrument_id.canonical_symbol(), "BTC/USDT");
}

#[tokio::test(start_paused = true)]
async fn disconnected_item_is_followed_by_quotes_after_reconnect() {
    let server = TestWebSocketServer::start(vec![Vec::new(), vec![binance_ticker_message()]]).await;
    let client = binance_with_stream_url(server.url());

    let mut stream = client
        .subscribe(&[btc_instrument()])
        .await
        .expect("subscribe");
    let error = stream
        .next()
        .await
        .expect("disconnect event")
        .expect_err("typed disconnect");
    let quote = stream
        .next()
        .await
        .expect("reconnected event")
        .expect("quote");

    assert_eq!(error.kind, kabu_core::MarketDataErrorKind::Disconnected);
    assert_eq!(error.message, "Binance stream disconnected");
    assert_eq!(quote.instrument_id.canonical_symbol(), "BTC/USDT");
}

#[tokio::test(start_paused = true)]
async fn successful_quote_resets_retry_attempt() {
    let server =
        TestWebSocketServer::start(vec![Vec::new(), vec![binance_ticker_message()], Vec::new()])
            .await;
    let client = binance_with_stream_url(server.url());

    let mut stream = client
        .subscribe(&[btc_instrument()])
        .await
        .expect("subscribe");
    let first_disconnect = stream
        .next()
        .await
        .expect("first disconnect")
        .expect_err("typed disconnect");
    stream.next().await.expect("ticker").expect("quote");
    let post_quote_disconnect = stream
        .next()
        .await
        .expect("post-quote disconnect")
        .expect_err("typed disconnect");

    let reset_upper_bound = std::time::Duration::from_millis(1_200);
    assert!(first_disconnect.retry_after.expect("retry delay") <= reset_upper_bound);
    assert!(post_quote_disconnect.retry_after.expect("retry delay") <= reset_upper_bound);
}

#[tokio::test]
async fn dropping_stream_cancels_connected_producer() {
    let server = TestWebSocketServer::hold_connection_open().await;
    let client = binance_with_stream_url(server.url());

    let stream = client
        .subscribe(&[btc_instrument()])
        .await
        .expect("subscribe");
    server.wait_for_connections(1).await;
    drop(stream);

    server.wait_for_client_disconnect().await;
}

#[tokio::test]
async fn dropping_stream_cancels_pending_websocket_handshake() {
    let server = StalledHandshakeServer::start().await;
    let client = binance_with_stream_url(server.url());

    let stream = client
        .subscribe(&[btc_instrument()])
        .await
        .expect("subscribe");
    server.wait_for_connection().await;
    drop(stream);

    server.wait_for_client_disconnect().await;
}

#[tokio::test]
async fn cancelling_session_awaits_the_real_adapter_producer() {
    let server = TestWebSocketServer::hold_connection_open().await;
    let client = binance_with_stream_url(server.url());

    let stream = client
        .subscribe(&[btc_instrument()])
        .await
        .expect("subscribe");
    server.wait_for_connections(1).await;

    stream.cancel().await;

    server.wait_for_client_disconnect().await;
}

#[tokio::test]
async fn direct_client_wss_failure_is_typed_instead_of_panicking() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("local address");
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept");
        let mut buffer = [0_u8; 1_024];
        let _ =
            tokio::time::timeout(std::time::Duration::from_secs(2), socket.read(&mut buffer)).await;
    });
    let client = BinanceClient::new(
        reqwest::Client::builder().build().expect("test client"),
        "http://127.0.0.1/".parse().expect("REST URL"),
        format!("wss://{address}").parse().expect("WSS URL"),
        None,
    );

    let mut stream = client
        .subscribe(&[btc_instrument()])
        .await
        .expect("subscribe");
    let error = tokio::time::timeout(std::time::Duration::from_secs(3), stream.next())
        .await
        .expect("stream completed promptly")
        .expect("typed disconnect event")
        .expect_err("TLS handshake must fail without panicking");

    assert_eq!(error.kind, kabu_core::MarketDataErrorKind::Disconnected);
    stream.cancel().await;
    server.await.expect("server task");
}

struct TestWebSocketServer {
    url: String,
    paths: Arc<Mutex<Vec<String>>>,
    connected: Arc<Notify>,
    client_disconnected: Arc<AtomicBool>,
    disconnected: Arc<Notify>,
    task: JoinHandle<()>,
}

impl TestWebSocketServer {
    async fn start(connection_messages: Vec<Vec<String>>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("local address");
        let paths = Arc::new(Mutex::new(Vec::new()));
        let connected = Arc::new(Notify::new());
        let client_disconnected = Arc::new(AtomicBool::new(false));
        let disconnected = Arc::new(Notify::new());
        let task_paths = Arc::clone(&paths);
        let task_connected = Arc::clone(&connected);
        let task = tokio::spawn(async move {
            for messages in connection_messages {
                let (socket, _) = listener.accept().await.expect("accept");
                let captured_path = Arc::new(std::sync::Mutex::new(None));
                let callback_path = Arc::clone(&captured_path);
                let mut websocket = accept_hdr_async(socket, capture_path(callback_path))
                    .await
                    .expect("websocket handshake");
                let path = captured_path
                    .lock()
                    .expect("request path lock")
                    .take()
                    .expect("request path");
                task_paths.lock().await.push(path);
                task_connected.notify_one();
                for message in messages {
                    websocket
                        .send(Message::Text(message.into()))
                        .await
                        .expect("send message");
                }
                websocket.close(None).await.expect("close websocket");
            }
        });
        Self {
            url: format!("ws://{address}"),
            paths,
            connected,
            client_disconnected,
            disconnected,
            task,
        }
    }

    async fn hold_connection_open() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("local address");
        let paths = Arc::new(Mutex::new(Vec::new()));
        let connected = Arc::new(Notify::new());
        let client_disconnected = Arc::new(AtomicBool::new(false));
        let disconnected = Arc::new(Notify::new());
        let task_paths = Arc::clone(&paths);
        let task_connected = Arc::clone(&connected);
        let task_client_disconnected = Arc::clone(&client_disconnected);
        let task_disconnected = Arc::clone(&disconnected);
        let task = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.expect("accept");
            let captured_path = Arc::new(std::sync::Mutex::new(None));
            let callback_path = Arc::clone(&captured_path);
            let mut websocket = accept_hdr_async(socket, capture_path(callback_path))
                .await
                .expect("websocket handshake");
            let path = captured_path
                .lock()
                .expect("request path lock")
                .take()
                .expect("request path");
            task_paths.lock().await.push(path);
            task_connected.notify_one();
            let _ = websocket.next().await;
            task_client_disconnected.store(true, Ordering::Release);
            task_disconnected.notify_one();
        });
        Self {
            url: format!("ws://{address}"),
            paths,
            connected,
            client_disconnected,
            disconnected,
            task,
        }
    }

    fn url(&self) -> reqwest::Url {
        self.url.parse().expect("stream URL")
    }

    async fn connection_paths(&self) -> Vec<String> {
        self.paths.lock().await.clone()
    }

    async fn wait_for_connections(&self, expected: usize) {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if self.paths.lock().await.len() >= expected {
                    break;
                }
                self.connected.notified().await;
            }
        })
        .await
        .expect("connections completed");
    }

    async fn wait_for_client_disconnect(&self) {
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                if self.client_disconnected.load(Ordering::Acquire) {
                    break;
                }
                self.disconnected.notified().await;
            }
        })
        .await
        .expect("producer disconnected");
    }
}

impl Drop for TestWebSocketServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct StalledHandshakeServer {
    url: String,
    connected: Arc<Notify>,
    client_disconnected: Arc<AtomicBool>,
    disconnected: Arc<Notify>,
    task: JoinHandle<()>,
}

impl StalledHandshakeServer {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("local address");
        let connected = Arc::new(Notify::new());
        let client_disconnected = Arc::new(AtomicBool::new(false));
        let disconnected = Arc::new(Notify::new());
        let task_connected = Arc::clone(&connected);
        let task_client_disconnected = Arc::clone(&client_disconnected);
        let task_disconnected = Arc::clone(&disconnected);
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            task_connected.notify_one();
            let mut buffer = [0_u8; 1_024];
            loop {
                match socket.read(&mut buffer).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {}
                }
            }
            task_client_disconnected.store(true, Ordering::Release);
            task_disconnected.notify_one();
        });
        Self {
            url: format!("ws://{address}"),
            connected,
            client_disconnected,
            disconnected,
            task,
        }
    }

    fn url(&self) -> reqwest::Url {
        self.url.parse().expect("stream URL")
    }

    async fn wait_for_connection(&self) {
        tokio::time::timeout(std::time::Duration::from_secs(2), self.connected.notified())
            .await
            .expect("connection accepted");
    }

    async fn wait_for_client_disconnect(&self) {
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                if self.client_disconnected.load(Ordering::Acquire) {
                    break;
                }
                self.disconnected.notified().await;
            }
        })
        .await
        .expect("producer disconnected");
    }
}

impl Drop for StalledHandshakeServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[allow(clippy::result_large_err)]
fn capture_path(
    captured_path: Arc<std::sync::Mutex<Option<String>>>,
) -> impl FnOnce(&Request, Response) -> Result<Response, ErrorResponse> {
    move |request: &Request, response: Response| {
        *captured_path.lock().expect("request path lock") = Some(request.uri().to_string());
        Ok(response)
    }
}

fn binance_with_stream_url(stream_url: reqwest::Url) -> BinanceClient {
    BinanceClient::new(
        reqwest::Client::builder().build().expect("test client"),
        "http://127.0.0.1/".parse().expect("REST URL"),
        stream_url,
        None,
    )
}

fn btc_instrument() -> Instrument {
    Instrument::crypto("BTC", "USDT", "Bitcoin").expect("instrument")
}

fn eth_instrument() -> Instrument {
    Instrument::crypto("ETH", "USDT", "Ethereum").expect("instrument")
}

fn binance_ticker_message() -> String {
    r#"{
        "stream":"btcusdt@ticker",
        "data":{
            "e":"24hrTicker",
            "E":1787112345678,
            "s":"BTCUSDT",
            "c":"93742.18",
            "o":"91100.00",
            "h":"94500.00",
            "l":"90200.00",
            "v":"1234.567",
            "q":"114000000.00",
            "x":"91000.00"
        }
    }"#
    .into()
}
