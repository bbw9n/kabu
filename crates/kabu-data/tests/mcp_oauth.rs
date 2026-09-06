use std::{
    collections::HashMap,
    io::{self, Write},
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use kabu_core::{Brokerage, BrokerageErrorKind};
use kabu_data::{BrokerCredentialStore, BrokerOAuth, SecretStore, SecretStoreError};
use rmcp::transport::auth::{CredentialStore, StoredCredentials};
use rmcp::transport::streamable_http_client::{
    StreamableHttpClient, StreamableHttpClientTransport, StreamableHttpClientTransportConfig,
};
use serde_json::json;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tokio_util::sync::CancellationToken;
use tracing::{Level, instrument::WithSubscriber};
use tracing_subscriber::fmt::MakeWriter;
use url::Url;
use wiremock::{
    Mock, MockServer, Request, ResponseTemplate,
    matchers::{method, path},
};

const SERVICE: &str = "com.kabu.mcp.oauth";
const PRIVATE_AUTHORIZATION_CODE: &str = "kabu-private-authorization-code-8317";
const PRIVATE_ACCESS_TOKEN: &str = "kabu-private-access-token-9428";
const PRIVATE_REFRESH_TOKEN: &str = "kabu-private-refresh-token-0539";

#[derive(Clone)]
struct TraceCapture {
    bytes: Arc<Mutex<Vec<u8>>>,
}

impl TraceCapture {
    fn new() -> Self {
        Self {
            bytes: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn contents(&self) -> String {
        String::from_utf8(self.bytes.lock().expect("trace capture lock").clone())
            .expect("trace capture is UTF-8")
    }
}

struct TraceCaptureWriter {
    bytes: Arc<Mutex<Vec<u8>>>,
}

impl Write for TraceCaptureWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.bytes
            .lock()
            .map_err(|_| io::Error::other("trace capture lock failed"))?
            .extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'writer> MakeWriter<'writer> for TraceCapture {
    type Writer = TraceCaptureWriter;

    fn make_writer(&'writer self) -> Self::Writer {
        TraceCaptureWriter {
            bytes: self.bytes.clone(),
        }
    }
}

#[derive(Default)]
struct MemorySecretStore {
    values: Mutex<HashMap<(String, String), Vec<u8>>>,
}

impl MemorySecretStore {
    fn service_names(&self) -> Vec<String> {
        let mut names: Vec<_> = self
            .values
            .lock()
            .expect("secret store lock")
            .keys()
            .map(|(service, _)| service.clone())
            .collect();
        names.sort();
        names.dedup();
        names
    }

    fn account_names(&self) -> Vec<String> {
        let mut names: Vec<_> = self
            .values
            .lock()
            .expect("secret store lock")
            .keys()
            .map(|(_, account)| account.clone())
            .collect();
        names.sort();
        names.dedup();
        names
    }
}

impl SecretStore for MemorySecretStore {
    fn get(&self, service: &str, account: &str) -> Result<Option<Vec<u8>>, SecretStoreError> {
        Ok(self
            .values
            .lock()
            .expect("secret store lock")
            .get(&(service.to_string(), account.to_string()))
            .cloned())
    }

    fn set(&self, service: &str, account: &str, value: &[u8]) -> Result<(), SecretStoreError> {
        self.values
            .lock()
            .expect("secret store lock")
            .insert((service.to_string(), account.to_string()), value.to_vec());
        Ok(())
    }

    fn delete(&self, service: &str, account: &str) -> Result<(), SecretStoreError> {
        self.values
            .lock()
            .expect("secret store lock")
            .remove(&(service.to_string(), account.to_string()));
        Ok(())
    }
}

#[derive(Default)]
struct SlowSecretStore {
    values: Mutex<HashMap<(String, String), Vec<u8>>>,
    set_started: AtomicUsize,
    set_finished: AtomicUsize,
    set_release: (Mutex<bool>, Condvar),
}

impl SlowSecretStore {
    fn release_set(&self) {
        let (released, wake) = &self.set_release;
        *released.lock().expect("slow secret release lock") = true;
        wake.notify_all();
    }
}

impl SecretStore for SlowSecretStore {
    fn get(&self, service: &str, account: &str) -> Result<Option<Vec<u8>>, SecretStoreError> {
        Ok(self
            .values
            .lock()
            .expect("slow secret values lock")
            .get(&(service.to_string(), account.to_string()))
            .cloned())
    }

    fn set(&self, service: &str, account: &str, value: &[u8]) -> Result<(), SecretStoreError> {
        self.set_started.fetch_add(1, Ordering::SeqCst);
        let (released, wake) = &self.set_release;
        let mut released = released.lock().expect("slow secret release lock");
        while !*released {
            released = wake.wait(released).expect("slow secret release wait");
        }
        drop(released);
        self.values
            .lock()
            .expect("slow secret values lock")
            .insert((service.to_string(), account.to_string()), value.to_vec());
        self.set_finished.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn delete(&self, service: &str, account: &str) -> Result<(), SecretStoreError> {
        self.values
            .lock()
            .expect("slow secret values lock")
            .remove(&(service.to_string(), account.to_string()));
        Ok(())
    }
}

struct UnavailableSecretStore;

impl SecretStore for UnavailableSecretStore {
    fn get(&self, _service: &str, _account: &str) -> Result<Option<Vec<u8>>, SecretStoreError> {
        Err(SecretStoreError::Unavailable)
    }

    fn set(&self, _service: &str, _account: &str, _value: &[u8]) -> Result<(), SecretStoreError> {
        Err(SecretStoreError::Unavailable)
    }

    fn delete(&self, _service: &str, _account: &str) -> Result<(), SecretStoreError> {
        Err(SecretStoreError::Unavailable)
    }
}

fn fixture_credentials(access_token: &str, refresh_token: &str) -> StoredCredentials {
    serde_json::from_value(json!({
        "client_id": "fixture-client",
        "token_response": {
            "access_token": access_token,
            "token_type": "bearer",
            "refresh_token": refresh_token,
            "expires_in": 3600
        },
        "granted_scopes": ["portfolio.read"],
        "token_received_at": null,
        "issuer": null
    }))
    .expect("valid credential fixture")
}

#[tokio::test]
async fn broker_credentials_round_trip_without_json_persistence() {
    let secrets = Arc::new(MemorySecretStore::default());
    let store = BrokerCredentialStore::new(Brokerage::Robinhood, secrets.clone());
    let value = fixture_credentials("access-secret", "refresh-secret");

    store.save(value.clone()).await.expect("save");
    let loaded = store.load().await.expect("load").expect("stored value");
    let loaded_json = serde_json::to_value(loaded).expect("loaded JSON");
    let expected_json = serde_json::to_value(value).expect("expected JSON");
    assert!(
        loaded_json == expected_json,
        "credential round trip differed"
    );
    assert_eq!(secrets.service_names(), vec![SERVICE]);
    assert_eq!(secrets.account_names(), vec!["robinhood"]);

    store.clear().await.expect("clear");
    assert!(store.load().await.expect("load after clear").is_none());
}

#[tokio::test]
async fn credential_refresh_guards_serialize_concurrent_refreshes() {
    let store = Arc::new(BrokerCredentialStore::new(
        Brokerage::Robinhood,
        Arc::new(MemorySecretStore::default()),
    ));
    let first = store
        .acquire_refresh_guard()
        .await
        .expect("first guard")
        .expect("refresh coordination enabled");
    let other_store = store.clone();
    let second = tokio::spawn(async move { other_store.acquire_refresh_guard().await });

    tokio::time::sleep(Duration::from_millis(25)).await;
    assert!(
        !second.is_finished(),
        "a second refresh entered concurrently"
    );
    drop(first);

    let second_guard = tokio::time::timeout(Duration::from_secs(1), second)
        .await
        .expect("second refresh unblocked")
        .expect("refresh task")
        .expect("refresh guard result");
    assert!(second_guard.is_some());
}

#[tokio::test]
async fn secret_store_failures_do_not_expose_accounts_or_credentials() {
    let store = BrokerCredentialStore::new(Brokerage::Robinhood, Arc::new(UnavailableSecretStore));
    let error = store
        .save(fixture_credentials("access-secret", "refresh-secret"))
        .await
        .expect_err("save must fail");
    let display = error.to_string();

    assert!(!display.contains("access-secret"));
    assert!(!display.contains("refresh-secret"));
    assert!(!display.contains("robinhood"));
    assert!(!display.contains(SERVICE));
}

async fn oauth_fixture() -> (
    BrokerOAuth,
    Arc<AtomicUsize>,
    Arc<MemorySecretStore>,
    MockServer,
) {
    let secrets = Arc::new(MemorySecretStore::default());
    let (oauth, exchange_counter, server) = oauth_fixture_with_secret_store(secrets.clone()).await;
    (oauth, exchange_counter, secrets, server)
}

async fn oauth_fixture_with_secret_store(
    secrets: Arc<dyn SecretStore>,
) -> (BrokerOAuth, Arc<AtomicUsize>, MockServer) {
    let server = MockServer::start().await;
    let endpoint = Url::parse(&server.uri())
        .expect("server URL")
        .join("resource")
        .expect("resource URL");
    let issuer = server.uri();

    Mock::given(method("GET"))
        .and(path("/resource"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "resource": endpoint.as_str(),
            "authorization_servers": [issuer]
        })))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/.well-known/oauth-authorization-server"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "issuer": server.uri(),
            "authorization_endpoint": server.uri() + "/authorize",
            "token_endpoint": server.uri() + "/token",
            "registration_endpoint": server.uri() + "/register",
            "scopes_supported": ["portfolio.read"],
            "response_types_supported": ["code"],
            "code_challenge_methods_supported": ["S256"]
        })))
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path("/register"))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({
            "client_id": "fixture-client",
            "redirect_uris": []
        })))
        .mount(&server)
        .await;

    let exchange_counter = Arc::new(AtomicUsize::new(0));
    let token_counter = exchange_counter.clone();
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(move |_request: &Request| {
            token_counter.fetch_add(1, Ordering::SeqCst);
            ResponseTemplate::new(200).set_body_json(json!({
                "access_token": PRIVATE_ACCESS_TOKEN,
                "token_type": "bearer",
                "refresh_token": PRIVATE_REFRESH_TOKEN,
                "expires_in": 3600
            }))
        })
        .mount(&server)
        .await;

    let credentials = Arc::new(BrokerCredentialStore::new(Brokerage::Robinhood, secrets));
    let oauth = BrokerOAuth::new(Brokerage::Robinhood, endpoint, credentials);
    (oauth, exchange_counter, server)
}

async fn delayed_oauth_fixture(
    metadata_delay: Duration,
    token_delay: Duration,
) -> (
    BrokerOAuth,
    Arc<AtomicUsize>,
    Arc<AtomicUsize>,
    Arc<MemorySecretStore>,
    MockServer,
) {
    let server = MockServer::start().await;
    let endpoint = Url::parse(&server.uri())
        .expect("server URL")
        .join("resource")
        .expect("resource URL");
    let issuer = server.uri();
    let metadata_started = Arc::new(AtomicUsize::new(0));
    let metadata_probe = metadata_started.clone();
    let metadata_endpoint = endpoint.clone();

    Mock::given(method("GET"))
        .and(path("/resource"))
        .respond_with(move |_request: &Request| {
            metadata_probe.fetch_add(1, Ordering::SeqCst);
            ResponseTemplate::new(200)
                .set_delay(metadata_delay)
                .set_body_json(json!({
                    "resource": metadata_endpoint.as_str(),
                    "authorization_servers": [issuer]
                }))
        })
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/.well-known/oauth-authorization-server"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "issuer": server.uri(),
            "authorization_endpoint": server.uri() + "/authorize",
            "token_endpoint": server.uri() + "/token",
            "registration_endpoint": server.uri() + "/register",
            "scopes_supported": ["portfolio.read"],
            "response_types_supported": ["code"],
            "code_challenge_methods_supported": ["S256"]
        })))
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path("/register"))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({
            "client_id": "fixture-client",
            "redirect_uris": []
        })))
        .mount(&server)
        .await;

    let token_started = Arc::new(AtomicUsize::new(0));
    let token_probe = token_started.clone();
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(move |_request: &Request| {
            token_probe.fetch_add(1, Ordering::SeqCst);
            ResponseTemplate::new(200)
                .set_delay(token_delay)
                .set_body_json(json!({
                    "access_token": PRIVATE_ACCESS_TOKEN,
                    "token_type": "bearer",
                    "refresh_token": PRIVATE_REFRESH_TOKEN,
                    "expires_in": 3600
                }))
        })
        .mount(&server)
        .await;

    let secrets = Arc::new(MemorySecretStore::default());
    let credentials = Arc::new(BrokerCredentialStore::new(
        Brokerage::Robinhood,
        secrets.clone(),
    ));
    let oauth = BrokerOAuth::new(Brokerage::Robinhood, endpoint, credentials);
    (oauth, metadata_started, token_started, secrets, server)
}

async fn wait_for_atomic(counter: &AtomicUsize, expected: usize) {
    tokio::time::timeout(Duration::from_secs(1), async {
        while counter.load(Ordering::SeqCst) < expected {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("request started");
}

fn authorization_state(url: &Url) -> String {
    url.query_pairs()
        .find_map(|(key, value)| (key == "state").then(|| value.into_owned()))
        .expect("authorization state")
}

#[tokio::test]
async fn cancellation_stops_stalled_metadata_inside_begin_authorization() {
    let (oauth, metadata_started, _token_started, _secrets, _server) =
        delayed_oauth_fixture(Duration::from_secs(60), Duration::ZERO).await;
    let oauth = Arc::new(oauth);
    let cancellation = CancellationToken::new();
    let task = tokio::spawn({
        let oauth = oauth.clone();
        let cancellation = cancellation.clone();
        async move { oauth.begin_authorization_cancellable(cancellation).await }
    });

    wait_for_atomic(&metadata_started, 1).await;
    cancellation.cancel();
    let result = tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .expect("metadata cancellation finishes")
        .expect("metadata task joins");
    let Err(error) = result else {
        panic!("cancelled metadata cannot authorize");
    };

    assert_eq!(error.kind, BrokerageErrorKind::AuthorizationDenied);
    assert_eq!(oauth.pending_authorization_count().await, 0);
}

#[tokio::test]
async fn cancellation_discards_pending_state_during_stalled_token_exchange() {
    let (oauth, _metadata_started, token_started, secrets, _server) =
        delayed_oauth_fixture(Duration::ZERO, Duration::from_secs(60)).await;
    let oauth = Arc::new(oauth);
    let pending = oauth
        .begin_authorization()
        .await
        .expect("begin authorization");
    let state = authorization_state(&pending.authorization_url);
    let callback = format!("code=abc&state={state}");
    let cancellation = CancellationToken::new();
    let task = tokio::spawn({
        let oauth = oauth.clone();
        let cancellation = cancellation.clone();
        async move {
            oauth
                .complete_authorization_cancellable(pending, &callback, cancellation)
                .await
        }
    });

    wait_for_atomic(&token_started, 1).await;
    cancellation.cancel();
    let error = tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .expect("token cancellation finishes")
        .expect("token task joins")
        .expect_err("cancelled token exchange cannot authorize");

    assert_eq!(error.kind, BrokerageErrorKind::AuthorizationDenied);
    assert_eq!(oauth.pending_authorization_count().await, 0);
    assert!(
        BrokerCredentialStore::new(Brokerage::Robinhood, secrets)
            .load()
            .await
            .expect("load credentials after cancellation")
            .is_none(),
        "a cancelled exchange must not retain credentials"
    );
}

#[tokio::test]
async fn cancelled_authorization_waits_for_its_credential_write_before_disconnect_clears() {
    let secrets = Arc::new(SlowSecretStore::default());
    let (oauth, _exchange_counter, _server) =
        oauth_fixture_with_secret_store(secrets.clone()).await;
    let oauth = Arc::new(oauth);
    let pending = oauth
        .begin_authorization()
        .await
        .expect("begin authorization");
    let state = authorization_state(&pending.authorization_url);
    let callback = format!("code=abc&state={state}");
    let cancellation = CancellationToken::new();
    let completion = tokio::spawn({
        let oauth = oauth.clone();
        let cancellation = cancellation.clone();
        async move {
            oauth
                .complete_authorization_cancellable(pending, &callback, cancellation)
                .await
        }
    });

    wait_for_atomic(&secrets.set_started, 1).await;
    cancellation.cancel();
    let completion_finished_before_write =
        tokio::time::timeout(Duration::from_millis(100), async {
            while !completion.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .is_ok();

    secrets.release_set();
    let error = completion
        .await
        .expect("authorization task joins")
        .expect_err("cancelled authorization cannot succeed");
    wait_for_atomic(&secrets.set_finished, 1).await;
    assert_eq!(error.kind, BrokerageErrorKind::AuthorizationDenied);

    oauth
        .disconnect()
        .await
        .expect("disconnect clears credentials");

    assert!(
        !completion_finished_before_write,
        "the cancelled authorization returned before its credential write completed"
    );
    assert!(
        BrokerCredentialStore::new(Brokerage::Robinhood, secrets)
            .load()
            .await
            .expect("load credentials after disconnect")
            .is_none(),
        "disconnect must not be undone by a cancelled credential write"
    );
}

#[tokio::test]
async fn oauth_callback_rejects_wrong_state_before_exchanging_code() {
    let (oauth, exchange_counter, _secrets, _server) = oauth_fixture().await;
    let pending = oauth.begin_authorization().await.expect("begin");
    let error = oauth
        .complete_authorization(pending, "code=abc&state=wrong")
        .await
        .expect_err("state mismatch");

    assert_eq!(error.kind, BrokerageErrorKind::AuthorizationDenied);
    assert_eq!(exchange_counter.load(Ordering::SeqCst), 0);
    assert_eq!(oauth.pending_authorization_count().await, 0);
}

#[tokio::test]
async fn oauth_callback_expires_after_ten_minutes_before_exchanging_code() {
    let (oauth, exchange_counter, _secrets, _server) = oauth_fixture().await;
    let pending = oauth.begin_authorization().await.expect("begin");
    let state = authorization_state(&pending.authorization_url);
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(601)).await;
    let callback = format!("code=abc&state={state}");

    let error = oauth
        .complete_authorization(pending, &callback)
        .await
        .expect_err("expired callback");

    assert_eq!(error.kind, BrokerageErrorKind::AuthorizationDenied);
    assert_eq!(exchange_counter.load(Ordering::SeqCst), 0);
    assert_eq!(oauth.pending_authorization_count().await, 0);
}

#[tokio::test]
async fn cancelling_pending_authorization_deletes_state() {
    let (oauth, exchange_counter, _secrets, _server) = oauth_fixture().await;
    let pending = oauth.begin_authorization().await.expect("begin");
    assert_eq!(oauth.pending_authorization_count().await, 1);

    pending.cancel().await.expect("cancel authorization");

    assert_eq!(oauth.pending_authorization_count().await, 0);
    assert_eq!(exchange_counter.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn callback_missing_state_deletes_pending_authorization() {
    let (oauth, exchange_counter, _secrets, _server) = oauth_fixture().await;
    let pending = oauth.begin_authorization().await.expect("begin");
    assert_eq!(oauth.pending_authorization_count().await, 1);

    let error = oauth
        .complete_authorization(pending, "code=abc")
        .await
        .expect_err("missing state");

    assert_eq!(error.kind, BrokerageErrorKind::AuthorizationDenied);
    assert_eq!(oauth.pending_authorization_count().await, 0);
    assert_eq!(exchange_counter.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn expired_authorization_state_becomes_unavailable_without_a_callback() {
    let (oauth, exchange_counter, _secrets, _server) = oauth_fixture().await;
    let pending = oauth.begin_authorization().await.expect("begin");
    assert_eq!(oauth.pending_authorization_count().await, 1);
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(601)).await;
    tokio::task::yield_now().await;

    let error = pending
        .cancel()
        .await
        .expect_err("expired state is unavailable");
    assert_eq!(error.kind, BrokerageErrorKind::AuthorizationDenied);
    assert_eq!(oauth.pending_authorization_count().await, 0);
    assert_eq!(exchange_counter.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn oauth_callback_exchanges_code_and_saves_credentials() {
    let (oauth, exchange_counter, secrets, _server) = oauth_fixture().await;
    let pending = oauth.begin_authorization().await.expect("begin");
    let state = authorization_state(&pending.authorization_url);
    let callback = format!("code=abc&state={state}");

    oauth
        .complete_authorization(pending, &callback)
        .await
        .expect("complete authorization");
    let stored = BrokerCredentialStore::new(Brokerage::Robinhood, secrets)
        .load()
        .await
        .expect("load completed credentials");

    assert_eq!(exchange_counter.load(Ordering::SeqCst), 1);
    assert!(stored.is_some());
    assert_eq!(oauth.pending_authorization_count().await, 0);
}

#[tokio::test]
async fn successful_oauth_exchange_keeps_secrets_out_of_debug_tracing() {
    let (oauth, exchange_counter, _secrets, _server) = oauth_fixture().await;
    let pending = oauth.begin_authorization().await.expect("begin");
    let state = authorization_state(&pending.authorization_url);
    let callback = format!("code={PRIVATE_AUTHORIZATION_CODE}&state={state}");
    let capture = TraceCapture::new();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(Level::DEBUG)
        .without_time()
        .with_writer(capture.clone())
        .finish();

    oauth
        .complete_authorization(pending, &callback)
        .with_subscriber(subscriber)
        .await
        .expect("complete authorization");
    let traces = capture.contents();

    assert_eq!(exchange_counter.load(Ordering::SeqCst), 1);
    assert!(
        !traces.contains(PRIVATE_AUTHORIZATION_CODE),
        "authorization code entered debug tracing"
    );
    assert!(
        !traces.contains(PRIVATE_ACCESS_TOKEN),
        "access token entered debug tracing"
    );
    assert!(
        !traces.contains(PRIVATE_REFRESH_TOKEN),
        "refresh token entered debug tracing"
    );
}

#[tokio::test]
async fn one_shot_callback_yields_only_the_query_and_returns_a_fixed_page() {
    let (oauth, _exchange_counter, _secrets, _server) = oauth_fixture().await;
    let mut pending = oauth.begin_authorization().await.expect("begin");
    let redirect = pending.redirect_uri.clone();
    let state = authorization_state(&pending.authorization_url);
    let client = tokio::spawn(async move {
        let address = format!(
            "{}:{}",
            redirect.host_str().expect("callback host"),
            redirect.port().expect("callback port")
        );
        let mut stream = TcpStream::connect(address).await.expect("connect callback");
        let request =
            format!("GET /callback?code=abc&state={state} HTTP/1.1\r\nHost: localhost\r\n\r\n");
        stream
            .write_all(request.as_bytes())
            .await
            .expect("write callback");
        let mut response = Vec::new();
        stream
            .read_to_end(&mut response)
            .await
            .expect("read callback response");
        response
    });

    let query = pending.wait_for_callback().await.expect("callback query");
    let response = client.await.expect("callback client");

    assert!(query.starts_with("code=abc&state="));
    assert!(!query.contains("/callback"));
    assert!(String::from_utf8_lossy(&response).contains("Authorization received"));
}

#[tokio::test]
async fn one_shot_callback_rejects_request_lines_over_eight_kib() {
    let (oauth, _exchange_counter, _secrets, _server) = oauth_fixture().await;
    let mut pending = oauth.begin_authorization().await.expect("begin");
    let redirect = pending.redirect_uri.clone();
    let client = tokio::spawn(async move {
        let address = format!(
            "{}:{}",
            redirect.host_str().expect("callback host"),
            redirect.port().expect("callback port")
        );
        let mut stream = TcpStream::connect(address).await.expect("connect callback");
        let request = format!(
            "GET /callback?padding={} HTTP/1.1\r\n\r\n",
            "x".repeat(8192)
        );
        stream
            .write_all(request.as_bytes())
            .await
            .expect("write callback");
        let mut response = Vec::new();
        stream
            .read_to_end(&mut response)
            .await
            .expect("read callback response");
        response
    });

    let error = pending
        .wait_for_callback()
        .await
        .expect_err("oversized request line");
    let response = client.await.expect("callback client");

    assert_eq!(error.kind, BrokerageErrorKind::MalformedResponse);
    assert!(String::from_utf8_lossy(&response).contains("Authorization failed"));
}

#[tokio::test]
async fn authorized_http_client_uses_stored_credentials() {
    let server = MockServer::start().await;
    let endpoint = Url::parse(&server.uri()).expect("server URL");
    let saw_bearer = Arc::new(AtomicBool::new(false));
    let responder_flag = saw_bearer.clone();

    Mock::given(method("GET"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "resource": endpoint.as_str(),
            "authorization_servers": [server.uri()]
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/.well-known/oauth-authorization-server"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "issuer": server.uri(),
            "authorization_endpoint": server.uri() + "/authorize",
            "token_endpoint": server.uri() + "/token",
            "registration_endpoint": server.uri() + "/register",
            "code_challenge_methods_supported": ["S256"]
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/protected"))
        .respond_with(move |request: &Request| {
            let matches = request
                .headers
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| value == "Bearer access-secret");
            responder_flag.store(matches, Ordering::SeqCst);
            ResponseTemplate::new(200)
        })
        .mount(&server)
        .await;

    let secrets = Arc::new(MemorySecretStore::default());
    let credentials = Arc::new(BrokerCredentialStore::new(Brokerage::Robinhood, secrets));
    credentials
        .save(fixture_credentials("access-secret", "refresh-secret"))
        .await
        .expect("save fixture credentials");
    let oauth = BrokerOAuth::new(Brokerage::Robinhood, endpoint.clone(), credentials);
    let client = oauth
        .authorized_http_client()
        .await
        .expect("authorized client");

    client
        .get(endpoint.join("protected").expect("protected URL"))
        .await
        .expect("authorized request");
    assert!(saw_bearer.load(Ordering::SeqCst));
}

#[tokio::test]
async fn initialized_oauth_client_satisfies_streamable_transport_boundary() {
    let server = MockServer::start().await;
    let endpoint = Url::parse(&server.uri()).expect("server URL");

    Mock::given(method("GET"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "resource": endpoint.as_str(),
            "authorization_servers": [server.uri()]
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/.well-known/oauth-authorization-server"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "issuer": server.uri(),
            "authorization_endpoint": server.uri() + "/authorize",
            "token_endpoint": server.uri() + "/token",
            "registration_endpoint": server.uri() + "/register",
            "code_challenge_methods_supported": ["S256"]
        })))
        .mount(&server)
        .await;

    let secrets = Arc::new(MemorySecretStore::default());
    let credentials = Arc::new(BrokerCredentialStore::new(Brokerage::Robinhood, secrets));
    credentials
        .save(fixture_credentials("access-secret", "refresh-secret"))
        .await
        .expect("save fixture credentials");
    let oauth = BrokerOAuth::new(Brokerage::Robinhood, endpoint.clone(), credentials);

    let client = oauth
        .authorized_transport_client()
        .await
        .expect("authorized transport client");
    fn assert_streamable<C: StreamableHttpClient>(_client: &C) {}
    assert_streamable(&client);

    let transport = StreamableHttpClientTransport::with_client(
        client,
        StreamableHttpClientTransportConfig::with_uri(endpoint.as_str()),
    );
    drop(transport);
}
