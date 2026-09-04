use std::{collections::HashMap, future::Future, net::Ipv4Addr, sync::Arc, time::Duration};

use async_trait::async_trait;
use kabu_core::{Brokerage, BrokerageError, BrokerageErrorKind};
use rmcp::transport::auth::{
    AuthClient, AuthError, AuthorizationManager, AuthorizationRequest, AuthorizationSession,
    AuthorizedHttpClient, CredentialRefreshGuard, CredentialStore, StateStore,
    StoredAuthorizationState, StoredCredentials,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Mutex, RwLock},
    task::spawn_blocking,
    time::{Instant, sleep_until},
};
use tokio_util::sync::CancellationToken;
use tracing::{instrument::WithSubscriber, subscriber::NoSubscriber};
use url::{Url, form_urlencoded};
use zeroize::Zeroizing;

use crate::{SecretStore, SecretStoreError};

use super::capabilities::BrokerMcpHttpClient;

const KEYCHAIN_SERVICE: &str = "com.kabu.mcp.oauth";
const CALLBACK_TTL: Duration = Duration::from_secs(600);
const MAX_REQUEST_LINE_BYTES: usize = 8 * 1024;
const CALLBACK_SUCCESS_PAGE: &str = "<!doctype html><title>Kabu authorization</title><p>Authorization received. You can return to Kabu.</p>";
const CALLBACK_FAILURE_PAGE: &str = "<!doctype html><title>Kabu authorization</title><p>Authorization failed. Return to Kabu and try again.</p>";

async fn suppress_oauth_tracing<F>(future: F) -> F::Output
where
    F: Future,
{
    future.with_subscriber(NoSubscriber::default()).await
}

fn authorization_cancelled() -> BrokerageError {
    brokerage_error(BrokerageErrorKind::AuthorizationDenied)
}

async fn await_authorization_phase<T>(
    cancellation: &CancellationToken,
    operation: impl Future<Output = Result<T, BrokerageError>>,
) -> Result<T, BrokerageError> {
    if cancellation.is_cancelled() {
        return Err(authorization_cancelled());
    }
    tokio::select! {
        biased;
        result = operation => result,
        _ = cancellation.cancelled() => Err(authorization_cancelled()),
    }
}

/// OAuth credentials scoped to one brokerage account in secure storage.
#[derive(Clone)]
pub struct BrokerCredentialStore {
    brokerage: Brokerage,
    secrets: Arc<dyn SecretStore>,
    refresh_lock: Arc<Mutex<()>>,
}

impl BrokerCredentialStore {
    pub fn new(brokerage: Brokerage, secrets: Arc<dyn SecretStore>) -> Self {
        Self {
            brokerage,
            secrets,
            refresh_lock: Arc::new(Mutex::new(())),
        }
    }

    fn account_name(&self) -> &'static str {
        match self.brokerage {
            Brokerage::Robinhood => "robinhood",
            Brokerage::Longbridge => "longbridge",
        }
    }

    async fn read_secret(&self) -> Result<Option<Vec<u8>>, AuthError> {
        let secrets = self.secrets.clone();
        let account = self.account_name();
        spawn_blocking(move || secrets.get(KEYCHAIN_SERVICE, account))
            .await
            .map_err(|_| credential_task_error())?
            .map_err(credential_store_error)
    }

    async fn write_secret(&self, value: Vec<u8>) -> Result<(), AuthError> {
        let secrets = self.secrets.clone();
        let account = self.account_name();
        spawn_blocking(move || {
            let value = Zeroizing::new(value);
            secrets.set(KEYCHAIN_SERVICE, account, value.as_slice())
        })
        .await
        .map_err(|_| credential_task_error())?
        .map_err(credential_store_error)
    }

    async fn delete_secret(&self) -> Result<(), AuthError> {
        let secrets = self.secrets.clone();
        let account = self.account_name();
        spawn_blocking(move || secrets.delete(KEYCHAIN_SERVICE, account))
            .await
            .map_err(|_| credential_task_error())?
            .map_err(credential_store_error)
    }
}

fn credential_store_error(_error: SecretStoreError) -> AuthError {
    AuthError::CredentialStoreError("secure credential storage failed".to_string())
}

fn credential_task_error() -> AuthError {
    AuthError::CredentialStoreError("secure credential storage task failed".to_string())
}

#[async_trait]
impl CredentialStore for BrokerCredentialStore {
    async fn load(&self) -> Result<Option<StoredCredentials>, AuthError> {
        let Some(bytes) = self.read_secret().await? else {
            return Ok(None);
        };
        let bytes = Zeroizing::new(bytes);
        serde_json::from_slice(bytes.as_slice())
            .map(Some)
            .map_err(|_| {
                AuthError::CredentialStoreError("stored broker credentials are invalid".to_string())
            })
    }

    async fn save(&self, credentials: StoredCredentials) -> Result<(), AuthError> {
        let bytes = serde_json::to_vec(&credentials).map_err(|_| {
            AuthError::CredentialStoreError("broker credentials could not be encoded".to_string())
        })?;
        self.write_secret(bytes).await
    }

    async fn clear(&self) -> Result<(), AuthError> {
        self.delete_secret().await
    }

    async fn acquire_refresh_guard(&self) -> Result<Option<CredentialRefreshGuard>, AuthError> {
        let guard = self.refresh_lock.clone().lock_owned().await;
        Ok(Some(CredentialRefreshGuard::new(guard)))
    }
}

struct AuthorizationStateEntry {
    state: StoredAuthorizationState,
    expires_at: Instant,
}

/// Ephemeral PKCE state owned by one OAuth client instance.
///
/// It is deliberately memory-only: callback state and PKCE verifiers must not
/// survive a process restart, cancellation, or the ten-minute callback window.
#[derive(Clone)]
struct ExpiringStateStore {
    states: Arc<RwLock<HashMap<String, AuthorizationStateEntry>>>,
}

impl ExpiringStateStore {
    fn new() -> Self {
        Self {
            states: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    async fn discard(&self, csrf_token: &str) -> bool {
        let now = Instant::now();
        let mut states = self.states.write().await;
        match states.remove(csrf_token) {
            Some(entry) if entry.expires_at > now => true,
            Some(_) | None => false,
        }
    }

    async fn live_count(&self) -> usize {
        let now = Instant::now();
        let mut states = self.states.write().await;
        states.retain(|_, entry| entry.expires_at > now);
        states.len()
    }
}

#[async_trait]
impl StateStore for ExpiringStateStore {
    async fn save(
        &self,
        csrf_token: &str,
        state: StoredAuthorizationState,
    ) -> Result<(), AuthError> {
        let expires_at = Instant::now() + CALLBACK_TTL;
        self.states.write().await.insert(
            csrf_token.to_string(),
            AuthorizationStateEntry { state, expires_at },
        );

        let states = Arc::downgrade(&self.states);
        let csrf_token = csrf_token.to_string();
        tokio::spawn(async move {
            sleep_until(expires_at).await;
            let Some(states) = states.upgrade() else {
                return;
            };
            let mut states = states.write().await;
            if states
                .get(&csrf_token)
                .is_some_and(|entry| entry.expires_at <= Instant::now())
            {
                states.remove(&csrf_token);
            }
        });
        Ok(())
    }

    async fn load(&self, csrf_token: &str) -> Result<Option<StoredAuthorizationState>, AuthError> {
        let now = Instant::now();
        let mut states = self.states.write().await;
        if states
            .get(csrf_token)
            .is_some_and(|entry| entry.expires_at <= now)
        {
            states.remove(csrf_token);
            return Ok(None);
        }
        Ok(states.get(csrf_token).map(|entry| entry.state.clone()))
    }

    async fn delete(&self, csrf_token: &str) -> Result<(), AuthError> {
        self.discard(csrf_token).await;
        Ok(())
    }
}

struct OneShotCallback {
    listener: Option<TcpListener>,
}

impl OneShotCallback {
    async fn bind() -> Result<(Self, Url), BrokerageError> {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .map_err(|_| brokerage_error(BrokerageErrorKind::Unavailable))?;
        let port = listener
            .local_addr()
            .map_err(|_| brokerage_error(BrokerageErrorKind::Internal))?
            .port();
        let redirect_uri = Url::parse(&format!("http://127.0.0.1:{port}/callback"))
            .map_err(|_| brokerage_error(BrokerageErrorKind::Internal))?;
        Ok((
            Self {
                listener: Some(listener),
            },
            redirect_uri,
        ))
    }

    async fn wait(&mut self) -> Result<String, BrokerageError> {
        let listener = self
            .listener
            .take()
            .ok_or_else(|| brokerage_error(BrokerageErrorKind::Internal))?;
        let (mut stream, _) = listener
            .accept()
            .await
            .map_err(|_| brokerage_error(BrokerageErrorKind::Unavailable))?;
        match read_callback_query(&mut stream).await {
            Ok(query) => {
                write_callback_page(&mut stream, 200, CALLBACK_SUCCESS_PAGE).await?;
                Ok(query)
            }
            Err(error) => {
                let _ = write_callback_page(&mut stream, 400, CALLBACK_FAILURE_PAGE).await;
                Err(error)
            }
        }
    }
}

async fn read_callback_query(stream: &mut TcpStream) -> Result<String, BrokerageError> {
    let mut bytes = Vec::with_capacity(1024);
    let mut chunk = [0_u8; 1024];
    loop {
        let count = stream
            .read(&mut chunk)
            .await
            .map_err(|_| brokerage_error(BrokerageErrorKind::MalformedResponse))?;
        if count == 0 {
            return Err(brokerage_error(BrokerageErrorKind::MalformedResponse));
        }
        bytes.extend_from_slice(&chunk[..count]);
        if let Some(end) = bytes.windows(2).position(|window| window == b"\r\n") {
            if end > MAX_REQUEST_LINE_BYTES {
                return Err(brokerage_error(BrokerageErrorKind::MalformedResponse));
            }
            let line = std::str::from_utf8(&bytes[..end])
                .map_err(|_| brokerage_error(BrokerageErrorKind::MalformedResponse))?;
            return parse_request_target(line);
        }
        if bytes.len() > MAX_REQUEST_LINE_BYTES + 1 {
            return Err(brokerage_error(BrokerageErrorKind::MalformedResponse));
        }
    }
}

fn parse_request_target(request_line: &str) -> Result<String, BrokerageError> {
    let mut fields = request_line.split_ascii_whitespace();
    let method = fields.next();
    let target = fields.next();
    let version = fields.next();
    if method != Some("GET") || version != Some("HTTP/1.1") || fields.next().is_some() {
        return Err(brokerage_error(BrokerageErrorKind::MalformedResponse));
    }
    let target = target.ok_or_else(|| brokerage_error(BrokerageErrorKind::MalformedResponse))?;
    let url = Url::parse("http://127.0.0.1")
        .and_then(|base| base.join(target))
        .map_err(|_| brokerage_error(BrokerageErrorKind::MalformedResponse))?;
    if url.path() != "/callback" {
        return Err(brokerage_error(BrokerageErrorKind::MalformedResponse));
    }
    url.query()
        .filter(|query| !query.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| brokerage_error(BrokerageErrorKind::MalformedResponse))
}

async fn write_callback_page(
    stream: &mut TcpStream,
    status: u16,
    page: &str,
) -> Result<(), BrokerageError> {
    let reason = if status == 200 { "OK" } else { "Bad Request" };
    let header = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        page.len()
    );
    stream
        .write_all(header.as_bytes())
        .await
        .map_err(|_| brokerage_error(BrokerageErrorKind::Unavailable))?;
    stream
        .write_all(page.as_bytes())
        .await
        .map_err(|_| brokerage_error(BrokerageErrorKind::Unavailable))
}

/// An authorization attempt with a bound loopback callback.
pub struct PendingAuthorization {
    pub authorization_url: Url,
    pub redirect_uri: Url,
    started_at: Instant,
    csrf_token: String,
    callback: OneShotCallback,
    brokerage: Brokerage,
    session: AuthorizationSession,
    states: ExpiringStateStore,
}

impl PendingAuthorization {
    pub async fn wait_for_callback(&mut self) -> Result<String, BrokerageError> {
        self.callback.wait().await
    }

    /// Abandon this authorization attempt and securely discard its PKCE state.
    pub async fn cancel(self) -> Result<(), BrokerageError> {
        if self.states.discard(&self.csrf_token).await {
            Ok(())
        } else {
            Err(brokerage_error(BrokerageErrorKind::AuthorizationDenied))
        }
    }
}

/// OAuth orchestration for one hosted brokerage MCP endpoint.
pub struct BrokerOAuth {
    brokerage: Brokerage,
    endpoint: Url,
    credentials: Arc<BrokerCredentialStore>,
    states: ExpiringStateStore,
}

impl BrokerOAuth {
    pub fn new(
        brokerage: Brokerage,
        endpoint: Url,
        credentials: Arc<BrokerCredentialStore>,
    ) -> Self {
        Self {
            brokerage,
            endpoint,
            credentials,
            states: ExpiringStateStore::new(),
        }
    }

    pub async fn begin_authorization(&self) -> Result<PendingAuthorization, BrokerageError> {
        self.begin_authorization_cancellable(CancellationToken::new())
            .await
    }

    /// Begin authorization while allowing the owning runtime to stop metadata discovery safely.
    pub async fn begin_authorization_cancellable(
        &self,
        cancellation: CancellationToken,
    ) -> Result<PendingAuthorization, BrokerageError> {
        let (callback, redirect_uri) =
            await_authorization_phase(&cancellation, OneShotCallback::bind()).await?;
        let started_at = Instant::now();
        let mut manager =
            await_authorization_phase(&cancellation, self.authorization_manager()).await?;
        let resolution = await_authorization_phase(&cancellation, async {
            suppress_oauth_tracing(manager.resolve_metadata())
                .await
                .map_err(map_auth_error)
        })
        .await?;
        manager.set_metadata(resolution.metadata);
        let request = AuthorizationRequest::new(redirect_uri.as_str()).with_client_name("Kabu");
        let session = await_authorization_phase(&cancellation, async {
            suppress_oauth_tracing(AuthorizationSession::new(manager, request))
                .await
                .map_err(|(_, _)| brokerage_error(BrokerageErrorKind::AuthorizationDenied))
        })
        .await?;
        let authorization_url = Url::parse(session.get_authorization_url())
            .map_err(|_| brokerage_error(BrokerageErrorKind::MalformedResponse))?;
        let csrf_token = callback_parameter(&authorization_url, "state")
            .ok_or_else(|| brokerage_error(BrokerageErrorKind::MalformedResponse))?;
        let pending = PendingAuthorization {
            authorization_url,
            redirect_uri,
            started_at,
            csrf_token,
            callback,
            brokerage: self.brokerage,
            session,
            states: self.states.clone(),
        };
        if cancellation.is_cancelled() {
            let _ = pending.cancel().await;
            return Err(authorization_cancelled());
        }
        Ok(pending)
    }

    pub async fn complete_authorization(
        &self,
        pending: PendingAuthorization,
        callback_query: &str,
    ) -> Result<(), BrokerageError> {
        self.complete_authorization_cancellable(pending, callback_query, CancellationToken::new())
            .await
    }

    /// Complete authorization while retaining pending state until cancellation cleanup is awaited.
    pub async fn complete_authorization_cancellable(
        &self,
        pending: PendingAuthorization,
        callback_query: &str,
        cancellation: CancellationToken,
    ) -> Result<(), BrokerageError> {
        let result = await_authorization_phase(
            &cancellation,
            self.complete_authorization_inner(&pending, callback_query),
        )
        .await;
        _ = pending.cancel().await;
        result
    }

    async fn complete_authorization_inner(
        &self,
        pending: &PendingAuthorization,
        callback_query: &str,
    ) -> Result<(), BrokerageError> {
        if pending.brokerage != self.brokerage || pending.started_at.elapsed() > CALLBACK_TTL {
            return Err(brokerage_error(BrokerageErrorKind::AuthorizationDenied));
        }

        let callback = ParsedCallback::parse(callback_query)?;
        if callback.state != pending.csrf_token {
            return Err(brokerage_error(BrokerageErrorKind::AuthorizationDenied));
        }

        suppress_oauth_tracing(pending.session.handle_callback_with_issuer(
            &callback.code,
            &callback.state,
            callback.issuer.as_deref(),
        ))
        .await
        .map_err(|_| brokerage_error(BrokerageErrorKind::AuthorizationDenied))?;
        Ok(())
    }

    pub async fn disconnect(&self) -> Result<(), BrokerageError> {
        self.credentials
            .clear()
            .await
            .map_err(|_| brokerage_error(BrokerageErrorKind::SecureStorage))
    }

    pub async fn authorized_http_client(&self) -> Result<AuthorizedHttpClient, BrokerageError> {
        let manager = self.initialized_authorization_manager().await?;
        Ok(AuthorizedHttpClient::new(Arc::new(manager), None))
    }

    /// Build the rmcp authentication wrapper used by streamable HTTP sessions.
    pub async fn authorized_transport_client(
        &self,
    ) -> Result<AuthClient<BrokerMcpHttpClient>, BrokerageError> {
        let manager = self.initialized_authorization_manager().await?;
        Ok(AuthClient::new(BrokerMcpHttpClient::default(), manager))
    }

    async fn initialized_authorization_manager(
        &self,
    ) -> Result<AuthorizationManager, BrokerageError> {
        let mut manager = self.authorization_manager().await?;
        let resolution = suppress_oauth_tracing(manager.resolve_metadata())
            .await
            .map_err(map_auth_error)?;
        manager.set_metadata(resolution.metadata);
        let initialized = suppress_oauth_tracing(manager.initialize_from_store())
            .await
            .map_err(map_auth_error)?;
        if !initialized {
            return Err(brokerage_error(BrokerageErrorKind::AuthorizationRequired));
        }
        Ok(manager)
    }

    /// Number of live, in-memory authorization attempts.
    pub async fn pending_authorization_count(&self) -> usize {
        self.states.live_count().await
    }

    async fn authorization_manager(&self) -> Result<AuthorizationManager, BrokerageError> {
        let mut manager = suppress_oauth_tracing(AuthorizationManager::new(self.endpoint.clone()))
            .await
            .map_err(map_auth_error)?;
        manager.set_credential_store((*self.credentials).clone());
        manager.set_state_store(self.states.clone());
        Ok(manager)
    }
}

struct ParsedCallback {
    code: String,
    state: String,
    issuer: Option<String>,
}

impl ParsedCallback {
    fn parse(query: &str) -> Result<Self, BrokerageError> {
        let mut code = None;
        let mut state = None;
        let mut issuer = None;
        for (key, value) in form_urlencoded::parse(query.trim_start_matches('?').as_bytes()) {
            match key.as_ref() {
                "code" => code = Some(value.into_owned()),
                "state" => state = Some(value.into_owned()),
                "iss" => issuer = Some(value.into_owned()),
                "error" => return Err(brokerage_error(BrokerageErrorKind::AuthorizationDenied)),
                _ => {}
            }
        }
        Ok(Self {
            code: code.ok_or_else(|| brokerage_error(BrokerageErrorKind::AuthorizationDenied))?,
            state: state.ok_or_else(|| brokerage_error(BrokerageErrorKind::AuthorizationDenied))?,
            issuer,
        })
    }
}

fn callback_parameter(url: &Url, name: &str) -> Option<String> {
    url.query_pairs()
        .find_map(|(key, value)| (key == name).then(|| value.into_owned()))
}

fn brokerage_error(kind: BrokerageErrorKind) -> BrokerageError {
    BrokerageError::new(kind)
}

fn map_auth_error(error: AuthError) -> BrokerageError {
    match error {
        AuthError::AuthorizationRequired => {
            brokerage_error(BrokerageErrorKind::AuthorizationRequired)
        }
        AuthError::CredentialStoreError(_) => brokerage_error(BrokerageErrorKind::SecureStorage),
        AuthError::HttpError(_) | AuthError::TokenRefreshFailed(_) => {
            brokerage_error(BrokerageErrorKind::Unavailable)
        }
        AuthError::TokenExpired | AuthError::TokenRefreshRejected(_) => {
            brokerage_error(BrokerageErrorKind::AuthorizationRequired)
        }
        _ => brokerage_error(BrokerageErrorKind::AuthorizationDenied),
    }
}
