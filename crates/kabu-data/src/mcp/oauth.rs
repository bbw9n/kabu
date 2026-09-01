use std::{net::Ipv4Addr, sync::Arc, time::Duration};

use async_trait::async_trait;
use kabu_core::{Brokerage, BrokerageError, BrokerageErrorKind};
use rmcp::transport::auth::{
    AuthError, AuthorizationManager, AuthorizationRequest, AuthorizationSession,
    AuthorizedHttpClient, CredentialRefreshGuard, CredentialStore, StateStore,
    StoredAuthorizationState, StoredCredentials,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
    task::spawn_blocking,
    time::Instant,
};
use url::{Url, form_urlencoded};
use zeroize::Zeroizing;

use crate::{SecretStore, SecretStoreError};

const KEYCHAIN_SERVICE: &str = "com.kabu.mcp.oauth";
const CALLBACK_TTL: Duration = Duration::from_secs(600);
const MAX_REQUEST_LINE_BYTES: usize = 8 * 1024;
const CALLBACK_SUCCESS_PAGE: &str = "<!doctype html><title>Kabu authorization</title><p>Authorization received. You can return to Kabu.</p>";
const CALLBACK_FAILURE_PAGE: &str = "<!doctype html><title>Kabu authorization</title><p>Authorization failed. Return to Kabu and try again.</p>";

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

struct SharedStateStore(Arc<dyn StateStore>);

#[async_trait]
impl StateStore for SharedStateStore {
    async fn save(
        &self,
        csrf_token: &str,
        state: StoredAuthorizationState,
    ) -> Result<(), AuthError> {
        self.0.save(csrf_token, state).await
    }

    async fn load(&self, csrf_token: &str) -> Result<Option<StoredAuthorizationState>, AuthError> {
        self.0.load(csrf_token).await
    }

    async fn delete(&self, csrf_token: &str) -> Result<(), AuthError> {
        self.0.delete(csrf_token).await
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
}

impl PendingAuthorization {
    pub async fn wait_for_callback(&mut self) -> Result<String, BrokerageError> {
        self.callback.wait().await
    }
}

/// OAuth orchestration for one hosted brokerage MCP endpoint.
pub struct BrokerOAuth {
    brokerage: Brokerage,
    endpoint: Url,
    credentials: Arc<BrokerCredentialStore>,
    states: Arc<dyn StateStore>,
}

impl BrokerOAuth {
    pub fn new(
        brokerage: Brokerage,
        endpoint: Url,
        credentials: Arc<BrokerCredentialStore>,
        states: Arc<dyn StateStore>,
    ) -> Self {
        Self {
            brokerage,
            endpoint,
            credentials,
            states,
        }
    }

    pub async fn begin_authorization(&self) -> Result<PendingAuthorization, BrokerageError> {
        let (callback, redirect_uri) = OneShotCallback::bind().await?;
        let started_at = Instant::now();
        let mut manager = self.authorization_manager().await?;
        let resolution = manager.resolve_metadata().await.map_err(map_auth_error)?;
        manager.set_metadata(resolution.metadata);
        let request = AuthorizationRequest::new(redirect_uri.as_str()).with_client_name("Kabu");
        let session = AuthorizationSession::new(manager, request)
            .await
            .map_err(|(_, _)| brokerage_error(BrokerageErrorKind::AuthorizationDenied))?;
        let authorization_url = Url::parse(session.get_authorization_url())
            .map_err(|_| brokerage_error(BrokerageErrorKind::MalformedResponse))?;
        let csrf_token = callback_parameter(&authorization_url, "state")
            .ok_or_else(|| brokerage_error(BrokerageErrorKind::MalformedResponse))?;

        Ok(PendingAuthorization {
            authorization_url,
            redirect_uri,
            started_at,
            csrf_token,
            callback,
            brokerage: self.brokerage,
            session,
        })
    }

    pub async fn complete_authorization(
        &self,
        pending: PendingAuthorization,
        callback_query: &str,
    ) -> Result<(), BrokerageError> {
        if pending.brokerage != self.brokerage || pending.started_at.elapsed() > CALLBACK_TTL {
            self.discard_state(&pending.csrf_token).await;
            return Err(brokerage_error(BrokerageErrorKind::AuthorizationDenied));
        }

        let callback = ParsedCallback::parse(callback_query)?;
        if callback.state != pending.csrf_token {
            self.discard_state(&pending.csrf_token).await;
            return Err(brokerage_error(BrokerageErrorKind::AuthorizationDenied));
        }

        pending
            .session
            .handle_callback_with_issuer(
                &callback.code,
                &callback.state,
                callback.issuer.as_deref(),
            )
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
        let mut manager = self.authorization_manager().await?;
        let resolution = manager.resolve_metadata().await.map_err(map_auth_error)?;
        manager.set_metadata(resolution.metadata);
        let initialized = manager
            .initialize_from_store()
            .await
            .map_err(map_auth_error)?;
        if !initialized {
            return Err(brokerage_error(BrokerageErrorKind::AuthorizationRequired));
        }
        Ok(AuthorizedHttpClient::new(Arc::new(manager), None))
    }

    async fn authorization_manager(&self) -> Result<AuthorizationManager, BrokerageError> {
        let mut manager = AuthorizationManager::new(self.endpoint.clone())
            .await
            .map_err(map_auth_error)?;
        manager.set_credential_store((*self.credentials).clone());
        manager.set_state_store(SharedStateStore(self.states.clone()));
        Ok(manager)
    }

    async fn discard_state(&self, csrf_token: &str) {
        let _ = self.states.delete(csrf_token).await;
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
