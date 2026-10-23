# Kabu Hosted Brokerage MCP Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Connect Kabu to the official Robinhood and Longbridge hosted MCP services, keep OAuth credentials in macOS Keychain, and render normalized read-only accounts and positions in Portfolio and Connections pages.

**Architecture:** Provider-neutral brokerage values and errors live in `kabu-core`; `kabu-data` owns Keychain, OAuth, MCP transport, tool allowlists, payload bounds, and provider normalization. `kabu-app` owns a separately cancellable brokerage runtime lane and session-only GPUI pages, leaving the existing Yahoo/Binance quote coordinator unchanged.

**Tech Stack:** Rust 1.96, edition 2024, `rust_decimal`, `rmcp 3.2.0`, Reqwest/Rustls, Tokio, `security-framework 3.7`, GPUI 0.2.2, gpui-component 0.5.1, Serde, Chrono, Wiremock.

**Spec:** `docs/superpowers/specs/2026-09-01-kabu-mcp-brokerage-design.md`

## Global Constraints

- Target macOS only and keep one standalone window.
- Robinhood endpoint is exactly `https://agent.robinhood.com/mcp/trading`; Longbridge endpoint is exactly `https://mcp.longbridge.com`.
- Remote transport is MCP Streamable HTTP with OAuth 2.1 Authorization Code plus PKCE S256.
- Invoke only compile-time-allowlisted read tools after validating their input schemas; never expose a generic tool console or forward arbitrary tool names/arguments.
- Never invoke order, transfer, strategy, alert, DCA, grid, or watchlist mutation capabilities.
- Existing Yahoo polling and Binance REST/WebSocket behavior remains the only live quote path.
- OAuth credentials use Keychain service `com.kabu.mcp.oauth` with accounts `robinhood` and `longbridge`.
- No token, raw callback URL, raw account number, raw MCP payload, balance, position, or portfolio snapshot enters `state.json`, `cache.json`, logs, or user-facing errors.
- Portfolio values use `rust_decimal::Decimal`; absent numeric values remain `None` and render as an em dash.
- Portfolio refresh is immediate after authorization, manual on demand, on page entry when older than 60 seconds, and every 60 seconds while visible; at most one refresh runs per broker.
- Hiding/minimizing cancels and awaits OAuth and refresh jobs; restoring reconciles once before periodic refresh resumes.
- Default tests are hermetic. Real Keychain and broker OAuth tests are ignored and opt-in.
- Keep `unsafe_code = "deny"`; use safe `security-framework` wrappers.

---

## File Structure

```text
Cargo.toml                                      Shared decimal, MCP, and macOS security dependencies
Cargo.lock                                      Resolved dependency graph
crates/kabu-core/
  Cargo.toml
  src/lib.rs                                    Public brokerage exports
  src/brokerage.rs                              Normalized accounts, positions, errors, connection state
  src/provider.rs                               PortfolioProvider async port
crates/kabu-data/
  Cargo.toml
  src/lib.rs                                    Public brokerage adapter exports
  src/secret_store.rs                           Injected secret store and macOS Keychain implementation
  src/mcp/mod.rs                                Shared OAuth/MCP session facade and response bounds
  src/mcp/oauth.rs                              PKCE authorization and rmcp credential-store bridge
  src/mcp/capabilities.rs                       Exact read capability resolution and schema validation
  src/robinhood.rs                              Robinhood tool calls and normalization
  src/longbridge.rs                             Longbridge tool calls and normalization
  tests/mcp_oauth.rs                            Local OAuth/MCP discovery and callback contracts
  tests/mcp_adapters.rs                         Complete broker fixtures and normalization contracts
  tests/fixtures/robinhood/*.json               Tool catalogs and read payloads
  tests/fixtures/longbridge/*.json               Tool catalogs and read payloads
crates/kabu-app/
  src/runtime.rs                                Brokerage commands, event slots, jobs, shutdown
  src/app_model.rs                              Connection and in-memory portfolio state
  src/commands.rs                               Page actions
  src/views/mod.rs                              New page exports
  src/views/app_shell.rs                        Title-bar navigation and page selection
  src/views/portfolio.rs                        Portfolio account/holding presentation
  src/views/connections.rs                      Broker authorization/status cards
  tests/app_model.rs                            Model state transitions and refresh intent tests
  tests/gpui_interactions.rs                    Navigation, focus, Portfolio, Connections contracts
docs/manual-test.md                             Real broker authorization acceptance steps
README.md                                       Read-only integration and Keychain disclosure
```

`kabu-core` never imports MCP wire types. `kabu-app` never parses tool payloads or reads Keychain directly. Provider sessions remain separate from `ProviderSet` and `Coordinator`.

---

### Task 1: Add the Provider-Neutral Brokerage Domain

**Files:**
- Modify: `Cargo.toml`
- Modify: `crates/kabu-core/Cargo.toml`
- Create: `crates/kabu-core/src/brokerage.rs`
- Modify: `crates/kabu-core/src/lib.rs`
- Modify: `crates/kabu-core/src/provider.rs`

**Interfaces:**
- Consumes: existing `Instrument`, async-trait, Chrono.
- Produces: `Brokerage`, `BrokerageAssetKind`, `BrokerageAccountId`, `BrokerageAccount`, `BrokeragePosition`, `PortfolioSnapshot`, `BrokerageConnectionState`, `BrokerageError`, `BrokerageErrorKind`, and `PortfolioProvider`.

- [ ] **Step 1: Write failing domain tests**

Add unit tests in `brokerage.rs` that specify decimal precision, redaction, and absent values:

```rust
#[test]
fn account_ids_are_redacted_from_debug() {
    let id = BrokerageAccountId::new("RH-123456789");
    assert_eq!(format!("{id:?}"), "BrokerageAccountId([REDACTED])");
    assert_eq!(id.as_str(), "RH-123456789");
}

#[test]
fn portfolio_keeps_missing_money_distinct_from_zero() {
    let account = BrokerageAccount {
        brokerage: Brokerage::Robinhood,
        id: BrokerageAccountId::new("opaque"),
        display_label: "Robinhood ••••6789".into(),
        base_currency: "USD".into(),
        equity: None,
        cash: Some(Decimal::ZERO),
        buying_power: None,
    };
    assert_eq!(account.equity, None);
    assert_eq!(account.cash, Some(Decimal::ZERO));
}

#[test]
fn brokerage_errors_expose_only_fixed_safe_messages() {
    let error = BrokerageError::new(BrokerageErrorKind::AuthorizationRequired);
    assert_eq!(error.user_message(), "Connect this broker to load its portfolio.");
    assert!(!format!("{error:?}").contains("token"));
}
```

- [ ] **Step 2: Verify RED**

Run: `cargo test -p kabu-core brokerage -- --nocapture`

Expected: compilation fails because the brokerage module and its types do not exist.

- [ ] **Step 3: Add dependencies and implement the domain**

Add this workspace dependency and enable Serde in core:

```toml
rust_decimal = { version = "1", features = ["serde"] }
```

Implement the public shapes below. `BrokerageAccountId` keeps its string private, provides `new`/`as_str`, and has a manual redacted `Debug` implementation. `BrokerageError` contains only its kind plus an optional retry delay; its `Display`, `Debug`, and `user_message` must not accept or retain a raw provider message.

```rust
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum Brokerage { Robinhood, Longbridge }

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum BrokerageAssetKind { Equity, Crypto, Option, Fund, Cash, Other }

#[derive(Clone, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct BrokerageAccountId(String);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BrokerageAccount {
    pub brokerage: Brokerage,
    pub id: BrokerageAccountId,
    pub display_label: String,
    pub base_currency: String,
    pub equity: Option<Decimal>,
    pub cash: Option<Decimal>,
    pub buying_power: Option<Decimal>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BrokeragePosition {
    pub brokerage: Brokerage,
    pub account_id: BrokerageAccountId,
    pub native_symbol: String,
    pub display_name: String,
    pub asset_kind: BrokerageAssetKind,
    pub instrument: Option<Instrument>,
    pub quantity: Decimal,
    pub average_cost: Option<Decimal>,
    pub market_value: Option<Decimal>,
    pub unrealized_pnl: Option<Decimal>,
    pub currency: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PortfolioSnapshot {
    pub brokerage: Brokerage,
    pub accounts: Vec<BrokerageAccount>,
    pub positions: Vec<BrokeragePosition>,
    pub received_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BrokerageConnectionState {
    Disconnected,
    Authorizing,
    Connecting,
    Connected { last_synced_at: Option<DateTime<Utc>> },
    Refreshing { last_synced_at: Option<DateTime<Utc>> },
    Error { message: String },
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum BrokerageErrorKind {
    AuthorizationRequired,
    AuthorizationDenied,
    Permission,
    Timeout,
    RateLimited,
    Unavailable,
    MalformedResponse,
    IncompatibleToolSchema,
    Disconnected,
    SecureStorage,
    Internal,
}
```

Add this port to `provider.rs` and re-export it:

```rust
#[async_trait::async_trait]
pub trait PortfolioProvider: Send + Sync {
    fn brokerage(&self) -> Brokerage;
    async fn refresh_portfolio(&self) -> Result<PortfolioSnapshot, BrokerageError>;
    async fn disconnect(&self) -> Result<(), BrokerageError>;
}
```

- [ ] **Step 4: Verify GREEN**

Run: `cargo test -p kabu-core`

Expected: all core tests pass, including the new redaction and decimal tests.

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml Cargo.lock crates/kabu-core
git commit -m "feat(core): add brokerage portfolio domain"
```

---

### Task 2: Build the Keychain-Backed OAuth Foundation

**Files:**
- Modify: `Cargo.toml`
- Modify: `crates/kabu-data/Cargo.toml`
- Modify: `crates/kabu-data/src/lib.rs`
- Create: `crates/kabu-data/src/secret_store.rs`
- Create: `crates/kabu-data/src/mcp/mod.rs`
- Create: `crates/kabu-data/src/mcp/oauth.rs`
- Create: `crates/kabu-data/tests/mcp_oauth.rs`

**Interfaces:**
- Consumes: `Brokerage`, `rmcp::transport::auth::{CredentialStore, StateStore, StoredCredentials, StoredAuthorizationState}`.
- Produces: `SecretStore`, `MacKeychainSecretStore`, `BrokerCredentialStore`, `BrokerOAuth`, `PendingAuthorization`, and `AuthorizedHttpClient` construction.

- [ ] **Step 1: Write failing credential and callback tests**

Use an injected `MemorySecretStore` and local Wiremock server. The tests must prove save/load/clear round trips, concurrent refresh serialization, callback-state rejection, ten-minute expiration, and sanitized errors:

```rust
#[tokio::test]
async fn broker_credentials_round_trip_without_json_persistence() {
    let secrets = Arc::new(MemorySecretStore::default());
    let store = BrokerCredentialStore::new(Brokerage::Robinhood, secrets.clone());
    let value = fixture_credentials("access-secret", "refresh-secret");
    store.save(value.clone()).await.expect("save");
    let loaded = store.load().await.expect("load").expect("stored value");
    assert_eq!(serde_json::to_value(loaded).expect("loaded JSON"), serde_json::to_value(value).expect("expected JSON"));
    store.clear().await.expect("clear");
    assert!(store.load().await.expect("load after clear").is_none());
    assert_eq!(secrets.service_names(), vec!["com.kabu.mcp.oauth"]);
}

#[tokio::test]
async fn oauth_callback_rejects_wrong_state_before_exchanging_code() {
    let (oauth, exchange_counter) = oauth_fixture();
    let pending = oauth.begin_authorization().await.expect("begin");
    let error = oauth
        .complete_authorization(pending, "code=abc&state=wrong")
        .await
        .expect_err("state mismatch");
    assert_eq!(error.kind(), BrokerageErrorKind::AuthorizationDenied);
    assert_eq!(exchange_counter.load(Ordering::SeqCst), 0);
}
```

- [ ] **Step 2: Verify RED**

Run: `cargo test -p kabu-data --test mcp_oauth`

Expected: compilation fails because the secret-store and OAuth types are absent.

- [ ] **Step 3: Add exact dependencies**

Add these workspace entries and wire them into `kabu-data`; keep `security-framework` macOS-target-only:

```toml
rmcp = { version = "3.2.0", default-features = false, features = [
  "auth",
  "client",
  "transport-streamable-http-client-reqwest",
] }
tokio-util = { version = "0.7", features = ["rt"] }
zeroize = { version = "1", features = ["derive"] }

[target.'cfg(target_os = "macos")'.dependencies]
security-framework = "3.7"
```

- [ ] **Step 4: Implement the injected secret store and Keychain adapter**

Define a synchronous boundary because Security.framework is synchronous; call it through `tokio::task::spawn_blocking` inside the async credential adapter:

```rust
pub trait SecretStore: Send + Sync {
    fn get(&self, service: &str, account: &str) -> Result<Option<Vec<u8>>, SecretStoreError>;
    fn set(&self, service: &str, account: &str, value: &[u8]) -> Result<(), SecretStoreError>;
    fn delete(&self, service: &str, account: &str) -> Result<(), SecretStoreError>;
}

pub struct MacKeychainSecretStore;
```

Use `security_framework::passwords::{generic_password, set_generic_password, delete_generic_password}`. Map item-not-found to `Ok(None)`/successful delete, and map all other OS errors to `SecretStoreError` variants whose `Display` contains no value or account identifier.

Implement `CredentialStore` on `BrokerCredentialStore`. Serialize only `StoredCredentials` into Keychain service `com.kabu.mcp.oauth`, use the exact lower-case broker account name, and use one async mutex per adapter for `CredentialRefreshGuard`.

- [ ] **Step 5: Implement PKCE authorization state and callback completion**

Wrap `rmcp::transport::auth::AuthorizationManager` behind:

```rust
pub struct PendingAuthorization {
    pub authorization_url: Url,
    pub redirect_uri: Url,
    started_at: Instant,
    csrf_token: String,
    callback: OneShotCallback,
}

pub struct BrokerOAuth {
    brokerage: Brokerage,
    endpoint: Url,
    credentials: Arc<BrokerCredentialStore>,
    states: Arc<dyn StateStore>,
}

impl BrokerOAuth {
    pub async fn begin_authorization(&self) -> Result<PendingAuthorization, BrokerageError>;
    pub async fn complete_authorization(
        &self,
        pending: PendingAuthorization,
        callback_query: &str,
    ) -> Result<(), BrokerageError>;
    pub async fn disconnect(&self) -> Result<(), BrokerageError>;
}
```

`begin_authorization` binds a one-shot `127.0.0.1:0` callback listener before discovery/dynamic registration and returns ownership inside `PendingAuthorization`. `PendingAuthorization::wait_for_callback(&mut self)` consumes its internal listener, caps the request line at 8 KiB, returns a fixed browser success/failure page, and yields only the callback query; the caller then moves the pending value into `complete_authorization`. `complete_authorization` parses the query with `url::form_urlencoded`, rejects wrong/missing state and elapsed time over 600 seconds before token exchange, and saves credentials through `BrokerCredentialStore`. Do not derive `Debug` for pending state, callback values, stores, or clients.

- [ ] **Step 6: Verify GREEN and regression coverage**

Run:

```bash
cargo test -p kabu-data --test mcp_oauth
cargo test -p kabu-data
```

Expected: OAuth/credential tests and all existing Yahoo/Binance/persistence tests pass.

- [ ] **Step 7: Commit**

```bash
git add Cargo.toml Cargo.lock crates/kabu-data
git commit -m "feat(data): add secure broker OAuth foundation"
```

---

### Task 3: Add the Shared Read-Only MCP Session and Capability Gate

**Files:**
- Modify: `crates/kabu-data/src/mcp/mod.rs`
- Create: `crates/kabu-data/src/mcp/capabilities.rs`
- Create: `crates/kabu-data/tests/mcp_capabilities.rs`
- Create: `crates/kabu-data/tests/fixtures/robinhood/tools.json`
- Create: `crates/kabu-data/tests/fixtures/longbridge/tools.json`

**Interfaces:**
- Consumes: authorized Reqwest client from Task 2 and `BrokerageError` from Task 1.
- Produces: `ReadCapability`, `ResolvedCapabilities`, `BrokerMcpSession`, `BoundedToolResult`, and awaited `close()`.

- [ ] **Step 1: Write failing capability-gate tests**

Fixtures must contain complete `tools/list` objects, including representative write tools. Prove required reads resolve, write names never resolve, schema drift fails closed, and oversized results are rejected:

```rust
#[test]
fn longbridge_resolves_only_exact_read_tools() {
    let tools = tool_fixture("longbridge/tools.json");
    let resolved = resolve_capabilities(Brokerage::Longbridge, &tools).expect("compatible");
    assert_eq!(resolved.name(ReadCapability::Accounts), "account_balance");
    assert_eq!(resolved.name(ReadCapability::EquityPositions), "stock_positions");
    assert!(!resolved.names().any(|name| name.contains("submit_order")));
}

#[test]
fn changed_required_input_schema_fails_closed() {
    let mut tools = tool_fixture("robinhood/tools.json");
    replace_required_property(&mut tools, "get_accounts", "order_id");
    let error = resolve_capabilities(Brokerage::Robinhood, &tools).expect_err("schema drift");
    assert_eq!(error.kind(), BrokerageErrorKind::IncompatibleToolSchema);
}

#[test]
fn result_over_one_mebibyte_is_rejected_before_deserialization() {
    let payload = vec![b'x'; 1_048_577];
    assert_eq!(BoundedToolResult::parse(&payload).expect_err("oversized").kind(), BrokerageErrorKind::MalformedResponse);
}
```

- [ ] **Step 2: Verify RED**

Run: `cargo test -p kabu-data --test mcp_capabilities`

Expected: compilation fails because the capability and session types are absent.

- [ ] **Step 3: Implement exact capability descriptors**

Use a private static descriptor table. Longbridge exact required mappings are `account_balance` with optional string `currency`, and `stock_positions` with an empty object schema. Because Robinhood does not publish its full tool catalog outside an authorized session, its checked-in fixture defines a conservative compile-time candidate set for each required read role: accounts (`get_accounts`, `list_accounts`), portfolio summary (`get_portfolio`, `get_portfolio_summary`), equity positions (`get_equity_positions`, `list_equity_positions`), and crypto positions (`get_crypto_positions`, `list_crypto_positions`). A candidate resolves only when its complete input schema matches the read-only descriptor; otherwise connection reports `IncompatibleToolSchema`. The ignored live acceptance test captures only tool names and schemas after user authorization and must be used to update the fixture before claiming current Robinhood compatibility.

```rust
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ReadCapability { Accounts, PortfolioSummary, EquityPositions, CryptoPositions }

pub struct ResolvedCapabilities(BTreeMap<ReadCapability, Arc<str>>);

pub fn resolve_capabilities(
    brokerage: Brokerage,
    tools: &[rmcp::model::Tool],
) -> Result<ResolvedCapabilities, BrokerageError>;
```

Reject descriptors containing write-oriented names (`order`, `buy`, `sell`, `cancel`, `replace`, `transfer`, `alert`, `dca`, `grid`, `create`, `update`, `delete`) even if they appear in the fixture. Compare the required input schema structurally, allowing only additional optional descriptive metadata.

- [ ] **Step 4: Implement the bounded MCP session**

Build transport exactly through `StreamableHttpClientTransport::with_client` and `().serve(transport)`. Set maximum concurrent requests to 4, control timeout to 5 seconds, session recovery timeout to 5 seconds, and expired-session reinitialization to true. On connect, call `list_tools(None)` once and resolve the provider table before returning `Connected`.

```rust
pub struct BrokerMcpSession { /* running service, capabilities, cancel token */ }

impl BrokerMcpSession {
    pub async fn connect(config: BrokerMcpConfig) -> Result<Self, BrokerageError>;
    pub async fn call_read(
        &self,
        capability: ReadCapability,
        arguments: Option<serde_json::Map<String, serde_json::Value>>,
    ) -> Result<BoundedToolResult, BrokerageError>;
    pub async fn close(self) -> Result<(), BrokerageError>;
}
```

`call_read` looks up the already-resolved exact name; callers cannot pass a string tool name. Bound raw serialized results to 1 MiB, arrays to 2,000 items, strings to 16 KiB, nesting to 32, and decimal text to 38 significant digits. Treat `CallToolResult::is_error == true` as a sanitized provider failure. `close` cancels the running service and awaits terminal completion.

- [ ] **Step 5: Verify GREEN**

Run:

```bash
cargo test -p kabu-data --test mcp_capabilities
cargo test -p kabu-data
```

Expected: all capability, bound, and existing data tests pass.

- [ ] **Step 6: Commit**

```bash
git add crates/kabu-data
git commit -m "feat(data): gate broker MCP reads by capability"
```

---

### Task 4: Normalize Robinhood and Longbridge Portfolios

**Files:**
- Modify: `crates/kabu-data/Cargo.toml`
- Modify: `crates/kabu-data/src/lib.rs`
- Create: `crates/kabu-data/src/robinhood.rs`
- Create: `crates/kabu-data/src/longbridge.rs`
- Create: `crates/kabu-data/tests/mcp_adapters.rs`
- Create: `crates/kabu-data/tests/fixtures/robinhood/accounts.json`
- Create: `crates/kabu-data/tests/fixtures/robinhood/portfolio.json`
- Create: `crates/kabu-data/tests/fixtures/robinhood/equity_positions.json`
- Create: `crates/kabu-data/tests/fixtures/robinhood/equity_positions_with_option.json`
- Create: `crates/kabu-data/tests/fixtures/robinhood/crypto_positions.json`
- Create: `crates/kabu-data/tests/fixtures/longbridge/account_balance.json`
- Create: `crates/kabu-data/tests/fixtures/longbridge/stock_positions.json`

**Interfaces:**
- Consumes: `BrokerMcpSession::call_read` and Task 1 domain types.
- Produces: `BrokerageClient`, `RobinhoodPortfolioProvider`, `LongbridgePortfolioProvider`, and complete connection/portfolio implementations.

Add `rust_decimal.workspace = true` to `kabu-data` for bounded wire-number parsing.

- [ ] **Step 1: Write failing fixture-normalization tests**

Tests call pure `normalize_*` helpers with complete fixture objects, not mocks. Cover zero, null, missing optional values, unsupported asset types, excessive precision, and raw-account redaction:

```rust
#[test]
fn longbridge_position_preserves_decimal_and_normalizes_us_equity() {
    let snapshot = normalize_longbridge(
        fixture("longbridge/account_balance.json"),
        fixture("longbridge/stock_positions.json"),
        fixed_now(),
    ).expect("valid snapshot");
    let aapl = snapshot.positions.iter().find(|p| p.native_symbol == "AAPL.US").expect("AAPL");
    assert_eq!(aapl.quantity, Decimal::from_str("12.345678").expect("decimal"));
    assert_eq!(aapl.instrument.as_ref().map(|i| i.id.canonical_symbol()), Some("AAPL".into()));
}

#[test]
fn unknown_broker_instrument_is_retained_without_coercion() {
    let snapshot = normalize_robinhood_fixture("equity_positions_with_option.json");
    let option = snapshot.positions.iter().find(|p| p.asset_kind == BrokerageAssetKind::Option).expect("option");
    assert_eq!(option.instrument, None);
    assert!(!option.native_symbol.is_empty());
}

#[test]
fn malformed_decimal_is_a_typed_sanitized_failure() {
    let error = normalize_longbridge_value(json!({"total_cash": "NaN-secret"})).expect_err("invalid decimal");
    assert_eq!(error.kind(), BrokerageErrorKind::MalformedResponse);
    assert!(!error.to_string().contains("NaN-secret"));
}
```

- [ ] **Step 2: Verify RED**

Run: `cargo test -p kabu-data --test mcp_adapters`

Expected: compilation fails because both adapters are absent.

- [ ] **Step 3: Implement pure wire normalization**

Keep wire structs private and use `#[serde(deny_unknown_fields)]` only for required structural envelopes; optional broker fields may be ignored. Parse decimal strings through one bounded helper that rejects non-finite spellings, exponent overflow, and more than 38 significant digits.

Longbridge calls exactly:

```rust
session.call_read(ReadCapability::Accounts, Some(json_map! { "currency" => "USD" })).await?;
session.call_read(ReadCapability::EquityPositions, None).await?;
```

Robinhood calls accounts, portfolio summary, equity positions, and crypto positions. If the account list is empty, return a valid empty snapshot. Create display labels by broker plus the last four alphanumeric account characters; never pass the complete identifier to the UI.

Normalize `AAPL.US`/US equity values into `InstrumentId::equity`; normalize crypto pairs only when both base and quote are explicit; retain options, funds, cash, and unknown values with `instrument: None`.

- [ ] **Step 4: Implement provider session ownership**

Each adapter owns `Mutex<Option<BrokerMcpSession>>` plus its `BrokerOAuth`. Add a data-layer extension port that lets the runtime supply a browser-opening callback without exposing OAuth internals:

```rust
#[async_trait::async_trait]
pub trait BrokerageClient: PortfolioProvider {
    async fn connect(
        &self,
        open_authorization_url: &(dyn Fn(&Url) -> Result<(), BrokerageError> + Send + Sync),
    ) -> Result<(), BrokerageError>;
}
```

`connect` first loads stored credentials and starts the MCP session without opening a browser when refresh succeeds. Otherwise it binds the one-shot callback listener, invokes the supplied callback with the generated authorization URL, then waits for and validates the callback before starting the MCP session. `refresh_portfolio` requires an authorized live session, performs the bounded calls sequentially, and returns one snapshot timestamped after successful normalization. `disconnect` takes and closes the session before clearing Keychain credentials.

Constructors are exact and non-fatal:

```rust
pub fn robinhood_provider(secrets: Arc<dyn SecretStore>) -> Result<Arc<dyn BrokerageClient>, BrokerageError>;
pub fn longbridge_provider(secrets: Arc<dyn SecretStore>) -> Result<Arc<dyn BrokerageClient>, BrokerageError>;
```

- [ ] **Step 5: Verify GREEN and all data regressions**

Run:

```bash
cargo test -p kabu-data --test mcp_adapters
cargo test -p kabu-data
```

Expected: normalization, OAuth, capability, Yahoo, Binance, and persistence tests pass.

- [ ] **Step 6: Commit**

```bash
git add crates/kabu-data
git commit -m "feat(data): add read-only brokerage MCP adapters"
```

---

### Task 5: Add a Cancellable Brokerage Runtime Lane

**Files:**
- Modify: `crates/kabu-app/src/runtime.rs`
- Modify: `crates/kabu-app/Cargo.toml`

**Interfaces:**
- Consumes: `BTreeMap<Brokerage, Arc<dyn BrokerageClient>>` and Task 1 connection/snapshot types.
- Produces: brokerage `RuntimeCommand`/`RuntimeEvent` variants, fixed event slots, coalesced refresh jobs, and ordered session shutdown.

Add `url.workspace = true` to `kabu-app` for the browser-opener boundary.

- [ ] **Step 1: Write failing runtime tests**

Use a controllable `PortfolioProvider` whose refresh blocks on a channel and whose disconnect records completion. Tests must prove fixed-slot coalescing, one job per broker, cancellation, hide/restore, and shutdown ordering:

```rust
#[test]
fn newer_portfolio_event_replaces_pending_event_for_same_broker() {
    let (sender, events) = runtime_event_channel();
    sender.publish(RuntimeEvent::PortfolioUpdated(snapshot(Brokerage::Robinhood, 1))).expect("first");
    sender.publish(RuntimeEvent::PortfolioUpdated(snapshot(Brokerage::Robinhood, 2))).expect("second");
    let RuntimeEvent::PortfolioUpdated(actual) = events.try_recv().expect("latest") else {
        panic!("expected portfolio event");
    };
    assert_eq!(actual, snapshot(Brokerage::Robinhood, 2));
}

#[tokio::test]
async fn repeated_refresh_keeps_one_job_per_broker() {
    let provider = BlockingPortfolioProvider::new(Brokerage::Robinhood);
    let runtime = live_runtime_with_broker(provider.clone());
    runtime.send(RuntimeCommand::RefreshPortfolio(Brokerage::Robinhood)).expect("first");
    runtime.send(RuntimeCommand::RefreshPortfolio(Brokerage::Robinhood)).expect("second");
    provider.wait_until_started().await;
    assert_eq!(provider.max_concurrent_refreshes(), 1);
}
```

- [ ] **Step 2: Verify RED**

Run:

```bash
cargo test -p kabu-app runtime::tests::newer_portfolio_event_replaces_pending_event_for_same_broker
cargo test -p kabu-app runtime::tests::repeated_refresh_keeps_one_job_per_broker
```

Expected: compilation fails because brokerage commands/events and provider construction are absent.

- [ ] **Step 3: Add commands, events, and fixed slots**

Add these variants exactly:

```rust
RuntimeCommand::ConnectBroker(Brokerage)
RuntimeCommand::CancelBrokerConnection(Brokerage)
RuntimeCommand::RefreshPortfolio(Brokerage)
RuntimeCommand::DisconnectBroker(Brokerage)
RuntimeCommand::SetPortfolioVisible(bool)

RuntimeEvent::BrokerConnectionChanged { brokerage: Brokerage, state: BrokerageConnectionState }
RuntimeEvent::PortfolioUpdated(PortfolioSnapshot)
```

Add `RuntimeEventSlot::BrokerConnection(Brokerage)` and `RuntimeEventSlot::Portfolio(Brokerage)`. Both are fixed semantic slots and do not consume quote/history dynamic capacity. Classify connect/cancel/disconnect/visibility as reliable controls; refresh is latest-intent-wins per broker.

- [ ] **Step 4: Implement broker job ownership**

Add a separate `BrokerageProviders` map to `RuntimeHandle::start_inner`; do not add fields to `ProviderSet`. Add an injected `AuthorizationUrlOpener` whose production implementation invokes the macOS default browser and whose tests record URLs. Track one authorization task and one refresh task per broker. Starting a replacement cancels and awaits the prior task. A successful connection publishes `Connecting`, calls `BrokerageClient::connect` with the opener, then refreshes, publishes `PortfolioUpdated`, then `Connected { last_synced_at }`. All failures publish sanitized `Error` state.

```rust
pub trait AuthorizationUrlOpener: Send + Sync {
    fn open(&self, url: &url::Url) -> Result<(), BrokerageError>;
}
```

When `SetPortfolioVisible(false)` arrives, cancel and await OAuth/refresh work. When it becomes true, refresh connected brokers whose snapshot is absent or older than 60 seconds, then schedule 60-second refreshes while visible.

- [ ] **Step 5: Integrate shutdown ordering**

Before persistence final flush, reject new broker commands, cancel and await broker tasks, then call and await `disconnect`/session close without clearing stored credentials on ordinary app shutdown. Explicit `DisconnectBroker` closes the session, clears credentials, and publishes `Disconnected`.

- [ ] **Step 6: Verify GREEN and runtime regressions**

Run:

```bash
cargo test -p kabu-app runtime::tests
cargo test -p kabu-app coordinator::tests
```

Expected: all brokerage lifecycle and existing market lifecycle tests pass.

- [ ] **Step 7: Commit**

```bash
git add crates/kabu-app/Cargo.toml crates/kabu-app/src/runtime.rs
git commit -m "feat(app): add brokerage runtime lifecycle"
```

---

### Task 6: Add App Model Portfolio and Connection State

**Files:**
- Modify: `crates/kabu-app/src/app_model.rs`
- Modify: `crates/kabu-app/tests/app_model.rs`

**Interfaces:**
- Consumes: Task 5 runtime commands/events.
- Produces: read-only model getters and UI intents for connection/refresh/disconnect/portfolio visibility.

- [ ] **Step 1: Write failing model tests**

```rust
#[test]
fn portfolio_events_replace_only_the_matching_broker() {
    let mut model = model_with_recording_runtime();
    model.apply_runtime_event(RuntimeEvent::PortfolioUpdated(snapshot(Brokerage::Robinhood, 1)));
    model.apply_runtime_event(RuntimeEvent::PortfolioUpdated(snapshot(Brokerage::Longbridge, 2)));
    assert_eq!(model.portfolio(Brokerage::Robinhood).expect("RH").positions.len(), 1);
    assert_eq!(model.portfolio(Brokerage::Longbridge).expect("LB").positions.len(), 2);
}

#[test]
fn disconnect_intent_clears_only_after_runtime_confirmation() {
    let mut model = connected_model(Brokerage::Robinhood);
    model.disconnect_broker(Brokerage::Robinhood);
    assert!(matches!(model.connection_state(Brokerage::Robinhood), BrokerageConnectionState::Connected { .. }));
    assert_eq!(model.runtime().recorded_commands().last(), Some(&RuntimeCommand::DisconnectBroker(Brokerage::Robinhood)));
}
```

- [ ] **Step 2: Verify RED**

Run:

```bash
cargo test -p kabu-app --test app_model portfolio_events_replace_only_the_matching_broker
cargo test -p kabu-app --test app_model disconnect_intent_clears_only_after_runtime_confirmation
```

Expected: compilation fails because portfolio state/getters/intents are absent.

- [ ] **Step 3: Implement model state and event application**

Add:

```rust
broker_connections: HashMap<Brokerage, BrokerageConnectionState>,
portfolios: HashMap<Brokerage, PortfolioSnapshot>,
portfolio_visible: bool,
```

Initialize Robinhood and Longbridge as `Disconnected`. Provide `connection_state`, `portfolio`, `portfolios`, `connect_broker`, `cancel_broker_connection`, `refresh_portfolio`, `disconnect_broker`, and `set_portfolio_visible`. Only `RuntimeEvent::PortfolioUpdated` mutates snapshot data; only `BrokerConnectionChanged::Disconnected` clears that broker's snapshot. Runtime dispatch failures become keyed safe warnings without exposing provider payloads.

- [ ] **Step 4: Verify GREEN**

Run:

```bash
cargo test -p kabu-app --test app_model
cargo test -p kabu-app
```

Expected: all model and existing application tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/kabu-app/src/app_model.rs crates/kabu-app/tests/app_model.rs
git commit -m "feat(app): model broker connections and portfolios"
```

---

### Task 7: Render Portfolio and Connections Pages

**Files:**
- Modify: `crates/kabu-app/Cargo.toml`
- Modify: `crates/kabu-app/src/commands.rs`
- Modify: `crates/kabu-app/src/views/mod.rs`
- Modify: `crates/kabu-app/src/views/app_shell.rs`
- Create: `crates/kabu-app/src/views/portfolio.rs`
- Create: `crates/kabu-app/src/views/connections.rs`
- Modify: `crates/kabu-app/tests/gpui_interactions.rs`

**Interfaces:**
- Consumes: Task 6 model getters/intents.
- Produces: `ShellPage`, three page buttons, Portfolio presentation, broker connection cards, and preserved search/focus behavior.

Add `rust_decimal.workspace = true` to `kabu-app` for view formatting without a floating-point conversion.

- [ ] **Step 1: Write failing GPUI interaction tests**

Add tests using `AppShell::with_model` and `publish_recorded_event`:

```rust
#[gpui::test]
fn page_navigation_preserves_inspector_selection_and_focus(cx: &mut gpui::TestAppContext) {
    let (window, shell) = shell_with_cached_watchlist(cx);
    click_by_id(cx, window, "page-portfolio");
    assert_eq!(shell.read_with(cx, |shell, _| shell.page()), ShellPage::Portfolio);
    click_by_id(cx, window, "page-inspector");
    assert_eq!(selected_symbol(cx, &shell), "AAPL");
    assert!(watchlist_has_focus(cx, &shell));
}

#[gpui::test]
fn empty_portfolio_links_to_connections(cx: &mut gpui::TestAppContext) {
    let (window, shell) = shell_with_default_model(cx);
    click_by_id(cx, window, "page-portfolio");
    assert!(rendered_text(cx, window).contains("Connect a broker to see holdings"));
    click_by_id(cx, window, "portfolio-open-connections");
    assert_eq!(shell.read_with(cx, |shell, _| shell.page()), ShellPage::Connections);
}
```

Also test Connected/Error card actions, em-dash rendering for missing values, unsupported holdings, and Command-K/Escape returning to the prior page.

- [ ] **Step 2: Verify RED**

Run:

```bash
cargo test -p kabu-app --test gpui_interactions page_navigation_preserves_inspector_selection_and_focus
cargo test -p kabu-app --test gpui_interactions empty_portfolio_links_to_connections
```

Expected: compilation fails because `ShellPage` and page elements are absent.

- [ ] **Step 3: Add transient shell navigation**

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShellPage { Inspector, Portfolio, Connections }
```

Add `page: ShellPage` initialized to Inspector, `page()`, and `set_page()`. Place buttons with IDs `page-inspector`, `page-portfolio`, and `page-connections` in the title bar. Keep the current Inspector sidebar/divider/QuoteDetail subtree intact. Opening search records the current page; Escape restores focus without changing it.

On page transitions call `model.set_portfolio_visible(page == ShellPage::Portfolio && model.is_visible())`. Existing visibility changes must recompute this value.

- [ ] **Step 4: Render the Portfolio page**

Render a full-width scrollable panel. Empty state copy is exactly `Connect a broker to see holdings`. For each snapshot, render broker/account safe label, base currency, equity/cash/buying power, timestamp/stale state, and positions. Use a helper:

```rust
fn money(value: Option<Decimal>, currency: &str) -> String {
    value.map_or_else(|| "—".into(), |value| format!("{value} {currency}"))
}
```

Never call `BrokerageAccountId::as_str` in any view module.

- [ ] **Step 5: Render broker connection cards**

Cards display the exact official endpoint, current typed state, last sync, read-only text, and exact state actions: Connect when disconnected; disabled progress plus Cancel while authorizing/connecting; Refresh and Disconnect when connected; Retry and Disconnect after error. Button handlers call model intents only.

- [ ] **Step 6: Verify GREEN and Inspector regressions**

Run:

```bash
cargo test -p kabu-app --test gpui_interactions
cargo test -p kabu-app
```

Expected: navigation/page tests pass and all existing watchlist, search, quote, divider, theme, and close tests remain green.

- [ ] **Step 7: Commit**

```bash
git add crates/kabu-app/Cargo.toml crates/kabu-app/src/commands.rs crates/kabu-app/src/views crates/kabu-app/tests/gpui_interactions.rs
git commit -m "feat(app): add portfolio and broker connection pages"
```

---

### Task 8: Wire Production Providers and Document Live Acceptance

**Files:**
- Modify: `crates/kabu-app/src/runtime.rs`
- Modify: `crates/kabu-app/src/main.rs`
- Modify: `README.md`
- Modify: `docs/manual-test.md`

**Interfaces:**
- Consumes: Tasks 2-7.
- Produces: non-fatal production adapter construction and exact manual OAuth verification.

- [ ] **Step 1: Write the failing production-construction test**

Add a runtime test proving missing Keychain credentials do not fail startup and both providers begin disconnected:

```rust
#[test]
fn production_broker_setup_is_non_fatal_without_credentials() {
    let launch = ProductionComponents::from_repository_with_secrets(
        test_repository(),
        Arc::new(MemorySecretStore::default()),
    ).expect("market startup succeeds").start().expect("runtime starts");
    assert_eq!(launch.initial_broker_state(Brokerage::Robinhood), BrokerageConnectionState::Disconnected);
    assert_eq!(launch.initial_broker_state(Brokerage::Longbridge), BrokerageConnectionState::Disconnected);
}
```

- [ ] **Step 2: Verify RED**

Run: `cargo test -p kabu-app runtime::tests::production_broker_setup_is_non_fatal_without_credentials`

Expected: compilation fails because production broker construction is not wired.

- [ ] **Step 3: Wire non-fatal production construction**

Construct one shared `MacKeychainSecretStore`, then create Robinhood and Longbridge adapters separately. A provider construction/credential lookup failure publishes only that broker's safe Error state after runtime start; it does not become `ProductionStartError` and does not affect Yahoo/Binance startup.

Opening authorization URLs uses the injected runtime `AuthorizationUrlOpener`; the production macOS implementation spawns `/usr/bin/open` with one URL argument, does not retain or log it, and tests use a recorder. Callback listeners remain owned by the brokerage runtime task.

- [ ] **Step 4: Replace outdated documentation with exact boundaries**

README must list:

- official endpoints and outbound network hosts;
- read-only Kabu behavior despite brokers exposing write tools;
- Keychain service/account names;
- in-memory-only portfolio storage;
- disconnect behavior;
- eligibility warning that Robinhood live acceptance requires an eligible Agentic account.

`docs/manual-test.md` must contain numbered tests for Robinhood connect/refresh/disconnect, Longbridge connect/refresh/disconnect, hide/restore cancellation, missing/null rendering, and Inspector/search regression. It must state that live compatibility is unverified until the user completes both authorizations.

- [ ] **Step 5: Run complete hosted-brokerage verification**

Run:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo build --release -p kabu-app
```

Expected: every command exits 0 with no warnings. Ignored real-provider tests remain opt-in and are reported separately.

- [ ] **Step 6: Commit**

```bash
git add Cargo.lock crates/kabu-app/src/runtime.rs crates/kabu-app/src/main.rs README.md docs/manual-test.md
git commit -m "feat: wire hosted brokerage MCP integrations"
```
