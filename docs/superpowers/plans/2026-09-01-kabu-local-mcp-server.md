# Kabu Local MCP Server Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose Kabu's current watchlist, attributed quotes, normalized portfolio, and connection status through an opt-in authenticated loopback MCP server that shuts down with the application.

**Architecture:** A bounded immutable snapshot store separates GPUI-owned state from request handlers. An `rmcp` Streamable HTTP service is wrapped by Axum authentication/origin/body/session controls and owned by the existing network runtime, while Keychain contains the bearer token and JSON persists only the disabled-by-default preference.

**Tech Stack:** Rust 1.96, edition 2024, `rmcp 3.2.0` server/macros, Axum 0.8, Tokio/Tokio-util, `subtle`, base64url, `security-framework 3.7`, GPUI 0.2.2, Serde/Schemars.

**Spec:** `docs/superpowers/specs/2026-09-01-kabu-mcp-brokerage-design.md`

**Dependency Plan:** `docs/superpowers/plans/2026-09-01-kabu-hosted-brokerage-mcp.md` must be complete first because this plan reuses `Brokerage`, portfolio state, and `SecretStore`.

## Global Constraints

- Target macOS only and keep one standalone window.
- Local MCP is disabled by default and binds only `127.0.0.1`.
- Release endpoint is exactly `http://127.0.0.1:43127/mcp`; debug endpoint is exactly `http://127.0.0.1:43128/mcp`.
- Require `Authorization: Bearer <token>` and compare token bytes in constant time.
- The token is 32 random bytes encoded as base64url without padding, stored in Keychain service `com.kabu.mcp.local`, account `bearer-token`.
- Reject a non-local `Origin` when present, bodies over 1 MiB, more than eight active MCP sessions, and sessions idle longer than 15 minutes.
- Expose exactly four read-only tools: `kabu_get_watchlist`, `kabu_get_quote`, `kabu_get_portfolio`, and `kabu_get_connections`.
- Tool handlers read immutable bounded snapshots only; they never call GPUI, Yahoo/Binance providers, remote broker MCP services, or mutation commands.
- No tool can place a trade, mutate a watchlist, change settings, rotate tokens, or start/stop the server.
- Local MCP output uses safe account labels and never includes raw account identifiers, credentials, or raw provider payloads.
- Hiding the window does not stop an enabled local server; shutdown cancels the listener and awaits all sessions before close acknowledgement.
- `state.json` persists only `local_mcp_enabled: bool` with `#[serde(default)]`; existing schema-version-1 files remain compatible.
- Default tests are hermetic and bind only loopback ephemeral test ports; production fixed-port behavior is tested through injected addresses.

---

## File Structure

```text
Cargo.toml                                      Shared server, HTTP, auth, and schema dependencies
Cargo.lock                                      Resolved dependency graph
crates/kabu-core/src/state.rs                   Backward-compatible local server preference
crates/kabu-app/
  Cargo.toml
  src/lib.rs                                    Local MCP module exports for integration tests
  src/local_mcp/mod.rs                          Public service handle/status and construction
  src/local_mcp/snapshot.rs                     Immutable bounded agent-facing projection
  src/local_mcp/tools.rs                        Four typed read-only MCP tools
  src/local_mcp/auth.rs                         Token generation, Keychain persistence, redaction
  src/local_mcp/server.rs                       Axum/rmcp listener, middleware, sessions, shutdown
  src/runtime.rs                                Reliable local-service commands/events and ownership
  src/app_model.rs                              Preference/status and snapshot publication
  src/views/connections.rs                      Local MCP connection card
  tests/local_mcp.rs                            HTTP/MCP security, tools, bounds, lifecycle
  tests/app_model.rs                            Preference/event/snapshot model behavior
  tests/gpui_interactions.rs                    Local card opt-in/status/rotate contracts
crates/kabu-data/tests/persistence.rs            Schema-v1 backward compatibility
README.md                                       Exposure, Keychain, endpoint, rotation docs
docs/manual-test.md                             Local client acceptance procedure
```

The bearer token is never stored in an `AppModel` snapshot or printed through `Debug`. Request handlers receive the authenticated request only after middleware succeeds.

---

### Task 1: Persist Only the Disabled-by-Default Preference

**Files:**
- Modify: `crates/kabu-core/src/state.rs`
- Modify: `crates/kabu-data/tests/persistence.rs`

**Interfaces:**
- Consumes: existing `PersistedState`, `JsonRepository`, and app persistence lane.
- Produces: backward-compatible `PersistedState::local_mcp_enabled` for Task 4.

- [ ] **Step 1: Write failing compatibility and model tests**

```rust
#[test]
fn schema_one_without_local_mcp_field_defaults_to_disabled() {
    let mut old_json = serde_json::to_value(PersistedState::starter()).expect("serialize fixture");
    old_json.as_object_mut().expect("state object").remove("local_mcp_enabled");
    let state: PersistedState = serde_json::from_value(old_json).expect("old state");
    assert!(!state.local_mcp_enabled);
}

#[test]
fn starter_state_keeps_local_mcp_disabled() {
    assert!(!PersistedState::starter().local_mcp_enabled);
}

```

- [ ] **Step 2: Verify RED**

Run: `cargo test -p kabu-core state::tests::starter_state_keeps_local_mcp_disabled && cargo test -p kabu-data --test persistence schema_one_without_local_mcp_field_defaults_to_disabled`

Expected: compilation fails because the preference field is absent.

- [ ] **Step 3: Add the additive schema-v1 field**

```rust
#[serde(default)]
pub local_mcp_enabled: bool,
```

Keep `STATE_SCHEMA_VERSION` equal to 1. Initialize it to false in `starter` and carry it through every `PersistedState` literal. App-model propagation is deferred to Task 4, where the runtime command exists.

- [ ] **Step 4: Verify GREEN**

Run:

```bash
cargo test -p kabu-core
cargo test -p kabu-data --test persistence
```

Expected: old state loads disabled, enabled state round-trips, and all existing persistence/model tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/kabu-core/src/state.rs crates/kabu-data/tests/persistence.rs
git commit -m "feat: persist local MCP opt-in preference"
```

---

### Task 2: Build the Immutable Snapshot and Four Read Tools

**Files:**
- Modify: `Cargo.toml`
- Modify: `crates/kabu-app/Cargo.toml`
- Modify: `crates/kabu-app/src/lib.rs`
- Create: `crates/kabu-app/src/local_mcp/mod.rs`
- Create: `crates/kabu-app/src/local_mcp/snapshot.rs`
- Create: `crates/kabu-app/src/local_mcp/tools.rs`
- Create: `crates/kabu-app/tests/local_mcp.rs`

**Interfaces:**
- Consumes: watchlist/instrument/quote/portfolio/connection model values.
- Produces: `AgentSnapshot`, `AgentSnapshotStore`, `KabuMcpTools`, and exact typed output schemas.

- [ ] **Step 1: Write failing snapshot/tool tests**

```rust
#[test]
fn snapshot_projection_removes_raw_account_ids() {
    let snapshot = AgentSnapshot::from_model_fixture(model_fixture_with_account("secret-account-1234"));
    let json = serde_json::to_string(&snapshot).expect("serialize");
    assert!(json.contains("••••1234"));
    assert!(!json.contains("secret-account-1234"));
}

#[tokio::test]
async fn server_lists_exactly_four_read_tools() {
    let tools = KabuMcpTools::new(AgentSnapshotStore::new(fixture_snapshot()));
    let names = tools.tool_names();
    assert_eq!(names, ["kabu_get_connections", "kabu_get_portfolio", "kabu_get_quote", "kabu_get_watchlist"]);
}

#[tokio::test]
async fn quote_lookup_is_bounded_and_keeps_provenance() {
    let result = tools_fixture().get_quote(GetQuoteRequest { symbol: "AAPL".into() }).await.expect("quote");
    assert_eq!(result.source, "Yahoo");
    assert!(result.provider_timestamp.is_some());
    assert!(result.freshness.is_some());
}
```

- [ ] **Step 2: Verify RED**

Run:

```bash
cargo test -p kabu-app --test local_mcp snapshot_projection_removes_raw_account_ids
cargo test -p kabu-app --test local_mcp server_lists_exactly_four_read_tools
cargo test -p kabu-app --test local_mcp quote_lookup_is_bounded_and_keeps_provenance
```

Expected: compilation fails because the local MCP modules do not exist.

- [ ] **Step 3: Add server-side dependencies**

Extend the existing `rmcp` features and add HTTP/schema dependencies:

```toml
rmcp = { version = "3.2.0", default-features = false, features = [
  "auth",
  "client",
  "macros",
  "server",
  "transport-streamable-http-client-reqwest",
  "transport-streamable-http-server",
] }
axum = "0.8"
base64 = "0.22"
schemars = "1"
subtle = "2"
tower = "0.5"
tokio-util = { version = "0.7", features = ["rt"] }
```

Wire `axum`, `base64`, `rmcp`, `schemars`, `serde`, `serde_json`, `subtle`, `tokio-util`, `tower`, and `zeroize` into `kabu-app`; add `zeroize.workspace = true` even though the workspace entry was introduced by the hosted-brokerage plan.

- [ ] **Step 4: Implement the bounded snapshot projection**

`AgentSnapshot` contains generated-at time, at most 256 watchlist entries, one quote per entry, at most two broker snapshots, at most 2,000 total holdings, and two connection statuses. Its public account output has `display_label` but no account-id field.

`AgentSnapshotStore` is `Arc<RwLock<Arc<AgentSnapshot>>>`; `replace` validates caps before atomically replacing, and `current` clones only the `Arc`.

- [ ] **Step 5: Implement exactly four typed tools**

Use `#[rmcp::tool_router]` and `#[rmcp::tool_handler]`. Tool methods only clone `store.current()` and filter it:

```rust
#[tool(name = "kabu_get_watchlist", description = "Read Kabu's current watchlist and attributed quote summaries")]
async fn get_watchlist(&self) -> Result<Json<WatchlistOutput>, ErrorData>;

#[tool(name = "kabu_get_quote", description = "Read one watched symbol's latest attributed quote")]
async fn get_quote(&self, Parameters<GetQuoteRequest>) -> Result<Json<QuoteOutput>, ErrorData>;

#[tool(name = "kabu_get_portfolio", description = "Read normalized in-memory brokerage accounts and holdings")]
async fn get_portfolio(&self, Parameters<GetPortfolioRequest>) -> Result<Json<PortfolioOutput>, ErrorData>;

#[tool(name = "kabu_get_connections", description = "Read Kabu market-data and brokerage connection status")]
async fn get_connections(&self) -> Result<Json<ConnectionsOutput>, ErrorData>;
```

`GetQuoteRequest.symbol` is capped at 32 characters. `GetPortfolioRequest.brokerage` is optional and accepts only `robinhood` or `longbridge`. Tool errors use fixed invalid-argument/not-found strings and contain no serialized internal errors.

- [ ] **Step 6: Verify GREEN**

Run: `cargo test -p kabu-app --test local_mcp`

Expected: snapshot bounds, redaction, exact tool list, schemas, and quote provenance tests pass.

- [ ] **Step 7: Commit**

```bash
git add Cargo.toml Cargo.lock crates/kabu-app
git commit -m "feat(app): add local MCP snapshot tools"
```

---

### Task 3: Add Keychain Token and Hardened Loopback HTTP Service

**Files:**
- Create: `crates/kabu-app/src/local_mcp/auth.rs`
- Create: `crates/kabu-app/src/local_mcp/server.rs`
- Modify: `crates/kabu-app/src/local_mcp/mod.rs`
- Modify: `crates/kabu-app/tests/local_mcp.rs`

**Interfaces:**
- Consumes: shared `SecretStore` from the hosted brokerage plan and `KabuMcpTools` from Task 2.
- Produces: `LocalMcpTokenStore`, `LocalMcpServerConfig`, `LocalMcpService`, `LocalMcpStatus`, `start`, `rotate`, and awaited `stop`.

- [ ] **Step 1: Write failing security and lifecycle tests**

Bind injected `127.0.0.1:0` in tests and use a real HTTP client/MCP initialize call:

```rust
#[tokio::test]
async fn missing_or_wrong_bearer_is_rejected() {
    let server = start_test_server().await;
    assert_eq!(post_initialize(server.url(), None).await.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(post_initialize(server.url(), Some("wrong")).await.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn non_local_origin_and_oversized_body_are_rejected() {
    let server = start_test_server().await;
    assert_eq!(post_with_origin(&server, "https://evil.example").await.status(), StatusCode::FORBIDDEN);
    assert_eq!(post_body(&server, vec![b'x'; 1_048_577]).await.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test(start_paused = true)]
async fn ninth_session_is_rejected_and_idle_sessions_expire() {
    let server = start_test_server().await;
    let sessions = initialize_sessions(&server, 8).await;
    assert_eq!(initialize_session(&server).await.status(), StatusCode::SERVICE_UNAVAILABLE);
    tokio::time::advance(Duration::from_secs(901)).await;
    server.run_idle_cleanup_once().await;
    assert_eq!(server.active_sessions().await, 0);
    drop(sessions);
}

#[tokio::test]
async fn stop_awaits_inflight_requests() {
    let server = start_server_with_blocking_tool().await;
    let request = spawn_blocking_tool_call(&server);
    server.stop().await.expect("stop");
    assert!(request.await.is_err());
    assert!(server.is_terminal());
}
```

- [ ] **Step 2: Verify RED**

Run:

```bash
cargo test -p kabu-app --test local_mcp missing_or_wrong_bearer_is_rejected
cargo test -p kabu-app --test local_mcp non_local_origin_and_oversized_body_are_rejected
cargo test -p kabu-app --test local_mcp ninth_session_is_rejected_and_idle_sessions_expire
cargo test -p kabu-app --test local_mcp stop_awaits_inflight_requests
```

Expected: compilation fails because token and server modules are absent.

- [ ] **Step 3: Implement bearer generation and Keychain storage**

```rust
pub struct LocalMcpToken(Zeroizing<Vec<u8>>);

impl LocalMcpTokenStore {
    pub fn load_or_create(&self) -> Result<LocalMcpToken, LocalMcpError>;
    pub fn rotate(&self) -> Result<LocalMcpToken, LocalMcpError>;
    pub fn delete(&self) -> Result<(), LocalMcpError>;
}
```

Generate exactly 32 bytes with `rand::rngs::OsRng`, encode with `base64::engine::general_purpose::URL_SAFE_NO_PAD`, and store under the exact Keychain service/account. Implement manual redacted `Debug`; zeroize decoded bytes on drop. `rotate` writes the new token before returning it.

- [ ] **Step 4: Implement the hardened service**

`LocalMcpServerConfig::production()` chooses port 43128 under `cfg!(debug_assertions)` and 43127 otherwise. Reject any configured IP other than `Ipv4Addr::LOCALHOST` before binding.

Wrap `StreamableHttpService<KabuMcpTools, CappedSessionManager>` in an Axum route `/mcp`. Middleware order is body limit (1 MiB), host/origin validation, bearer validation, then MCP. Missing `Origin` is allowed; present origins must parse to `http://127.0.0.1:<bound-port>` or `http://localhost:<bound-port>`. Compare decoded authorization bytes with `subtle::ConstantTimeEq`.

`CappedSessionManager` delegates `SessionManager` to `LocalSessionManager`, serializes `create_session`, returns a typed capacity error when `sessions.len() == 8`, records `Instant` activity by session ID on every accepted request, and closes entries idle for more than 900 seconds. A tracked cleanup task runs every 60 seconds.

`LocalMcpService` owns the Axum serve task, rmcp cancellation token, cleanup cancellation token, and active request tasks. `stop(self)` cancels acceptance/sessions, awaits the server and cleanup tasks, and returns only after terminal completion.

- [ ] **Step 5: Verify GREEN**

Run:

```bash
cargo test -p kabu-app --test local_mcp
cargo test -p kabu-app
```

Expected: all HTTP security, session, lifecycle, tool, and existing app tests pass.

- [ ] **Step 6: Commit**

```bash
git add crates/kabu-app/src/local_mcp crates/kabu-app/tests/local_mcp.rs
git commit -m "feat(app): serve authenticated loopback MCP"
```

---

### Task 4: Own the Local Server in Runtime and Publish Snapshots

**Files:**
- Modify: `crates/kabu-app/src/runtime.rs`
- Modify: `crates/kabu-app/src/app_model.rs`
- Modify: `crates/kabu-app/tests/app_model.rs`

**Interfaces:**
- Consumes: persisted preference, `AgentSnapshotStore`, and `LocalMcpService`.
- Produces: reliable start/stop/rotate commands, status events, snapshot updates, and ordered application shutdown.

- [ ] **Step 1: Write failing runtime/model lifecycle tests**

```rust
#[tokio::test]
async fn enable_starts_once_and_disable_awaits_stop() {
    let factory = RecordingLocalMcpFactory::default();
    let runtime = runtime_with_local_factory(factory.clone());
    runtime.send(RuntimeCommand::SetLocalMcpEnabled(true)).expect("enable");
    runtime.send(RuntimeCommand::SetLocalMcpEnabled(true)).expect("duplicate enable");
    factory.wait_until_started().await;
    assert_eq!(factory.start_count(), 1);
    runtime.send(RuntimeCommand::SetLocalMcpEnabled(false)).expect("disable");
    factory.wait_until_stopped().await;
}

#[tokio::test]
async fn shutdown_waits_for_local_service_before_acknowledging() {
    let factory = BlockingStopLocalMcpFactory::default();
    let runtime = runtime_with_local_factory(factory.clone());
    runtime.send(RuntimeCommand::SetLocalMcpEnabled(true)).expect("enable");
    let shutdown = runtime.begin_shutdown();
    assert!(!shutdown.is_finished());
    factory.release_stop();
    assert!(shutdown.await.is_success());
}

#[test]
fn model_events_publish_a_bounded_agent_snapshot() {
    let mut model = connected_model_with_quotes();
    model.apply_runtime_event(RuntimeEvent::PortfolioUpdated(snapshot(Brokerage::Robinhood, 3)));
    let command = model.runtime().recorded_commands().last().expect("snapshot command");
    assert!(matches!(command, RuntimeCommand::UpdateAgentSnapshot(_)));
}

#[test]
fn enabling_local_mcp_persists_preference_and_dispatches_control() {
    let mut model = model_with_recording_runtime();
    model.set_local_mcp_enabled(true);
    assert!(model.local_mcp_enabled());
    let commands = model.runtime().recorded_commands();
    assert!(commands.iter().any(|command| matches!(command, RuntimeCommand::SetLocalMcpEnabled(true))));
    assert!(commands.iter().any(|command| matches!(command, RuntimeCommand::PersistState(state) if state.local_mcp_enabled)));
}
```

- [ ] **Step 2: Verify RED**

Run:

```bash
cargo test -p kabu-app runtime::tests::enable_starts_once_and_disable_awaits_stop
cargo test -p kabu-app runtime::tests::shutdown_waits_for_local_service_before_acknowledging
cargo test -p kabu-app --test app_model model_events_publish_a_bounded_agent_snapshot
```

Expected: compilation fails because runtime local-service ownership is absent.

- [ ] **Step 3: Add reliable commands and fixed status slot**

Add:

```rust
RuntimeCommand::SetLocalMcpEnabled(bool)
RuntimeCommand::RotateLocalMcpToken
RuntimeCommand::UpdateAgentSnapshot(Arc<AgentSnapshot>)
RuntimeEvent::LocalMcpStatusChanged(LocalMcpStatus)
```

Use `RuntimeEventSlot::LocalMcpStatus`. Enable/disable/rotate are reliable controls. Snapshot updates are latest-value-wins and replace the store value without starting a job.

- [ ] **Step 4: Implement runtime ownership and ordered shutdown**

Add an injected `LocalMcpFactory` to `LiveRuntime`; production uses the real factory. Enabling starts at most one service and publishes Starting then Running with the endpoint or Error with fixed safe copy. Disabling takes the service, awaits `stop`, then publishes Stopped. Rotation writes Keychain first, stops the old service, and starts a new service with the new token.

Pass the recovered `PersistedState::local_mcp_enabled` into runtime startup so a previously enabled server starts without waiting for a view action. Startup bind/Keychain failures publish `LocalMcpStatus::Error` and do not fail Yahoo/Binance startup.

Hiding/minimizing does not alter the service. Shutdown rejects new controls, stops and awaits the service before final state/cache flush and before publishing `ShutdownOutcome`. A stop/join failure sets `runtime_failed` so the existing close veto remains active for retry.

`AppModel` publishes a new bounded `AgentSnapshot` after any watchlist, quote, freshness, portfolio, or connection-state change while the local server is enabled. It stores only `LocalMcpStatus`, never the token.

Add `AppModel::{local_mcp_enabled,set_local_mcp_enabled}` and include the preference in its persisted-state projection. The setter updates the preference, sends the reliable runtime control, and schedules the existing state persistence lane; it does not claim Running until the runtime status event arrives.

- [ ] **Step 5: Verify GREEN and shutdown regressions**

Run:

```bash
cargo test -p kabu-app runtime::tests
cargo test -p kabu-app --test app_model
cargo test -p kabu-app --test gpui_interactions
```

Expected: local lifecycle tests and all existing close-veto/final-flush tests pass.

- [ ] **Step 6: Commit**

```bash
git add crates/kabu-app/src/runtime.rs crates/kabu-app/src/app_model.rs crates/kabu-app/tests/app_model.rs
git commit -m "feat(app): own local MCP service lifecycle"
```

---

### Task 5: Add the Connections Card and Complete Verification

**Files:**
- Modify: `crates/kabu-app/src/views/connections.rs`
- Modify: `crates/kabu-app/src/views/app_shell.rs`
- Modify: `crates/kabu-app/tests/gpui_interactions.rs`
- Modify: `README.md`
- Modify: `docs/manual-test.md`

**Interfaces:**
- Consumes: `AppModel` local preference/status and enable/rotate intents.
- Produces: opt-in local MCP card, copyable endpoint/config guidance, token rotation, and user-facing security documentation.

- [ ] **Step 1: Write failing GPUI card tests**

```rust
#[gpui::test]
fn local_mcp_card_is_disabled_by_default_and_enables_explicitly(cx: &mut gpui::TestAppContext) {
    let (window, shell) = shell_with_default_model(cx);
    click_by_id(cx, window, "page-connections");
    assert!(rendered_text(cx, window).contains("Local MCP server"));
    assert!(rendered_text(cx, window).contains("Disabled"));
    click_by_id(cx, window, "local-mcp-enable");
    assert_eq!(last_runtime_command(cx, &shell), RuntimeCommand::SetLocalMcpEnabled(true));
}

#[gpui::test]
fn running_local_card_shows_endpoint_but_never_token(cx: &mut gpui::TestAppContext) {
    let (window, shell) = shell_with_default_model(cx);
    publish_local_status(cx, &shell, LocalMcpStatus::Running { endpoint: "http://127.0.0.1:43128/mcp".parse().expect("url") });
    let text = rendered_text(cx, window);
    assert!(text.contains("http://127.0.0.1:43128/mcp"));
    assert!(!text.contains("bearer-token-fixture"));
}

#[gpui::test]
fn copy_token_reads_keychain_into_clipboard_without_model_storage(cx: &mut gpui::TestAppContext) {
    let secrets = Arc::new(MemorySecretStore::with_local_token("bearer-token-fixture"));
    let (window, _shell) = shell_with_secrets(cx, secrets);
    click_by_id(cx, window, "local-mcp-copy-token");
    cx.run_until_parked();
    assert_eq!(cx.read_from_clipboard().expect("clipboard").text(), Some("bearer-token-fixture".into()));
    assert!(!rendered_text(cx, window).contains("bearer-token-fixture"));
}
```

- [ ] **Step 2: Verify RED**

Run:

```bash
cargo test -p kabu-app --test gpui_interactions local_mcp_card_is_disabled_by_default_and_enables_explicitly
cargo test -p kabu-app --test gpui_interactions running_local_card_shows_endpoint_but_never_token
cargo test -p kabu-app --test gpui_interactions copy_token_reads_keychain_into_clipboard_without_model_storage
```

Expected: tests fail because the local card is absent.

- [ ] **Step 3: Render the local MCP card**

Show Disabled, Starting, Running, Stopping, or Error status. Provide Enable/Disable and Rotate Token actions; rotation is a two-click confirmation (`Rotate Token`, then `Confirm Rotation`) because connected clients will be disconnected. Show a permanently masked token field (`••••••••`), Copy Token, Copy Endpoint, and a copyable configuration snippet containing a `<TOKEN FROM KEYCHAIN>` marker.

Inject `Arc<LocalMcpTokenStore>` into `AppShell` through a testable constructor. Copy Token starts a GPUI background task that calls `load_existing`, returns the `Zeroizing<String>` directly to the UI callback, writes it to the clipboard, and drops it immediately. The value is never assigned to an `AppShell` or `AppModel` field, included in a notification, or rendered. Endpoint/config copy actions use the normal GPUI clipboard API.

- [ ] **Step 4: Document exact operation and manual acceptance**

README and `docs/manual-test.md` must document:

- disabled-by-default behavior and fixed debug/release endpoints;
- Keychain service/account and how to retrieve the token locally;
- sample MCP client configuration with the bearer header;
- four exact read tools and output privacy boundary;
- token rotation disconnect behavior;
- loopback/origin/body/session/idle limits;
- shutdown ownership and no availability after Kabu exits.

- [ ] **Step 5: Run full workspace verification**

Run:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo build --release -p kabu-app
```

Expected: every command exits 0 with no warnings.

- [ ] **Step 6: Manually smoke the local endpoint**

Run Kabu in debug, enable Local MCP from Connections, read the token from Keychain, and connect an MCP inspector to `http://127.0.0.1:43128/mcp`. Verify `tools/list` contains exactly four names, each tool returns current snapshots, a missing bearer receives 401, hiding Kabu keeps the endpoint available, and quitting Kabu makes the port refuse connections.

- [ ] **Step 7: Commit**

```bash
git add crates/kabu-app/src/views/connections.rs crates/kabu-app/tests/gpui_interactions.rs README.md docs/manual-test.md
git commit -m "feat: expose opt-in local Kabu MCP server"
```
