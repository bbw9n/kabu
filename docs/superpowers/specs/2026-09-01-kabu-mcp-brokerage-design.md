# Kabu MCP Brokerage Integration Design

**Date:** 2026-09-01  
**Status:** Awaiting implementation approval  
**Scope:** macOS-only Kabu application in `feat/kabu-mvp`

## Purpose

Kabu will connect to Robinhood and Longbridge through their hosted MCP
services, normalize read-only account and portfolio information, and display
that information in the standalone GPUI window. Kabu will also expose an
opt-in, loopback-only MCP server so a local MCP client can read Kabu's
watchlist, attributed quotes, and normalized portfolio snapshot.

This change makes Kabu both an MCP client and an MCP server, but the two roles
remain separate. Remote broker credentials never cross into Kabu's local MCP
responses, and the local MCP server never forwards arbitrary calls to a broker.

## Approved Product Boundaries

- Robinhood MCP endpoint: `https://agent.robinhood.com/mcp/trading`.
- Longbridge MCP endpoint: `https://mcp.longbridge.com`.
- Both remote integrations use MCP Streamable HTTP and OAuth 2.1 with PKCE.
- Kabu requests and invokes read capabilities only.
- Kabu never registers, exposes, or invokes order placement, modification,
  cancellation, DCA, grid, alert mutation, watchlist mutation, or other remote
  write tools.
- Kabu does not provide a generic MCP tool console or pass arbitrary tool names
  or arguments from the UI to a remote server.
- Kabu's local MCP server is read-only, disabled by default, bound only to
  `127.0.0.1`, and protected by a bearer token stored in macOS Keychain.
- Existing Yahoo polling and Binance REST/WebSocket behavior remains the live
  quote path. Direct Longbridge OpenAPI/WebSocket routing is a separate future
  feature and is not part of this implementation.
- No brokerage account or portfolio snapshot is written to Kabu's JSON state
  or market cache. The last successful portfolio exists in memory only.
- No telemetry, analytics, cloud synchronization, or embedded language model is
  added.

## Why This Approach

Three approaches were considered:

1. **MCP-only, including live quotes.** This is the smallest integration but
   hosted MCP tools are request/response-oriented and do not replace Kabu's
   existing bounded live-stream lifecycle. It also makes quote rendering
   dependent on authenticated brokerage availability.
2. **Remote MCP for portfolios plus Kabu's existing live feeds.** This is the
   selected approach. It closes the account/position gap without destabilizing
   the proven Yahoo/Binance refresh and cancellation paths.
3. **Remote MCP plus direct Longbridge OpenAPI streaming in the same change.**
   This provides official push quotes but couples two OAuth clients, provider
   routing, entitlements, and a new quote stream to the portfolio work. It is
   deferred so each integration can be verified independently.

## User Experience

### Navigation

The existing single-window title bar gains three page buttons:

```text
[Kabu]  [Inspector] [Portfolio] [Connections]       [Search] [Theme]
```

- **Inspector** retains the existing watchlist, divider, quote detail, search,
  themes, focus behavior, and keyboard shortcuts unchanged.
- **Portfolio** displays normalized accounts, balances, holdings, timestamps,
  and refresh state.
- **Connections** manages Robinhood, Longbridge, and the local Kabu MCP server.
- `Command-K` remains available from every page and returns to the prior page
  when search closes.
- Page selection is session-only; Kabu still opens to Inspector.

### Connections Page

Robinhood and Longbridge each have a connection card with:

- provider name and official endpoint;
- `Disconnected`, `Authorizing`, `Connecting`, `Connected`, `Refreshing`, or
  `Error` state;
- a sanitized, actionable error message;
- `Connect` while disconnected;
- a disabled progress action plus `Cancel` while authorizing or connecting;
- `Refresh` and `Disconnect` while connected;
- `Retry` and `Disconnect` after an error;
- last successful synchronization time when connected;
- explicit text that the integration is read-only in Kabu.

Selecting `Connect` starts a loopback OAuth callback listener, registers the
native client dynamically when required, opens the authorization URL in the
default browser, validates callback state and issuer, exchanges the code, and
stores the resulting client registration and tokens in Keychain. Cancellation,
timeout, browser rejection, malformed callbacks, and callback-port failures all
return the card to a terminal, retryable state.

Disconnecting clears that provider's Keychain credential, terminates the MCP
session, removes its in-memory portfolio snapshot, and leaves the other broker
and market-data providers untouched.

The local MCP card has an opt-in toggle, stable endpoint, masked token field,
copy actions, token rotation, and server status. Enabling it persists only the
boolean preference in `state.json`; the bearer token remains in Keychain.

### Portfolio Page

When no broker is connected, Portfolio shows a single explicit empty state with
a button leading to Connections. A connected provider renders one section per
account with:

- broker and safe display label;
- base currency;
- total equity, cash, and buying power when returned;
- last synchronization timestamp and stale/error state;
- holdings with symbol, name, asset kind, quantity, average cost, market value,
  and unrealized P&L when returned.

Numbers absent from a provider response render as an em dash, never zero.
Broker-native instruments outside Kabu's US-equity/Binance identity model are
preserved as labeled unsupported holdings rather than dropped or coerced.
Raw account numbers, access tokens, refresh tokens, OAuth client secrets, and
unredacted MCP payloads are never rendered.

Portfolio refreshes immediately after authorization, when the user opens the
Portfolio page after data is older than 60 seconds, on explicit refresh, and
every 60 seconds while Portfolio is visible. Hiding or minimizing Kabu cancels
and awaits broker refresh jobs. Restoring Kabu reconciles once before the
periodic schedule resumes. If the local MCP server is enabled, it remains
available while the application process is alive and serves the most recent
immutable snapshot while the window is hidden.

## Domain Model

`kabu-core` gains a provider-neutral brokerage module:

```rust
pub enum Brokerage {
    Robinhood,
    Longbridge,
}

pub enum BrokerageAssetKind {
    Equity,
    Crypto,
    Option,
    Fund,
    Cash,
    Other,
}

pub struct BrokerageAccount {
    pub brokerage: Brokerage,
    pub id: BrokerageAccountId,
    pub display_label: String,
    pub base_currency: String,
    pub equity: Option<Decimal>,
    pub cash: Option<Decimal>,
    pub buying_power: Option<Decimal>,
}

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

pub struct PortfolioSnapshot {
    pub brokerage: Brokerage,
    pub accounts: Vec<BrokerageAccount>,
    pub positions: Vec<BrokeragePosition>,
    pub received_at: DateTime<Utc>,
}
```

`BrokerageAccountId` is an opaque provider value with a redacted `Debug`
implementation. Monetary and quantity values use `rust_decimal::Decimal` so
normalization and totals do not introduce binary floating-point error.

The async port is intentionally narrow:

```rust
#[async_trait]
pub trait PortfolioProvider: Send + Sync {
    fn brokerage(&self) -> Brokerage;
    async fn refresh_portfolio(&self) -> Result<PortfolioSnapshot, BrokerageError>;
    async fn disconnect(&self) -> Result<(), BrokerageError>;
}
```

`BrokerageErrorKind` distinguishes authorization-required, authorization-
denied, permission, timeout, rate-limit, unavailable, malformed response,
incompatible tool schema, disconnected, secure-storage, and internal failures.
Its user-facing conversion is fixed and sanitized; raw response bodies and
tokens never enter error messages.

## Remote MCP Client Architecture

`kabu-data` gains a shared `mcp_client` module built on official `rmcp 3.2.x`
with these features:

- `client`;
- `auth`;
- `transport-streamable-http-client-reqwest`;
- the existing Rustls AWS-LC provider selection.

Each broker has an isolated client, OAuth credential store, active session,
tool catalog, and cancellation owner. MCP initialization performs `tools/list`
and resolves a provider-specific read-only capability table. A provider is not
reported connected until every required account/position tool has a compatible
input schema.

The adapter uses only a compile-time allowlist. Unknown tools, tools with names
associated with writes, and tools whose schemas differ from the validated
fixture are ignored. Runtime tool results are bounded by response byte count,
array count, string length, and numeric precision before normalization.

Initial capability mappings are:

- Robinhood: accounts, portfolio summary, equity positions, crypto positions,
  and read-only watchlists/order history when present.
- Longbridge: account balance, stock positions, cash/asset summary, and
  read-only watchlist groups when present.

Only accounts, balances, and positions are required for the first Portfolio
snapshot. Optional read tools may be listed in connection diagnostics but do
not expand the first UI.

### OAuth and Keychain

`rmcp::transport::auth::CredentialStore` is implemented by
`MacKeychainCredentialStore`. One generic-password item is used per broker:

- service: `com.kabu.mcp.oauth`;
- account: `robinhood` or `longbridge`;
- value: the serialized `StoredCredentials` value.

The store provides a per-provider refresh guard, uses add-or-update semantics,
and treats item-not-found distinctly from access denial. PKCE authorization
state remains in memory and expires after ten minutes. OAuth uses a one-shot
listener on `127.0.0.1` and validates `state`, PKCE S256, issuer, redirect URI,
and authorization-server discovery. Keychain values and raw callback URLs are
redacted from `Debug` and tracing.

Tests use an injected in-memory credential store and fake OAuth/MCP servers.
Real Keychain and broker authorization are opt-in macOS acceptance tests.

## Runtime and Cancellation

Broker work is separate from `ProviderSet` and `Coordinator`; it is not modeled
as Yahoo/Binance market data. `RuntimeCommand` gains:

- `ConnectBroker(Brokerage)`;
- `CancelBrokerConnection(Brokerage)`;
- `RefreshPortfolio(Brokerage)`;
- `DisconnectBroker(Brokerage)`;
- `SetPortfolioVisible(bool)`;
- `SetLocalMcpEnabled(bool)`;
- `RotateLocalMcpToken`.

`RuntimeEvent` gains coalesced per-broker connection-state and portfolio slots,
plus one local-MCP status slot. Control commands use a reliable coalescing lane;
refresh jobs are at most one per broker and a newer refresh cancels and awaits
the previous job. Broker jobs are not admitted to the existing ordinary market
job cap.

Hiding Kabu cancels OAuth and portfolio refresh tasks but does not clear valid
Keychain credentials or stop the enabled local MCP server. Application
shutdown:

1. rejects new broker and local MCP commands;
2. cancels and awaits broker refresh/OAuth work;
3. closes remote MCP sessions;
4. stops the local listener and active sessions;
5. runs the existing state/cache final flush;
6. acknowledges shutdown only after every owned service is terminal.

A service that fails to stop within the existing bounded close workflow yields
a typed shutdown failure and keeps the close veto active for retry. No detached
MCP, OAuth, or listener task may survive application shutdown.

## Local Kabu MCP Server

The server uses MCP Streamable HTTP and fixed loopback ports:

- release: `http://127.0.0.1:43127/mcp`;
- debug: `http://127.0.0.1:43128/mcp`.

It refuses non-loopback binds, enforces an `Authorization: Bearer` header using
constant-time comparison, rejects non-local `Origin` values, caps request bodies
at 1 MiB, caps active sessions at eight, and expires idle sessions after 15
minutes. The bearer token is 32 random bytes encoded as base64url without
padding, stored under service `com.kabu.mcp.local`, account `bearer-token`.
Rotation writes the new token before restarting the listener.

The server reads immutable snapshots copied from `AppModel` into a bounded,
thread-safe `AgentSnapshotStore`; it never calls GPUI or provider clients from a
request handler. It exposes exactly these read-only tools:

- `kabu_get_watchlist`;
- `kabu_get_quote`;
- `kabu_get_portfolio`;
- `kabu_get_connections`.

Results have explicit JSON schemas, stable provenance fields, timestamps, and
freshness labels. Portfolio results use safe account labels and never include
raw account identifiers. Tool arguments and output arrays are bounded. No tool
can mutate Kabu, call a remote MCP server, or place a trade.

## Persistence and Privacy

`PersistedState` adds only `local_mcp_enabled: bool` with a Serde default of
`false`, retaining compatibility with existing schema-version-1 files. OAuth
credentials, local bearer token, account identifiers, balances, positions, and
broker payloads never enter `state.json` or `cache.json`.

README documentation will list the new Keychain items, network endpoints,
read-only boundary, local MCP exposure, token rotation, and disconnect/delete
behavior. Kabu continues to send no analytics or telemetry.

## Testing and Verification

Implementation follows test-first development. Required automated evidence:

- core decimal/account/position normalization and redacted-debug tests;
- MCP OAuth discovery, PKCE callback validation, credential refresh, and
  disconnect tests against local fake servers;
- provider capability allowlist and schema-drift rejection tests using complete
  captured fixtures;
- malformed, oversized, missing, null, and non-finite broker payload tests;
- runtime command coalescing, one-refresh-per-broker, hide/restore cancellation,
  and bounded shutdown tests;
- GPUI tests for page switching, empty Portfolio, connected/error states,
  refresh, Connections actions, and Inspector/search/focus preservation;
- local MCP bearer/origin/body/session/idle-limit tests and read-only tool
  contract tests;
- state backward-compatibility and `local_mcp_enabled` persistence tests;
- full `cargo fmt --all -- --check`, workspace Clippy with warnings denied,
  `cargo test --workspace`, and release build.

Opt-in live acceptance requires the user to authorize each real broker. It
verifies authentication, tools discovery, portfolio refresh, disconnect/token
removal behavior, and that no write tool is invoked. Lack of real account
authorization does not weaken hermetic coverage, but live compatibility cannot
be claimed until those two acceptance checks pass.

## Explicit Non-Goals

- Placing, reviewing, modifying, or cancelling orders.
- Automated strategies, DCA, grid trading, alerts, transfers, staking, lending,
  or watchlist writes.
- Acting as a generic MCP proxy or tool explorer.
- Importing or synchronizing broker watchlists into Kabu in this release.
- Persisting portfolio snapshots for offline viewing.
- Direct Longbridge OpenAPI or WebSocket quote streaming.
- Replacing Yahoo or Binance as Kabu's live market-data providers.
- Options analytics or position-level tax-lot views.
- Exposing Kabu's MCP server beyond the local Mac.

## Completion Criteria

The feature is complete when Kabu can authorize Robinhood and Longbridge through
their official hosted MCP endpoints, validate and invoke only the required
read-only tools, render normalized account/position data, disconnect without
affecting unrelated providers, run an opt-in authenticated local MCP server with
the four specified read tools, and shut down every owned task cleanly. All
hermetic verification must pass; real-provider compatibility is reported
separately for whichever accounts the user authorizes.
