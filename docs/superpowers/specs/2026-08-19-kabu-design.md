# Kabu Design

Date: 2026-08-19

## Summary

Kabu is a macOS-only native market watcher built in Rust with
[GPUI](https://gpui.rs/) and
[gpui-component](https://github.com/longbridge/gpui-component). It takes
inspiration from [Pulse](https://github.com/fatwang2/Pulse), but its first
release is a standalone desktop window rather than a menu-bar application.

The product is optimized for one quick loop: find a US stock or cryptocurrency,
add it to a watchlist, and inspect its current quote and recent price history
without opening a trading terminal.

## Goals

- Ship a polished, keyboard-friendly macOS application rendered entirely with
  GPUI and gpui-component.
- Support live US stock and cryptocurrency watchlists without requiring an API
  key.
- Provide symbol search, add/remove operations, current quote state, daily
  movement, and line charts for common time ranges.
- Stream cryptocurrency updates while the window is visible and poll stock
  quotes every 15 seconds.
- Preserve the last useful state locally and degrade visibly when a provider is
  stale or unavailable.
- Keep provider, domain, and presentation code in separate crates so additional
  markets or frontends can be added without restructuring the application.

## Non-goals

- Trading, brokerage connectivity, orders, or account authentication.
- Position tracking, cost basis, portfolio P&L, alerts, news, or sharing.
- Hong Kong stocks, China A-shares, funds, indices, or derivatives.
- A menu-bar status item, Windows/Linux support, or mobile clients.
- Tick-perfect professional market data or hidden claims of real-time stock
  entitlements.
- Candlestick rendering in v1. The domain retains OHLCV samples, but the v1
  chart presents their close values as a line.
- Telemetry, analytics, user accounts, or cloud synchronization.

## Product Experience

### Window and layout

Kabu opens a single 980 by 640 point window with a minimum size of 760 by 500.
The window uses the gpui-component root and title-bar infrastructure and an
Inspector layout:

- A persistent, resizable left sidebar contains the watchlist.
- The main panel contains the selected instrument's quote, chart, range tabs,
  market metrics, source, timestamp, and freshness state.
- The selected watchlist row remains visually distinct in every theme.
- Empty, loading, stale, unavailable, and populated states have deliberate
  presentations; the UI never substitutes an empty number for missing data.

The first launch seeds a removable starter watchlist containing AAPL, NVDA,
BTC/USDT, and ETH/USDT so the product is immediately demonstrable. Subsequent
launches restore the user's exact watchlist order and selection.

### Search and navigation

- Command-K opens a centered symbol-search overlay and focuses its input.
- Search is debounced for 250 milliseconds, then queries US equities and the
  cached Binance Spot catalog concurrently.
- Exact ticker, company-name, crypto-base, and crypto-pair matches rank first.
- Results label the asset class, venue or pair, and active provider.
- Arrow keys move through results; Return adds and selects an instrument;
  Escape closes the overlay.
- Up and Down move through the watchlist when it has focus.
- Delete removes the selected instrument. Removal is immediately reversible by
  adding the symbol again, so it does not require a confirmation dialog.
- The search action is also available as a visible toolbar button.

Selecting a watchlist row changes the quote header immediately from cached
state. Missing chart history loads asynchronously without blocking selection.

### Quote detail

The detail panel shows:

- display name, canonical symbol or crypto pair, asset class, and venue;
- last price, absolute daily change, and percentage daily change;
- market or stream state and whether the value is fresh, delayed, stale, or
  unavailable;
- a line chart for 1D, 1W, 1M, 3M, and 1Y;
- open, high, low, previous close, and volume when supplied by the provider;
- provider name, provider timestamp, and age of the last successful update.

Positive and negative colors are not the only signal: changes also include a
sign and text. Numeric columns use tabular digits.

### Themes

Kabu ships all three approved visual treatments:

- **Graphite:** a restrained light macOS workspace.
- **Midnight:** a subdued dark workspace with brighter market signals.
- **Signal:** an indigo-accented, more branded treatment.

The theme setting has four values: System, Graphite, Midnight, and Signal.
System maps light macOS appearance to Graphite and dark appearance to Midnight.
A title-bar theme control changes the setting without restarting, and the choice
is persisted locally.

## Workspace Architecture

Kabu is a Cargo workspace with three crates and one-way dependencies:

```text
kabu-app  ───────▶  kabu-core  ◀───────  kabu-data
  binary              library               library
   GPUI           domain + ports         adapters + I/O
```

`kabu-app` depends on both library crates and constructs concrete adapters.
`kabu-data` depends on `kabu-core` to implement its provider and repository
ports. `kabu-core` has no dependency on GPUI, HTTP, WebSockets, filesystem
paths, or provider wire formats.

### kabu-core

The pure core library owns:

- `InstrumentId`, which distinguishes US equities from crypto base/quote pairs;
- `Instrument`, `Quote`, `Candle`, `ChartPeriod`, `Watchlist`, `MarketState`,
  `DataSource`, and `Freshness`;
- provider-neutral search results and provider capability metadata;
- the `InstrumentSearch`, `QuoteSnapshots`, `QuoteSubscription`,
  `ChartHistory`, and `StateRepository` ports;
- refresh policy, result ranking, watchlist deduplication, quote merge rules,
  and fresh/stale/unavailable state transitions;
- a clock abstraction used by deterministic freshness and retry tests.

US equity identity uses a normalized uppercase ticker plus venue where known.
Crypto identity always stores separate uppercase base and quote assets. Wire
symbols such as `BTCUSDT` never replace the canonical `BTC/USDT` identity.

Every quote includes its instrument identity, price and daily movement,
available session metrics, provider timestamp, receipt timestamp, source, and
market state. Every candle includes a start time, open, high, low, close, and
volume.

### kabu-data

The data library owns three adapter families:

1. **Yahoo adapter:** US equity search, snapshot data, and historical chart
   data from Yahoo Finance's public endpoints. These endpoints are unofficial,
   have no SLA, and remain completely isolated behind the core ports.
2. **Binance adapter:** a cached public Spot symbol catalog, REST snapshots and
   klines, and WebSocket ticker subscriptions. No Binance account or API key is
   required.
3. **Local repository:** versioned JSON persistence for user state and provider
   cache using the macOS Application Support directory.

The Binance catalog is cached for 24 hours. A base-only query such as `BTC`
ranks common active quote pairs such as BTC/USDT ahead of unrelated matches,
while an exact pair query always wins.

Provider wire models are private to this crate. Conversion into core types is
explicit and rejects malformed numeric values or timestamps rather than
inventing defaults.

### kabu-app

The binary crate owns:

- GPUI startup, the application window, and gpui-component initialization;
- the `AppModel`, one observable entity containing the watchlist, selected
  instrument, quote states, search state, chart state, theme, and task status;
- the Inspector shell, watchlist view, search overlay, quote header, chart,
  metrics, theme control, loading states, and toasts;
- command/key bindings and translation from UI intents into core operations;
- background task lifecycle and delivery of typed provider events to the model.

Views only format and render model state. They do not deserialize provider
responses, open sockets, choose retry delays, or write files.

GPUI and gpui-component are pinned to a mutually compatible revision in
`Cargo.toml`, and the workspace commits `Cargo.lock`. This avoids accidental API
breakage from GPUI's active upstream development.

## Runtime Data Flow

### Launch

1. The local repository loads versioned user state and the last good quote
   cache.
2. `AppModel` renders that state immediately and restores the selected
   instrument and theme.
3. The refresh coordinator requests fresh snapshots for the watchlist.
4. It starts a Binance ticker subscription for crypto instruments and schedules
   15-second Yahoo polls for US equities.
5. Typed quote events merge into core state, after which `AppModel` notifies the
   affected views.

Losing keyboard focus does not pause updates. Minimizing or hiding the only
window pauses polling and WebSocket activity; restoring it performs a snapshot
reconciliation before resuming. Closing the window exits the v1 application
after pending state has been flushed.

### Search and add

1. The UI sends a debounced query.
2. Yahoo search and local Binance-catalog search run concurrently.
3. Core ranking normalizes and deduplicates the results.
4. Selecting a result adds it to the ordered watchlist, selects it, and writes
   the new state atomically.
5. Snapshot and default 1D history requests begin concurrently.
6. A crypto addition also updates the active WebSocket subscription set.

One provider's failure does not discard successful results from the other. The
search overlay shows a compact provider-specific warning with an inline retry.

### Refresh and charts

- US equities poll every 15 seconds while the window is visible. Rate limiting
  or repeated failure increases the interval using capped exponential backoff
  with jitter.
- Cryptocurrency tickers stream while visible. REST snapshots reconcile state
  at initial connection and after every reconnect.
- Binance reconnects wait 1, 2, 4, 8, 16, 32, and at most 60 seconds, apply up
  to 20 percent jitter, and rebuild the subscription from the current watchlist.
- A failed Yahoo poll increases its interval from 15 to 30, 60, and at most 120
  seconds with up to 20 percent jitter. A success resets it to 15 seconds.
- Charts load on selection and period change. Successful history is cached by
  instrument and period: 1D for 60 seconds, 1W/1M for 5 minutes, and 3M/1Y for
  30 minutes.
- A newer request token supersedes an older chart request so rapid selection
  changes cannot paint the wrong instrument's data.

Crypto is Live when its most recent tick is no more than 5 seconds old and
Stale after 15 seconds without a tick. During a regular US equity session, a
stock snapshot is Fresh through 30 seconds and Stale after 2 minutes; the UI
shows its age between those thresholds. Provider-supplied delayed status is
rendered as Delayed regardless of receipt age. Outside a regular session, a
valid last close is labeled Closed rather than Stale.

## Persistence

Kabu stores data below `~/Library/Application Support/Kabu/`:

- `state.json`: schema version, ordered watchlist, selected instrument, window
  size, sidebar width, and theme setting;
- `cache.json`: last good quotes, cached chart summaries, Binance catalog
  metadata, and provider timestamps.

Writes use a temporary file in the same directory followed by rename. A corrupt
file is retained with a timestamped `.corrupt` suffix, safe defaults are loaded,
and a visible warning explains that recovery occurred. User state and provider
cache are independent so a disposable cache failure cannot erase a watchlist.

## Failure Handling

Provider failures use a typed `MarketDataError` category: timeout, rate limited,
unavailable, disconnected, malformed response, unsupported instrument, or
internal adapter failure.

- The last good quote stays visible through transient failures.
- Freshness is derived from provider and receipt timestamps; stale values are
  labeled rather than silently presented as current.
- An instrument with no cached value renders an unavailable state, never zero.
- Provider errors appear near the affected data and in a non-blocking toast only
  when user action is useful.
- Retries are cancellable and bounded. Closing, hiding, removing an instrument,
  or replacing a search/chart request stops obsolete work.
- Logs exclude search text and persisted watchlist contents by default. No
  credentials or personal identifiers exist in v1.
- A panic in parsing fixture-shaped external data is a bug; adapters return
  typed errors for untrusted responses.

## Component Boundaries

gpui-component supplies the root container, title bar, buttons, icon buttons,
input, virtualized lists, tabs, tooltips, skeletons, and toasts. Kabu supplies
thin composition views and a custom GPUI line-chart element. The chart consumes
only core `Candle` values and owns no networking or caching.

Large views are separated by responsibility: `AppShell`, `WatchlistPane`,
`SymbolSearch`, `QuoteDetail`, `PriceChart`, `MetricGrid`, and `ThemeSwitcher`.
No view file should also become the refresh coordinator or provider adapter.

## Verification Strategy

### Core tests

Deterministic unit tests cover:

- equity and crypto identity normalization;
- watchlist ordering, deduplication, add, and removal;
- cross-provider search ranking and partial-provider failure;
- quote merge ordering and rejection of older events;
- fresh, delayed, stale, and unavailable transitions under a fake clock;
- refresh and retry policy boundaries;
- chart request token supersession.

### Data adapter tests

- Recorded Yahoo and Binance fixtures cover successful and malformed payloads.
- Local HTTP servers cover timeouts, HTTP errors, rate limits, and recovery.
- A local WebSocket server covers subscription, ticker parsing, disconnect,
  backoff, resubscription, and REST reconciliation.
- Temporary directories cover atomic persistence, schema version handling,
  independent state/cache corruption, and diagnostic backup behavior.
- Default tests never depend on public internet access.

### Application tests

GPUI test contexts cover command dispatch, search result selection, watchlist
selection and removal, chart-period changes, stale/error rendering decisions,
and theme persistence. Presentation tests assert meaningful state and component
behavior rather than brittle pixel snapshots.

### Live and manual acceptance

An ignored, opt-in live smoke test verifies the current Yahoo and Binance public
contracts. It is explicitly separate from the hermetic default suite because
public endpoints can be rate-limited or unavailable.

Before completion:

1. `cargo fmt --check` passes.
2. `cargo clippy --workspace --all-targets -- -D warnings` passes.
3. `cargo test --workspace` passes without network access.
4. The release-mode application builds and launches on macOS.
5. A manual run verifies search/add/remove, keyboard navigation, all chart
   periods, Graphite/Midnight/Signal/System themes, persistence across restart,
   live crypto updates, stock polling, and offline degradation.
6. The repository includes a step-by-step manual test guide with exact commands
   and expected results.

## Risks and Mitigations

- **Yahoo endpoint instability:** keep its wire protocol inside one adapter,
  maintain recorded fixtures, show the data source, and provide an opt-in live
  contract smoke.
- **GPUI API churn:** pin compatible revisions and commit the lockfile.
- **WebSocket/task leaks:** tie every long-lived task to visible-window and
  watchlist lifetimes, and test cancellation and reconnection explicitly.
- **False freshness:** carry source timestamps through core types and render
  freshness labels next to every selected quote.
- **Scope growth:** keep positions, menu-bar behavior, additional markets,
  candlesticks, alerts, and provider accounts outside this release.

## Completion Criteria

The MVP is complete when a fresh macOS checkout can build and launch Kabu, the
starter watchlist renders, a user can search for and add a US equity or Binance
Spot pair, quotes refresh at the agreed cadence, detail periods load correctly,
all themes work, state survives restart, provider failures remain understandable
and non-destructive, and the hermetic verification suite plus documented manual
acceptance pass.
