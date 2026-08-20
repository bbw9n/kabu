# Kabu

Kabu is a macOS-only desktop market watcher built in Rust with GPUI 0.2.2. It
keeps an ordered watchlist of US equities and Binance Spot pairs, searches both
asset classes, shows attributed quotes and metrics, and renders line charts for
1D, 5D, 1M, 6M, and 1Y.

Kabu is an informational viewer, not a broker or trading system. Prices can be
late, incomplete, or unavailable. Do not use Kabu alone to make trading or
investment decisions.

## Prerequisites

- macOS. Kabu does not build or run as an application on Linux or Windows.
- Rust 1.96.0 with `rustfmt` and `clippy`. The checked-in
  `rust-toolchain.toml` selects it automatically when Rust is managed by
  rustup.
- Apple developer command-line tools and a selected developer directory.
- The macOS Metal compiler and `metallib`. GPUI 0.2.2 invokes both through
  `xcrun` while building its shaders. Command Line Tools alone may not provide
  them; select a full Xcode installation and download its Metal toolchain when
  necessary.

Verify the toolchain before building:

```bash
rustup toolchain install 1.96.0 --component rustfmt,clippy
rustc --version
xcode-select -p
xcrun -f metal
xcrun -f metallib
```

If either Metal lookup fails with a valid Xcode installed at the standard
location:

```bash
sudo xcode-select --switch /Applications/Xcode.app/Contents/Developer
xcodebuild -downloadComponent MetalToolchain
xcrun -f metal
xcrun -f metallib
```

## Build, test, and run

Run these from the repository root.

```bash
# Development build of all three crates
cargo build --workspace

# Hermetic default suite; ignored live tests do not contact public endpoints
cargo test --workspace

# Development application
cargo run -p kabu-app

# Release application
cargo build --release -p kabu-app
./target/release/kabu-app
```

The opt-in provider contract smoke uses production endpoints and can fail
because of network policy, endpoint changes, or rate limiting:

```bash
KABU_LIVE_TESTS=1 cargo test -p kabu-data --test live_market -- --ignored --nocapture
```

Running `cargo test -p kabu-data --test live_market` without `--ignored` only
compiles the three live contracts and reports them ignored. Even an explicit
`--ignored` run does not make a public request unless `KABU_LIVE_TESTS=1`.

See [docs/manual-test.md](docs/manual-test.md) for the complete, non-destructive
macOS acceptance procedure.

## What the app does

- Seeds the first launch with AAPL, NVDA, BTC/USDT, and ETH/USDT.
- Opens search with Command-K, waits 250 ms after input, and combines Yahoo US
  equity results with the cached Binance Spot catalog.
- Adds a result at the end of the watchlist, selects an existing row on a
  duplicate add, and supports keyboard selection and removal.
- Polls visible US equities every 15 seconds and applies bounded backoff after
  failures.
- Reconciles crypto quotes with REST, then streams Binance ticker updates while
  the window is visible and reconnects with bounded backoff.
- Shows source, provider timestamp, age, market/stream state, daily movement,
  available session metrics, and 1D/5D/1M/6M/1Y line charts.
- Offers System, Graphite, Midnight, and Signal theme preferences. System maps
  the current macOS light appearance to Graphite and dark appearance to
  Midnight.
- Persists watchlist order and selection, window size, sidebar width, theme,
  last good quotes, charts, and Binance catalog data.

Kabu balances visibility and data use deliberately. Losing keyboard focus does
not stop updates. Minimizing the window or hiding the application cancels stock
polling, crypto streaming, search, and chart work; restoring it reconciles
quotes before steady-state updates resume. Closing the only window first
flushes state and cache, then exits.

## Freshness and failures

Freshness is based on the older of provider time and local receipt time; Kabu
never renders a missing numeric value as zero.

- Crypto is `Live` through 5 seconds, `Aging` after that, and `Stale` after 15
  seconds.
- An equity in a regular session is `Fresh` through 30 seconds, `Aging` after
  that, and `Stale` after 120 seconds.
- Provider-marked delay is always `Delayed`; a closed equity is `Closed` rather
  than becoming stale merely because the market is shut.
- When no cached quote exists, the state is `Unavailable` and the price is an
  em dash, not a fabricated numeric value.

Transient failures preserve the last good value and show a typed,
provider-specific warning or retry control. Current errors include timeouts,
rate limits, unavailable endpoints, disconnects, malformed responses, and
unsupported instruments. Search can still show one provider's results when the
other provider fails.

## Architecture and provider boundaries

The workspace has one-way responsibilities:

```text
kabu-app  ->  kabu-core  <-  kabu-data
  GPUI        domain        providers and persistence
```

- `kabu-core` owns provider-neutral instruments, quotes, periods, freshness,
  retries, ordered-watchlist policy, request generations, and async ports. It
  has no GPUI, HTTP, WebSocket, provider-wire, or filesystem dependency.
- `kabu-data` keeps Yahoo/Binance wire formats private, validates external
  values fallibly, implements the typed ports, and owns atomic JSON
  persistence.
- `kabu-app` constructs concrete adapters, runs I/O on a Tokio runtime thread,
  merges typed events into `AppModel`, and renders GPUI views. Views do not
  perform HTTP, WebSocket, or JSON work.

Yahoo Finance powers US equity search, quotes, and history. Binance public Spot
endpoints power the crypto catalog, quotes, history, and ticker stream. Both are
unofficial public integrations with no availability, latency, or compatibility
SLA from Kabu. Neither requires an account or API key.

Kabu attempts to install Rustls's AWS-LC provider once before each production
HTTP or WebSocket client boundary. Reqwest, Kabu's direct HTTP stack, already
requires AWS-LC, while GPUI's internal HTTP dependency also enables Ring. The
explicit first-install choice prevents the otherwise ambiguous combined
feature set from panicking. If another process component has already installed
a Rustls provider, Kabu safely reuses that immutable process-wide choice.

## Local data, privacy, and security

Kabu stores two plain JSON files under:

```text
~/Library/Application Support/Kabu/state.json
~/Library/Application Support/Kabu/cache.json
```

- `state.json` contains the schema version, ordered watchlist and selection,
  window size, sidebar width, and theme preference.
- `cache.json` contains last good quotes, chart caches, provider timestamps,
  and Binance catalog metadata.

Writes use a temporary file in the same directory followed by rename. Invalid
JSON is moved to `state.json.corrupt-<UTC timestamp>` or
`cache.json.corrupt-<UTC timestamp>` (with a numeric suffix on collision), a
safe default is loaded for only that file, and Kabu shows a recovery warning.
State and cache recovery are independent.

Kabu has no user account, brokerage connection, credential store, analytics,
telemetry, or cloud synchronization. Provider requests necessarily expose the
computer's public network address and the searched query or requested symbols
to Yahoo or Binance. Local JSON is not application-level encrypted; protect the
macOS account and its Application Support directory accordingly.

## Troubleshooting

**`xcrun: error: unable to find utility "metal"` or a shader build failure**

Select Xcode and install the Metal toolchain using the prerequisite commands
above. Confirm both `xcrun -f metal` and `xcrun -f metallib` succeed before
retrying the Cargo build.

**The window opens but prices or search results do not load**

Confirm ordinary HTTPS and WebSocket access to Yahoo Finance and Binance public
data endpoints. A visible timeout, rate-limit, reconnecting, stale, or
unavailable state is expected when a provider or local network blocks access;
do not delete local cache to hide that condition.

**Restored values are stale**

The cached value remains intentionally visible with its source, timestamp, and
freshness label. Restore network access and keep the window visible so Kabu can
reconcile it. Minimized or hidden windows pause provider activity.

**Kabu reports recovered local data**

Inspect the timestamped `.corrupt-*` file beside `state.json` or `cache.json`.
The other file is not reset. Follow the backup and restore procedure in the
manual guide before replacing either file.

**The app cannot shrink further**

The native minimum window size is 760 by 500 points. The sidebar divider is
clamped from 220 through 360 points and resets to 280 points on double-click.
