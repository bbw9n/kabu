# Final Holistic-Review Fixes Report

## Status and commits

The six Important findings from the final holistic review are repaired and the
adjacent initial-loading and one-candle-chart minors were addressed. The final
hermetic gates, release build, public-provider contracts, and bounded native
launch all completed after the implementation was finalized.

- Starting commit: `819dc67dd9c8896f439f45a91ad1396a4efa82be`
- Implementation commit: `1e0148c3bdcfa1caba745f2a4bb1da68554ad35d`
  (`fix: preserve lifecycle and search semantics`)
- Branch: `feat/kabu-mvp`
- Production changes: 16 files, 2,273 insertions, 149 deletions
- Progress ledger: not modified (`git diff -- progress.md` remained empty)

## Architecture and outcomes

### 1. Persistence outcomes and shutdown ownership

Live persistence remains serialized and coalesced through one bounded-one wake
lane, but repository results are no longer discarded. Every attempted state or
market-cache write publishes a typed `PersistenceCompleted` result with an
explicit `State` or `MarketCache` category. `AppModel` upserts one warning per
category, uses fixed sanitized copy, and clears only that category after a
successful write. Queue saturation is also classified separately for settings
and cached market data; neither is mislabeled as ordinary market-data work.

Shutdown now has a typed `ShutdownOutcome`. The network thread publishes
completion only after search/history/catalog/coordinator work is cancelled and
joined, queued persistence is drained, and both requested final writes have
been attempted. A failed final write is not a successful acknowledgement.

The first close remains vetoed while shutdown finishes. If either final write
fails, the window and runtime ownership remain alive and a fixed actionable
persistence warning is rendered. A later close retries only the failed final
state/cache writes on the background executor; providers are not restarted.
The window is removed only after the retry outcome succeeds. Each synchronous
wait attempt remains bounded to two seconds and never runs on the GPUI thread.

Coverage includes live state/cache failure and same-category recovery,
category-distinct saturated commands, queued-write drain, a repository held
past a bounded wait, typed final failure, successful retry, close veto, warning
redaction, and eventual window removal.

### 2. Startup failure UI

`ProductionComponents::production` and `ProductionComponents::start` are now
matched explicitly on the GPUI executor. Failures are reduced to one of four
safe categories: persisted settings, market cache, provider setup, or
background runtime. The raw persistence, HTTP, or I/O error never crosses into
rendered copy or structured logging.

Failure opens a styled 540 x 300 launch-failure window with fixed category
text, recovery guidance, and an accessible `Close Kabu` action. Removing that
only window reaches the existing deterministic last-window quit action. If the
failure window itself cannot be opened, Kabu emits one fixed diagnostic and
quits instead of leaving a detached silent task.

Tests exercise unreadable state/cache paths, unsupported schema, simulated
client and runtime initialization failures, fixed-copy redaction, visible view
construction, and the close action. Normal production launch follows the same
successful path as before.

### 3. Binance quote priority in core search ranking

The merged core ranker now applies a provider-neutral crypto quote tie-break
only after ordinary match quality. When the normalized query exactly equals a
crypto base asset, canonical quotes sort as USDT, USDC, BTC, ETH, then all
other quotes by the existing canonical-symbol order. The rule examines only
`InstrumentId`, so `kabu-core` has no Binance dependency.

Source preference, identity deduplication, exact-pair rank, and equity ordering
remain unchanged. Core and runtime integration tests prove the BTC order is
`BTC/USDT`, `BTC/USDC`, `BTC/ETH`, `BTC/TRY`, then the quote-side match
`ETH/BTC`. They also state the important domain fact that `BTC/BTC` is rejected
by the instrument constructor, so it is not a possible row to order.

### 4. Yahoo failure backoff

Stock polling continues every 15 seconds after success. Consecutive failures
now feed the shared stock retry policy with attempts 1, 2, 3, and 4, producing
30, 60, 120, and capped 120 second delays. A successful snapshot resets the
counter, making the next delay exactly 15 seconds.

The fake-clock test advances to one second before every boundary, proves that
no provider call occurs early, then advances the final second and proves the
exact call. The separate crypto-stream attempt accounting was not changed.

### 5. Reliable coalesced event delivery

The coordinator-to-runtime capacity-128 channel and the runtime-to-GPUI
capacity-32 forwarding bridge were replaced by one semantic mailbox shared by
the coordinator, runtime, and GPUI consumer. Its physical notification channel
has capacity one. Pending state is coalesced as follows:

- latest quote per `InstrumentId`;
- latest quote error per `InstrumentId` and typed error class;
- one latest `StreamStatus`;
- latest persistence outcome per persistence category;
- latest full search, provider retry per source, catalog result, and history
  result per request key.

Non-quote events are drained ahead of quote data, so a quote flood cannot
starve status, typed errors, persistence, search, or history. If a quote and a
later error are pending for the same instrument, the preceding quote is
delivered first. The bounded-one wake is nonblocking, cancellation-safe, and
closed only after owned producers finish; pending semantic state is drained
before the receiver observes closure.

A live runtime pressure test publishes 300 quote updates, status transitions,
and a typed disconnect under stalled consumption. It proves the final quote is
399, the final required status is `Reconnecting`, the typed disconnect arrives,
and shutdown joins. A GPUI pressure test repeats the flood through the actual
shell and proves `AppModel` receives quote 399, final `Paused`, and the typed
error, then closes without deadlock. Existing hide, cancel, stream-producer,
and shutdown join tests remain green.

### 6. Search provenance and provider retry

Every visible result now renders explicit provenance. Exact regression labels
include `US Equity · NASDAQ · Yahoo` and
`Crypto · BTC/USDT · Binance Spot`.

Search provider failures render an inline accessible `Retry` button. Retry
uses a typed command/event containing the existing generation, current query,
and failed source. The runtime calls only that provider and uses the same owned,
cancellable search job slot as a normal query. The model retains results from
the healthy provider, replaces only the selected provider's rows, reruns the
core ranker, and clears only the recovered provider warning.

New query text advances the generation and clears failed-provider state. A new
query cancels an in-flight provider retry before it can emit; a late event is
also rejected by the generation guard. Closing the overlay advances the same
guard, so a stale retry cannot repopulate it. Provider bodies and query text are
never interpolated into warning text. When both providers fail with no rows,
the overlay renders a provider-outage state instead of `No matching
instruments`.

## Adjacent minors addressed

- Initial visibility and every visible reconciliation mark the current
  watchlist as loading until each quote or typed error arrives. Missing cached
  data therefore no longer looks terminal while the first provider request is
  active.
- A chart containing one finite candle now paints a centered 6 x 6 signal
  marker. Empty and multi-point chart behavior is unchanged.
- This report corrects the historical Task 13 live-test wording. The exact
  current ignored contract names are:
  `live_yahoo_snapshot_contract`, `live_yahoo_search_contract`, and
  `live_binance_snapshot_contract`.

## Strict RED-GREEN evidence

Every finding started with a focused failing test or compile contract before
its implementation.

1. Persistence RED: the new live failure/recovery and shutdown-outcome tests
   initially failed to compile because typed persistence categories/results
   did not exist. The first AppModel application test then observed zero
   category warnings where two were required. GREEN: the runtime persistence,
   shutdown timeout/failure/retry, AppModel warning, saturated-command, and GPUI
   close-veto/retry tests all pass.
2. Startup RED:
   `cargo test -p kabu-app --test startup_failure --no-run` failed with missing
   startup failure categories, presentation module, and view. GREEN: all four
   startup integration tests pass, including a rendered window and clickable
   accessible close action.
3. Ranking RED: the BTC core regression produced
   `BTC/ETH, BTC/TRY, BTC/USDC, BTC/USDT, ETH/BTC` rather than the required
   common-quote order. GREEN: the core and integrated runtime regressions pass.
4. Backoff RED: the pre-boundary fake-clock assertion observed a second Yahoo
   call before the new first 30-second boundary. GREEN: the exact
   30/60/120/120/15 sequence and every no-early-call assertion pass.
5. Delivery RED: under a 300-update live flood the old bounded channel exposed
   quote 225 instead of final quote 399. The first GPUI pressure slice also
   lacked a pressure-safe test publishing seam. GREEN: both live and GPUI
   pressure tests observe the final quote, required final status, typed error,
   and bounded shutdown/hide behavior.
6. Search retry RED: focused model/runtime/UI tests failed to compile because
   no provider-retry command, completion event, model action, or provenance
   helper existed. A later assertion also proved that beginning a new query did
   not yet clear failed-provider state. GREEN: provider-only recovery, retained
   healthy rows, exact labels, Retry action dispatch, stale new-query and
   closed-overlay rejection, in-flight cancellation, dual outage state, and
   query/body redaction all pass.

Adjacent RED evidence: initial reconciliation left the model's loading set
empty, and the one-candle test first had no marker helper; an intermediate
eager `then_some(points[0])` implementation also panicked for an empty series.
The final slice uses safe slice matching and both regressions pass.

The first final Clippy attempt found only a test module located before later
items (`clippy::items-after-test-module`). The module was moved to the end of
the file, formatting was rerun, and the complete required gate sequence was
restarted from the beginning.

## Final automated gates

The exact final commands ran in the requested order after the implementation
commit content was finalized:

| Order | Command | Result |
| --- | --- | --- |
| 1 | `cargo fmt --check` | Pass, exit 0 |
| 2 | `cargo clippy --workspace --all-targets -- -D warnings` | Pass, exit 0 |
| 3 | `cargo test --workspace` | Pass, 226 passed, 0 failed, 3 ignored |
| 4 | `cargo build --release -p kabu-app` | Pass, exit 0 |
| 5 | `git diff --check` | Pass, exit 0 |

Hermetic breakdown:

- `kabu-app` library: 43 passed
- `kabu-app` AppModel integration: 64 passed
- `kabu-app` GPUI interactions: 26 passed
- `kabu-app` chart/price: 9 passed
- startup failure integration: 4 passed
- `kabu-core`: 35 passed
- `kabu-data` library: 12 passed
- Binance REST: 9 passed
- Binance stream: 11 passed
- persistence integration: 4 passed
- Yahoo integration: 9 passed
- live market: 3 ignored by default
- application binary and doc-test targets: 0 tests

The suite increased from the reviewed 205 hermetic passes to 226. The only
build notice is the pre-existing future-incompatibility warning for transitive
`block v0.1.6`; Clippy itself is clean under `-D warnings`.

## Public-provider evidence

The ignored contract target was rerun with public network access under a
60-second process-group timeout:

```text
$ KABU_LIVE_TESTS=1 cargo test -p kabu-data --test live_market -- \
    --ignored --nocapture --test-threads=1

test live_binance_snapshot_contract ... ok
test live_yahoo_search_contract ... ok
test live_yahoo_snapshot_contract ... ok

test result: ok. 3 passed; 0 failed; 0 ignored; finished in 3.49s
```

The command completed naturally in 8.6 seconds including compilation; the
timeout did not fire.

## Native release evidence

`target/release/kabu-app` launched as PID 49612 with no stdout/stderr panic or
diagnostic. CoreGraphics independently reported exactly one on-screen window
owned by that process. The process remained healthy through the bounded
observation.

The macOS session was locked. Computer Use could not unlock it, so an actual
user-level close gesture and a safe Kabu-only screenshot were not possible.
Direct synthetic input was not accepted by the locked session. To avoid
leaking a smoke process, only PID 49612 was terminated with `SIGTERM`; the
process exited 143. No screenshot was captured or committed.

The non-manual close contract is covered by passing GPUI tests: the first close
with failed final persistence is vetoed and retains one window, the second
close retries successfully and removes it, and the existing lifecycle test
confirms that removing the final window invokes the registered application
quit action. Native launch is therefore green for build, registration,
single-window rendering, and no-panic behavior; the physical close gesture is
explicitly unverified because the desktop was locked.

## Final self-review

- Bounds and cancellation: command/control/persistence notification channels
  remain bounded; the event wake channel is bounded one; quotes are coalesced
  by identity and cannot starve control/error state; hide and shutdown await
  owned provider jobs and stream producers.
- Persistence ownership: failed final writes retain the complete flush payload,
  repository, cache merge state, runtime outcome, model, and window needed for
  retry. Retry performs no provider work.
- Redaction: startup, provider search, live quote, history, persistence, and
  shutdown UI use fixed category text. Test-only private paths/provider bodies
  never appear in rendered messages.
- Startup: every error returned by production load/start is handled on the GPUI
  executor with either a visible fatal window or deterministic fixed-log quit.
- Unsafe boundary: `rg` finds one production `unsafe` block, the pre-existing
  guarded AppKit view conversion in `macos_visibility.rs`. The other matches
  are assertions proving that single seam; no unsafe code was added.
- Scope: no provider contracts, persisted schema, public endpoint behavior, or
  native visibility seam changed. The progress ledger remains untouched.

## Remaining concerns

- Public Yahoo and Binance endpoints have no availability SLA; the ignored live
  contracts may fail independently of a hermetic regression.
- The semantic event mailbox is physically bounded by one wake and coalesces
  per active identity/request key. Its pending map cardinality follows active
  domain identities rather than an arbitrary event count; repeated updates to
  the same identity never increase it.
- Startup UI intentionally exposes only a safe category and recovery guidance,
  not raw provider bodies, persisted content, paths, or I/O detail.
- The locked desktop prevented a new search-provenance screenshot and the
  physical only-window close gesture. No unverified screenshot was produced.
