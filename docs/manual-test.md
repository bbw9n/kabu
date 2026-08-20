# Kabu macOS Manual Acceptance

This guide exercises the release binary and all eleven MVP acceptance
scenarios without discarding existing Kabu data. It uses the shipped chart
periods: **1D, 5D, 1M, 6M, and 1Y**. Older planning prose that names 1W or 3M is
superseded by the source and UI labels covered here.

Do not mark a checkbox passed from expectation alone. Record `Pass`, `Fail`, or
`Not run - <reason>` next to each scenario and add observable evidence such as
a timestamp, filename, or screenshot.

## 0. Build and protect existing data

Run from the repository root in one Terminal window:

```bash
cargo build --release -p kabu-app

KABU_APP_BIN="$(pwd)/target/release/kabu-app"
KABU_SUPPORT_DIR="$HOME/Library/Application Support/Kabu"
KABU_ACCEPTANCE_ROOT="$HOME/Desktop/Kabu-Acceptance-$(date -u +%Y%m%dT%H%M%SZ)"

mkdir -p "$KABU_ACCEPTANCE_ROOT/screenshots"
if [ -e "$KABU_SUPPORT_DIR" ]; then
  mv "$KABU_SUPPORT_DIR" "$KABU_ACCEPTANCE_ROOT/original-Kabu"
fi
printf 'Acceptance evidence: %s\n' "$KABU_ACCEPTANCE_ROOT"
```

Expected: the release binary exists, the timestamped acceptance folder is
outside the repository, and any pre-existing `Kabu` directory now exists only
as `original-Kabu` inside that folder. Nothing has been deleted.

- [ ] Record `rustc --version`, `sw_vers`, and `git rev-parse HEAD` in the
  evidence table.
- [ ] Confirm `pgrep -x kabu-app` prints nothing before the first launch.
- [ ] If `original-Kabu` exists, confirm it contains the exact files that were
  present before testing. Do not open or edit those files during acceptance.

Launch from the same Terminal when a scenario says **Launch Kabu**:

```bash
"$KABU_APP_BIN"
```

Close with the red window control and wait for the process to exit. If a broken
build does not exit, use the scoped termination command below, then verify
again:

```bash
pkill -TERM -x kabu-app
pgrep -x kabu-app
```

## 1. Fresh-state launch and starter watchlist

Status: **Not run - the automated attempt could confirm only the process; the
macOS session was locked. An initial network-runtime panic was fixed, and the
post-fix 15-second release run had no panic output.**

- [ ] Launch Kabu with no active `$KABU_SUPPORT_DIR`.
  Expected: one native Kabu window appears and remains responsive.
- [ ] Inspect the sidebar before adding anything.
  Expected: exactly AAPL, NVDA, BTC/USDT, and ETH/USDT appear in that order,
  with AAPL selected.
- [ ] Inspect the detail area during and after its first provider requests.
  Expected: loading is explicit; a successful quote replaces it, while a
  failure remains `Unavailable` or shows a warning. Missing values are em
  dashes, never numeric zeroes.

## 2. Default and minimum window bounds

Status: **Not run - manual required**

- [ ] On the fresh launch, inspect the initial window size with a macOS
  screenshot or Accessibility Inspector.
  Expected: the content window is 980 by 640 points, subject only to clamping
  if the current display is smaller.
- [ ] Drag the lower-right corner smaller in both directions.
  Expected: resizing stops at 760 by 500 points; content remains reachable and
  does not collapse below that bound.
- [ ] Resize to a visibly different size above the minimum and leave it there
  for at least 300 ms.
  Expected: resizing remains smooth and the chosen size is eligible for the
  restart-persistence check in scenario 8.

## 3. Command-K search, stock/crypto add, duplicate, and remove

Status: **Not run - manual required**

- [ ] Press Command-K.
  Expected: the centered search overlay opens and its “Search stocks and
  crypto” input has keyboard focus.
- [ ] Type `MSFT`, pause for less than 250 ms, then continue typing or deleting.
  Expected: obsolete partial work does not repaint the final result set. After
  at least 250 ms idle, Yahoo equity results appear or a Yahoo-specific warning
  explains the provider failure.
- [ ] Use Up/Down to select the Microsoft equity result and Return to add it.
  Expected: the overlay closes, MSFT is appended once, selected, labeled as a
  US equity, and its quote/chart start loading without freezing the window.
- [ ] Press Command-K, search `SOL/USDT` (or another active USDT Spot pair),
  select it with the keyboard, and press Return.
  Expected: the Binance Spot pair is appended once and selected as crypto.
- [ ] Press Command-K, search `AAPL`, and add the already-present AAPL result.
  Expected: AAPL becomes selected and the watchlist count/order do not change.
- [ ] With the watchlist focused, use Up/Down, select the newly added test
  symbol, and press Delete or Backspace.
  Expected: only that row is removed and the nearest remaining row becomes
  selected. Re-adding the symbol remains possible.
- [ ] Press Command-K and then Escape.
  Expected: the overlay closes, pending search work is cancelled, and focus
  returns to the watchlist.

## 4. AAPL/NVDA stock updates and Yahoo attribution

Status: **Not run - live provider access required**

- [ ] Select AAPL and then NVDA while the window is visible.
  Expected: each selection changes the symbol immediately and never shows the
  other instrument's late quote or chart.
- [ ] For each stock, inspect the footer and metrics after a successful load.
  Expected: source is `Yahoo`, mode is `Snapshot`, provider timestamp and age
  are present, last price is finite and positive, and any supplied open/high/
  low/previous-close/volume values are finite.
- [ ] Keep the window visible for at least 20 seconds.
  Expected: a second stock poll can update timestamps/values at the 15-second
  cadence. If Yahoo blocks the request, the last good quote remains visible
  with a typed warning rather than becoming zero.

## 5. BTC/USDT live ticks and Binance attribution

Status: **Not run - live provider access required**

- [ ] Select BTC/USDT and observe it for at least 20 seconds.
  Expected: source is `Binance`; the footer moves through `Connecting` to
  `Live` when the stream is available, and the provider timestamp, age, and
  finite positive price are visible.
- [ ] Watch the price, timestamp, and freshness label.
  Expected: ticker events can advance the quote without UI blocking. A stream
  interruption shows `Reconnecting` and retains the last useful value.
- [ ] If Binance is unreachable, record the exact sanitized warning.
  Expected: the row/detail show unavailable, stale, paused, or reconnecting
  state; the app remains usable and does not claim a live price.

## 6. Every period and rapid request supersession

Status: **Not run - manual required**

- [ ] For AAPL, click each period once: 1D, 5D, 1M, 6M, and 1Y.
  Expected: the clicked tab is selected and its chart reaches a non-blocking
  loading, ready, or explicit unavailable state.
- [ ] Repeat 1D, 5D, 1M, 6M, and 1Y for BTC/USDT.
  Expected: all five Binance kline requests have the same state guarantees.
- [ ] Rapidly click AAPL -> 1Y -> BTC/USDT -> 5D -> NVDA -> 6M without waiting
  for intermediate loads.
  Expected: the final header remains NVDA, 6M remains selected, and only an
  NVDA 6M chart may become visible. No late AAPL/BTC chart repaints it.
- [ ] Switch back and forth between a recently loaded period and symbol.
  Expected: fresh cached charts appear immediately; stale/missing charts load
  without blocking selection.

## 7. System, Graphite, Midnight, and Signal themes

Status: **Not run - manual required**

- [ ] Choose Graphite and inspect selection, positive/negative text, chart,
  metrics, sidebar, and warnings. Capture:
  `$KABU_ACCEPTANCE_ROOT/screenshots/graphite.png`.
  Expected: a restrained light palette with readable contrast and a distinct
  selected row.
- [ ] Choose Midnight and capture
  `$KABU_ACCEPTANCE_ROOT/screenshots/midnight.png`.
  Expected: a dark palette with readable content and signals not conveyed by
  color alone.
- [ ] Choose Signal and capture
  `$KABU_ACCEPTANCE_ROOT/screenshots/signal.png`.
  Expected: the indigo-accented light treatment applies to the whole window.
- [ ] Choose System, record the current macOS appearance, and capture
  `$KABU_ACCEPTANCE_ROOT/screenshots/system.png`.
  Expected: light appearance resolves to Graphite and dark appearance resolves
  to Midnight.
- [ ] Leave a non-System theme selected, close Kabu, relaunch it, and then
  repeat with System.
  Expected: both preferences survive restart; System continues to track the
  current macOS appearance.

For an interactive screenshot without changing repository files:

```bash
screencapture -i "$KABU_ACCEPTANCE_ROOT/screenshots/graphite.png"
```

Repeat with each filename above after selecting the matching theme.

## 8. Order, selection, sidebar width, and window size persistence

Status: **Not run - manual required**

- [ ] Establish a recognizable order by adding a stock and crypto pair, remove
  one starter row, and select a middle row. Record the exact order/selection.
  Expected: adds append, removal changes only the selected row, and selection
  is visually distinct.
- [ ] Drag the 8-point sidebar divider close to both extremes.
  Expected: width is clamped between 220 and 360 points and updates live.
- [ ] Double-click the divider, then drag it to a non-default width and release.
  Expected: double-click resets to 280 points; the final drag persists once on
  release.
- [ ] Resize the window to a distinctive size above 760 by 500, wait 300 ms,
  close Kabu, and relaunch.
  Expected: exact watchlist order, selection, theme, sidebar width, and window
  size return. The window never restores below its minimum or beyond the
  current display.

## 9. Minimize, application hide, restore, and reconciliation

Status: **Not run - real AppKit behavior required; the macOS session was locked
during the automated acceptance attempt**

- [ ] With a live BTC/USDT quote visible, click another app without minimizing
  Kabu and wait 5 seconds.
  Expected: ordinary focus loss does not pause updates.
- [ ] Minimize Kabu for at least 20 seconds.
  Expected: the process stays alive, visible-window polling/stream/history work
  is cancelled, and no hidden UI freshness tick claims new data.
- [ ] Restore Kabu.
  Expected: reconciliation snapshots start before normal stock polling and
  crypto streaming; stale cached content remains visible until replaced.
- [ ] Hide Kabu with Command-H, wait, then reveal it from the Dock or app
  switcher.
  Expected: hide/reveal has the same pause/reconcile behavior as minimize/
  restore.
- [ ] Close the only window and immediately observe the process.
  Expected: the first AppKit close is vetoed while state/cache flush and runtime
  shutdown complete; the window then closes and `pgrep -x kabu-app` eventually
  prints nothing.

## 10. Offline cached and unavailable degradation

Status: **Not run - network change and manual restore required**

- [ ] While online, wait for at least one stock and one crypto quote, close
  Kabu, and confirm both support files exist:

  ```bash
  ls -l "$KABU_SUPPORT_DIR/state.json" "$KABU_SUPPORT_DIR/cache.json"
  cp -p "$KABU_SUPPORT_DIR/state.json" "$KABU_ACCEPTANCE_ROOT/offline-state.before.json"
  cp -p "$KABU_SUPPORT_DIR/cache.json" "$KABU_ACCEPTANCE_ROOT/offline-cache.before.json"
  ```

  Expected: timestamped acceptance evidence contains recoverable copies.
- [ ] Disconnect Wi-Fi/Ethernet using macOS Control Center, noting the original
  network state first, and launch Kabu.
  Expected: cached prices render immediately with their original source and
  timestamp; crypto becomes `Aging` then `Stale` after 15 seconds, while a
  closed equity remains `Closed`. Provider failures do not erase cached values.
- [ ] Close Kabu. Move only the acceptance cache aside, then relaunch offline:

  ```bash
  mv "$KABU_SUPPORT_DIR/cache.json" "$KABU_ACCEPTANCE_ROOT/cache.with-values.json"
  "$KABU_APP_BIN"
  ```

  Expected: instruments without cached quotes show `Unavailable` and em dashes,
  never zero. The watchlist remains because `state.json` was not moved.
- [ ] Close Kabu and restore the held cache before restoring networking:

  ```bash
  if [ -e "$KABU_SUPPORT_DIR/cache.json" ]; then
    mv "$KABU_SUPPORT_DIR/cache.json" "$KABU_ACCEPTANCE_ROOT/cache.empty-output.json"
  fi
  mv "$KABU_ACCEPTANCE_ROOT/cache.with-values.json" "$KABU_SUPPORT_DIR/cache.json"
  ```

  Expected: no cache file was overwritten or deleted.
- [ ] Restore the original network state, relaunch, and keep the window visible.
  Expected: quotes/search recover, errors clear by their own operation class,
  and refreshed values retain correct source/timestamp metadata.

## 11. Corrupt state recovery without lasting cross-loss

Status: **Not run - controlled corruption with backup required**

- [ ] Close Kabu and make explicit evidence copies:

  ```bash
  cp -p "$KABU_SUPPORT_DIR/state.json" "$KABU_ACCEPTANCE_ROOT/scenario11-state.before.json"
  cp -p "$KABU_SUPPORT_DIR/cache.json" "$KABU_ACCEPTANCE_ROOT/scenario11-cache.before.json"
  printf '{ intentionally invalid JSON for Kabu acceptance\n' > "$KABU_SUPPORT_DIR/state.json"
  "$KABU_APP_BIN"
  ```

  Expected: only the active, explicitly backed-up `state.json` is corrupted.
- [ ] Observe startup and inspect the support directory.
  Expected: Kabu reports that it recovered `state.json`; the invalid file is
  preserved as `state.json.corrupt-<UTC timestamp>` (with a numeric collision
  suffix if needed); safe starter state loads. `cache.json` is not renamed and
  cached starter-symbol quotes remain usable.
- [ ] Close Kabu, preserve the fallback output, and restore the exact pre-test
  state:

  ```bash
  if [ -e "$KABU_SUPPORT_DIR/state.json" ]; then
    mv "$KABU_SUPPORT_DIR/state.json" "$KABU_ACCEPTANCE_ROOT/state.recovery-output.json"
  fi
  cp -p "$KABU_ACCEPTANCE_ROOT/scenario11-state.before.json" "$KABU_SUPPORT_DIR/state.json"
  "$KABU_APP_BIN"
  ```

  Expected: the earlier custom watchlist, order, selection, geometry, and theme
  return, while cached data was not reset. The `.corrupt-*` diagnostic remains
  available for inspection.
- [ ] Close Kabu and confirm the process exits before final cleanup.

## 12. Restore the user's pre-acceptance data

This is mandatory even after a failed scenario. It preserves acceptance output
instead of deleting it and refuses to overwrite an unexpected target.

```bash
pkill -TERM -x kabu-app 2>/dev/null || true

if [ -e "$KABU_SUPPORT_DIR" ]; then
  mv "$KABU_SUPPORT_DIR" "$KABU_ACCEPTANCE_ROOT/test-Kabu-output"
fi

if [ -d "$KABU_ACCEPTANCE_ROOT/original-Kabu" ]; then
  if [ -e "$KABU_SUPPORT_DIR" ]; then
    printf 'STOP: refusing to overwrite %s\n' "$KABU_SUPPORT_DIR"
  else
    mv "$KABU_ACCEPTANCE_ROOT/original-Kabu" "$KABU_SUPPORT_DIR"
  fi
fi

pgrep -x kabu-app
```

Expected: `pgrep` prints nothing. If there was original data, it is restored at
the exact Application Support path. Otherwise that path remains absent. Test
output, diagnostic backups, and screenshots remain recoverable in the single
timestamped acceptance folder; no broad or recursive deletion was used.

## Acceptance evidence

| Field | Recorded result |
| --- | --- |
| Date (local) | 2026-08-20 |
| macOS | 26.5 (25F71) |
| Commit/build input | `d2e8d22eb489d4c378595d53dbb046b8d20169e6` plus the Task 13 working-tree diff; final commit is recorded in the Task 13 report |
| Release build | Pass: the post-fix `cargo build --release -p kabu-app` exited 0 in 6.10 seconds; final gate timing is recorded in the Task 13 report. |
| Live provider smoke | Mixed provider result under a 30-second outer bound: Binance BTC/USDT passed with a finite positive attributed quote; Yahoo AAPL reached the adapter but failed `MalformedResponse: chart response was malformed`. Result: 1 passed, 1 failed, 0 ignored in 1.00 second. This is a current Yahoo contract/provider response failure, not an external timeout. Hermetic status is separate. |
| Manual scenarios | Partial attempt, not a UI pass: the release process registered with Launch Services as `kabu-app`, stayed alive and not hidden for about 15 seconds, and was terminated by its exact PID. The initial run exposed a Rustls process-provider panic. After the TDD fix, a second 15-second release run produced a zero-byte log, no network-thread panic, and a Binance catalog cache. Computer Use still could not inspect the window because the macOS session was locked, so all UI checkboxes and theme screenshots remain unchecked. No Kabu process remains; the generated cache was moved to `/tmp/kabu-task13-acceptance.sALZfB/test-Kabu-output/cache.json`, and the original Application Support path is absent as it was before testing. The screenshot directory `/tmp/kabu-task13-acceptance.sALZfB/screenshots` is empty. |
| Provider limitations | Yahoo Finance and Binance Spot are unofficial public/no-SLA integrations. Binance passed in the post-fix smoke; Yahoo returned a response the current typed adapter rejected as malformed. Network policy, geographic restrictions, rate limits, endpoint changes, and market hours can also affect observations. |
