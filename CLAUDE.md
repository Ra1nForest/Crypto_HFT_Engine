# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

A C++17 HFT toolkit. Its main program is a terminal app that streams incremental L2 order books
from Binance, OKX and Bybit over TLS WebSockets (Boost.Beast + OpenSSL, parsed with simdjson)
and flags cross-exchange arbitrage on a live dashboard. It only monitors: nothing places orders.

Alongside it the repo has:
- a price-time-priority matching engine with a threaded exchange core (`exchange_sim`)
- an offline mock exchange (`tools/mock_exchange`) that speaks all three wire formats
- the lock-free and allocation-free building blocks under `include/hft/`

## Build and run

Builds on Linux (Ubuntu 22.04+ / WSL2), not natively on Windows: POSIX sockets and `mkdir`,
`pthread_*`, and `Setup.sh` uses apt.

```bash
bash Setup.sh                                   # apt deps + simdjson v5.1.1 into third_party/
cmake -S . -B build && cmake --build build -j   # app, exchange_sim, mock_exchange, 7 tests, 5 benchmarks
ctest --test-dir build --output-on-failure      # 670 cases, about 2 min
./build/crypto_hft_engine                       # must run from the repo root
```

- `third_party/` is gitignored, and CMake fails early without it. simdjson is compiled once as a
  static library shared by every target.
- `config.json` and `logs/` are resolved against the current directory. If the implicit
  `config.json` is missing, the app warns and uses built-in defaults that connect to the real
  exchanges. An explicit `--config` that cannot be loaded exits with code 2.
- `CMakeLists.txt` hard-codes `-Wall -Wextra -O2`, so Debug builds are optimized too.
  `-DHFT_SANITIZE=address,undefined` or `=thread` makes a sanitizer build (it adds `-O1 -g`;
  for GCC TSan it adds `-Wno-tsan`, because of Boost.Asio fences). Use a separate build dir per
  configuration (`build-asan/`, `build-tsan/`, `build-clang/`; `build*/` is gitignored). A
  clean build has no warnings with g++ 13 or clang++ 18. Add `-DCMAKE_EXPORT_COMPILE_COMMANDS=ON`
  for clangd.
- Bounded runs: `./build/crypto_hft_engine --headless --duration 20`. Headless mode prints feed
  events, signals and one status line per second instead of the TUI. The run ends with the final
  report (latency tables, feed statistics, arbitrage summary).
  - `--busy-poll` spins on the queues for minimum latency, at the cost of a full core.
  - SIGINT, SIGTERM and SIGHUP shut down gracefully. A second signal within 1 s is treated as a
    duplicate, because `timeout` and similar wrappers forward the terminal's SIGINT. A later
    second signal force-exits. Ctrl+Z restores the cursor before stopping.
  - Against the real venues it needs outbound access to `stream.binance.com:9443`,
    `api.binance.com:443` (REST snapshot), `ws.okx.com:8443` and `stream.bybit.com:443`.
  - In a healthy run every feed reaches `LIVE` with rising message counts and zero
    gaps/checksum failures.
  - Without network access, feeds cycle `CONNECTING`/`RECONNECTING` with backoff, and shutdown
    still takes about 1 ms.
- Offline end to end: `tools/run_mock_demo.sh 20 --headless`. It starts `build/mock_exchange`
  on ports 19443/18443/18444, waits for "mock_exchange: ready", and runs the monitor with
  `tools/mock_config.json`. That config trusts the CA the mock writes to
  `/tmp/mock_exchange_ca.pem` on every start. All feeds should be LIVE within a second. The
  shipped fee rate of 0.0 makes signals continuous; `--fee-rate 0.001` makes them rare, and
  `--drop-rate 0.02` exercises gap detection and resync. Two demos at once collide on the ports:
  start `mock_exchange` and the monitor by hand with other ports and a copied config.
- `exchange_sim` (no arguments) runs 2M commands through the threaded engine in about a second
  and exits non-zero if any consistency check fails.

### From a Windows checkout

Windows has no toolchain for this project, so run commands through WSL from PowerShell. `wsl`
starts in the matching `/mnt/c/...` directory:

```powershell
wsl -- bash -c 'cmake -S . -B build/wsl && cmake --build build/wsl -j'
wsl -- bash -c './build/wsl/crypto_hft_engine --headless --duration 20 2>&1 | tail -n 40'
```

- `wsl` passes the command line through the default Linux shell a second time, so `$vars` and
  nested quotes in inline commands get mangled. Put anything non-trivial in a script and run
  `wsl -- bash /mnt/c/.../script.sh`.
- WSL's `/tmp` is wiped whenever the instance restarts, and the instance stops shortly after it
  goes idle. Don't keep build output there between commands. The mock exchange rewrites its CA
  file on every start, so that file is safe there.
- `build/wsl` is still gitignored. Using it avoids a `build/` that was configured from another
  source path, which CMake refuses to reuse.

## Tests and benchmarks

- **Framework.** `tests/test_framework.h` is dependency-free: `TEST_CASE`, `CHECK*`/`REQUIRE*`,
  and `hft_test::registerTest` for templated suites. Each test executable is exactly one
  `tests/<name>.cpp`. CMake registers it only if its name is listed in `hft_add_test(...)`, so
  add new suites there.
  - Run a subset by name substring, e.g. `./build/test_matching optimized/`.
  - `--list` prints the case names.
  - `CHECK_EQ` binds its arguments by `const auto&`: never pass a member of a temporary
    (`f().front().x`).
- **Suites.**
  - `test_hft`: core containers.
  - `test_matching`: spec suite plus differential fuzz. `HFT_LONG_TESTS=1` runs 10× more.
  - `test_engine_runner`: concurrent replay. `HFT_TEST_SCALE=k` scales the soak.
  - `test_market_data`: L2 book, CRC32, adapters.
  - `test_feed_integration`: real feeds against an in-process TLS mock.
  - `test_mock_exchange`: the mock exchange.
  - `test_app`: config, sweep, display widths, end-to-end monitor runs.
  - Several suites replace the global `operator new` with a counting version to assert "zero
    allocations once warmed up". That is another reason each test stays a single translation
    unit.
- **Opt-in asserts.** `HFT_ENABLE_ASSERTS` must be defined before the first `hft/` header
  (tests do this).
- **Allocation counter.** The counting global allocator is `include/hft/alloc_counter.h`:
  `total()`, plus `startWindow(thread)` / `stopWindow()`. It replaces every `operator new` and
  `operator delete` form, so exactly one translation unit per executable may include it.
- **Include directories.** The test targets get both `tests/` and `tools/`. The in-process TLS
  mock server `tools/mock_exchange_server.h` is shared by the tests and `mock_exchange`.
  `matching_engine_benchmark` gets `tests/`, for the reference model. Don't use `../` includes.
- **Benchmarks** are built into `build/`.
  - `hft_benchmark`, `matching_engine_benchmark` and `market_data_benchmark` exercise the real
    headers.
  - `orderbook_benchmark` and `parse_benchmark` are the v1 standalone re-implementations
    (map vs vector, stod vs from_chars) and never touch `include/`.
  - Numbers vary by about 20–50% on shared VMs: compare within one run.
- No CI and no lint/format config. Compiler warnings, the test suites, sanitizer builds and the
  mock demo are the checks.

## Architecture

Every module is header-only under `include/`. Translation units: `src/main.cpp` (monitor),
`src/exchange_sim.cpp`, `tools/mock_exchange.cpp`, the tests, the benchmarks and
`third_party/simdjson.cpp`. Namespaces: `hft` (core), `hft::matching` (engine); the monitor
classes are global.

**Monitor threads and data flow.**
1. `main()` handles signals and config, then runs `MonitorApp` (`include/app.h`, a header so
   `test_app` can drive the real loop). It creates one `MarketDataFeed` per enabled venue, each
   on its own thread, with SIGINT/SIGTERM blocked so only the main thread gets signals.
2. A feed thread runs synchronous Boost.Beast: resolve (on an interruptible helper thread) →
   TCP → SNI + certificate/hostname verification → TLS → WebSocket handshake → the adapter's
   subscribe frames → a blocking `ws.read()` loop.
   - Every message goes to an `ExchangeAdapter` (`include/exchanges/`). The adapter is a pure
     protocol state machine with no I/O, so it is unit-tested with fixture strings. It updates a
     private full-depth `OrderBook` and returns an `AdapterResult`: book changed, frames to send,
     need a REST snapshot, reconnect.
   - The feed thread does the REST GET itself (`include/net/https_client.h`). Meanwhile the
     WebSocket keeps buffering in the kernel.
3. After a change the feed copies the top 20 levels into a trivially copyable `BookSnapshot`
   and `tryPush`es it onto its `hft::SpscQueue` (full queue → dropped and counted; it never
   blocks). Every session end publishes an empty book, so consumers never act on stale data.
4. The main thread drains every queue. For each update it runs `ArbitrageDetector` on every
   ordered pair that involves the updated venue (event-driven, not polled). A venue takes part
   only while its feed is LIVE and its book is non-empty.
   - Every `refresh_ms` it renders, calls each feed's `checkHealth(nowNs())`, and logs spreads
     every `spread_log_interval` ticks.
   - When idle it waits for the next refresh tick or for a feed's publish callback
     (`setPublishCallback`), whichever comes first. The callback uses a "sleeping" flag, so a
     feed that publishes while the main thread is awake takes no lock and makes no syscall.
5. **Kick.** Reads have no timeout. Instead, each feed keeps a mutex-protected registry of
   `dup`ed socket fds, and `kick()` calls `::shutdown(fd, SHUT_RDWR)` so the blocked read fails.
   `stop()` and `checkHealth()` both kick: stall after `stall_timeout_ms`, or a handshake slower
   than `connect_timeout_ms`.
   - Stall and connect-timeout recovery depend on the main loop calling `checkHealth`.
   - The feed reconnects with exponential backoff and ±20% jitter. The attempt counter resets
     after 30 s of LIVE. Adapter-requested reconnects are immediate, at most 3 in a row.
6. **Connect.** TCP connect is non-blocking with `poll` slices, and it re-checks the kick flag.
   Each resolved address gets its own deadline, `max(1 s, budget / n)`. After a timeout the next
   session starts at the next address, so a blackholed first address (e.g. broken IPv6) is
   skipped.
7. **Streaming and keepalive.** Once subscribed, the feed thread runs `ws.async_read` plus a
   `steady_timer` on a per-session `io_context` (`run_one`). All writes go through one async
   write queue.
   - After `min(ping interval, stall_timeout_ms / 2)` of silence it pings: OKX `"ping"`, Bybit
     `{"op":"ping"}`, Binance a WebSocket ping frame. Every frame received, pongs included,
     counts as activity.
   - The REST snapshot is a synchronous GET inside the read handler. Requests per session are
     capped by `rest_max_attempts` and spaced by `rest_min_interval_ms`, doubling on failure.
   - A disconnect's empty book is kept pending until it fits in the queue.

**Order book model.** `OrderBook` keeps sorted flat vectors with the best level at index 0
(bids descending, asks ascending). The display and detector read `getBids()[0]`.
- `setLevel` upserts: it checks the tail (O(1) append for sorted rebuilds), then the top 8
  levels, then binary-searches. A quantity of 0 or less erases the level.
- Incremental messages go through `applyLevels`: more than 8 changes are sorted, deduplicated
  (last update per price wins) and merged in one pass with a caller-owned `LevelBatchScratch`.
  Per-level upserts deep in a 1000+ level book cost about 1 µs each in `memmove`.
- Keep the Binance REST `limit` at 1000 or lower, and the adapters' `kMaxBookLevels` (5000)
  bound.
- Prices compare as exact doubles: identical exchange strings parse to identical doubles. The
  parser is a Clinger fast path (`json_util.h`) that is bit-identical to `from_chars`.

**Protocols** (details in the adapter headers).
- **Binance**: diff depth plus a REST snapshot, synced with the official algorithm: buffer,
  refetch if the snapshot is older than the first buffered `U`, drop `u <= lastUpdateId`. A gap
  means `U > local+1`. `serverShutdown` reconnects. Partial `depth5/10/20` streams also work.
- **OKX**: `books` updates are chained by `prevSeqId == last seqId`. The CRC32 checksum covers
  the top 25 levels interleaved bid/ask, built from the original strings kept per level in a
  side table.
- **Bybit**: `orderbook.N` snapshots and deltas with `u == last+1`; `u == 1` resets.
- A gap, a checksum mismatch or a crossed book triggers a resync: REST refetch for Binance,
  unsubscribe + subscribe frames for OKX and Bybit.
- Safety valves exist because the live venues cannot be tested here:
  - OKX: if a fresh snapshot fails its checksum, the checksum construction is wrong, so
    verification is disabled with an error.
  - Bybit: three resyncs in a row on the first delta after a snapshot disable the strict `+1`
    check.
- simdjson On-Demand hazard: after a nested error, never touch the document again. The adapters
  abort the whole message.

**Arbitrage.** `ArbitrageDetector::sweep` walks the buy venue's asks against the sell venue's
bids while `bid*(1-f_sell) - ask*(1+f_buy) > 0`, capped by `max_qty` (0 = unlimited) and
`max_levels`. It reports VWAPs, fees and net.
- A signal needs `net > min_profit` and, per ordered pair, the best ask or best bid moved by at
  least 0.01 since that pair's last signal.
- With one level per side and equal fees it reproduces the v1 formula exactly (tested).
- History is a bounded deque (1000) plus running totals.

**Latency stats.** `LatencyTracker` wraps `hft::LatencyHistogram`: nanosecond samples,
log-linear buckets (≤1/64 relative error), single writer with relaxed atomics so the display can
read concurrently, O(buckets) percentiles. It is about 30 KB inline per tracker. The per feed
metrics are:
- processing: `ws.read()` return → snapshot pushed
- exchange: wall clock minus the exchange timestamp, in ms, subject to clock skew
- queue: push → consumed by the main thread

**Display.** `Display` builds the whole frame in one `std::string` and writes it with one
`fwrite`, clipped to the terminal size.
- All padding goes through `text_util.h` (`visibleWidth` counts UTF-8 code points and skips
  ANSI CSI), so `μ`, box drawing and colours never break alignment.
- `test_app` renders frames for 1–3 venues and asserts that every boxed line has the same visible
  width. Run it after any layout change.

**Matching engine** (`include/matching/`; full rules in `docs/MATCHING_ENGINE.md`).
- Integer ticks and lots; client-assigned order ids.
- Validation order, FOK pre-check, the STP modes, the modify priority rules and the event order
  are normative. They are pinned by `tests/matching_spec_tests.h`, which runs unchanged on
  `ReferenceOrderBook` (`std::map`), `NaiveOrderBook` (linear scans) and the optimized
  `LimitOrderBook`.
- **Change semantics in the spec, the reference model and the docs together; never "fix" the
  oracle to match the engine.**
- `LimitOrderBook` design:
  - pooled orders with intrusive FIFO lists
  - pooled levels
  - per side, a `std::vector<{key, Level*}>` with the best level at the back (key = price for
    bids, -price for asks)
  - `hft::FlatHashMap` index
- It does no allocation while live orders stay below `reserve_orders`. The listener sees the book
  already updated at every callback and must not re-enter.
- A limit GTC order, or the re-rest path of a modify, that would push its price level's total
  past `INT64_MAX` is rejected with `InvalidQuantity` before `onAccepted`. This bound is part of
  the normative rules.
- `EngineRunner`: one engine thread, per-gateway inbound/outbound SPSC queues, 64-byte
  command/event PODs, one `Fill` per side, cross-gateway cancel/modify rejected. Back-pressure
  spins and never drops, so gateways must keep draining their events.
- `hft::FlatHashMap` pointers are invalidated by growth **and by erase** (backward shift).

**Adding an exchange.** Add an `Exchange` value and its `exchangeName`, an adapter registered in
`makeAdapter` (`exchanges/adapters.h`), its defaults in `ConfigReader` (the venue list there
defines the fixed display order), and a block in `config.json`. Optionally add its wire format
to `tools/mock_*.h` for the offline demo. The feed, detector, display and logger are already
N-venue generic (the detector accepts at most 16 venues).

## Configuration gotchas

- Loading is per field. A missing or mistyped field gets its default plus a stderr warning that
  names the field (`[Config] okx.port: ...`). Out-of-range values are clamped with a warning.
  `port` may be a string or an integer. A disabled venue's other fields are not validated. A new
  field must be added to the struct, the loader, the defaults and `config.json` together.
- The shipped `config.json` sets `fee_rate: 0.0`, so almost any cross-venue gap signals. The
  code default is `0.001`. A per-venue `fee_rate` overrides the global one.
- The `symbol` fields are only labels. The instrument comes from `binance.path`,
  `binance.snapshot_path` and the OKX/Bybit `subscribe` messages, so change them together.
- `connection.tls_verify` defaults to true: it needs the system CA store (`ca-certificates`).
  `connection.ca_file` adds a trusted PEM, which the mock demo uses.

## Conventions

- Hot paths are `receiveLoop` → adapter → `applyLevels`/snapshot publish, and the matching
  engine's `submit/cancel/modify`. Perf changes ship with a benchmark (the three real-header
  benchmarks have sections to extend) and before→after numbers in the commit message.
  Allocation-freedom is asserted in tests, so keep it.
- Commits use conventional prefixes (`feat:`, `perf:`, `docs:`, `chore:`, `build:`, `test:`).
- Code comments are in Simplified Chinese, and every header starts with the `//\n// name.h\n//
  中文说明\n//` banner. README, docs, commit messages, program output, log lines and test names
  are in English.
- Naming: classes `PascalCase`, methods `camelCase`, members `trailing_underscore_`, constants
  `kName`. Plain structs may use snake_case fields.
