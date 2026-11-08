# Crypto HFT Engine

A low-latency C++17 toolkit built around a live cross-exchange arbitrage monitor. It streams
incremental L2 order books from **Binance, OKX and Bybit** over TLS WebSockets, detects
fee-aware arbitrage across every venue pair on each book update, and shows it on a terminal
dashboard. The repository also contains the HFT building blocks the monitor is made of:
- a price-time-priority **matching engine** with a threaded exchange core and simulator
- a lock-free **SPSC queue**
- an **object pool**
- an open-addressing **hash map**
- an HDR-style **latency histogram**

It only monitors: nothing places orders.

## Screenshots

### Live dashboard (offline demo against the mock exchange)
![Dashboard](docs/screenshot_dashboard.png)

### Final report
![Report](docs/screenshot_report.png)

## What's inside

| Component | Where | Highlights |
|-----------|-------|------------|
| Market data feeds | `include/market_data_feed.h`, `include/exchanges/` | Incremental books for all three venues with sequence/checksum validation and automatic resync; reconnect with backoff; stall watchdog; TLS certificate + hostname verification |
| L2 order book | `include/orderbook.h` | Sorted flat vectors (best level at index 0), upsert/erase, crossed-book checks |
| Arbitrage monitor | `include/app.h`, `include/arbitrage_detector.h`, `include/display.h` | Event-driven detection fed by lock-free queues; depth-aware sweep with per-venue fees; any number of venues |
| Matching engine | `include/matching/` | Limit/market, GTC/IOC/FOK, post-only, self-trade prevention, cancel/modify; zero-allocation hot path; differential-tested against two independent models |
| Exchange core + simulator | `include/matching/engine_runner.h`, `src/exchange_sim.cpp` | Engine thread serving many gateway threads through SPSC queues |
| HFT core | `include/hft/` | `SpscQueue`, `ObjectPool`, `FlatHashMap`, `LatencyHistogram`, `TscClock`, CRC32 |
| Mock exchange | `tools/` | Offline TLS exchange speaking all three wire formats, with books produced by real matching engines |

## Architecture

```
  ┌──────────── feed thread (one per venue, blocking Boost.Beast) ────────────┐
  │ TLS WebSocket ──▶ protocol adapter ──▶ local full-depth L2 book            │
  │  (reconnect + backoff,  (parse with simdjson,   (Binance REST snapshot,    │
  │   stall/stop "kick")     sync, gap/checksum)     OKX/Bybit resubscribe)    │
  └───────────────────────────────────┬────────────────────────────────────────┘
                                      │ BookSnapshot (top 20 levels, timestamps)
                                      ▼ hft::SpscQueue (lock-free, one per feed)
  ┌──────────────────────────────── main thread ──────────────────────────────┐
  │ drain queues ──▶ detect arbitrage for every pair involving the updated     │
  │ venue ──▶ log signals ──▶ every refresh_ms: render dashboard, health check │
  └────────────────────────────────────────────────────────────────────────────┘
```

1. **Feeds.** Each enabled venue gets a thread that runs a synchronous Boost.Beast WebSocket
   session (resolve → TCP → TLS with SNI and certificate verification → WebSocket handshake →
   subscribe). A protocol adapter turns every message into updates of a private full-depth book:
   - **Binance**: the diff stream is synced with a REST snapshot, following the official
     algorithm.
   - **OKX**: `books` updates are chained by `prevSeqId`/`seqId` and verified with the CRC32
     checksum.
   - **Bybit**: snapshot/delta messages are chained by update id.

   A gap, a checksum mismatch or a crossed book triggers a resync: a new REST snapshot for
   Binance, unsubscribe + subscribe for OKX and Bybit.
2. **Publishing.** After every change the feed copies the top 20 levels into a fixed-size,
   trivially copyable `BookSnapshot` and pushes it onto a lock-free SPSC queue. It never blocks:
   when the queue is full the snapshot is dropped and counted.
3. **Detection.** The main thread drains every queue and, on **each** book update, sweeps the
   order books of every venue pair involving the updated venue, in both directions. It buys up
   the cheaper venue's asks and sells into the other's bids for as long as the marginal unit is
   still profitable after both venues' fees. It stops at `max_qty` and `max_levels`. A signal
   fires when the net profit exceeds `min_profit` and a best price has moved since that pair's
   last signal.
4. **Display and health.** Every `refresh_ms` the main thread renders the dashboard (one
   `fwrite` per frame), logs spreads every `spread_log_interval` refreshes, and runs each feed's
   health check. A connection that is silent for `stall_timeout_ms`, or a handshake that takes
   longer than its connect timeout, is "kicked". The kick calls `shutdown()` on the socket, so
   the blocked read fails and the feed reconnects with exponential backoff and jitter. The same
   mechanism makes Ctrl+C take effect within a millisecond.
   - Idle sessions send keepalive pings before the watchdog would fire: OKX `"ping"`, Bybit
     `{"op":"ping"}`, WebSocket ping frames for Binance.
   - Every resolved address gets its own connect deadline, so one unreachable address (such as
     broken IPv6) cannot keep a venue down.
   - When idle, the main thread sleeps until a feed publishes. It does not poll.

## Quick start

Linux (Ubuntu 22.04+ or WSL2), g++ 11+ (C++17 with floating-point `std::from_chars`), CMake 3.16+.

```bash
bash Setup.sh                                   # apt deps + simdjson v5.1.1 into third_party/
cmake -S . -B build && cmake --build build -j
ctest --test-dir build --output-on-failure      # 7 suites, 641 test cases, ~2 min
./build/crypto_hft_engine                       # run from the repo root (config.json, logs/)
```

```
Usage: crypto_hft_engine [--config <path>] [--headless] [--duration <seconds>] [--busy-poll] [--help]
  --config <path>       configuration file (default: config.json in the current directory)
  --headless            no terminal UI: print feed events, signals and one status line per second
  --duration <seconds>  stop automatically after this many seconds
  --busy-poll           spin on the feed queues instead of sleeping until a feed publishes
```

Ctrl+C, SIGTERM or SIGHUP stops the monitor gracefully and prints the final report. A second
signal more than a second later forces an immediate exit. Logs are appended to
`logs/arb_<date>.log`.

### Offline demo (no exchange access needed)

`tools/mock_exchange` runs three venues whose books come from real `MatchingEngine` instances
driven by simulated market makers and takers. It streams them over TLS in the exact Binance,
OKX and Bybit wire formats: diff depth plus a REST snapshot, `books` with checksums, and
`orderbook.50`. The demo script starts it and points the monitor at it with
`tools/mock_config.json`:

```bash
tools/run_mock_demo.sh 30                    # TUI for 30 s (headless when stdout is not a terminal)
tools/run_mock_demo.sh 30 --fee-rate 0.001   # with 0.1% fees per side: only real dislocations signal
tools/run_mock_demo.sh 30 --drop-rate 0.02   # fault injection: feeds detect gaps and resync
```

## Configuration

`config.json` (read from the current directory, or `--config`) has one object per venue plus
`arbitrage`, `display` and `connection` blocks. **Each field is loaded on its own.** A missing or
mistyped field falls back to its default with a warning that names it, and out-of-range values
are clamped with a warning. An unreadable file or invalid JSON falls back to the built-in
defaults, except that an explicitly given `--config` must load or the program exits with code 2.

| Key | Meaning |
|-----|---------|
| `<venue>.enabled` | Start this feed (`binance`, `okx`, `bybit`) |
| `<venue>.host`, `port`, `path` | WebSocket endpoint (`port` may be a string or an integer) |
| `binance.path` | Stream, which selects market and mode: `/ws/btcusdt@depth@100ms` (incremental, needs REST) or `/ws/btcusdt@depth20@100ms` (snapshots) |
| `binance.rest_host`, `rest_port`, `snapshot_path` | REST depth snapshot for the incremental stream, e.g. `/api/v3/depth?symbol=BTCUSDT&limit=1000` |
| `okx.subscribe`, `bybit.subscribe` | Subscribe message; selects market and channel (`books`, `books5`, `bbo-tbt`; `orderbook.50.BTCUSDT`, ...) |
| `<venue>.symbol` | Display label only |
| `<venue>.fee_rate` | Optional per-venue fee overriding `arbitrage.fee_rate` |
| `arbitrage.fee_rate` | Fee per side as a fraction (`0.001` = 0.1%); the shipped file uses `0.0` |
| `arbitrage.min_profit` | Minimum net profit per signal, in the quote currency |
| `arbitrage.max_qty` | Cap on the swept quantity (`0` = unlimited) |
| `arbitrage.max_levels` | Levels walked per side when sweeping |
| `display.refresh_ms` | Dashboard refresh and health-check period |
| `display.levels` | Levels shown per book |
| `display.spread_log_interval` | Log a spread snapshot every N refreshes |
| `connection.tls_verify` | Verify certificates and host names (default `true`) |
| `connection.ca_file` | Extra trusted CA in PEM format (used by the offline demo) |
| `connection.stall_timeout_ms` | Reconnect when a live connection is silent this long |
| `connection.reconnect_initial_ms`, `reconnect_max_ms` | Exponential backoff bounds (±20% jitter) |

The `symbol` fields are only labels. The streamed instrument comes from `binance.path` and the
subscribe messages, so change them together. To switch to ETH/USDT, for example, use
`/ws/ethusdt@depth@100ms` and `symbol=ETHUSDT` in `snapshot_path`, `ETH-USDT` for OKX and
`orderbook.50.ETHUSDT` for Bybit.

### Exchange protocols

| Venue | Default stream | Sync | Integrity checks | Heartbeat |
|-------|----------------|------|------------------|-----------|
| Binance | `<sym>@depth@100ms` + REST `/api/v3/depth?limit=1000` | Buffer events, fetch a snapshot, drop `u <= lastUpdateId`, apply the rest | `U <= local+1` continuity, crossed book | WebSocket pings (answered automatically) |
| OKX | `books` (400 levels) | Snapshot, then updates | `prevSeqId == last seqId`, CRC32 checksum of the top 25 levels from the original strings, crossed book | `"ping"` every 20 s |
| Bybit | `orderbook.50` | Snapshot, then deltas (`u == 1` resets) | `u == last + 1`, crossed book | `{"op":"ping"}` every 20 s |

Snapshot-only streams are also supported: Binance `depth5/10/20`, OKX `books5`/`bbo-tbt`.

## Matching engine

`include/matching/` is a single-threaded limit order book with exact, documented semantics:
price-time priority, limit and market orders, GTC/IOC/FOK, post-only, three self-trade-prevention
modes, cancel, and modify (keeps priority on a same-price size-down, loses it otherwise). Prices
and quantities are integer ticks and lots.

- Orders and price levels come from object pools.
- Each side is a sorted `{key, Level*}` vector with the best level at the back, and orders are
  indexed in an open-addressing hash map.
- `submit`/`cancel`/`modify` do no heap allocation once capacity is reserved.

`EngineRunner` puts the engine on its own thread, serving any number of gateway threads through
per-gateway SPSC queues. `exchange_sim` drives it with simulated makers and takers and verifies
the result. See [docs/MATCHING_ENGINE.md](docs/MATCHING_ENGINE.md) for the full rules, data
structures and how they are verified.

```bash
./build/exchange_sim                                 # 2 gateways, 1 symbol, 2M commands
./build/exchange_sim --gateways 4 --symbols 3 --pin
```

## Performance

All numbers below come from one run on a shared 4-vCPU cloud VM (2.8 GHz, g++ 13, `-O2`). Expect
±20–50% between runs, and rerun the benchmarks on your own machine before quoting them.

**Matching engine** (`build/matching_engine_benchmark`, optimized engine vs the `std::map` reference model;
both produce identical event streams)

| Workload | Optimized | Reference | Speedup | Allocations/op |
|----------|-----------|-----------|--------:|---------------:|
| Passive add/cancel near the touch | 38.8 ns/op (p99 129 ns) | 80.4 ns/op | 2.1× | 0 |
| Mixed order flow (random generator) | 42.7 ns/op (p99 173 ns) | 72.5 ns/op | 1.7× | 0 |
| Aggressive sweep of 20 levels (40 fills) | 491 ns/sweep | 2562 ns/sweep | 5.2× | 0 |

**Exchange simulator** (`build/exchange_sim`, 2 gateways, 2M commands): 3.46M commands/s and 1.13M
trades/s. Round-trip latency is p50 1.9 µs and p99 28 µs, with 8 commands in flight per gateway.
There are 0 heap allocations while trading.

**Market data** (`build/market_data_benchmark`, per message through the real adapters, using the
streams the mock exchange generates)

| Stream | Levels/msg | p50 | p99 |
|--------|-----------:|----:|----:|
| Binance diff depth @100ms (1000-level book) | 190 | 9.0 µs | 122 µs |
| OKX `books` update (400 levels, checksum verified) | 111 | 7.5 µs | 91 µs |
| Bybit `orderbook.50` delta | 29 | 2.0 µs | 10 µs |
| Binance `depth20` snapshot | 40 | 2.3 µs | 2.9 µs |
| OKX `books5` push | 10 | 1.1 µs | 1.5 µs |

The batched book apply (`OrderBook::applyLevels`) replaced the per-level upserts. On the same Binance
diff stream, p50 went from 158 µs to 5.5 µs (29×) and p99 from 3.2 ms to 54 µs. On a synthetic
5000-level book with 130 changes per message it went from 716 µs to 13.5 µs (53×).

**HFT core** (`build/hft_benchmark`)

| Component | Result | Baseline | Speedup |
|-----------|--------|----------|--------:|
| `SpscQueue`, 2 threads | 5.0 ns/item | `std::mutex` + `std::deque`: 217 ns | 44× |
| `ObjectPool` create + destroy | 4.7 ns | `new` / `delete`: 11.7 ns | 2.5× |
| `FlatHashMap` insert, 1M random keys | 34 ns | `std::unordered_map`: 236 ns | 7.0× |
| `FlatHashMap` find (hit) | 42 ns | 91 ns | 2.1× |
| `LatencyHistogram` percentile | 0.85 µs (record: 3.9 ns) | copy + sort of 10k samples: 235 µs | 276× |

The v1 benchmarks remain in `benchmark/orderbook_benchmark.cpp` (`std::map` vs flat vector; output in
`docs/screenshot_benchmark.png`) and `benchmark/parse_benchmark.cpp` (`std::stod` vs `std::from_chars`).

## Testing

| Suite | Cases | Covers |
|-------|------:|--------|
| `test_hft` | 57 | SPSC (incl. 2-thread stress), object pool, flat hash map (differential vs `std::unordered_map`), histogram error bounds |
| `test_matching` | 373 | Normative spec suite on three implementations, millions of differential-fuzz commands |
| `test_engine_runner` | 23 | Concurrent gateways replayed on a single-threaded engine, back-pressure, shutdown |
| `test_market_data` | 76 | L2 book, CRC32, every Binance/OKX/Bybit adapter branch, malformed input |
| `test_feed_integration` | 40 | Real feeds against an in-process TLS mock: sync, resync, reconnect, stall kick, keepalive pings, per-address connect deadlines, prompt stop, TLS failures |
| `test_mock_exchange` | 24 | Mock wire formats round-trip through the real adapters and feeds |
| `test_app` | 48 | Config fallback, sweep math, dedup, display alignment, signal handling, end-to-end monitor runs |

```bash
ctest --test-dir build --output-on-failure
./build/test_matching optimized/            # run a subset by name
HFT_LONG_TESTS=1 ./build/test_matching      # 10x longer differential fuzzing
cmake -S . -B build-asan -DHFT_SANITIZE=address,undefined && cmake --build build-asan -j
cmake -S . -B build-tsan -DHFT_SANITIZE=thread && cmake --build build-tsan -j
```

## Design decisions

**Why synchronous WebSocket reads?** The bottleneck is the network (milliseconds), not the
thread (microseconds). One blocking thread per venue keeps the protocol code linear and easy to
verify. The usual drawback of blocking reads is that they cannot be interrupted. It is solved
without async I/O: a mutex-protected registry of the feed's socket descriptors lets the main
thread `shutdown()` a socket. That makes the blocked read fail at once, which gives prompt stop,
stall recovery and connect timeouts.

**Why a lock-free SPSC queue instead of a mutex-protected book?** v1 copied each book under a
mutex every 200 ms, so detection lagged by up to a refresh. Now every book change becomes a
fixed-size snapshot on a single-producer/single-consumer ring. It uses acquire/release ordering
only, cached indices and cache-line-separated counters, so the feed thread never waits on the
consumer, and detection runs on every update.

**Why flat sorted vectors for L2 books?** Almost every update hits the top few levels: the code
checks the head and tail first, then binary-searches. The vectors are contiguous and the display
and detector read the best level at index 0. The trade-off is an O(n) `memmove` when a level is
inserted or erased deep in a large book.

**Why integer ticks in the matching engine?** Matching must be exact and deterministic.
Floating point is kept at the edges, in exchange data and the display.

**How do we know the matching engine is right?** The rules are written down
([docs/MATCHING_ENGINE.md](docs/MATCHING_ENGINE.md)) and implemented three times: an obvious
`std::map` reference, a structurally different naive model, and the optimized engine. A spec
suite with exact expected event sequences runs on all three. Differential fuzzing compares the
optimized engine with the reference event by event over millions of random commands.

**Why signal deduplication?** Without it, the same opportunity fires on every update while
prices stand still. Each ordered venue pair remembers the prices of its last signal and fires
again only when the best ask or bid has moved by at least 0.01.

## Project structure

```
Crypto_HFT_Engine/
├── CMakeLists.txt            # app, exchange_sim, mock_exchange, tests (CTest), benchmarks
├── config.json               # runtime configuration (three venues)
├── Setup.sh                  # apt deps + simdjson v5.1.1
├── include/
│   ├── hft/                  # SpscQueue, ObjectPool, FlatHashMap, LatencyHistogram, timing, CRC32
│   ├── matching/             # types, LimitOrderBook, MatchingEngine, EngineRunner
│   ├── exchanges/            # Binance / OKX / Bybit protocol adapters (no networking)
│   ├── net/https_client.h    # blocking HTTPS GET for REST snapshots
│   ├── market_data_feed.h    # connection manager (reconnect, kick, publish)
│   ├── orderbook.h           # L2 book;  book_snapshot.h: fixed-size snapshot
│   ├── arbitrage_detector.h  # depth-aware N-venue detector
│   ├── app.h                 # monitor event loop + CLI;  display.h, text_util.h, logger.h, config.h
│   └── latency_tracker.h
├── src/                      # main.cpp (monitor), exchange_sim.cpp
├── tools/                    # mock exchange, demo config and script, bilingual comment checker
├── tests/                    # test framework, reference models, 7 suites, mock TLS server
├── benchmark/                # hft, matching engine, market data (+ the v1 map/parse benchmarks)
└── docs/                     # MATCHING_ENGINE.md, screenshots
```

## Roadmap

- [x] Dual exchange WebSocket data (Binance + OKX)
- [x] Cross-exchange arbitrage detection
- [x] Latency histogram (p50/p95/p99)
- [x] Signal logging to file
- [x] Flat vector order book optimization
- [x] JSON configuration
- [x] Add Bybit exchange support
- [x] Lock-free SPSC queue for order book updates
- [x] Custom memory pool allocator (`hft::ObjectPool`)
- [x] Incremental order books with sequence and checksum validation
- [x] Reconnect, stall detection and TLS verification
- [x] Matching engine, threaded exchange core and simulator
- [ ] SIMD-optimized price comparison
- [ ] Paper trading: execute signals against matching engines that mirror the live books
- [ ] Capture and replay of raw market data

## License

MIT License - see [LICENSE](LICENSE) for details.
