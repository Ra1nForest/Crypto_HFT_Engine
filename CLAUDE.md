# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

A C++17 terminal app. It streams live order books from Binance and OKX over TLS WebSockets (Boost.Beast + OpenSSL), parses them with simdjson, and flags cross-exchange arbitrage on a live dashboard. It only monitors: nothing places orders.

## Build and run

Builds on Linux (Ubuntu 22.04+ / WSL2), not natively on Windows: `include/logger.h` uses POSIX `mkdir(path, mode)`, and `Setup.sh` uses apt.

```bash
bash Setup.sh                                   # apt deps + downloads simdjson.h/.cpp into third_party/
cmake -S . -B build && cmake --build build -j
./build/crypto_hft_engine                       # must run from the repo root
```

- `third_party/` is gitignored and the build fails without it. `Setup.sh` fetches simdjson from `master`, unpinned. Its printed "next steps" say to run the binary from inside `build/`, which falls back to defaults, so ignore them and run from the repo root.
- `config.json` and `logs/` are resolved against the current directory. If started from anywhere else, the app silently falls back to built-in defaults and creates `logs/` there.
- `CMakeLists.txt` hard-codes `-Wall -Wextra -O2`, so Debug builds are optimized too. Add `-DCMAKE_EXPORT_COMPILE_COMMANDS=ON` for clangd. A clean build has no warnings (g++ 13).
- The app is an interactive TUI: it redraws every `refresh_ms` and quits on Ctrl+C. It needs outbound access to `stream.binance.com:9443` and `ws.okx.com:8443`. SIGINT shuts it down gracefully and prints the final latency/arbitrage report, so a bounded smoke run works non-interactively. In a healthy run both feeds show `[LIVE]` and their message counts are non-zero:
  ```bash
  timeout -s INT -k 5 20 ./build/crypto_hft_engine 2>&1 | tail -n 30
  ```

### From a Windows checkout

Windows has no toolchain for this project, so run commands through WSL from PowerShell. `wsl` starts in the matching `/mnt/c/...` directory:

```powershell
wsl -- bash -c 'cmake -S . -B build/wsl && cmake --build build/wsl -j'
wsl -- bash -c 'timeout -s INT -k 5 20 ./build/wsl/crypto_hft_engine 2>&1 | tail -n 30'
```

- `wsl` passes the command line through the default Linux shell a second time, so `$vars` and nested quotes in inline commands get mangled. Put anything non-trivial in a script and run `wsl -- bash /mnt/c/.../script.sh`.
- WSL's `/tmp` is wiped whenever the instance restarts, and the instance stops shortly after it goes idle. Don't keep build output there between commands.
- `build/wsl` is still gitignored. Using it avoids a `build/` that was configured from another source path, which CMake refuses to reuse.

## Tests, lint, benchmarks

There is no test suite, CI, or lint/format config. Compiler warnings and the smoke run above are the only checks. Benchmarks are standalone programs outside CMake:

```bash
g++ -std=c++17 -O2 -o benchmark/orderbook_bench benchmark/orderbook_benchmark.cpp && ./benchmark/orderbook_bench  # std::map vs flat vector
g++ -std=c++17 -O2 -o benchmark/parse_bench benchmark/parse_benchmark.cpp && ./benchmark/parse_bench              # std::stod vs std::from_chars
```

- The benchmarks are self-contained re-implementations (for example, `orderbook_benchmark.cpp` defines its own `VecOrderBook`), so they never exercise the code in `include/`.
- The `*_bench` binaries are gitignored but were committed before that rule, so rebuilding them in place dirties `git status`. Write them to `/tmp` unless you mean to update them.

## Architecture

Every module is a header-only class in `include/`. The only translation units are `src/main.cpp` and `third_party/simdjson.cpp`. A new `.cpp` must be added to `add_executable` in `CMakeLists.txt`.

**Threads and data flow**
1. `main()` loads the config and creates a `Logger`, one `MarketDataFeed` per exchange, and an `ArbitrageDetector`. It then starts one `std::thread` per feed and waits 3 s.
2. Each feed thread runs `MarketDataFeed::receiveLoop()`. This uses blocking, synchronous Boost.Beast (resolve → TCP → SNI → TLS → WS handshake → optional subscribe message), then a `ws.read()` loop. Messages are parsed with simdjson On-Demand and dispatched on the `Exchange` enum to `parseBinanceDepth` or `parseOKXDepth`, which rebuild the feed's `OrderBook` while holding its `mutex_`.
3. Every `refresh_ms`, the main thread copies both books with `getOrderBookSnapshot()` (a copy taken under the lock). `Display::render`, `ArbitrageDetector::detect` and `Logger` then work on those copies without locking. Spread snapshots are logged every `spread_log_interval` loop iterations (a count, not a time).
4. Shutdown: SIGINT clears `g_running`, and the main thread then calls `feed.stop()` and `join()`. A feed thread only notices after its next `ws.read()` returns, because reads have no timeout or cancellation. There is also no reconnect: any exception ends that feed's thread, and the dashboard shows the feed `DOWN` for the rest of the run.

**Order book model.** Both subscriptions push full snapshots, not diffs. Binance `<symbol>@depth20@100ms` has 20 levels and OKX `books5` has 5. Each message clears the book and re-appends the levels in the order they arrive, skipping zero quantities. `updateBid`/`updateAsk` are plain `push_back`s with no upsert and no sort. Index 0 is the best level only because exchanges send bids high→low and asks low→high. An incremental-diff channel would need a different book.

**Arbitrage.** `detect(book_a, book_b)` uses top of book only and checks both directions: buy A's ask and sell B's bid, and the reverse.
- qty = min of the two top-level quantities.
- fee = (buy + sell) × qty × `fee_rate`.
- A signal fires when net > `min_profit` and at least one price has moved ≥ 0.01 since that direction's last signal (dedup).
- Signal exchange labels come from `OrderBook::getSymbol()`, which the feed sets to `"<name>:<symbol>"`.

**Latency stats.** `LatencyTracker` measures local processing only: the time from `ws.read()` returning until the book is updated, not network latency. It keeps the last 10,000 samples in a ring buffer, and every percentile call sorts a copy. It has no locking: the feed thread writes it while the main thread reads it for the dashboard.

**Display.** `Display` is stateless. It clears the screen with ANSI escapes and draws Unicode boxes with hard-coded widths (87/89/43/42). The padding helpers (`center`, `padRow`, `makeStatRow`) count bytes, not terminal columns, so multi-byte characters break alignment. To compensate, rows ending in `μs` are padded to `w+1`. After changing any label, precision or width, check the alignment in a real terminal.

**Two exchanges are hard-wired.** `AppConfig` has fixed `binance`/`okx` members, and `main.cpp` builds both feeds explicitly. `Display`, `Logger::logFinalReport` and `ArbitrageDetector::detect` take exactly two books or trackers. Some names are string literals: `"Binance"`/`"OKX"` in `main.cpp` and `Display::printArbitragePanel`, and `" BTC"` in `ArbitrageDetector::printSignal`. Adding an exchange (Bybit is on the README roadmap) needs a new `Exchange` value and a `parse<X>Depth` function, plus changes in each of those places.

## Configuration gotchas

- Loading is all-or-nothing. If any field is missing or has the wrong type (for example, `port` must be a JSON string), the whole config silently becomes `ConfigReader::getDefaults()`, with only one line on stderr. A new field has to be added to the struct, the parse block, `getDefaults()` and `config.json` together.
- The shipped `config.json` sets `fee_rate: 0.0`, so almost any positive cross-exchange gap produces a signal. The code default (`getDefaults()` and the `ArbitrageDetector` constructor) is `0.001`.
- The `symbol` fields are only labels. The instrument that is actually streamed comes from `binance.path` and from the `instId` inside `okx.subscribe`, so change all of them together.
- `enabled` is parsed but ignored, so both feeds always start. Binance's subscribe message is hard-coded to empty because Binance subscribes through the URL path.

## Conventions

- The hot path is `receiveLoop` → `parse*Depth`. Earlier perf changes (`std::map` → flat vector, `std::stod` → `std::from_chars` on simdjson `string_view`s) each shipped with a standalone benchmark in `benchmark/` and before→after numbers in the commit message.
- Commits use conventional prefixes (`feat:`, `perf:`, `docs:`, `chore:`).
- Code comments are in Simplified Chinese. The README, commit messages and program output are in English.
