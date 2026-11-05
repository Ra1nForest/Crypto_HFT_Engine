# Matching Engine

A header-only, single-threaded, price-time-priority limit order book (`include/matching/`), a
multi-symbol router, and a threaded "exchange core" that serves many gateway threads through
lock-free SPSC queues. `src/exchange_sim.cpp` drives it with simulated traders.

| Header | Contents |
|--------|----------|
| `matching/types.h` | Integer price/quantity types, enums, `OrderRequest`, `Trade`, query rows |
| `matching/order_book.h` | `LimitOrderBook<Listener>`: the optimized book (one symbol) |
| `matching/matching_engine.h` | `MatchingEngine<Listener>`: routes commands to one book per symbol |
| `matching/engine_runner.h` | `EngineRunner`: engine thread + per-gateway inbound/outbound SPSC queues |

## Order model

- **Prices are integer ticks and quantities integer lots** (`int64_t`). No floating point is
  involved in matching, so results are exact and deterministic. Converting to and from decimal
  prices (for example 0.01 USDT ticks) is the caller's job.
- **A price level holds at most `INT64_MAX` lots** (`kMaxLevelQty` in `matching/types.h`): the
  total remaining quantity resting at one price on one side never exceeds it, so level totals
  in `depth()` and `volumeAt()` are always exact. A request that would push a level past the
  bound is rejected with `InvalidQuantity` before anything else happens (submit rule 6, modify
  rule 4). Orders that never rest (IOC, FOK, market) are not subject to it.
- **Order ids are client-assigned** 64-bit values and must be unique among live (resting)
  orders. An id can be reused once its order has left the book. `0` and `UINT64_MAX` are
  ordinary ids.
- **Accounts** (`AccountId`) identify owners for self-trade prevention. Account `0` is an
  ordinary account.

`OrderRequest` fields: `id`, `account`, `side` (`Buy`/`Sell`), `type` (`Limit`/`Market`),
`tif` (`GTC`/`IOC`/`FOK`), `price` (ignored for market orders), `qty`, `post_only`, and `stp`
(`None`/`CancelTaker`/`CancelMaker`/`CancelBoth`).

## Semantics

The book reports everything through a listener template parameter (static dispatch, no virtual
calls):

```cpp
void onAccepted(const OrderRequest& req);
void onRejected(OrderId id, RequestType request, RejectReason reason);
void onTrade(const Trade& trade);
void onCancelled(OrderId id, Qty cancelled_qty, CancelReason reason);
void onModified(OrderId id, Price new_price, Qty new_qty);
```

The optimized engine and the reference model emit identical event streams, and both update the
book before every callback, so a listener may query the book (const methods only) from inside a
callback. Listeners must not call `submit`/`cancel`/`modify` re-entrantly.

### submit

Validation runs in this order. The first failing rule produces exactly one `onRejected(id, New, reason)`
and nothing else:

| # | Condition | Reason |
|---|-----------|--------|
| 1 | `qty <= 0` | `InvalidQuantity` |
| 2 | limit order with `price <= 0` | `InvalidPrice` |
| 3 | `post_only` on anything but a limit GTC order | `InvalidFlags` |
| 4 | `id` is already live | `DuplicateOrderId` |
| 5 | book holds `max_orders` live orders and the order is limit GTC | `CapacityExceeded` |
| 6 | limit GTC order with `qty > INT64_MAX - volumeAt(side, price)` (resting it would push its level past the bound) | `InvalidQuantity` |
| 7 | `post_only` and the price crosses (or locks) the opposite best price | `PostOnlyWouldCross` |

Rule 6 is exact. Matching never touches the order's own side, and an order whose price already
has a level on its own side cannot cross an uncrossed book, so it would rest in full. With no
level at that price the rule always passes. Because it only fires when such a level exists, it
never coincides with rule 2 (no level has a price <= 0) or rule 7 (the order cannot cross).

Otherwise the order is accepted (`onAccepted`) and:

- **FOK** orders are pre-checked by walking the opposite side in exact matching order. If the
  liquidity they could take is less than `qty`, they are cancelled with `FokUnfillable` and
  nothing else happens.
- **Matching**: while quantity remains and the best opposite level is acceptable (any level for
  market orders), the order trades against the head of that level, FIFO within a level and best
  price first. **The trade price is always the resting (maker) order's price.** Each fill emits
  `onTrade`. A maker that reaches zero leaves the book silently: `maker_remaining == 0` in the
  trade tells you it was filled. Trade ids start at 1 and increase by one per trade, per book.
- **Remainder**: market orders (any time in force) cancel with `MarketRemainder`, limit IOC with
  `IocRemainder`, and limit GTC rests at the back of its price level's FIFO.

### Self-trade prevention

STP applies when the incoming order has `stp != None` and the resting order belongs to the
same account. The incoming order's mode decides what happens:

| Mode | Effect |
|------|--------|
| `CancelMaker` | Cancel the resting order (`SelfTradePrevention`) and keep matching |
| `CancelTaker` | Cancel the incoming order's remainder; it never rests |
| `CancelBoth` | Cancel the resting order, then the incoming remainder |

The FOK pre-check follows the same rules: under `CancelMaker` same-account orders contribute
nothing and the walk continues; under `CancelTaker` and `CancelBoth` it stops at the first
same-account order.

### cancel and modify

- `cancel(id)`: an unknown id is rejected with `UnknownOrder`. Otherwise the order is removed
  and `onCancelled(id, remaining, UserRequested)` is emitted.
- `modify(id, new_price, new_qty)` sets a new price and a new **remaining** quantity. Validation
  runs in this order, and the first failing rule produces exactly one
  `onRejected(id, Modify, reason)` and leaves the order untouched:

  | # | Condition | Reason |
  |---|-----------|--------|
  | 1 | `id` is not live | `UnknownOrder` |
  | 2 | `new_qty <= 0` | `InvalidQuantity` |
  | 3 | `new_price <= 0` | `InvalidPrice` |
  | 4 | price change or quantity increase with `new_qty > INT64_MAX - (volumeAt(side, new_price) - (new_price == price ? remaining : 0))` (re-resting would push the target level past the bound; the order's own old quantity does not count) | `InvalidQuantity` |
  | 5 | post-only order moved to a different price that crosses (or locks) the opposite best price | `PostOnlyWouldCross` |

  Like submit rule 6, rule 4 only fires when the target level already exists on the order's
  side, so it never coincides with rule 3 or rule 5.
  - **Same price and a smaller or equal quantity**: changed in place, so the order **keeps its
    time priority**. Rule 4 never applies here, because the level total does not grow.
  - **Any price change or a quantity increase**: the order **loses priority**. It is taken out
    of the book, `onModified` is emitted, and it re-enters as an aggressive GTC limit order
    (it may trade) before resting at the back of its new level.

### Queries

`bestBid()`/`bestAsk()` (`std::optional`), `orderCount()`, `levelCount(side)`,
`depth(side, n)` (best first: price, total quantity, order count), `ordersAt(side, price)`
(FIFO order), `findOrder(id)`, `volumeAt(side, price)`, `lastTradeId()`, and
`checkInvariants(&why)`. The invariant check covers: sides strictly sorted, no empty levels,
cached level totals and counts correct, level totals within `INT64_MAX`, every order positive,
index consistent, book not crossed.

## Data structures

`LimitOrderBook` is designed so that the hot path does no heap allocation and touches as few
cache lines as possible:

- **Orders** (48 bytes) come from `hft::ObjectPool`: stable addresses, O(1) create/destroy, LIFO
  reuse keeps recently freed slots cache-hot. Orders at a price are linked into an intrusive
  doubly-linked FIFO.
- **Price levels** are pooled too. Each caches its total quantity and order count, so depth
  queries are O(levels), not O(orders).
- **Each side is a sorted flat `std::vector<{key, Level*}>` with the best level at the back.**
  Taking out the top level is a `pop_back`, and new levels near the touch are short `memmove`s.
  The key is `price` for bids and `-price` for asks, so both sides share one lookup routine and
  one crossing test. Lookups probe linearly from the touch for 8 entries, then binary-search the
  rest, because almost all activity happens near the top of the book.
- **Order index**: `hft::FlatHashMap<OrderId, Order*>`, an open-addressing map with linear
  probing, a splitmix64 mixer and backward-shift deletion (no tombstones).
- Integer arithmetic only, and none of it can overflow. The cached level totals fit in `int64_t`
  only because of the level-total bound, and the FOK pre-check relies on them: without STP it
  compares the still-needed quantity against whole levels and subtracts a level's total only when
  it is smaller, so the count-down stays positive across any number of full levels. (The
  reference model sums makers one by one, so its fill decisions do not depend on cached totals.)

Capacity is reserved up front (`reserve_orders`, default 1024). While the live order count stays
below it, `submit`/`cancel`/`modify` perform **zero heap allocations**. The benchmark verifies
this by counting `operator new`. Beyond the reservation the structures grow by doubling. Growth
happens before any state change or event, so a `std::bad_alloc` leaves the book and the event
stream untouched.

## MatchingEngine (multi-symbol)

`MatchingEngine<Listener>` owns one book per symbol. `addSymbol(name, max_orders, reserve_orders)`
returns a dense `SymbolId`, so routing is an array index. The listener receives the symbol id as
its first argument, as in `onTrade(SymbolId, const Trade&)`. Commands for unknown symbols are
rejected with `UnknownSymbol`.

## EngineRunner (threaded exchange core)

```
 gateway thread 0 ──EngineCommand──▶ inbound SPSC 0 ──┐                 ┌──▶ outbound SPSC 0 ──▶ gateway 0
 gateway thread 1 ──EngineCommand──▶ inbound SPSC 1 ──┼─▶ engine thread ─┼──▶ outbound SPSC 1 ──▶ gateway 1
 gateway thread N ──EngineCommand──▶ inbound SPSC N ──┘   (round-robin,  └──▶ outbound SPSC N ──▶ gateway N
                                                            busy-spin)
```

- One engine thread polls every gateway's inbound queue round-robin, draining up to `batch`
  commands per queue per turn. It busy-spins with `pause`, optionally yields after N idle spins,
  and can be pinned to a CPU.
- `EngineCommand` and `EngineEvent` are trivially copyable 64-byte PODs (one cache line per queue
  slot). Every event echoes the originating command's client timestamp (TSC ticks) for
  round-trip latency measurement.
- **Routing**: events go to the gateway that owns the order. A trade becomes one `Fill` per side
  (maker and taker), each describing the receiving gateway's own order, like a FIX
  ExecutionReport. Counterparty ids are not disclosed.
- **Session isolation**: a gateway cannot cancel or modify another gateway's order. Such
  requests are rejected as `UnknownOrder` without reaching the engine.
- **Back-pressure**: when a gateway's outbound queue is full, the engine spins and never drops
  events. A gateway must keep draining its events, even while waiting for inbound space.
- `stop()` drains every inbound queue before the engine thread exits.

## exchange_sim

```bash
./build/exchange_sim                       # 2 gateways (maker + taker), 1 symbol, 2M commands
./build/exchange_sim --gateways 4 --symbols 3 --pin
./build/exchange_sim --help
```

Market makers quote both sides around a random-walking mid with cancel/replace; takers send IOC
and market orders. Each gateway measures round-trip latency (command sent to first event
received) with `hft::LatencyHistogram`. The report covers throughput, latency percentiles per
gateway, event counts by type and the final books. The run ends with consistency checks: book
invariants, no sequence gaps, every command acknowledged in order, events published equal to
events received, maker fills equal to taker fills, and client-side order state matching the
books. The exit status is non-zero if any check fails.

Latency here is closed-loop: `--window` limits the commands each gateway keeps in flight, so
larger windows raise throughput but add queueing delay to the round trip.

## How it is verified

- **Two independent oracles.** `tests/reference_order_book.h` mirrors the rules with
  `std::map<Price, std::list<...>>`. `tests/naive_order_book.h` takes a deliberately different
  approach: one unordered vector, linear scans, and a FOK decided by dry-running the matching
  loop. The two are cross-checked against each other.
- **Normative spec suite** (`tests/matching_spec_tests.h`). It pins every rule above with exact
  expected event sequences and runs unchanged on the reference, the naive model and the
  optimized engine.
- **Differential fuzzing** (`tests/test_matching.cpp`). Millions of random commands from several
  profiles (balanced, aggressive sweeps, STP-heavy, FOK/IOC-heavy, modify-heavy, deep sparse
  books, capacity-limited books, extreme quantities whose same-price totals reach and would
  exceed `INT64_MAX`) run on the optimized engine and the reference. Event streams are compared
  after every command, and full book state plus invariants every 64 commands. Set
  `HFT_LONG_TESTS=1` for a run ten times longer.
- **Runner replay** (`tests/test_engine_runner.cpp`). Concurrent gateways submit random streams.
  The commands are replayed on a single-threaded engine in the order the runner processed them,
  and every gateway's event stream must match the replay.
- ASan, UBSan and TSan builds of all of the above (`-DHFT_SANITIZE=...`).
