//
// test_engine_runner.cpp
// 撮合引擎运行器测试：命令/事件 POD 布局、单网关事件流、跨网关路由（成交双方、STP、会话隔离、重复 id）、
// 多交易对、未知交易对、非法命令、序号断号、背压（极小出站队列）、stop() 排空、重启、绑核、零分配，
// 以及多网关线程并发 + 单线程回放的差分测试（逐网关事件流逐条相等）。
// Matching engine runner tests: command/event POD layout, single-gateway event flow, cross-gateway routing
// (both sides of a trade, STP, session isolation, duplicate ids), multiple symbols, unknown symbols, invalid
// commands, sequence gaps, back-pressure (tiny outbound queue), stop() draining, restart, CPU pinning, zero
// allocations, and a differential test of concurrent gateway threads + single-threaded replay (per-gateway
// event streams equal event by event).
//

// 启用 hft 头文件中的调试断言：必须在包含任何 hft 头文件之前定义
// Enable the debug asserts in the hft headers: must be defined before any hft header is included
#define HFT_ENABLE_ASSERTS 1

#include "hft/alloc_counter.h"
#include "matching/engine_runner.h"
#include "matching/matching_engine.h"
#include "matching/types.h"
#include "matching_test_util.h"
#include "test_framework.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <mutex>
#include <optional>
#include <random>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#if defined(__linux__)
#include <sched.h>
#endif

// 堆分配计数见 hft/alloc_counter.h（计数版全局 operator new/delete，本程序唯一包含它的翻译单元）
// Heap allocation counting: see hft/alloc_counter.h (counting global operator new/delete; this is the only
// translation unit of this program that includes it)
namespace {
std::uint64_t heapAllocations() { return hft::alloc_counter::total(); }
}  // namespace

// Sanitizer 构建下缩小并发测试规模（TSan 慢 5~15 倍）
// Scale down the concurrent tests in sanitizer builds (TSan is 5~15 times slower)
#if defined(__SANITIZE_THREAD__) || defined(__SANITIZE_ADDRESS__)
#define HFT_TEST_SANITIZED 1
#elif defined(__has_feature)
#if __has_feature(thread_sanitizer) || __has_feature(address_sanitizer)
#define HFT_TEST_SANITIZED 1
#endif
#endif
#ifndef HFT_TEST_SANITIZED
#define HFT_TEST_SANITIZED 0
#endif

using namespace hft::matching;
namespace mt = hft::matching::test;

namespace {

constexpr bool kSanitized = HFT_TEST_SANITIZED != 0;

// 并发测试的规模：sanitizer 构建减半；环境变量 HFT_TEST_SCALE=k（正整数）再放大 k 倍，用于长时间浸泡测试
// Size of the concurrent tests: halved in sanitizer builds; the environment variable HFT_TEST_SCALE=k (a
// positive integer) further multiplies it by k, for long soak tests
std::size_t scaled(std::size_t n) {
    static const std::size_t factor = [] {
        const char* env = std::getenv("HFT_TEST_SCALE");
        const long v = env != nullptr ? std::strtol(env, nullptr, 10) : 1;
        return static_cast<std::size_t>(v >= 1 && v <= 1000 ? v : 1);
    }();
    const std::size_t base = kSanitized ? std::max<std::size_t>(n / 2, 200) : n;
    return base * factor;
}

// ============================================================================
// 事件描述、期望事件构造、事件流比较
// Event descriptions, expected-event construction, event stream comparison
// ============================================================================

std::string describe(const EngineEvent& e) {
    std::string s = toString(e.type);
    s += "{seq=" + std::to_string(e.engine_seq) + " ts=" + std::to_string(e.client_ts) +
         " gw=" + std::to_string(e.gateway) + " sym=" + std::to_string(e.symbol) + " id=" + std::to_string(e.order_id);
    switch (e.type) {
        case EventType::Accepted:
            s += std::string(" ") + toString(e.accepted.side) + " " + toString(e.accepted.order_type) + " " +
                 toString(e.accepted.tif) + " px=" + std::to_string(e.accepted.price) +
                 " qty=" + std::to_string(e.accepted.qty) + " acct=" + std::to_string(e.accepted.account);
            if (e.accepted.post_only) s += " post_only";
            s += std::string(" stp=") + toString(e.accepted.stp);
            break;
        case EventType::Rejected:
            s += std::string(" request=") + toString(e.rejected.request) + "(" +
                 std::to_string(static_cast<int>(e.rejected.request)) + ") reason=" + toString(e.rejected.reason);
            break;
        case EventType::Fill:
            s += std::string(" ") + toString(e.liquidity) + " tid=" + std::to_string(e.fill.trade_id) +
                 " px=" + std::to_string(e.fill.price) + " qty=" + std::to_string(e.fill.qty) +
                 " rem=" + std::to_string(e.fill.remaining);
            break;
        case EventType::Cancelled:
            s += " qty=" + std::to_string(e.cancelled.qty) + " reason=" + toString(e.cancelled.reason);
            break;
        case EventType::Modified:
            s += " px=" + std::to_string(e.modified.price) + " qty=" + std::to_string(e.modified.qty);
            break;
    }
    if (e.type != EventType::Fill && e.liquidity != Liquidity::None) s += std::string(" liq=") + toString(e.liquidity);
    return s + "}";
}

// 期望事件的公共头：引擎序号、原命令时间戳、发送网关、交易对
// Common header of expected events: engine sequence number, original command timestamp, sending gateway,
// symbol
struct Hdr {
    std::uint64_t seq;
    std::uint64_t ts;
    GatewayId gw;
    SymbolId sym;
};

EngineEvent header(const Hdr& h, EventType type, OrderId id) {
    EngineEvent e{};
    e.engine_seq = h.seq;
    e.client_ts = h.ts;
    e.order_id = id;
    e.symbol = h.sym;
    e.gateway = h.gw;
    e.type = type;
    e.liquidity = Liquidity::None;
    return e;
}

EngineEvent evAccepted(const Hdr& h, const OrderRequest& r) {
    EngineEvent e = header(h, EventType::Accepted, r.id);
    e.accepted = AcceptedInfo{r.price, r.qty, r.account, r.side, r.type, r.tif, r.post_only, r.stp};
    return e;
}

EngineEvent evRejected(const Hdr& h, OrderId id, RequestType request, RejectReason reason) {
    EngineEvent e = header(h, EventType::Rejected, id);
    e.rejected = RejectedInfo{request, reason};
    return e;
}

EngineEvent evFill(const Hdr& h, OrderId id, Liquidity liq, TradeId tid, Price px, Qty qty, Qty remaining) {
    EngineEvent e = header(h, EventType::Fill, id);
    e.liquidity = liq;
    e.fill = FillInfo{tid, px, qty, remaining};
    return e;
}

EngineEvent evCancelled(const Hdr& h, OrderId id, Qty qty, CancelReason reason) {
    EngineEvent e = header(h, EventType::Cancelled, id);
    e.cancelled = CancelledInfo{qty, reason};
    return e;
}

EngineEvent evModified(const Hdr& h, OrderId id, Price px, Qty qty) {
    EngineEvent e = header(h, EventType::Modified, id);
    e.modified = ModifiedInfo{px, qty};
    return e;
}

bool sameStream(const std::vector<EngineEvent>& actual, const std::vector<EngineEvent>& expected, std::string* why) {
    const std::size_t common = std::min(actual.size(), expected.size());
    std::size_t i = 0;
    while (i < common && actual[i] == expected[i]) ++i;
    if (i == common && actual.size() == expected.size()) return true;
    if (why != nullptr) {
        std::string s = "event streams differ at index " + std::to_string(i) + " (actual " +
                        std::to_string(actual.size()) + " events, expected " + std::to_string(expected.size()) + ")\n";
        const std::size_t from = i >= 3 ? i - 3 : 0;
        const std::size_t to = std::min(i + 4, std::max(actual.size(), expected.size()));
        for (std::size_t k = from; k < to; ++k) {
            s += "      " + std::string(k == i ? ">>" : "  ") + " [" + std::to_string(k) + "] expected: " +
                 (k < expected.size() ? describe(expected[k]) : std::string("<none>")) + "\n";
            s += "              actual:   " + (k < actual.size() ? describe(actual[k]) : std::string("<none>")) + "\n";
        }
        *why = s;
    }
    return false;
}

bool checkStream(const std::vector<EngineEvent>& actual, const std::vector<EngineEvent>& expected, const char* file,
                 int line) {
    std::string why;
    if (sameStream(actual, expected, &why)) return true;
    ::hft_test::reportFailure(file, line, why);
    return false;
}

// CHECK_STREAM(driver.drain(0), evAccepted(...), ...)：网关收到的完整事件序列必须逐条相等
// CHECK_STREAM(driver.drain(0), evAccepted(...), ...): the complete event sequence received by the gateway
// must be equal event by event
#define CHECK_STREAM(actual, ...) checkStream((actual), std::vector<EngineEvent>{__VA_ARGS__}, __FILE__, __LINE__)
// CHECK_SILENT(driver.drain(1))：该网关没有收到任何事件
// CHECK_SILENT(driver.drain(1)): the gateway received no events at all
#define CHECK_SILENT(actual) checkStream((actual), std::vector<EngineEvent>{}, __FILE__, __LINE__)

bool sameCommand(const EngineCommand& a, const EngineCommand& b) {
    return a.client_ts == b.client_ts && a.seq == b.seq && a.order_id == b.order_id && a.price == b.price &&
           a.qty == b.qty && a.symbol == b.symbol && a.account == b.account && a.gateway == b.gateway &&
           a.type == b.type && a.side == b.side && a.order_type == b.order_type && a.tif == b.tif &&
           a.post_only == b.post_only && a.stp == b.stp;
}

// ============================================================================
// 线程工具：看门狗（死锁时直接中止，而不是让 ctest 挂到超时）、自旋等待
// Thread utilities: watchdog (aborts outright on a deadlock instead of letting ctest hang until its
// timeout), spin waiting
// ============================================================================

class Watchdog {
public:
    Watchdog(const char* what, std::chrono::seconds limit) : what_(what) {
        thread_ = std::thread([this, limit] {
            std::unique_lock<std::mutex> lock(mu_);
            if (!cv_.wait_for(lock, limit, [this] { return done_; })) {
                std::fprintf(stderr, "WATCHDOG: '%s' did not finish within %lld s (deadlock?)\n", what_,
                             static_cast<long long>(limit.count()));
                std::fflush(stderr);
                std::abort();
            }
        });
    }
    ~Watchdog() {
        {
            std::lock_guard<std::mutex> lock(mu_);
            done_ = true;
        }
        cv_.notify_all();
        thread_.join();
    }
    Watchdog(const Watchdog&) = delete;
    Watchdog& operator=(const Watchdog&) = delete;

private:
    const char* what_;
    std::mutex mu_;
    std::condition_variable cv_;
    bool done_ = false;
    std::thread thread_;
};

std::chrono::seconds watchdogLimit() { return std::chrono::seconds(kSanitized ? 900 : 180); }

// 自旋等待一次：cpuRelax，每 64 次 yield 一次（测试线程数常多于 CPU 数）
// One spin-wait step: cpuRelax, with a yield every 64 times (tests often have more threads than CPUs)
void spinPause(std::uint32_t& spins) {
    if (++spins >= 64) {
        spins = 0;
        std::this_thread::yield();
    } else {
        hft::cpuRelax();
    }
}

EngineRunnerConfig testConfig(std::size_t gateways, std::size_t inbound = 256, std::size_t outbound = 1024,
                              std::size_t batch = 8) {
    EngineRunnerConfig c;
    c.gateways = gateways;
    c.inbound_capacity = inbound;
    c.outbound_capacity = outbound;
    c.batch = batch;
    c.idle_spins_before_yield = 64;  // 测试线程数常多于 CPU 数
                                     // tests often have more threads than CPUs
    return c;
}

std::string symbolName(std::size_t s) { return "SYM" + std::to_string(s); }

// ============================================================================
// 同步驱动：测试线程扮演全部网关，每次发一条命令并等待引擎处理完（之后读取各网关收到的事件）。
// 运行器必须是新建的、只有本驱动发送命令（引擎序号 == 已发送命令数）。
// Synchronous driver: the test thread plays all gateways, sending one command at a time and waiting until
// the engine has processed it (afterwards it reads the events each gateway received).
// The runner must be freshly created, and only this driver may send commands (engine sequence number ==
// number of commands sent).
// ============================================================================
template <class Runner>
class SyncDriver {
public:
    explicit SyncDriver(Runner& runner) : runner_(runner), seq_(runner.gatewayCount(), 0) {}

    // 以网关 g 的身份发送（自动填写网关序号与唯一的 client_ts），返回其事件的公共头
    // Sends as gateway g (filling in the gateway sequence number and a unique client_ts automatically) and
    // returns the common header of its events
    Hdr send(GatewayId g, const EngineCommand& command) {
        EngineCommand c = command;
        c.seq = ++seq_[g];
        c.gateway = g;
        c.client_ts = 1'000'000 + 7 * (sent_ + 1);
        std::uint32_t spins = 0;
        while (!runner_.inbound(g).tryPush(c)) spinPause(spins);
        ++sent_;
        waitProcessed(sent_);
        return Hdr{sent_, c.client_ts, g, c.symbol};
    }

    Hdr submit(GatewayId g, SymbolId s, const OrderRequest& r) { return send(g, EngineCommand::newOrder(g, 0, s, r)); }
    Hdr cancel(GatewayId g, SymbolId s, OrderId id) { return send(g, EngineCommand::cancel(g, 0, s, id)); }
    Hdr modify(GatewayId g, SymbolId s, OrderId id, Price p, Qty q) {
        return send(g, EngineCommand::modify(g, 0, s, id, p, q));
    }

    void waitProcessed(std::uint64_t n) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
        std::uint32_t spins = 0;
        while (runner_.processedCommands() < n) {
            if (std::chrono::steady_clock::now() > deadline) FAIL("engine did not process command " + std::to_string(n));
            spinPause(spins);
        }
    }

    // 取走网关 g 当前收到的全部事件
    // Takes all events that gateway g has received so far
    std::vector<EngineEvent> drain(GatewayId g) {
        std::vector<EngineEvent> out;
        EngineEvent e{};
        while (runner_.outbound(g).tryPop(e)) out.push_back(e);
        return out;
    }

    std::uint64_t sent() const noexcept { return sent_; }

private:
    Runner& runner_;
    std::vector<std::uint64_t> seq_;
    std::uint64_t sent_ = 0;
};

// ============================================================================
// 命令日志：引擎线程按处理顺序记下每条命令（外部存储，运行器析构后仍可读取）
// Command journal: the engine thread records every command in processing order (external storage, still
// readable after the runner is destroyed)
// ============================================================================
struct JournalEntry {
    std::uint64_t engine_seq;
    GatewayId gateway;
    EngineCommand command;
};

struct RecordingJournal {
    std::vector<JournalEntry>* sink = nullptr;
    void onCommand(std::uint64_t seq, GatewayId g, const EngineCommand& c) { sink->push_back(JournalEntry{seq, g, c}); }
};

// ============================================================================
// 并发驱动：每个网关一个线程，发送预先生成的命令流，同时持续消费自己的事件队列
// （等待入站空位时也消费，避免"入站满 + 出站满"死锁）。全部发完后控制线程调用 stop()（排空），
// 网关线程在 stop() 期间继续消费，最后收尾。
// Concurrent driver: one thread per gateway sends a pre-generated command stream while continuously
// consuming its own event queue (also while waiting for inbound space, to avoid an
// "inbound full + outbound full" deadlock). Once everything is sent, the control thread calls stop()
// (draining); the gateway threads keep consuming during stop() and finally wrap up.
// ============================================================================
struct GatewayScript {
    std::vector<EngineCommand> commands;
    std::vector<EngineEvent> received;
};

struct DriveOptions {
    bool slow_consumer = false;  // 每消费 64 个事件睡 20 µs：制造出站队列满（背压）
                                 // sleep 20 µs per 64 events consumed: forces a full outbound queue (back-pressure)
};

template <class Runner>
void driveConcurrently(Runner& runner, std::vector<GatewayScript>& scripts, const DriveOptions& opt) {
    const std::size_t n = scripts.size();
    std::atomic<std::size_t> finished_sending{0};
    std::atomic<bool> stopped{false};
    std::vector<std::thread> threads;
    threads.reserve(n);
    runner.start();
    for (std::size_t g = 0; g < n; ++g) {
        threads.emplace_back([&, g] {
            auto& in = runner.inbound(static_cast<GatewayId>(g));
            auto& out = runner.outbound(static_cast<GatewayId>(g));
            GatewayScript& sc = scripts[g];
            std::uint64_t consumed = 0;
            auto drain = [&]() {
                bool any = false;
                while (const EngineEvent* e = out.front()) {
                    sc.received.push_back(*e);
                    out.pop();
                    any = true;
                    if (opt.slow_consumer && (++consumed & 63) == 0)
                        std::this_thread::sleep_for(std::chrono::microseconds(20));
                }
                return any;
            };
            std::uint32_t spins = 0;
            for (std::size_t i = 0; i < sc.commands.size(); ++i) {
                while (!in.tryPush(sc.commands[i])) {
                    if (!drain()) spinPause(spins);
                }
                if ((i & 15) == 0) drain();
            }
            finished_sending.fetch_add(1, std::memory_order_release);
            while (!stopped.load(std::memory_order_acquire)) {
                if (!drain()) spinPause(spins);
            }
            drain();  // 引擎线程已退出：剩下的事件都已可见
                      // the engine thread has exited: all remaining events are visible
        });
    }
    while (finished_sending.load(std::memory_order_acquire) < n) std::this_thread::sleep_for(std::chrono::microseconds(200));
    runner.stop();
    stopped.store(true, std::memory_order_release);
    for (std::thread& t : threads) t.join();
}

// ============================================================================
// 回放模型：按引擎记录的处理顺序，在单线程 MatchingEngine 上重放同样的命令，并用独立实现的路由规则
// （std::map 归属表 + 每条命令后直接向订单簿查询订单是否仍在簿上）推出每个网关应收到的事件流。
// Replay model: replays the same commands on a single-threaded MatchingEngine in the processing order the
// engine recorded, and derives the event stream each gateway should receive with independently implemented
// routing rules (a std::map owner map + after each command, asking the order book directly whether the
// order is still in the book).
// ============================================================================
struct RawEvent {
    SymbolId symbol;
    mt::Event event;
};

struct ReplayRecorder {
    std::vector<RawEvent> events;
    void onAccepted(SymbolId s, const OrderRequest& r) { events.push_back(RawEvent{s, mt::accepted(r)}); }
    void onRejected(SymbolId s, OrderId id, RequestType t, RejectReason r) {
        events.push_back(RawEvent{s, mt::rejected(id, t, r)});
    }
    void onTrade(SymbolId s, const Trade& t) { events.push_back(RawEvent{s, mt::trade(t)}); }
    void onCancelled(SymbolId s, OrderId id, Qty q, CancelReason r) {
        events.push_back(RawEvent{s, mt::cancelled(id, q, r)});
    }
    void onModified(SymbolId s, OrderId id, Price p, Qty q) { events.push_back(RawEvent{s, mt::modified(id, p, q)}); }
};

class ReplayModel {
public:
    using Key = std::pair<SymbolId, OrderId>;

    ReplayModel(std::size_t gateways, std::size_t symbols, std::size_t reserve)
        : engine_(recorder_), expected_(gateways) {
        for (std::size_t s = 0; s < symbols; ++s) engine_.addSymbol(symbolName(s), 0, reserve);
    }

    // 重放一条命令；内部不一致（maker 找不到所属网关）时返回 false 并写明原因
    // Replays one command; on an internal inconsistency (no owning gateway found for a maker) returns false and
    // states the reason
    bool apply(std::uint64_t engine_seq, GatewayId g, const EngineCommand& cmd, std::string* why) {
        auto hdr = [&](SymbolId symbol) { return Hdr{engine_seq, cmd.client_ts, g, symbol}; };
        recorder_.events.clear();
        const SymbolId s = cmd.symbol;
        const bool known = s < engine_.symbolCount();
        switch (cmd.type) {
            case RequestType::New:
                engine_.submit(s, cmd.request());
                break;
            case RequestType::Cancel:
            case RequestType::Modify: {
                if (known) {
                    const auto it = owners_.find(Key{s, cmd.order_id});
                    if (it != owners_.end() && it->second != g) {  // 会话隔离：别人的订单
                                                                   // session isolation: someone else's order
                        expected_[g].push_back(evRejected(hdr(s), cmd.order_id, cmd.type, RejectReason::UnknownOrder));
                        ++foreign_;
                        return true;
                    }
                }
                if (cmd.type == RequestType::Cancel) engine_.cancel(s, cmd.order_id);
                else engine_.modify(s, cmd.order_id, cmd.price, cmd.qty);
                break;
            }
            default:
                expected_[g].push_back(evRejected(hdr(s), cmd.order_id, cmd.type, RejectReason::InvalidFlags));
                return true;
        }
        bool touched_own = false;  // 命令自身的订单被接受 / 改单生效 / 被撤
                                   // the command's own order was accepted / modified / cancelled
        for (const RawEvent& r : recorder_.events) {
            const mt::Event& ev = r.event;
            switch (ev.kind) {
                case mt::Event::Kind::Accepted:
                    expected_[g].push_back(evAccepted(hdr(r.symbol), ev.request));
                    touched_own = true;
                    break;
                case mt::Event::Kind::Rejected:
                    expected_[g].push_back(evRejected(hdr(r.symbol), ev.id, ev.request_type, ev.reject_reason));
                    break;
                case mt::Event::Kind::Trade: {
                    const Trade& t = ev.trade;
                    if (t.taker_id != cmd.order_id) return fail(why, "trade taker is not the command's order");
                    const auto it = owners_.find(Key{r.symbol, t.maker_id});
                    if (it == owners_.end()) return fail(why, "maker " + std::to_string(t.maker_id) + " has no owner");
                    const GatewayId maker_gw = it->second;
                    if (t.maker_remaining == 0) owners_.erase(it);
                    const Hdr h = hdr(r.symbol);
                    expected_[maker_gw].push_back(
                        evFill(h, t.maker_id, Liquidity::Maker, t.trade_id, t.price, t.qty, t.maker_remaining));
                    expected_[g].push_back(
                        evFill(h, t.taker_id, Liquidity::Taker, t.trade_id, t.price, t.qty, t.taker_remaining));
                    ++trades_;
                    break;
                }
                case mt::Event::Kind::Cancelled: {
                    const EngineEvent e = evCancelled(hdr(r.symbol), ev.id, ev.qty, ev.cancel_reason);
                    if (ev.id == cmd.order_id) {
                        expected_[g].push_back(e);
                        touched_own = true;
                    } else {
                        const auto it = owners_.find(Key{r.symbol, ev.id});
                        if (it == owners_.end())
                            return fail(why, "cancelled maker " + std::to_string(ev.id) + " has no owner");
                        const GatewayId maker_gw = it->second;
                        owners_.erase(it);
                        expected_[maker_gw].push_back(e);
                    }
                    break;
                }
                case mt::Event::Kind::Modified:
                    expected_[g].push_back(evModified(hdr(r.symbol), ev.id, ev.price, ev.qty));
                    touched_own = true;
                    break;
            }
        }
        if (touched_own) {
            // 订单最终是否挂在簿上：直接问订单簿（与运行器从事件推导的归属互相独立）
            // Whether the order ends up resting in the book: ask the order book directly (independent of the
            // ownership that the runner derives from events)
            if (engine_.book(s).findOrder(cmd.order_id)) owners_[Key{s, cmd.order_id}] = g;
            else owners_.erase(Key{s, cmd.order_id});
        }
        return true;
    }

    const std::vector<EngineEvent>& expected(std::size_t g) const { return expected_[g]; }
    const MatchingEngine<ReplayRecorder>& engine() const { return engine_; }
    const std::map<Key, GatewayId>& owners() const { return owners_; }
    std::uint64_t foreignRejects() const { return foreign_; }
    std::uint64_t trades() const { return trades_; }

    // 归属表恰好覆盖全部挂单（订单簿查询接口逐档展开）
    // The owner map covers exactly the resting orders (expanded level by level through the order book's
    // query interface)
    bool ownersCoverBooks(std::string* why) const {
        std::size_t resting = 0;
        for (SymbolId s = 0; s < engine_.symbolCount(); ++s) {
            const auto& book = engine_.book(s);
            for (Side side : {Side::Buy, Side::Sell}) {
                for (const LevelInfo& l : book.depth(side)) {
                    for (const OrderInfo& o : book.ordersAt(side, l.price)) {
                        if (owners_.count(Key{s, o.id}) == 0)
                            return fail(why, "resting order " + std::to_string(o.id) + " has no owner");
                        ++resting;
                    }
                }
            }
        }
        if (resting != owners_.size())
            return fail(why, "owner map has " + std::to_string(owners_.size()) + " entries for " +
                                 std::to_string(resting) + " resting orders");
        return true;
    }

private:
    static bool fail(std::string* why, std::string msg) {
        if (why != nullptr) *why = std::move(msg);
        return false;
    }

    ReplayRecorder recorder_;
    MatchingEngine<ReplayRecorder> engine_;
    std::vector<std::vector<EngineEvent>> expected_;
    std::map<Key, GatewayId> owners_;
    std::uint64_t foreign_ = 0;
    std::uint64_t trades_ = 0;
};

// ============================================================================
// 随机命令流：每个 (网关, 交易对) 一个 RandomCommandGenerator（各自跟踪自己的订单，撤单/改单多半命中
// 自己的订单），按命令随机选交易对；可选极少量未知交易对与非法命令类型。
// colliding：各网关共用 id 空间 [1, 4096]（大量跨网关重复 id、撤/改别人的订单）；
// 否则各网关 id 加不同偏移（边界 id UINT64_MAX 与 UINT64_MAX - 1 保持原样，仍会偶尔碰撞）。
// Random command stream: one RandomCommandGenerator per (gateway, symbol) (each tracks its own orders, so
// cancels/modifies mostly hit its own orders), with a random symbol per command; optionally a tiny share
// of unknown symbols and invalid command types.
// colliding: all gateways share the id space [1, 4096] (many cross-gateway duplicate ids, cancels/modifies
// of other gateways' orders);
// otherwise each gateway's ids get a different offset (the boundary ids UINT64_MAX and UINT64_MAX - 1 stay
// unchanged and still collide occasionally).
// ============================================================================
struct ScriptOptions {
    std::size_t symbols = 1;
    std::size_t commands = 1000;
    bool colliding_ids = true;
    double unknown_symbol_rate = 0.0;
    double invalid_type_rate = 0.0;
    std::uint64_t seed = 1;
};

std::uint64_t mix64(std::uint64_t x) {
    x += 0x9e3779b97f4a7c15ULL;
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
    return x ^ (x >> 31);
}

// 每条命令唯一的 client_ts 标记：高位是网关，低位是该网关内的序号
// Unique client_ts token of every command: the high bits are the gateway, the low bits the sequence number
// within that gateway
std::uint64_t tokenOf(std::size_t g, std::size_t i) { return (static_cast<std::uint64_t>(g + 1) << 40) | (i + 1); }

std::vector<EngineCommand> makeScript(const ScriptOptions& opt, GatewayId g) {
    mt::GeneratorConfig cfg;
    cfg.max_live = 120;
    std::vector<mt::RandomCommandGenerator> gens;
    gens.reserve(opt.symbols);
    for (std::size_t s = 0; s < opt.symbols; ++s) gens.emplace_back(mix64(opt.seed * 1000003 + g * 1009 + s), cfg);
    std::mt19937_64 rng(mix64(opt.seed ^ (0xabcdef12345ULL + g)));
    auto chance = [&rng](double p) { return static_cast<double>(rng() >> 11) * 0x1.0p-53 < p; };
    auto mapId = [&](OrderId id) -> OrderId {
        if (opt.colliding_ids || id >= (OrderId{1} << 40)) return id;
        return id + (static_cast<OrderId>(g) + 1) * (OrderId{1} << 40);
    };
    std::vector<EngineCommand> out;
    out.reserve(opt.commands);
    for (std::size_t i = 0; i < opt.commands; ++i) {
        const std::size_t s = static_cast<std::size_t>(rng() % opt.symbols);
        const mt::Command c = gens[s].next();
        SymbolId target = static_cast<SymbolId>(s);
        if (chance(opt.unknown_symbol_rate)) target = static_cast<SymbolId>(opt.symbols + rng() % 3);
        const std::uint64_t seq = i + 1;
        const std::uint64_t ts = tokenOf(g, i);
        EngineCommand ec;
        switch (c.type) {
            case RequestType::New: {
                OrderRequest r = c.order;
                r.id = mapId(r.id);
                ec = EngineCommand::newOrder(g, seq, target, r, ts);
                break;
            }
            case RequestType::Cancel:
                ec = EngineCommand::cancel(g, seq, target, mapId(c.id), ts);
                break;
            default:
                ec = EngineCommand::modify(g, seq, target, mapId(c.id), c.price, c.qty, ts);
                break;
        }
        if (chance(opt.invalid_type_rate)) ec.type = static_cast<RequestType>(3 + rng() % 200);
        out.push_back(ec);
    }
    return out;
}

// ============================================================================
// 差分场景：并发运行 → 校验命令日志与事件重建出的处理顺序 → 单线程回放 → 逐网关事件流比较 → 终态比较
// Differential scenario: concurrent run → check the command journal and the processing order rebuilt from
// events → single-threaded replay → per-gateway event stream comparison → final state comparison
// ============================================================================
struct Scenario {
    const char* name;
    std::size_t gateways;
    std::size_t symbols;
    std::size_t commands_per_gateway;
    std::size_t inbound_capacity;
    std::size_t outbound_capacity;
    std::size_t batch;
    bool colliding_ids;
    bool slow_consumer;
    double unknown_symbol_rate;
    double invalid_type_rate;
};

struct ScenarioResult {
    EngineRunnerStats stats;
    std::uint64_t total_events = 0;
    std::uint64_t trades = 0;
    std::uint64_t foreign = 0;
};

ScenarioResult runScenario(const Scenario& sc, std::uint64_t seed) {
    Watchdog watchdog(sc.name, watchdogLimit());
    ScenarioResult result;
    const std::size_t G = sc.gateways;
    const std::size_t per_gateway = scaled(sc.commands_per_gateway);
    std::vector<GatewayScript> scripts(G);
    std::size_t total = 0;
    for (std::size_t g = 0; g < G; ++g) {
        ScriptOptions so;
        so.symbols = sc.symbols;
        so.commands = per_gateway;
        so.colliding_ids = sc.colliding_ids;
        so.unknown_symbol_rate = sc.unknown_symbol_rate;
        so.invalid_type_rate = sc.invalid_type_rate;
        so.seed = seed;
        scripts[g].commands = makeScript(so, static_cast<GatewayId>(g));
        scripts[g].received.reserve(per_gateway * 4);
        total += per_gateway;
    }

    std::vector<JournalEntry> journal;
    journal.reserve(total);
    EngineRunner<RecordingJournal> runner(testConfig(G, sc.inbound_capacity, sc.outbound_capacity, sc.batch),
                                          RecordingJournal{&journal});
    for (std::size_t s = 0; s < sc.symbols; ++s) runner.addSymbol(symbolName(s), 0, 1 << 14);
    DriveOptions drive;
    drive.slow_consumer = sc.slow_consumer;
    driveConcurrently(runner, scripts, drive);
    result.stats = runner.stats();

    // ---- 1. 命令日志：引擎序号连续；每个网关的命令按发送顺序（FIFO）被处理，内容逐字段不变 ----
    // ---- 1. Command journal: engine sequence numbers are contiguous; each gateway's commands are processed in
    // sending order (FIFO), with every field unchanged ----
    REQUIRE_EQ(journal.size(), total);
    std::vector<std::size_t> next(G, 0);
    std::vector<std::vector<std::uint64_t>> journal_seqs(G);
    for (std::size_t i = 0; i < journal.size(); ++i) {
        const JournalEntry& e = journal[i];
        REQUIRE_EQ(e.engine_seq, static_cast<std::uint64_t>(i + 1));
        REQUIRE(e.gateway < G);
        REQUIRE(next[e.gateway] < scripts[e.gateway].commands.size());
        if (!sameCommand(e.command, scripts[e.gateway].commands[next[e.gateway]]))
            FAIL("journal entry " + std::to_string(i) + " does not match gateway " + std::to_string(e.gateway) +
                 "'s command #" + std::to_string(next[e.gateway]));
        ++next[e.gateway];
        journal_seqs[e.gateway].push_back(e.engine_seq);
    }

    // ---- 2. 仅凭事件重建处理顺序：每条命令都至少有一个发回本网关的事件，且按发送顺序到达 ----
    // ---- 2. Rebuild the processing order from events alone: every command has at least one event sent back
    // to its own gateway, arriving in sending order ----
    for (std::size_t g = 0; g < G; ++g) {
        std::vector<std::uint64_t> firsts;
        for (const EngineEvent& ev : scripts[g].received) {
            if (ev.gateway == g && (firsts.empty() || firsts.back() != ev.engine_seq)) firsts.push_back(ev.engine_seq);
        }
        CHECK_EQ(firsts.size(), journal_seqs[g].size());
        CHECK(firsts == journal_seqs[g]);
        result.total_events += scripts[g].received.size();
    }

    // ---- 3. 按日志顺序在单线程引擎上回放，逐网关比较事件流 ----
    // ---- 3. Replay in journal order on a single-threaded engine and compare the event streams per gateway ----
    ReplayModel replay(G, sc.symbols, 1 << 14);
    for (const JournalEntry& e : journal) {
        std::string why;
        if (!replay.apply(e.engine_seq, e.gateway, e.command, &why)) FAIL("replay: " + why);
    }
    for (std::size_t g = 0; g < G; ++g) {
        std::string why;
        if (!sameStream(scripts[g].received, replay.expected(g), &why))
            FAIL(std::string(sc.name) + ": gateway " + std::to_string(g) + ": " + why);
    }

    // ---- 4. 终态：订单簿逐项相同；运行器不变量（含归属表 == 挂单）；归属与回放模型一致 ----
    // ---- 4. Final state: the order books are identical item by item; runner invariants (including
    // owner map == resting orders); ownership agrees with the replay model ----
    for (SymbolId s = 0; s < sc.symbols; ++s) {
        std::string why;
        if (!mt::sameState(runner.engine().book(s), replay.engine().book(s), &why))
            FAIL(symbolName(s) + " final state differs: " + why);
    }
    {
        std::string why;
        if (!runner.checkInvariants(&why)) FAIL("runner invariants: " + why);
        if (!replay.ownersCoverBooks(&why)) FAIL("replay owners: " + why);
    }
    std::size_t resting = 0;
    for (SymbolId s = 0; s < sc.symbols; ++s) resting += runner.engine().book(s).orderCount();
    CHECK_EQ(resting, replay.owners().size());
    for (const auto& [key, g] : replay.owners()) {
        const std::optional<GatewayId> owner = runner.ownerOf(key.first, key.second);
        if (!owner || *owner != g) FAIL("owner of order " + std::to_string(key.second) + " differs");
    }

    // ---- 5. 统计 ----
    // ---- 5. Statistics ----
    CHECK_EQ(result.stats.commands, static_cast<std::uint64_t>(total));
    CHECK_EQ(result.stats.events, result.total_events);
    CHECK_EQ(result.stats.sequence_gaps, 0u);
    CHECK_EQ(result.stats.orphan_events, 0u);
    CHECK_EQ(result.stats.foreign_rejects, replay.foreignRejects());
    result.trades = replay.trades();
    result.foreign = replay.foreignRejects();
    return result;
}

std::size_t firstAllowedCpu() {
#if defined(__linux__)
    cpu_set_t set;
    CPU_ZERO(&set);
    if (sched_getaffinity(0, sizeof(set), &set) == 0) {
        for (int c = 0; c < CPU_SETSIZE; ++c) {
            if (CPU_ISSET(c, &set)) return static_cast<std::size_t>(c);
        }
    }
#endif
    return 0;
}

}  // namespace

// ============================================================================
// 命令 / 事件 POD
// Command / event PODs
// ============================================================================

TEST_CASE("runner: commands and events are one-cache-line PODs") {
    static_assert(std::is_trivially_copyable_v<EngineCommand> && std::is_trivial_v<EngineCommand>);
    static_assert(std::is_trivially_copyable_v<EngineEvent> && std::is_trivial_v<EngineEvent>);
    static_assert(std::is_standard_layout_v<EngineCommand> && std::is_standard_layout_v<EngineEvent>);
    static_assert(alignof(EngineCommand) == 64 && alignof(EngineEvent) == 64);
    CHECK_EQ(sizeof(EngineCommand), 64u);
    CHECK_EQ(sizeof(EngineEvent), 64u);
    CHECK_EQ(sizeof(AcceptedInfo), 32u);
    CHECK_EQ(sizeof(FillInfo), 32u);
    std::printf("    sizeof(EngineCommand) = %zu, sizeof(EngineEvent) = %zu\n", sizeof(EngineCommand),
                sizeof(EngineEvent));
}

TEST_CASE("runner: command factories round-trip every field") {
    const OrderRequest r = mt::marketSell(0xFFFF'FFFF'FFFF'FFFEULL, 123456789)
                               .account(77).fok().price(-5).postOnly().stp(StpMode::CancelBoth);
    const EngineCommand n = EngineCommand::newOrder(3, 9, 4, r, 555);
    CHECK_EQ(n.type, RequestType::New);
    CHECK_EQ(n.gateway, 3u);
    CHECK_EQ(n.seq, 9u);
    CHECK_EQ(n.symbol, 4u);
    CHECK_EQ(n.client_ts, 555u);
    CHECK(mt::sameRequest(n.request(), r));

    const EngineCommand c = EngineCommand::cancel(1, 2, 3, 42, 99);
    CHECK_EQ(c.type, RequestType::Cancel);
    CHECK_EQ(c.order_id, 42u);
    CHECK_EQ(c.symbol, 3u);
    CHECK_EQ(c.seq, 2u);
    CHECK_EQ(c.client_ts, 99u);

    const EngineCommand m = EngineCommand::modify(5, 6, 7, 43, 1001, 17, 98);
    CHECK_EQ(m.type, RequestType::Modify);
    CHECK_EQ(m.order_id, 43u);
    CHECK_EQ(m.price, 1001);
    CHECK_EQ(m.qty, 17);
    CHECK_EQ(m.gateway, 5u);
}

TEST_CASE("runner: event equality compares the header and only the active payload") {
    const Hdr h{1, 2, 3, 4};
    const EngineEvent a = evRejected(h, 9, RequestType::Cancel, RejectReason::UnknownOrder);
    EngineEvent b = a;
    // 改动非活动载荷的字节（拒单载荷只有 2 字节）：仍相等
    // Changing bytes of the inactive payload (the reject payload is only 2 bytes): still equal
    unsigned char junk[8] = {1, 2, 3, 4, 5, 6, 7, 8};
    std::memcpy(reinterpret_cast<unsigned char*>(&b) + 48, junk, sizeof(junk));
    CHECK(a == b);
    b = a;
    b.rejected.reason = RejectReason::InvalidPrice;
    CHECK(a != b);
    b = a;
    b.client_ts = 3;
    CHECK(a != b);
    b = a;
    b.gateway = 0;
    CHECK(a != b);
    const EngineEvent f1 = evFill(h, 9, Liquidity::Maker, 1, 100, 5, 0);
    EngineEvent f2 = f1;
    f2.liquidity = Liquidity::Taker;
    CHECK(f1 != f2);
    f2 = f1;
    f2.fill.remaining = 1;
    CHECK(f1 != f2);
    CHECK(evModified(h, 9, 100, 5) != evModified(h, 9, 100, 6));
    CHECK(evCancelled(h, 9, 5, CancelReason::UserRequested) != evCancelled(h, 9, 5, CancelReason::IocRemainder));
}

// ============================================================================
// 确定性路由（同步驱动）
// Deterministic routing (synchronous driver)
// ============================================================================

TEST_CASE("runner: single gateway event flow echoes sequence, timestamp and gateway") {
    EngineRunner<> runner(testConfig(1));
    runner.addSymbol("AAA");
    runner.start();
    SyncDriver<EngineRunner<>> d(runner);

    const OrderRequest s1 = mt::sell(1, 101, 5);
    Hdr h = d.submit(0, 0, s1);
    CHECK_EQ(h.seq, 1u);
    CHECK_STREAM(d.drain(0), evAccepted(h, s1));

    const OrderRequest b2 = mt::buy(2, 101, 3).account(2);
    h = d.submit(0, 0, b2);
    // 同一网关两侧：先 maker 的 Fill，后 taker 的 Fill
    // Both sides on the same gateway: the maker's Fill first, then the taker's Fill
    CHECK_STREAM(d.drain(0), evAccepted(h, b2), evFill(h, 1, Liquidity::Maker, 1, 101, 3, 2),
                 evFill(h, 2, Liquidity::Taker, 1, 101, 3, 0));

    h = d.modify(0, 0, 1, 103, 4);  // 改价：失去时间优先级，不穿价，静默挂单
                                    // reprice: loses time priority, does not cross, rests silently
    CHECK_STREAM(d.drain(0), evModified(h, 1, 103, 4));
    h = d.modify(0, 0, 1, 103, 2);  // 原地减量
                                    // in-place quantity decrease
    CHECK_STREAM(d.drain(0), evModified(h, 1, 103, 2));
    CHECK_EQ(runner.processedCommands(), 4u);

    h = d.cancel(0, 0, 1);
    CHECK_STREAM(d.drain(0), evCancelled(h, 1, 2, CancelReason::UserRequested));
    h = d.cancel(0, 0, 1);
    CHECK_STREAM(d.drain(0), evRejected(h, 1, RequestType::Cancel, RejectReason::UnknownOrder));

    const OrderRequest m3 = mt::marketBuy(3, 2).account(0);
    h = d.submit(0, 0, m3);  // 空簿上的市价单
                             // market order on an empty book
    CHECK_STREAM(d.drain(0), evAccepted(h, m3), evCancelled(h, 3, 2, CancelReason::MarketRemainder));

    const OrderRequest bad = mt::buy(4, 0, 1);  // 非法价格
                                                // invalid price
    h = d.submit(0, 0, bad);
    CHECK_STREAM(d.drain(0), evRejected(h, 4, RequestType::New, RejectReason::InvalidPrice));

    runner.stop();
    const EngineRunnerStats st = runner.stats();
    CHECK_EQ(st.commands, 8u);
    CHECK_EQ(st.events, 11u);
    CHECK_EQ(st.sequence_gaps, 0u);
    CHECK_EQ(st.foreign_rejects, 0u);
    std::string why;
    CHECK(runner.checkInvariants(&why));
    CHECK_EQ(runner.engine().book(0).orderCount(), 0u);
}

TEST_CASE("runner: trades are reported to both the maker's and the taker's gateway") {
    EngineRunner<> runner(testConfig(3));
    runner.addSymbol("AAA");
    runner.start();
    SyncDriver<EngineRunner<>> d(runner);

    const OrderRequest a = mt::sell(10, 100, 4);
    const OrderRequest b = mt::sell(11, 100, 4).account(2);
    Hdr ha = d.submit(0, 0, a);
    Hdr hb = d.submit(1, 0, b);
    CHECK_STREAM(d.drain(0), evAccepted(ha, a));
    CHECK_STREAM(d.drain(1), evAccepted(hb, b));

    const OrderRequest t = mt::buy(20, 100, 6).account(3);
    const Hdr ht = d.submit(2, 0, t);
    // maker 收到的 Fill 带 taker 命令的引擎序号、时间戳与网关
    // The Fill the maker receives carries the engine sequence number, timestamp and gateway of the taker's
    // command
    CHECK_STREAM(d.drain(0), evFill(ht, 10, Liquidity::Maker, 1, 100, 4, 0));
    CHECK_STREAM(d.drain(1), evFill(ht, 11, Liquidity::Maker, 2, 100, 2, 2));
    CHECK_STREAM(d.drain(2), evAccepted(ht, t), evFill(ht, 20, Liquidity::Taker, 1, 100, 4, 2),
                 evFill(ht, 20, Liquidity::Taker, 2, 100, 2, 0));

    runner.stop();
    CHECK(!runner.ownerOf(0, 10).has_value());  // 完全成交：移出归属表
                                                // fully filled: removed from the owner map
    CHECK(runner.ownerOf(0, 11) == std::optional<GatewayId>(1));
    CHECK(!runner.ownerOf(0, 20).has_value());  // 完全成交的 taker 从未挂单
                                                // a fully filled taker never rested
    std::string why;
    CHECK(runner.checkInvariants(&why));
}

TEST_CASE("runner: self-trade prevention cancels are routed to the maker's gateway") {
    EngineRunner<> runner(testConfig(2));
    runner.addSymbol("AAA");
    runner.start();
    SyncDriver<EngineRunner<>> d(runner);
    constexpr AccountId kShared = 7;  // 同一账户经由两个网关下单
                                      // the same account places orders through two gateways

    // CancelMaker：跨网关撤掉 maker；taker 继续撮合，无对手后挂单
    // CancelMaker: cancels the maker across gateways; the taker keeps matching and rests once no counterparty
    // is left
    const OrderRequest s1 = mt::sell(1, 100, 5).account(kShared);
    Hdr h = d.submit(0, 0, s1);
    CHECK_STREAM(d.drain(0), evAccepted(h, s1));
    const OrderRequest b2 = mt::buy(2, 100, 3).account(kShared).stp(StpMode::CancelMaker);
    h = d.submit(1, 0, b2);
    CHECK_STREAM(d.drain(0), evCancelled(h, 1, 5, CancelReason::SelfTradePrevention));
    CHECK_STREAM(d.drain(1), evAccepted(h, b2));

    // CancelBoth，maker 与 taker 同属网关 1：两条撤单都发给网关 1
    // CancelBoth with maker and taker both on gateway 1: both cancels go to gateway 1
    const OrderRequest s3 = mt::sell(3, 100, 1).account(kShared).stp(StpMode::CancelBoth);
    h = d.submit(1, 0, s3);
    CHECK_STREAM(d.drain(1), evAccepted(h, s3), evCancelled(h, 2, 3, CancelReason::SelfTradePrevention),
                 evCancelled(h, 3, 1, CancelReason::SelfTradePrevention));
    CHECK_SILENT(d.drain(0));

    // CancelBoth 跨网关：maker 的撤单发给网关 0，taker 的撤单发给网关 1
    // CancelBoth across gateways: the maker's cancel goes to gateway 0, the taker's cancel to gateway 1
    const OrderRequest s4 = mt::sell(4, 105, 2).account(kShared);
    h = d.submit(0, 0, s4);
    CHECK_STREAM(d.drain(0), evAccepted(h, s4));
    const OrderRequest b5 = mt::buy(5, 105, 1).account(kShared).stp(StpMode::CancelBoth);
    h = d.submit(1, 0, b5);
    CHECK_STREAM(d.drain(0), evCancelled(h, 4, 2, CancelReason::SelfTradePrevention));
    CHECK_STREAM(d.drain(1), evAccepted(h, b5), evCancelled(h, 5, 1, CancelReason::SelfTradePrevention));

    // CancelTaker：只有 taker 被撤，maker 原封不动、仍属网关 0
    // CancelTaker: only the taker is cancelled; the maker stays untouched and still belongs to gateway 0
    const OrderRequest s6 = mt::sell(6, 110, 2).account(kShared);
    h = d.submit(0, 0, s6);
    CHECK_STREAM(d.drain(0), evAccepted(h, s6));
    const OrderRequest b7 = mt::buy(7, 110, 1).account(kShared).stp(StpMode::CancelTaker);
    h = d.submit(1, 0, b7);
    CHECK_SILENT(d.drain(0));
    CHECK_STREAM(d.drain(1), evAccepted(h, b7), evCancelled(h, 7, 1, CancelReason::SelfTradePrevention));

    runner.stop();
    CHECK(runner.ownerOf(0, 6) == std::optional<GatewayId>(0));
    CHECK(!runner.ownerOf(0, 1).has_value());
    CHECK(!runner.ownerOf(0, 2).has_value());
    CHECK(!runner.ownerOf(0, 4).has_value());
    CHECK_EQ(runner.engine().book(0).orderCount(), 1u);
    std::string why;
    CHECK(runner.checkInvariants(&why));
}

TEST_CASE("runner: a gateway cannot cancel or modify another gateway's order") {
    EngineRunner<> runner(testConfig(2));
    runner.addSymbol("AAA");
    runner.start();
    SyncDriver<EngineRunner<>> d(runner);

    const OrderRequest b1 = mt::buy(1, 99, 5);
    Hdr h = d.submit(0, 0, b1);
    CHECK_STREAM(d.drain(0), evAccepted(h, b1));

    h = d.cancel(1, 0, 1);
    CHECK_STREAM(d.drain(1), evRejected(h, 1, RequestType::Cancel, RejectReason::UnknownOrder));
    h = d.modify(1, 0, 1, 98, 1);
    CHECK_STREAM(d.drain(1), evRejected(h, 1, RequestType::Modify, RejectReason::UnknownOrder));
    CHECK_SILENT(d.drain(0));  // 订单所有者什么都没收到
                               // the order's owner received nothing
    CHECK_EQ(runner.stats().foreign_rejects, 2u);

    // 重复 id：由引擎拒绝，回给发送方（不算会话隔离拒单）
    // Duplicate id: rejected by the engine and returned to the sender (not counted as a session isolation
    // reject)
    const OrderRequest dup = mt::buy(1, 98, 1);
    h = d.submit(1, 0, dup);
    CHECK_STREAM(d.drain(1), evRejected(h, 1, RequestType::New, RejectReason::DuplicateOrderId));
    CHECK_EQ(runner.stats().foreign_rejects, 2u);

    // 所有者自己可以改单 / 撤单
    // The owner itself can modify / cancel
    h = d.modify(0, 0, 1, 99, 4);
    CHECK_STREAM(d.drain(0), evModified(h, 1, 99, 4));
    h = d.cancel(0, 0, 1);
    CHECK_STREAM(d.drain(0), evCancelled(h, 1, 4, CancelReason::UserRequested));

    // id 释放后被网关 1 复用：现在轮到网关 0 动不了它
    // After the id is released, gateway 1 reuses it: now it is gateway 0 that cannot touch it
    const OrderRequest s1 = mt::sell(1, 105, 2);
    h = d.submit(1, 0, s1);
    CHECK_STREAM(d.drain(1), evAccepted(h, s1));
    h = d.cancel(0, 0, 1);
    CHECK_STREAM(d.drain(0), evRejected(h, 1, RequestType::Cancel, RejectReason::UnknownOrder));
    h = d.cancel(1, 0, 1);
    CHECK_STREAM(d.drain(1), evCancelled(h, 1, 2, CancelReason::UserRequested));

    // 不存在的订单：引擎自己的 UnknownOrder（不计入会话隔离）
    // Nonexistent order: the engine's own UnknownOrder (not counted as session isolation)
    h = d.cancel(0, 0, 12345);
    CHECK_STREAM(d.drain(0), evRejected(h, 12345, RequestType::Cancel, RejectReason::UnknownOrder));
    runner.stop();
    CHECK_EQ(runner.stats().foreign_rejects, 3u);
    std::string why;
    CHECK(runner.checkInvariants(&why));
}

TEST_CASE("runner: a crossing modify re-matches as taker and keeps ownership consistent") {
    EngineRunner<> runner(testConfig(2));
    runner.addSymbol("AAA");
    runner.start();
    SyncDriver<EngineRunner<>> d(runner);

    const OrderRequest b1 = mt::buy(1, 99, 5);
    const OrderRequest s2 = mt::sell(2, 101, 3).account(2);
    d.submit(0, 0, b1);
    d.submit(1, 0, s2);
    d.drain(0);
    d.drain(1);

    Hdr h = d.modify(0, 0, 1, 101, 5);  // 改价穿价：作为 taker 成交 3，剩余 2 挂在 101
                                        // crossing reprice: fills 3 as taker, the remaining 2 rest at 101
    CHECK_STREAM(d.drain(0), evModified(h, 1, 101, 5), evFill(h, 1, Liquidity::Taker, 1, 101, 3, 2));
    CHECK_STREAM(d.drain(1), evFill(h, 2, Liquidity::Maker, 1, 101, 3, 0));

    const OrderRequest s3 = mt::sell(3, 101, 2).account(2);
    h = d.submit(1, 0, s3);  // 吃掉改单后挂着的剩余
                             // takes the remainder left resting after the modify
    CHECK_STREAM(d.drain(0), evFill(h, 1, Liquidity::Maker, 2, 101, 2, 0));
    CHECK_STREAM(d.drain(1), evAccepted(h, s3), evFill(h, 3, Liquidity::Taker, 2, 101, 2, 0));

    h = d.modify(0, 0, 1, 101, 1);  // 已完全成交：引擎的 UnknownOrder
                                    // already fully filled: the engine's UnknownOrder
    CHECK_STREAM(d.drain(0), evRejected(h, 1, RequestType::Modify, RejectReason::UnknownOrder));

    // 改单后作为 taker 完全成交：订单离开订单簿，归属同步删除
    // Fully filled as a taker after a modify: the order leaves the order book, and its ownership is deleted
    // along with it
    const OrderRequest s4 = mt::sell(4, 102, 1).account(2);
    const OrderRequest b5 = mt::buy(5, 100, 1);
    d.submit(1, 0, s4);
    d.submit(0, 0, b5);
    d.drain(0);
    d.drain(1);
    h = d.modify(0, 0, 5, 102, 1);
    CHECK_STREAM(d.drain(0), evModified(h, 5, 102, 1), evFill(h, 5, Liquidity::Taker, 3, 102, 1, 0));
    CHECK_STREAM(d.drain(1), evFill(h, 4, Liquidity::Maker, 3, 102, 1, 0));

    // 改单后被 STP 撤销（CancelTaker）：订单同样离开订单簿
    // Cancelled by STP (CancelTaker) after a modify: the order leaves the order book as well
    const OrderRequest s6 = mt::sell(6, 104, 1).account(9);
    const OrderRequest b7 = mt::buy(7, 100, 2).account(9).stp(StpMode::CancelTaker);
    d.submit(1, 0, s6);
    d.submit(0, 0, b7);
    d.drain(0);
    d.drain(1);
    h = d.modify(0, 0, 7, 104, 2);
    CHECK_STREAM(d.drain(0), evModified(h, 7, 104, 2), evCancelled(h, 7, 2, CancelReason::SelfTradePrevention));
    CHECK_SILENT(d.drain(1));

    runner.stop();
    CHECK(!runner.ownerOf(0, 1).has_value());
    CHECK(!runner.ownerOf(0, 5).has_value());
    CHECK(!runner.ownerOf(0, 7).has_value());
    CHECK(runner.ownerOf(0, 6) == std::optional<GatewayId>(1));
    std::string why;
    CHECK(runner.checkInvariants(&why));
}

TEST_CASE("runner: requests that would push a level total past INT64_MAX are rejected to their sender") {
    // 一个网关的合法大单不能让价位总量溢出、进而让其他网关的 FOK 被误撤
    // A valid large order from one gateway must not overflow the level total and thereby cause another
    // gateway's FOK to be cancelled by mistake
    EngineRunner<> runner(testConfig(2));
    runner.addSymbol("AAA", 0, 64);
    runner.start();
    SyncDriver<EngineRunner<>> d(runner);
    constexpr Qty kP62 = Qty{1} << 62;

    const OrderRequest s1 = mt::sell(1, 100, kP62);
    Hdr h = d.submit(0, 0, s1);
    CHECK_STREAM(d.drain(0), evAccepted(h, s1));
    h = d.submit(0, 0, mt::sell(2, 100, kP62).account(2));
    CHECK_STREAM(d.drain(0), evRejected(h, 2, RequestType::New, RejectReason::InvalidQuantity));

    // 网关 1 的 FOK（不启用 STP 与 CancelMaker）照常成交；maker 一侧的 Fill 发给网关 0
    // Gateway 1's FOK orders (one without STP, one with CancelMaker) fill as usual; the maker-side Fills go
    // to gateway 0
    const OrderRequest f3 = mt::buy(3, 100, 10).account(9).fok();
    h = d.submit(1, 0, f3);
    CHECK_STREAM(d.drain(0), evFill(h, 1, Liquidity::Maker, 1, 100, 10, kP62 - 10));
    CHECK_STREAM(d.drain(1), evAccepted(h, f3), evFill(h, 3, Liquidity::Taker, 1, 100, 10, 0));
    const OrderRequest f4 = mt::buy(4, 100, 10).account(9).stp(StpMode::CancelMaker).fok();
    h = d.submit(1, 0, f4);
    CHECK_STREAM(d.drain(0), evFill(h, 1, Liquidity::Maker, 2, 100, 10, kP62 - 20));
    CHECK_STREAM(d.drain(1), evAccepted(h, f4), evFill(h, 4, Liquidity::Taker, 2, 100, 10, 0));

    // 改单加量同样受限：被拒的改单不改变订单与归属；恰好到上限的改单生效
    // Quantity-increasing modifies are bounded too: a rejected modify changes neither the order nor its
    // ownership; a modify that lands exactly on the bound takes effect
    const OrderRequest s5 = mt::sell(5, 100, 1);
    h = d.submit(0, 0, s5);
    CHECK_STREAM(d.drain(0), evAccepted(h, s5));
    h = d.modify(0, 0, 5, 100, kP62 + 20);
    CHECK_STREAM(d.drain(0), evRejected(h, 5, RequestType::Modify, RejectReason::InvalidQuantity));
    h = d.modify(0, 0, 5, 100, kP62 + 19);
    CHECK_STREAM(d.drain(0), evModified(h, 5, 100, kP62 + 19));
    CHECK_SILENT(d.drain(1));

    runner.stop();
    CHECK_EQ(runner.engine().book(0).volumeAt(Side::Sell, 100), kMaxLevelQty);
    CHECK(runner.ownerOf(0, 1) == std::optional<GatewayId>(0));
    CHECK(runner.ownerOf(0, 5) == std::optional<GatewayId>(0));
    CHECK(!runner.ownerOf(0, 2).has_value());
    std::string why;
    CHECK(runner.checkInvariants(&why));
}

TEST_CASE("runner: multi-symbol routing keeps books and owners separate") {
    EngineRunner<> runner(testConfig(3));
    const SymbolId A = runner.addSymbol("AAA");
    const SymbolId B = runner.addSymbol("BBB");
    CHECK_EQ(A, 0u);
    CHECK_EQ(B, 1u);
    runner.start();
    SyncDriver<EngineRunner<>> d(runner);

    // 同一个订单 id 在两个交易对上分属不同网关
    // The same order id belongs to different gateways on two symbols
    const OrderRequest a1 = mt::sell(1, 100, 5);
    const OrderRequest b1 = mt::sell(1, 200, 5);
    Hdr h = d.submit(0, A, a1);
    CHECK_STREAM(d.drain(0), evAccepted(h, a1));
    h = d.submit(1, B, b1);
    CHECK_STREAM(d.drain(1), evAccepted(h, b1));

    const OrderRequest ta = mt::buy(7, 100, 5);
    h = d.submit(2, A, ta);
    CHECK_STREAM(d.drain(0), evFill(h, 1, Liquidity::Maker, 1, 100, 5, 0));
    CHECK_SILENT(d.drain(1));
    CHECK_STREAM(d.drain(2), evAccepted(h, ta), evFill(h, 7, Liquidity::Taker, 1, 100, 5, 0));

    const OrderRequest tb = mt::buy(7, 200, 3);
    h = d.submit(2, B, tb);  // 成交 id 按交易对各自从 1 开始
                             // trade ids start from 1 separately per symbol
    CHECK_SILENT(d.drain(0));
    CHECK_STREAM(d.drain(1), evFill(h, 1, Liquidity::Maker, 1, 200, 3, 2));
    CHECK_STREAM(d.drain(2), evAccepted(h, tb), evFill(h, 7, Liquidity::Taker, 1, 200, 3, 0));

    h = d.cancel(0, B, 1);  // B 上的 1 属于网关 1
                            // 1 on B belongs to gateway 1
    CHECK_STREAM(d.drain(0), evRejected(h, 1, RequestType::Cancel, RejectReason::UnknownOrder));
    h = d.cancel(0, A, 1);  // A 上的 1 已成交
                            // 1 on A has already been filled
    CHECK_STREAM(d.drain(0), evRejected(h, 1, RequestType::Cancel, RejectReason::UnknownOrder));
    h = d.cancel(1, B, 1);
    CHECK_STREAM(d.drain(1), evCancelled(h, 1, 2, CancelReason::UserRequested));

    runner.stop();
    CHECK_EQ(runner.stats().foreign_rejects, 1u);
    CHECK_EQ(runner.engine().book(A).lastTradeId(), 1u);
    CHECK_EQ(runner.engine().book(B).lastTradeId(), 1u);
    std::string why;
    CHECK(runner.checkInvariants(&why));
}

TEST_CASE("runner: unknown symbols are rejected to the sender without side effects") {
    EngineRunner<> runner(testConfig(2));
    runner.addSymbol("AAA");
    runner.addSymbol("BBB");
    runner.start();
    SyncDriver<EngineRunner<>> d(runner);
    const OrderRequest r = mt::buy(1, 100, 1);
    Hdr h = d.submit(1, 2, r);
    CHECK_STREAM(d.drain(1), evRejected(h, 1, RequestType::New, RejectReason::UnknownSymbol));
    h = d.cancel(1, 7, 1);
    CHECK_STREAM(d.drain(1), evRejected(h, 1, RequestType::Cancel, RejectReason::UnknownSymbol));
    h = d.modify(0, 0xFFFF'FFFFu, 1, 100, 1);
    CHECK_STREAM(d.drain(0), evRejected(h, 1, RequestType::Modify, RejectReason::UnknownSymbol));
    CHECK_EQ(h.sym, 0xFFFF'FFFFu);  // 事件回显命令里的交易对 id
                                    // the event echoes the symbol id from the command
    runner.stop();
    CHECK(!runner.ownerOf(2, 1).has_value());
    CHECK(!runner.ownerOf(0xFFFF'FFFFu, 1).has_value());
    CHECK_EQ(runner.engine().book(0).orderCount(), 0u);
    CHECK_EQ(runner.engine().book(1).orderCount(), 0u);
    std::string why;
    CHECK(runner.checkInvariants(&why));
}

TEST_CASE("runner: invalid command types are rejected as InvalidFlags") {
    EngineRunner<> runner(testConfig(1));
    runner.addSymbol("AAA");
    runner.start();
    SyncDriver<EngineRunner<>> d(runner);
    EngineCommand c = EngineCommand::cancel(0, 0, 0, 42);
    c.type = static_cast<RequestType>(9);
    const Hdr h = d.send(0, c);
    CHECK_STREAM(d.drain(0), evRejected(h, 42, static_cast<RequestType>(9), RejectReason::InvalidFlags));
    runner.stop();
}

TEST_CASE("runner: the inbound queue index is the authoritative gateway") {
    std::vector<JournalEntry> journal;
    EngineRunner<RecordingJournal> runner(testConfig(2), RecordingJournal{&journal});
    runner.addSymbol("AAA");
    runner.start();
    const OrderRequest r = mt::buy(1, 100, 1);
    EngineCommand c = EngineCommand::newOrder(0, 1, 0, r, 77);  // 声称来自网关 0，实际经由网关 1 的队列
                                                                // claims to be from gateway 0, but actually
                                                                // arrives through gateway 1's queue
    while (!runner.inbound(1).tryPush(c)) std::this_thread::yield();
    while (runner.processedCommands() < 1) std::this_thread::yield();
    EngineEvent e{};
    CHECK(!runner.outbound(0).tryPop(e));
    REQUIRE(runner.outbound(1).tryPop(e));
    CHECK_EQ(e.gateway, GatewayId{1});
    CHECK_EQ(e.client_ts, 77u);
    runner.stop();
    CHECK(runner.ownerOf(0, 1) == std::optional<GatewayId>(1));
    REQUIRE_EQ(journal.size(), 1u);
    CHECK_EQ(journal[0].gateway, GatewayId{1});         // 日志记录权威发送方
                                                        // the journal records the authoritative sender
    CHECK_EQ(journal[0].command.gateway, GatewayId{0});  // 命令内容原样保留
                                                         // the command content is kept as is
    CHECK_EQ(runner.stats().sequence_gaps, 0u);          // 序号按入站队列（网关 1）计
                                                         // sequence numbers count per inbound queue (gateway 1)
}

TEST_CASE("runner: gateway sequence gaps are counted but commands are still processed") {
    EngineRunner<> runner(testConfig(2));
    runner.addSymbol("AAA");
    runner.start();
    const std::uint64_t seqs[] = {1, 2, 4, 5, 5, 6};  // 3 缺失，5 重复
                                                      // 3 missing, 5 duplicated
    std::uint64_t n = 0;
    for (std::uint64_t s : seqs) {
        EngineCommand c = EngineCommand::cancel(0, s, 0, 1000 + s);
        while (!runner.inbound(0).tryPush(c)) std::this_thread::yield();
        ++n;
    }
    EngineCommand other = EngineCommand::cancel(1, 1, 0, 1);  // 另一个网关独立计数
                                                              // another gateway counts independently
    while (!runner.inbound(1).tryPush(other)) std::this_thread::yield();
    ++n;
    while (runner.processedCommands() < n) std::this_thread::yield();
    runner.stop();
    CHECK_EQ(runner.stats().sequence_gaps, 2u);
    CHECK_EQ(runner.stats().commands, 7u);
    std::size_t events = 0;
    EngineEvent e{};
    while (runner.outbound(0).tryPop(e)) ++events;
    CHECK_EQ(events, 6u);
}

// ============================================================================
// 生命周期：stop() 排空、空闲停止、重启、析构即停止、配置校验、绑核
// Lifecycle: stop() draining, idle stop, restart, stop on destruction, configuration validation, CPU pinning
// ============================================================================

TEST_CASE("runner: stop() drains every inbound queue before the engine exits") {
    Watchdog watchdog("stop drains", watchdogLimit());
    constexpr std::size_t kGateways = 3;
    constexpr std::size_t kPerGateway = 3000;
    EngineRunner<> runner(testConfig(kGateways, 4096, 4096, 5));
    runner.addSymbol("AAA", 0, 1 << 14);
    // 引擎启动前就把命令全部入队（全是买单、互不成交，每条恰好产生一个 Accepted）
    // Enqueue every command before the engine starts (all buys that do not trade with each other, each
    // producing exactly one Accepted)
    for (std::size_t g = 0; g < kGateways; ++g) {
        for (std::size_t i = 0; i < kPerGateway; ++i) {
            const OrderId id = (g + 1) * 100'000 + i;
            const OrderRequest r = mt::buy(id, 1000 + static_cast<Price>(i % 50), 1);
            REQUIRE(runner.inbound(static_cast<GatewayId>(g))
                        .tryPush(EngineCommand::newOrder(static_cast<GatewayId>(g), i + 1, 0, r, tokenOf(g, i))));
        }
    }
    runner.start();
    runner.stop();  // 引擎线程很可能在处理第一条命令之前就看到了停止标志
                    // the engine thread most likely sees the stop flag before processing the first command
    CHECK(!runner.running());
    const EngineRunnerStats st = runner.stats();
    CHECK_EQ(st.commands, kGateways * kPerGateway);
    CHECK_EQ(st.events, kGateways * kPerGateway);
    for (std::size_t g = 0; g < kGateways; ++g) {
        auto& q = runner.outbound(static_cast<GatewayId>(g));
        std::size_t i = 0;
        std::uint64_t last_seq = 0;
        EngineEvent e{};
        while (q.tryPop(e)) {
            CHECK_EQ(e.type, EventType::Accepted);
            CHECK_EQ(e.client_ts, tokenOf(g, i));
            CHECK_GT(e.engine_seq, last_seq);
            last_seq = e.engine_seq;
            ++i;
        }
        CHECK_EQ(i, kPerGateway);
    }
    CHECK_EQ(runner.engine().book(0).orderCount(), kGateways * kPerGateway);
    std::string why;
    CHECK(runner.checkInvariants(&why));
    runner.stop();  // 幂等
                    // idempotent
}

TEST_CASE("runner: stop() while gateways are still producing drains what was submitted") {
    Watchdog watchdog("stop while producing", watchdogLimit());
    std::vector<GatewayScript> scripts(2);
    ScriptOptions so;
    so.commands = scaled(20000);
    for (std::size_t g = 0; g < scripts.size(); ++g) {
        so.seed = 11 + g;
        scripts[g].commands = makeScript(so, static_cast<GatewayId>(g));
    }
    // 大入站队列：网关很快就全部入队，引擎多半还积压着大量命令时 stop() 就被调用了
    // Large inbound queues: the gateways enqueue everything quickly, so stop() gets called while the engine
    // most likely still has a large backlog of commands
    EngineRunner<> runner(testConfig(2, 1 << 15, 256, 16));
    runner.addSymbol(symbolName(0), 0, 1 << 14);
    driveConcurrently(runner, scripts, DriveOptions{});
    CHECK_EQ(runner.stats().commands, 2 * so.commands);
    std::string why;
    CHECK(runner.checkInvariants(&why));
}

TEST_CASE("runner: stop with idle queues, restart, and engine sequence continuity") {
    EngineRunner<> runner(testConfig(1));
    runner.addSymbol("AAA");
    runner.start();
    CHECK(runner.running());
    CHECK_THROWS(runner.start());  // 已在运行
                                   // already running
    CHECK_THROWS(runner.addSymbol("BBB"));
    runner.stop();
    CHECK(!runner.running());
    CHECK_EQ(runner.stats().commands, 0u);

    runner.start();
    SyncDriver<EngineRunner<>> d(runner);
    const OrderRequest b1 = mt::buy(1, 100, 3);
    Hdr h = d.submit(0, 0, b1);
    CHECK_STREAM(d.drain(0), evAccepted(h, b1));
    runner.stop();

    runner.addSymbol("BBB");  // 停止期间可以继续添加交易对
                              // symbols can still be added while stopped
    runner.start();
    const OrderRequest s2 = mt::sell(2, 100, 1);
    h = d.submit(0, 0, s2);  // 状态与引擎序号都延续
                             // both the state and the engine sequence number carry over
    CHECK_EQ(h.seq, 2u);
    CHECK_STREAM(d.drain(0), evAccepted(h, s2), evFill(h, 1, Liquidity::Maker, 1, 100, 1, 2),
                 evFill(h, 2, Liquidity::Taker, 1, 100, 1, 0));
    const OrderRequest b3 = mt::buy(1, 50, 1);
    h = d.submit(0, 1, b3);
    CHECK_STREAM(d.drain(0), evAccepted(h, b3));
    runner.stop();
    CHECK_EQ(runner.symbolCount(), 2u);
    std::string why;
    CHECK(runner.checkInvariants(&why));
}

TEST_CASE("runner: destroying a running runner drains pending commands") {
    std::vector<JournalEntry> journal;
    {
        EngineRunner<RecordingJournal> runner(testConfig(1, 1024, 2048), RecordingJournal{&journal});
        runner.addSymbol("AAA");
        for (std::size_t i = 0; i < 500; ++i) {
            const OrderRequest r = mt::sell(i + 1, 200 + static_cast<Price>(i % 7), 1);
            REQUIRE(runner.inbound(0).tryPush(EngineCommand::newOrder(0, i + 1, 0, r)));
        }
        runner.start();
    }  // 析构 = stop()：排空后才退出
       // destruction = stop(): exits only after draining
    REQUIRE_EQ(journal.size(), 500u);
    for (std::size_t i = 0; i < journal.size(); ++i) CHECK_EQ(journal[i].engine_seq, i + 1);
}

TEST_CASE("runner: configuration is validated") {
    EngineRunnerConfig c;
    c.gateways = 0;
    CHECK_THROWS(EngineRunner<>(c));
    c.gateways = kMaxGateways + 1;
    CHECK_THROWS(EngineRunner<>(c));
    c.gateways = 2;
    c.batch = 0;
    CHECK_THROWS(EngineRunner<>(c));
    c.batch = 1;
    EngineRunner<> runner(c);
    CHECK_EQ(runner.gatewayCount(), 2u);
    CHECK_THROWS(runner.inbound(2));
    CHECK_THROWS(runner.outbound(5));
    runner.addSymbol("AAA");
    CHECK_THROWS(runner.addSymbol("AAA"));  // 重复名称：运行器不变
                                            // duplicate name: the runner is unchanged
    CHECK_EQ(runner.symbolCount(), 1u);
    CHECK_EQ(runner.engine().symbolCount(), 1u);
    std::string why;
    CHECK(runner.checkInvariants(&why));
    CHECK_GE(runner.inbound(0).capacity(), c.inbound_capacity);
    CHECK_GE(runner.outbound(1).capacity(), c.outbound_capacity);
}

TEST_CASE("runner: optional CPU pinning of the engine thread") {
    {
        EngineRunnerConfig c = testConfig(1);
        c.engine_cpu = static_cast<int>(firstAllowedCpu());
        EngineRunner<> runner(c);
        runner.addSymbol("AAA");
        runner.start();
        SyncDriver<EngineRunner<>> d(runner);
        d.submit(0, 0, mt::buy(1, 10, 1));
        CHECK(runner.pinned());  // 收到事件之后读取：绑核结果对本线程可见
                                 // read after receiving the event: the pinning result is visible to this thread
        runner.stop();
    }
    {
        EngineRunnerConfig c = testConfig(1);
        c.engine_cpu = 1 << 20;  // 不存在的 CPU：绑核失败，但运行器照常工作
                                 // nonexistent CPU: pinning fails, but the runner works as usual
        EngineRunner<> runner(c);
        runner.addSymbol("AAA");
        runner.start();
        SyncDriver<EngineRunner<>> d(runner);
        const OrderRequest r = mt::buy(1, 10, 1);
        const Hdr h = d.submit(0, 0, r);
        CHECK_STREAM(d.drain(0), evAccepted(h, r));
        runner.stop();
        CHECK(!runner.pinned());
    }
}

// ============================================================================
// 零分配：预留容量之后，引擎线程处理命令（含成交、撤单、改单、拒单）不调用全局 operator new
// Zero allocations: once capacity is reserved, the engine thread processes commands (including fills,
// cancels, modifies and rejects) without calling the global operator new
// ============================================================================

TEST_CASE("runner: the engine thread does not allocate once capacity is reserved") {
    Watchdog watchdog("zero allocation", watchdogLimit());
    const std::size_t total = scaled(30000);
    const std::size_t warmup = total / 10;
    std::vector<EngineCommand> cmds;
    cmds.reserve(total);
    {
        mt::RandomCommandGenerator gen(2024);
        for (std::size_t i = 0; i < total; ++i) {
            const mt::Command c = gen.next();
            const std::uint64_t seq = i + 1;
            switch (c.type) {
                case RequestType::New: cmds.push_back(EngineCommand::newOrder(0, seq, 0, c.order, seq)); break;
                case RequestType::Cancel: cmds.push_back(EngineCommand::cancel(0, seq, 0, c.id, seq)); break;
                default: cmds.push_back(EngineCommand::modify(0, seq, 0, c.id, c.price, c.qty, seq)); break;
            }
        }
    }
    EngineRunner<> runner(testConfig(1, 1024, 1 << 12, 32));
    runner.addSymbol("AAA", 0, 1 << 16);
    runner.start();
    std::uint64_t events = 0;
    std::uint64_t fills = 0;
    auto drain = [&] {
        auto& out = runner.outbound(0);
        while (const EngineEvent* e = out.front()) {
            ++events;
            if (e->type == EventType::Fill) ++fills;
            out.pop();
        }
    };
    auto pump = [&](std::size_t from, std::size_t to) {
        std::uint32_t spins = 0;
        for (std::size_t i = from; i < to; ++i) {
            while (!runner.inbound(0).tryPush(cmds[i])) {
                drain();
                spinPause(spins);
            }
        }
        while (runner.processedCommands() < to) {
            drain();
            spinPause(spins);
        }
        drain();
    };
    pump(0, warmup);
    const std::uint64_t before = heapAllocations();
    pump(warmup, total);
    const std::uint64_t allocations = heapAllocations() - before;
    runner.stop();
    CHECK_EQ(allocations, 0u);
    CHECK_EQ(runner.stats().commands, total);
    CHECK_EQ(runner.stats().events, events);
    CHECK_GT(fills, 100u);  // 负载里确实有成交
                            // the workload really contains trades
    std::string why;
    CHECK(runner.checkInvariants(&why));
}

// ============================================================================
// 并发差分：G 个网关线程并发 → 引擎处理顺序 → 单线程回放，逐网关事件流逐条相等
// Concurrent differential: G concurrent gateway threads → engine processing order → single-threaded
// replay, with per-gateway event streams equal event by event
// ============================================================================

namespace {

const Scenario kScenarios[] = {
    // name, G, S, cmds/gw, in, out, batch, colliding ids, slow consumer, unknown symbol, invalid type
    {"4 gateways, 3 symbols, colliding ids, batch 1, tiny queues", 4, 3, 15000, 4, 4, 1, true, false, 0.01, 0.002},
    {"4 gateways, 2 symbols, disjoint ids, batch 64, large queues", 4, 2, 20000, 4096, 8192, 64, false, false, 0.0,
     0.0},
    {"2 gateways, 1 symbol, minimum queues, batch 7, slow consumers", 2, 1, 6000, 2, 2, 7, true, true, 0.005, 0.0},
    {"8 gateways, 2 symbols, colliding ids, batch 16", 8, 2, 5000, 64, 64, 16, true, false, 0.005, 0.001},
    {"1 gateway, 2 symbols, batch 3", 1, 2, 20000, 16, 32, 3, true, false, 0.01, 0.001},
};

}  // namespace

TEST_CASE("runner: concurrent gateways match a single-threaded replay of the engine's order") {
    std::uint64_t trades = 0;
    std::uint64_t foreign = 0;
    std::uint64_t outbound_full = 0;
    for (const Scenario& sc : kScenarios) {
        for (std::uint64_t seed : {1u, 2u}) {
            const ScenarioResult r = runScenario(sc, seed);
            trades += r.trades;
            foreign += r.foreign;
            outbound_full += r.stats.outbound_full;
            if (::hft_test::currentFailures() > 0) {
                std::printf("    failed scenario: %s (seed %llu)\n", sc.name, static_cast<unsigned long long>(seed));
                return;
            }
        }
    }
    // 负载确实覆盖了成交、跨网关会话隔离与背压
    // The workload really covered trades, cross-gateway session isolation and back-pressure
    CHECK_GT(trades, 1000u);
    CHECK_GT(foreign, 10u);
    CHECK_GT(outbound_full, 0u);
    std::printf("    replayed: %llu trades, %llu foreign cancel/modify rejects, %llu outbound-full stalls\n",
                static_cast<unsigned long long>(trades), static_cast<unsigned long long>(foreign),
                static_cast<unsigned long long>(outbound_full));
}

TEST_CASE("runner: back-pressure with a tiny outbound queue loses nothing and does not deadlock") {
    // 出站容量 2 + 慢消费者 + 极小入站队列：引擎经常因出站满而自旋，网关经常因入站满而等待
    // Outbound capacity 2 + slow consumers + tiny inbound queues: the engine often spins because outbound is
    // full, and the gateways often wait because inbound is full
    const Scenario sc{"back-pressure", 3, 1, 4000, 2, 2, 4, true, true, 0.0, 0.0};
    const ScenarioResult r = runScenario(sc, 99);
    CHECK_GT(r.stats.outbound_full, 0u);
    CHECK_GT(r.total_events, r.stats.commands);  // 有成交：事件数多于命令数
                                                 // there are trades: more events than commands
}

int main(int argc, char** argv) { return hft_test::runAll(argc, argv); }
