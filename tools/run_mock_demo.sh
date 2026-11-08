#!/usr/bin/env bash
#
# run_mock_demo.sh
# 离线端到端演示：后台启动模拟交易所（build/mock_exchange），等它三个端口都开始监听后，用 tools/mock_config.json
# 运行监控程序（build/crypto_hft_engine）一段时间，结束后停止模拟交易所并打印它的汇总。
# Offline end-to-end demo: start the mock exchange (build/mock_exchange) in the background, wait until all
# three of its ports are listening, run the monitor (build/crypto_hft_engine) with tools/mock_config.json
# for a while, then stop the mock exchange and print its summary.
#
# 用法（任意目录均可，脚本会切换到仓库根目录）：
# Usage (from any directory; the script changes to the repository root):
#   tools/run_mock_demo.sh [SECONDS] [--headless | --tui] [--fee-rate F] [--seed N] [--drop-rate P] [--build-dir DIR]
# 示例：
# Examples:
#   tools/run_mock_demo.sh 60                    # 交互终端里显示 TUI，运行 60 s
#                                                  TUI in an interactive terminal, runs for 60 s
#   tools/run_mock_demo.sh 30 --fee-rate 0.001   # 收 0.1% 手续费：只有大错位时才出现套利信号
#                                                  0.1% fee: arbitrage signals only on large dislocations
#   tools/run_mock_demo.sh 20 --headless         # 无 TUI，每秒一行状态（stdout 不是终端时自动启用）
#                                                  no TUI, one status line per second
#                                                  (automatic when stdout is not a terminal)
#   tools/run_mock_demo.sh 30 --drop-rate 0.02   # 故障注入：随机丢推送，监控程序显示缺口与重同步；
#                                                  持续丢包时 Binance 偶尔重连（每会话的 REST 快照额度用完）
#                                                  fault injection: drops random pushes, the monitor shows
#                                                  gaps and resyncs; with sustained drops Binance sometimes
#                                                  reconnects (its per-session REST snapshot budget runs out)
# Ctrl+C：监控程序与模拟交易所都收到 SIGINT，各自正常退出并打印汇总。
# Ctrl+C: the monitor and the mock exchange both get SIGINT, exit normally and print their summaries.
#

set -euo pipefail

# 端口与 tools/mock_config.json 一致（修改其一时两边一起改）
# The ports match tools/mock_config.json (change both together)
readonly BINANCE_PORT=19443
readonly OKX_PORT=18443
readonly BYBIT_PORT=18444

usage() {
    cat <<'EOF'
Usage: tools/run_mock_demo.sh [SECONDS] [options]
Runs the arbitrage monitor end-to-end against the offline mock exchange: starts build/mock_exchange
in the background, waits until it listens, runs build/crypto_hft_engine --config tools/mock_config.json
for SECONDS, then stops the mock exchange and prints its summary.

  SECONDS          how long the monitor runs (default 30)
  --headless       run the monitor without the TUI (default when stdout is not a terminal)
  --tui            force the TUI even when stdout is not a terminal
  --fee-rate F     use a copy of the config with arbitrage.fee_rate = F (shipped: 0.0;
                   with 0.001 signals only appear during the occasional large dislocations)
  --seed N         mock exchange RNG seed (default 1)
  --drop-rate P    mock exchange fault injection: withhold each pushed message from a session with
                   probability P, so the monitor sees gaps and resyncs (default 0). Sustained loss
                   also makes Binance reconnect now and then: each gap costs a REST snapshot, and a
                   session may fetch at most 5 unless its book stays in sync for 30 s in between
  --build-dir DIR  directory with mock_exchange and crypto_hft_engine (default build)
  --help           show this help
EOF
}

duration=30
headless=auto
fee_rate=""
seed=1
drop_rate=0
build_dir=build
while [[ $# -gt 0 ]]; do
    case "$1" in
        --headless) headless=yes ;;
        --tui) headless=no ;;
        --fee-rate) fee_rate="${2:?--fee-rate needs a value}"; shift ;;
        --seed) seed="${2:?--seed needs a value}"; shift ;;
        --drop-rate) drop_rate="${2:?--drop-rate needs a value}"; shift ;;
        --build-dir) build_dir="${2:?--build-dir needs a value}"; shift ;;
        -h|--help) usage; exit 0 ;;
        [0-9]*) duration="$1" ;;
        *) echo "run_mock_demo: unknown argument '$1' (try --help)" >&2; exit 2 ;;
    esac
    shift
done
if ! [[ "$duration" =~ ^[0-9]+$ ]] || [[ "$duration" -lt 1 ]]; then
    echo "run_mock_demo: SECONDS must be a positive integer" >&2
    exit 2
fi
if [[ -n "$fee_rate" ]] && ! [[ "$fee_rate" =~ ^([0-9]+\.?[0-9]*|\.[0-9]+)([eE][-+]?[0-9]+)?$ ]]; then
    echo "run_mock_demo: --fee-rate must be a non-negative number" >&2
    exit 2
fi
if ! [[ "$drop_rate" =~ ^(0|1|0?\.[0-9]+|1\.0*)$ ]]; then
    echo "run_mock_demo: --drop-rate must be a probability in [0, 1]" >&2
    exit 2
fi
if ! [[ "$seed" =~ ^[0-9]+$ ]]; then
    echo "run_mock_demo: --seed must be a non-negative integer" >&2
    exit 2
fi

# 监控程序按当前目录解析相对路径（配置、logs/）：切到仓库根目录
# The monitor resolves relative paths (config, logs/) against the current directory: change to the
# repository root
cd "$(dirname "${BASH_SOURCE[0]}")/.."
mock_bin="$build_dir/mock_exchange"
monitor_bin="$build_dir/crypto_hft_engine"
for bin in "$mock_bin" "$monitor_bin"; do
    if [[ ! -x "$bin" ]]; then
        echo "run_mock_demo: $bin not found; build first: cmake -S . -B $build_dir -G Ninja && cmake --build $build_dir -j2" >&2
        exit 1
    fi
done

config=tools/mock_config.json
# CA 证书路径以配置文件的 connection.ca_file 为准：模拟交易所把证书写到这里，监控程序从这里加载
# The CA certificate path comes from the config's connection.ca_file: the mock exchange writes the
# certificate there and the monitor loads it from there
ca_file=$(sed -n 's/.*"ca_file"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "$config")

tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/mock_demo.XXXXXX")
if [[ -n "$fee_rate" ]]; then
    sed -E 's/("fee_rate"[[:space:]]*:[[:space:]]*)[0-9.eE+-]+/\1'"$fee_rate"'/' "$config" > "$tmp_dir/config.json"
    config="$tmp_dir/config.json"
fi

mkdir -p logs
mock_log=logs/mock_exchange.log
mock_pid=""
interrupted=no

stop_mock() {
    if [[ -n "$mock_pid" ]] && kill -0 "$mock_pid" 2>/dev/null; then
        kill -INT "$mock_pid" 2>/dev/null || true
        for _ in $(seq 1 50); do
            kill -0 "$mock_pid" 2>/dev/null || break
            sleep 0.1
        done
        kill -KILL "$mock_pid" 2>/dev/null || true
    fi
    if [[ -n "$mock_pid" ]]; then
        wait "$mock_pid" 2>/dev/null || true
    fi
    mock_pid=""
}

cleanup() {
    stop_mock
    rm -rf "$tmp_dir"
}
trap cleanup EXIT
# Ctrl+C 同时发给前台的监控程序（它自己正常退出）；脚本只记下来，随后照常收尾
# Ctrl+C also reaches the monitor in the foreground (it exits normally by itself); the script only
# records it and then cleans up as usual
trap 'interrupted=yes' INT TERM

# 1. 后台启动模拟交易所（比监控程序多运行一段时间，正常情况下由本脚本结束它）。
#    它自己安装 SIGINT/SIGTERM 处理函数，所以后台任务默认忽略 SIGINT 的继承设置不影响 stop_mock。
# 1. Start the mock exchange in the background (it runs longer than the monitor; normally this script ends it).
#    It installs its own SIGINT/SIGTERM handlers, so background jobs inheriting "ignore SIGINT" does not
#    affect stop_mock.
"$mock_bin" --binance-port "$BINANCE_PORT" --okx-port "$OKX_PORT" --bybit-port "$BYBIT_PORT" \
    --ca-out "$ca_file" --seed "$seed" --drop-rate "$drop_rate" --duration $((duration + 120)) --status-interval 5 --verbose \
    > "$mock_log" 2>&1 &
mock_pid=$!

# 2. 等它打印 ready（三个端口都已监听、CA 证书已写好）；进程提前退出（如端口被占用）则报错
# 2. Wait until it prints ready (all three ports listening, CA certificate written); fail if the process
#    exits early (for example, a port is already in use)
ready=no
for _ in $(seq 1 300); do
    [[ "$interrupted" == yes ]] && exit 130
    if grep -q "^mock_exchange: ready" "$mock_log" 2>/dev/null; then
        ready=yes
        break
    fi
    kill -0 "$mock_pid" 2>/dev/null || break
    sleep 0.1
done
if [[ "$ready" != yes ]]; then
    echo "run_mock_demo: the mock exchange did not start; its output:" >&2
    cat "$mock_log" >&2
    exit 1
fi
echo "run_mock_demo: mock exchange is listening (pid $mock_pid, log $mock_log):"
grep '^  ' "$mock_log" || true

# 3. 运行监控程序。--foreground：监控程序留在终端的前台进程组里，能收到 Ctrl+C；
#    timeout 只是保险（监控程序按 --duration 自己结束，SIGINT 时同样打印最终报告）。
# 3. Run the monitor. --foreground: the monitor stays in the terminal's foreground process group, so it gets
#    Ctrl+C; timeout is only a safety net (the monitor ends by itself after --duration and also prints the
#    final report on SIGINT).
monitor_args=(--config "$config" --duration "$duration")
if [[ "$headless" == yes || ( "$headless" == auto && ! -t 1 ) ]]; then
    monitor_args+=(--headless)
fi
echo "run_mock_demo: running $monitor_bin ${monitor_args[*]}"
set +e
timeout --foreground -s INT -k 10 $((duration + 30)) "$monitor_bin" "${monitor_args[@]}"
monitor_status=$?
set -e

# 4. 停止模拟交易所并打印它的汇总
# 4. Stop the mock exchange and print its summary
stop_mock
echo
echo "run_mock_demo: mock exchange summary (full log: $mock_log):"
sed -n '/^mock_exchange: stopped/,$p' "$mock_log"
if [[ "$interrupted" == yes ]]; then
    exit 130
fi
if [[ $monitor_status -ne 0 ]]; then
    echo "run_mock_demo: the monitor exited with status $monitor_status" >&2
fi
exit "$monitor_status"
