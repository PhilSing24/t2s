#!/bin/bash
# test_clock_lag.sh - a clock that lags the exchange after a wake (sandboxed).
#
# build/sim_trade_publisher publishes trades through the handlers' RowClock
# with a clock that reads behind for some rows, as the system clock does for
# the first seconds after a wake from sleep.
#   1. the clock is behind by more than a day boundary (its reading is
#      yesterday): those rows take `time` from the exchange event time, keep
#      the stale reading in fhRecvTimeUtcNs, are found by
#      .hdb.clockCorrectedRows (and .hdb.clockCorrected on a partitioned
#      HDB), and are stored under today's date; TP shows clockLagRows and
#      status.q raises it
#   2. trades backfilled while the clock is behind (no event time of their
#      own) take the most recent event time
#   3. a clock in step, and a lag under the threshold: nothing is corrected
#      and nothing is flagged; the threshold is configurable
#
# Exit code 0 on success.

set -u

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=t_lib.sh
source "$SCRIPT_DIR/t_lib.sh"
cd "$T2S_TEST_ROOT"

SIM="$T2S_TEST_ROOT/build/sim_trade_publisher"
TP_PID=""; WDB_PID=""; SIM_PID=""; FAILURES=0; SCENARIO=0
cleanup() {
    local rc=$?
    [[ -n "$SIM_PID" ]] && kill -9 "$SIM_PID" 2>/dev/null
    [[ -n "$WDB_PID" ]] && kill -9 "$WDB_PID" 2>/dev/null
    [[ -n "$TP_PID" ]]  && kill -9 "$TP_PID"  2>/dev/null
    t2s_kill_port "$T2S_PORT_TP"; t2s_kill_port "$T2S_PORT_WDB"
    if [[ $rc -eq 0 ]]; then t2s_sandbox_remove; else echo "  Sandbox preserved at: $T2S_SANDBOX (for inspection)"; fi
    exit $rc
}
trap cleanup EXIT INT TERM
if [[ ! -x "$SIM" ]]; then echo "SKIP: build/sim_trade_publisher not built"; exit 0; fi
for port in "$T2S_PORT_TP" "$T2S_PORT_WDB"; do
    if lsof -ti:"$port" >/dev/null 2>&1; then echo "WARN: Killing stale process on test port $port"; t2s_kill_port "$port"; sleep 0.2; fi
done

export TEST_TP_PORT=$T2S_PORT_TP TEST_WDB_PORT=$T2S_PORT_WDB
export SANDBOX_TMP_PATH=$T2S_SB_TMP SANDBOX_HDB_PATH=$T2S_SB_HDB SANDBOX_TPLOG_PATH=$T2S_SB_TPLOGS SANDBOX_CHECKPOINT=$T2S_SB_CHECKPOINT

fail() { echo "  FAIL: $*"; FAILURES=$((FAILURES + 1)); }
pass() { echo "  PASS: $*"; }
lag() { q "$SCRIPT_DIR/clock_lag_body.q" "$@" -q < /dev/null; local rc=$?; [[ $rc -ne 0 ]] && FAILURES=$((FAILURES + 1)); return $rc; }
wdb() { q "$SCRIPT_DIR/wdb_dur_body.q" "$@" -q < /dev/null; local rc=$?; [[ $rc -ne 0 ]] && FAILURES=$((FAILURES + 1)); return $rc; }
start_tp() {
    TP_LOG="$T2S_SANDBOX/tp_${SCENARIO}.log"
    TP_PID=$(t2s_spawn_tp "$T2S_PORT_TP" "$TP_LOG" "$@")
    t2s_wait_port "$T2S_PORT_TP" 6 || { fail "TP did not start"; cat "$TP_LOG"; return 1; }
    t2s_guard tp "$T2S_PORT_TP" > "$T2S_SANDBOX/guard_tp.log" || { fail "TP guard"; cat "$T2S_SANDBOX/guard_tp.log"; return 1; }
}
start_wdb() {
    WDB_LOG="$T2S_SANDBOX/wdb_${SCENARIO}.log"
    WDB_PID=$(t2s_spawn_wdb "$T2S_PORT_WDB" "$T2S_PORT_TP" "$WDB_LOG" T2S_WDB_ROLL_GRACE_SEC=0)
    t2s_wait_port "$T2S_PORT_WDB" 6 || { fail "WDB did not start"; cat "$WDB_LOG"; return 1; }
    t2s_guard wdb "$T2S_PORT_WDB" > "$T2S_SANDBOX/guard_wdb.log" || { fail "WDB guard"; return 1; }
    sleep 1
}
stop_tp() { [[ -n "$TP_PID" ]] && kill -TERM "$TP_PID" 2>/dev/null; sleep 0.3; TP_PID=""; t2s_kill_port "$T2S_PORT_TP"; }
wdb_graceful_stop() {
    wdb -step shutdown || return 1
    local i; for (( i = 0; i < 150; i++ )); do lsof -ti:"$T2S_PORT_WDB" >/dev/null 2>&1 || { WDB_PID=""; return 0; }; sleep 0.1; done
    fail "WDB did not exit"; kill -9 "$WDB_PID" 2>/dev/null; WDB_PID=""
}
sim_run() {
    "$SIM" --port "$T2S_PORT_TP" "$@" > "$T2S_SANDBOX/sim_${SCENARIO}.log" 2>&1 < /dev/null; SIM_RC=$?
    SIM_LINE=$(grep "^SIM " "$T2S_SANDBOX/sim_${SCENARIO}.log" | tail -1); echo "  $SIM_LINE"
}
sim_field() { echo "$SIM_LINE" | sed -n "s/.*$1=\([0-9]*\).*/\1/p"; }
qp() { q -q -p 0 < /dev/null <<QEOF
h:hopen (\`\$":localhost:$1"; 3000); -1 .Q.s1 h "$2"; hclose h; exit 0
QEOF
}
qtp() { qp "$T2S_PORT_TP" "$1"; }
qwdb() { qp "$T2S_PORT_WDB" "$1"; }
status() { T2S_STATUS_MARKETS=spot T2S_TP_PORT=$T2S_PORT_TP T2S_WDB_PORT=$T2S_PORT_WDB T2S_TMP_DIR="$T2S_SB_TMP" q kdb/utils/status.q < /dev/null 2>&1; }

# A clock that reads yesterday: behind by the time since UTC midnight plus
# ten minutes. Uncorrected, those rows would be dated yesterday.
LAG_MS=$(( ( $(date -u +%s) % 86400 ) * 1000 + 600000 ))

echo ""; SCENARIO=1
echo "=== scenario 1: the clock reads yesterday for 200 rows (lag ${LAG_MS} ms) ==="
t2s_sandbox_reset; rm -f "$T2S_SANDBOX/fhseq"
start_tp; start_wdb
sim_run --rows 600 --rate 400 --session 9301 --first-id 1000 --lag-ms "$LAG_MS" --lag-at 200 --lag-rows 200
[[ $SIM_RC -eq 0 ]] && pass "publisher finished" || fail "publisher rc=$SIM_RC"
[[ "$(sim_field clockCorrected)" == "200" ]] && pass "the publisher corrected 200 rows" || fail "sim: $SIM_LINE"
grep -q "CLOCK LAG: the system clock is" "$T2S_SANDBOX/sim_1.log" && pass "the start of the lag is logged" || fail "no CLOCK LAG line"
grep -q "CLOCK LAG over: .* 200 rows were stamped from the exchange time" "$T2S_SANDBOX/sim_1.log" && pass "the end of the lag is logged with the row count" || fail "no CLOCK LAG over line"
[[ "$(grep -c "CLOCK LAG" "$T2S_SANDBOX/sim_1.log")" == "2" ]] && pass "two log lines for the episode, not one per row" || fail "CLOCK LAG lines: $(grep -c "CLOCK LAG" "$T2S_SANDBOX/sim_1.log")"
[[ "$(qtp '.tp.fhDict[`trade_binance]`clockLagRows')" == "200" ]] && pass "TP has the handler counter clockLagRows = 200" || fail "TP clockLagRows $(qtp '.tp.fhDict[`trade_binance]`clockLagRows')"
[[ "$(qtp '.health[][`recent]`trade_binance.clockLagRows')" == "200" ]] && pass ".health[] recent: trade_binance.clockLagRows = 200" || fail "recent $(qtp '.health[]`recent')"
# TP's own status is about TP; a handler counter is raised by status.q below
[[ "$(qtp '.health[]`status')" == '`ok' ]] && pass "TP itself stays ok (the lag is a handler-side event)" || fail "status $(qtp '.health[]`status')"
OUT=$(status)
echo "$OUT" | grep -E "clockLagRows" | sed 's/^/    /'
echo "$OUT" | grep -q "trade_binance: clockLagRows +200 in the last" && pass "status.q raises it as attention" || fail "status.q does not raise clockLagRows"
[[ "$(qwdb '.health[]`unexpectedDateRows')" == "0" ]] && pass "WDB saw no row with an unexpected date" || fail "WDB unexpectedDateRows $(qwdb '.health[]`unexpectedDateRows')"
[[ "$(qtp '.tp.ctr.missed`trade_binance')" == "0" ]] && pass "TP missed = 0" || fail "TP missed"
wdb_graceful_stop
wdb -step assert_vs_tplog -table trade_binance
lag -step assert -total 600 -corrected 200 -lagMs "$LAG_MS" -crossDay 1
lag -step assert_hdb -corrected 200
[[ "$(ls "$T2S_SB_TMP" | grep -c '^tmp\.')" == "1" ]] && pass "one tmp directory: nothing was written under yesterday's date" || fail "tmp dirs: $(ls "$T2S_SB_TMP")"
stop_tp

echo ""; SCENARIO=2
echo "=== scenario 2: trades backfilled while the clock is behind ==="
t2s_sandbox_reset; rm -f "$T2S_SANDBOX/fhseq"
start_tp; start_wdb
# 40 ids are skipped at row 250, inside the lag (rows 200..499); the backfill
# replies are timed with the same stale clock
sim_run --rows 600 --rate 400 --session 9302 --first-id 5000 --lag-ms "$LAG_MS" --lag-at 200 --lag-rows 300 \
        --gap-at 250 --gap-size 40 --backfill 1
[[ $SIM_RC -eq 0 ]] && pass "publisher finished" || fail "publisher rc=$SIM_RC"
[[ "$(sim_field backfilled)" == "40" && "$(sim_field gapsRecovered)" == "1" ]] && pass "the gap was backfilled (40 trades)" || fail "sim: $SIM_LINE"
[[ "$(sim_field clockCorrected)" == "340" ]] && pass "340 rows corrected: 300 live and 40 backfilled" || fail "sim: $SIM_LINE"
[[ "$(qtp '.tp.fhDict[`trade_binance]`clockLagRows')" == "340" ]] && pass "TP has clockLagRows = 340" || fail "TP clockLagRows"
wdb_graceful_stop
wdb -step assert_vs_tplog -table trade_binance
lag -step assert -total 640 -corrected 340 -backfilled 40 -lagMs "$LAG_MS" -crossDay 1
stop_tp

echo ""; SCENARIO=3
echo "=== scenario 3: a clock in step, and a lag under the threshold ==="
t2s_sandbox_reset; rm -f "$T2S_SANDBOX/fhseq"
start_tp; start_wdb
sim_run --rows 300 --rate 600 --session 9303 --first-id 100
[[ "$(sim_field clockCorrected)" == "0" ]] && pass "clock in step: nothing corrected" || fail "sim: $SIM_LINE"
SCENARIO=3b
sim_run --rows 300 --rate 600 --session 9304 --first-id 1000 --lag-ms 1500 --lag-at 100 --lag-rows 100
[[ "$(sim_field clockCorrected)" == "0" ]] && pass "1.5 s behind with the 2 s threshold: nothing corrected" || fail "sim: $SIM_LINE"
grep -q "CLOCK LAG" "$T2S_SANDBOX/sim_3b.log" && fail "a lag was logged" || pass "nothing logged"
[[ "$(qtp '.tp.fhDict[`trade_binance]`clockLagRows')" == "0" ]] && pass "TP has clockLagRows = 0" || fail "TP clockLagRows"
status | grep -q "clockLagRows +" && fail "status.q flags clockLagRows" || pass "status.q does not flag anything"
SCENARIO=3
wdb_graceful_stop
lag -step assert -total 600 -corrected 0 -lagMs 0
stop_tp
SCENARIO=3c
t2s_sandbox_reset; rm -f "$T2S_SANDBOX/fhseq"
start_tp; start_wdb
sim_run --rows 300 --rate 600 --session 9305 --first-id 2000 --lag-ms 1500 --lag-at 100 --lag-rows 100 --lag-threshold-ms 1000
[[ "$(sim_field clockCorrected)" == "100" ]] && pass "the same lag with a 1 s threshold: 100 rows corrected" || fail "sim: $SIM_LINE"
wdb_graceful_stop
lag -step assert -total 300 -corrected 100 -lagMs 1500
stop_tp

echo ""; echo "==========================================="
if [[ $FAILURES -eq 0 ]]; then echo "Clock lag: all checks passed"; echo "==========================================="; exit 0; fi
echo "Clock lag: $FAILURES failure(s)"; echo "==========================================="; exit 1
