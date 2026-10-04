#!/bin/bash
# test_resend.sh - no row is lost between a handler and TP across a TP
# restart (sandboxed).
#
# build/sim_trade_publisher sends synthetic trades through the real
# TpPublisher (the class the feed handlers publish with): it keeps a ring of
# recent rows and, after reconnecting, resends everything TP says it has
# not logged.
#   1. kill -9 TP mid-stream, restart it: TP's missed counter stays 0, the
#      logged fhSeqNo is exactly 1..N (nothing missing, nothing twice), and
#      WDB's disk copy matches the logs
#   2. SIGTERM TP mid-stream: the same
#   3. a ring too small to cover the outage: whatever cannot be resent is
#      counted by TP as missed - the two numbers agree, nothing is silent
#   4. a restarted publisher (new session): TP reports a handler restart
#      and asks for no resend
#
# Exit code 0 on success.

set -u

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=t_lib.sh
source "$SCRIPT_DIR/t_lib.sh"
cd "$T2S_TEST_ROOT"

TP_PID=""; WDB_PID=""; SIM_PID=""; FAILURES=0; SCENARIO=0
SIM="$T2S_TEST_ROOT/build/sim_trade_publisher"

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
tp()  { q "$SCRIPT_DIR/tp_dur_body.q"  "$@" -q < /dev/null; local rc=$?; [[ $rc -ne 0 ]] && FAILURES=$((FAILURES + 1)); return $rc; }
wdb() { q "$SCRIPT_DIR/wdb_dur_body.q" "$@" -q < /dev/null; local rc=$?; [[ $rc -ne 0 ]] && FAILURES=$((FAILURES + 1)); return $rc; }
start_tp() {
    TP_LOG="$T2S_SANDBOX/tp_${SCENARIO}_$(date +%s%N).log"
    TP_PID=$(t2s_spawn_tp "$T2S_PORT_TP" "$TP_LOG")
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
stop_tp() {   # stop_tp KILL|TERM
    [[ -n "$TP_PID" ]] && kill -"$1" "$TP_PID" 2>/dev/null; sleep 0.3; TP_PID=""
    t2s_kill_port "$T2S_PORT_TP"
    echo "  TP stopped (SIG$1)"
}
wdb_graceful_stop() {
    wdb -step shutdown || return 1
    local i; for (( i = 0; i < 150; i++ )); do
        lsof -ti:"$T2S_PORT_WDB" >/dev/null 2>&1 || { WDB_PID=""; return 0; }
        sleep 0.1
    done
    fail "WDB did not exit"; kill -9 "$WDB_PID" 2>/dev/null; WDB_PID=""
}
sim_bg() { "$SIM" --port "$T2S_PORT_TP" "$@" > "$T2S_SANDBOX/sim_${SCENARIO}.log" 2>&1 < /dev/null & SIM_PID=$!; }
sim_wait() { wait "$SIM_PID"; SIM_RC=$?; SIM_PID=""; SIM_LINE=$(grep "^SIM " "$T2S_SANDBOX/sim_${SCENARIO}.log" | tail -1); echo "  $SIM_LINE"; }
sim_field() { echo "$SIM_LINE" | sed -n "s/.*$1=\([0-9]*\).*/\1/p"; }
tp_stat() { q -q -p 0 < /dev/null <<QEOF
h:hopen (\`\$":localhost:$T2S_PORT_TP"; 3000); -1 string h "$1"; hclose h; exit 0
QEOF
}

run_restart_scenario() {   # signal rows
    t2s_sandbox_reset; rm -f "$T2S_SANDBOX/fhseq"
    start_tp || return; start_wdb || return
    sim_bg --rows "$2" --rate 2000 --session 9001
    sleep 1
    stop_tp "$1"
    sleep 0.7
    start_tp || return
    sim_wait
    [[ $SIM_RC -eq 0 ]] && pass "publisher finished all $2 rows" || { fail "publisher rc=$SIM_RC"; tail -5 "$T2S_SANDBOX/sim_${SCENARIO}.log"; }
    [[ "$(sim_field reconnects)" -ge 1 ]] && pass "publisher noticed the lost connection ($(sim_field reconnects) reconnect)" || fail "publisher never reconnected"
    [[ "$(sim_field unresendable)" == "0" ]] && pass "every row TP lacked was still in the ring" || fail "unresendable=$(sim_field unresendable)"
    grep -q "session file loaded" "$TP_LOG" && pass "restarted TP knew the session from its session file" || fail "session file not loaded"
    grep -q "FH RECONNECT for trade_binance session 9001" "$TP_LOG" && pass "TP saw a reconnect of the same session, not a handler restart" || { fail "no RECONNECT line"; grep "trade_binance" "$TP_LOG" | tail -3; }
    [[ "$(tp_stat '.tp.ctr.missed`trade_binance')" == "0" ]] && pass "TP missed = 0" || fail "TP missed = $(tp_stat '.tp.ctr.missed`trade_binance')"
    [[ "$(tp_stat '.tp.ctr.gaps`trade_binance')" == "0" ]] && pass "TP gaps = 0" || fail "TP gaps = $(tp_stat '.tp.ctr.gaps`trade_binance')"
    [[ "$(tp_stat '.tp.ctr.outOfOrder`trade_binance')" == "0" ]] && pass "TP outOfOrder = 0 (nothing was sent twice)" || fail "TP outOfOrder = $(tp_stat '.tp.ctr.outOfOrder`trade_binance')"
    tp -step assert_fh_exact -table trade_binance -rows "$2"
    tp -step assert_log_monotone
    echo "  resent after the restart: $(sim_field resent) row(s)"
    sleep 7     # WDB's reconnect timer
    wdb_graceful_stop
    wdb -step assert_vs_tplog -table trade_binance
    wdb -step assert_tmp -table trade_binance -date "$(date -u +%Y.%m.%d)" -rows "$2"
}

echo ""; SCENARIO=1
echo "=== scenario 1: kill -9 TP mid-stream ==="
run_restart_scenario KILL 6000
stop_tp TERM

echo ""; SCENARIO=2
echo "=== scenario 2: SIGTERM TP mid-stream ==="
run_restart_scenario TERM 6000
stop_tp TERM

echo ""; SCENARIO=3
echo "=== scenario 3: ring too small - what cannot be resent is counted as missed ==="
t2s_sandbox_reset; rm -f "$T2S_SANDBOX/fhseq"
start_tp
sim_bg --rows 6000 --rate 4000 --session 9003 --ring 1
sleep 0.7
stop_tp KILL
sleep 0.7
start_tp
sim_wait
[[ $SIM_RC -eq 0 ]] && pass "publisher finished" || fail "publisher rc=$SIM_RC"
UNR=$(sim_field unresendable); MISSED=$(tp_stat '.tp.ctr.missed`trade_binance')
echo "  publisher could not resend $UNR row(s); TP counted $MISSED missed"
[[ "$UNR" == "$MISSED" ]] && pass "TP's missed counter equals the rows the ring had lost" || fail "unresendable=$UNR but TP missed=$MISSED"
LOGGED=$(q -q < /dev/null <<QEOF
rows:0; upd:{[t;d] if[t=\`trade_binance; rows+::1]}; {-11!x} each hsym each \`\$("$T2S_SB_TPLOGS/"),/:string {x where x like "*.log"} key \`\$":$T2S_SB_TPLOGS"; -1 string rows; exit 0
QEOF
)
[[ $((LOGGED + MISSED)) -eq 6000 ]] && pass "logged $LOGGED + missed $MISSED = 6000 published: every row is accounted for" || fail "logged $LOGGED + missed $MISSED != 6000"
stop_tp TERM

echo ""; SCENARIO=4
echo "=== scenario 4: a restarted publisher is a new session, no resend ==="
t2s_sandbox_reset; rm -f "$T2S_SANDBOX/fhseq"
start_tp
sim_bg --rows 300 --rate 2000 --session 9004; sim_wait
sim_bg --rows 200 --rate 2000 --session 9005 --first-id 1000; sim_wait
[[ "$(sim_field resent)" == "0" ]] && pass "new session resent nothing" || fail "new session resent $(sim_field resent)"
grep -q "FH RESTART detected for trade_binance: session 9004 -> 9005" "$TP_LOG" && pass "TP reports the handler restart" || { fail "no RESTART line"; grep trade_binance "$TP_LOG" | tail -3; }
[[ "$(tp_stat '.tp.ctr.missed`trade_binance')" == "0" ]] && pass "TP missed = 0" || fail "TP missed"
tp -step assert_fh_sessions -table trade_binance -restarts 1
stop_tp TERM

echo ""; echo "==========================================="
if [[ $FAILURES -eq 0 ]]; then echo "Resend: all scenarios passed"; echo "==========================================="; exit 0; fi
echo "Resend: $FAILURES failure(s)"; echo "==========================================="; exit 1
