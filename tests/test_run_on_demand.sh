#!/bin/bash
# test_run_on_demand.sh - a pipeline that is started and stopped by hand
# (sandboxed).
#
#   1. ops/pipeline_state.sh: stopped / partial / running
#   2. status.sh: a fully stopped pipeline is reported as "stopped" and is
#      not flagged; a partly running one still is
#   3. check_eod.sh: a day with nothing recorded is NOT RUN (exit 0); a day
#      left in tmp.<date> by a pipeline stopped before midnight is PENDING
#      (exit 0) and noted; the same day while the pipeline is partly up
#      fails; after the next start has rolled it, a run without arguments
#      checks it for real and clears the note
#
# Exit code 0 on success.

set -u

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=t_lib.sh
source "$SCRIPT_DIR/t_lib.sh"
cd "$T2S_TEST_ROOT"

TP_PID=""; WDB_PID=""; FAILURES=0
cleanup() {
    local rc=$?
    [[ -n "$WDB_PID" ]] && kill -9 "$WDB_PID" 2>/dev/null
    [[ -n "$TP_PID" ]]  && kill -9 "$TP_PID"  2>/dev/null
    t2s_kill_port "$T2S_PORT_TP"; t2s_kill_port "$T2S_PORT_WDB"
    if [[ $rc -eq 0 ]]; then t2s_sandbox_remove; else echo "  Sandbox preserved at: $T2S_SANDBOX (for inspection)"; fi
    exit $rc
}
trap cleanup EXIT INT TERM
for port in "$T2S_PORT_TP" "$T2S_PORT_WDB"; do
    if lsof -ti:"$port" >/dev/null 2>&1; then echo "WARN: Killing stale process on test port $port"; t2s_kill_port "$port"; sleep 0.2; fi
done

export TEST_TP_PORT=$T2S_PORT_TP TEST_WDB_PORT=$T2S_PORT_WDB
export SANDBOX_TMP_PATH=$T2S_SB_TMP SANDBOX_HDB_PATH=$T2S_SB_HDB SANDBOX_TPLOG_PATH=$T2S_SB_TPLOGS SANDBOX_CHECKPOINT=$T2S_SB_CHECKPOINT
D0=$(date -u -d '3 days ago' +%Y.%m.%d)
D1=$(date -u -d '2 days ago' +%Y.%m.%d)
DX=$(date -u -d '9 days ago' +%Y.%m.%d)

fail() { echo "  FAIL: $*"; FAILURES=$((FAILURES + 1)); }
pass() { echo "  PASS: $*"; }
tp()  { q "$SCRIPT_DIR/tp_dur_body.q"  "$@" -q < /dev/null; local rc=$?; [[ $rc -ne 0 ]] && FAILURES=$((FAILURES + 1)); return $rc; }
wdb() { q "$SCRIPT_DIR/wdb_dur_body.q" "$@" -q < /dev/null; local rc=$?; [[ $rc -ne 0 ]] && FAILURES=$((FAILURES + 1)); return $rc; }
SBENV=(T2S_TP_PORT="$T2S_PORT_TP" T2S_WDB_PORT="$T2S_PORT_WDB" T2S_TP_LOG_DIR="$T2S_SB_TPLOGS" T2S_HDB_DIR="$T2S_SB_HDB" T2S_TMP_DIR="$T2S_SB_TMP")
state()     { env "${SBENV[@]}" ops/pipeline_state.sh; }
status()    { env "${SBENV[@]}" ./status.sh 2>&1; }
check_eod() { env "${SBENV[@]}" ./check_eod.sh "$@" 2>&1; }
start_tp() {
    TP_PID=$(t2s_spawn_tp "$T2S_PORT_TP" "$T2S_SANDBOX/tp.log" "T2S_TP_FAKE_DATE=$1")
    t2s_wait_port "$T2S_PORT_TP" 6 || { fail "TP did not start"; cat "$T2S_SANDBOX/tp.log"; exit 1; }
    t2s_guard tp "$T2S_PORT_TP" > /dev/null || { fail "TP guard"; exit 1; }
}
start_wdb() {
    WDB_PID=$(t2s_spawn_wdb "$T2S_PORT_WDB" "$T2S_PORT_TP" "$T2S_SANDBOX/wdb.log" T2S_WDB_ROLL_GRACE_SEC=0 "T2S_WDB_FAKE_DATE=$1")
    t2s_wait_port "$T2S_PORT_WDB" 6 || { fail "WDB did not start"; cat "$T2S_SANDBOX/wdb.log"; exit 1; }
    t2s_guard wdb "$T2S_PORT_WDB" > /dev/null || { fail "WDB guard"; exit 1; }
    sleep 1
}
stop_wdb() {
    wdb -step shutdown > /dev/null
    local i; for (( i = 0; i < 150; i++ )); do lsof -ti:"$T2S_PORT_WDB" >/dev/null 2>&1 || break; sleep 0.1; done
    WDB_PID=""
}
stop_tp() { [[ -n "$TP_PID" ]] && kill -TERM "$TP_PID" 2>/dev/null; sleep 0.3; TP_PID=""; t2s_kill_port "$T2S_PORT_TP"; }

echo ""
echo "=== 1. stopped: state, status.sh, a day with nothing recorded ==="
t2s_sandbox_reset; rm -f "$T2S_SANDBOX/fhseq"; rm -rf "$T2S_RUN_DIR"
[[ "$(state)" == "stopped" ]] && pass "pipeline_state: stopped" || fail "state $(state)"
OUT=$(status)
echo "$OUT" | grep -E "^(PROC|PIPELINE)" | sed 's/^/    /'
echo "$OUT" | grep -q "^PIPELINE: stopped" && pass "status.sh says the pipeline is stopped" || fail "no PIPELINE: stopped line"
echo "$OUT" | grep -qE "TP is down|WDB is down|not reachable|handler is down" && fail "status.sh flags the stopped pipeline" || pass "status.sh does not flag TP, WDB or the handlers"
echo "$OUT" | grep -q "DOWN" && fail "PROC line says DOWN" || pass "PROC line says stopped, not DOWN"
# exit code: 0 unless this machine itself has something to report (clock, timers)
if echo "$OUT" | grep -q "nothing needs attention"; then
    env "${SBENV[@]}" ./status.sh > /dev/null 2>&1 && pass "status.sh exits 0" || fail "status.sh exit code with nothing to report"
else
    echo "  NOTE: status.sh reports something about this machine, exit code not checked:"; echo "$OUT" | grep "^       - " | sed 's/^/    /'
fi
OUT=$(check_eod "$DX"); RC=$?
echo "$OUT" | sed 's/^/    /'
[[ $RC -eq 0 ]] && echo "$OUT" | grep -q "=== $DX NOT RUN" && pass "a day with nothing recorded: NOT RUN, exit 0" || fail "check_eod $DX rc=$RC"
[[ ! -e "$T2S_RUN_DIR/eod.pending" ]] && pass "nothing noted as pending" || fail "eod.pending exists"

echo ""
echo "=== 2. a day recorded, pipeline stopped before midnight: PENDING ==="
start_tp "$D0"
[[ "$(state)" == "partial" ]] && pass "pipeline_state with TP only: partial" || fail "state $(state)"
OUT=$(status); RC=$?
[[ $RC -eq 1 ]] && echo "$OUT" | grep -q "WDB is down" && pass "status.sh flags a partly running pipeline (exit 1, WDB is down)" || fail "status.sh rc=$RC with TP only"
start_wdb "$D0"
[[ "$(state)" == "running" ]] && pass "pipeline_state with TP and WDB: running" || fail "state $(state)"
tp -step publish -table trade_binance -rows 80 -date "$D0" -session 9401
tp -step publish -table quote_binance -rows 40 -date "$D0" -session 9402
sleep 1
OUT=$(check_eod "$D0"); RC=$?
[[ $RC -eq 1 ]] && pass "while the pipeline runs, an unrolled day still fails the check" || { fail "check_eod $D0 rc=$RC while running"; echo "$OUT"; }
stop_wdb; stop_tp
[[ "$(state)" == "stopped" ]] && pass "stopped again" || fail "state $(state)"
[[ -d "$T2S_SB_TMP/tmp.$D0" && ! -d "$T2S_SB_HDB/$D0" ]] && pass "tmp.$D0 holds the day, no partition yet" || fail "unexpected layout: $(ls "$T2S_SB_TMP" "$T2S_SB_HDB")"
OUT=$(check_eod "$D0"); RC=$?
echo "$OUT" | sed 's/^/    /'
[[ $RC -eq 0 ]] && echo "$OUT" | grep -q "=== $D0 PENDING until the next start" && pass "PENDING, exit 0" || fail "check_eod $D0 rc=$RC"
[[ "$(cat "$T2S_RUN_DIR/eod.pending" 2>/dev/null)" == "$D0" ]] && pass "$D0 noted in eod.pending" || fail "eod.pending: $(cat "$T2S_RUN_DIR/eod.pending" 2>&1)"
OUT=$(status)
echo "$OUT" | grep -E "^(EOD|       past day)" | sed 's/^/    /'
echo "$OUT" | grep -q "waiting to be rolled into the HDB at the next start: tmp.$D0" && pass "status.sh shows the day waiting, without flagging it" || fail "status.sh on the waiting day"
echo "$OUT" | grep -q "past-date tmp dir(s) not rolled" && fail "status.sh flags the waiting day" || pass "no attention item for it"
echo "$OUT" | grep -q "^EOD  : end-of-day check pending for: $D0" && pass "status.sh shows the pending end-of-day check" || fail "no EOD line"
# TP up without WDB is not "stopped": the same day is a problem then
start_tp "$D1"
OUT=$(check_eod "$D0"); RC=$?
[[ $RC -eq 1 ]] && pass "with the pipeline partly up the unrolled day fails the check" || fail "check_eod $D0 rc=$RC with TP only"

echo ""
echo "=== 3. the next start rolls the day; the daily check then confirms it ==="
start_wdb "$D1"
for (( i = 0; i < 100; i++ )); do [[ -d "$T2S_SB_HDB/$D0" && ! -d "$T2S_SB_TMP/tmp.$D0" ]] && break; sleep 0.2; done
[[ -d "$T2S_SB_HDB/$D0" ]] && pass "WDB rolled $D0 into the HDB at start" || { fail "no partition for $D0"; tail -5 "$T2S_SANDBOX/wdb.log"; }
OUT=$(check_eod); RC=$?          # no argument: pending days, then yesterday
echo "$OUT" | sed 's/^/    /'
echo "$OUT" | grep -q "=== $D0 OK" && pass "the run without arguments checked the pending day: OK" || fail "$D0 not confirmed"
[[ $RC -eq 0 ]] && pass "exit 0 (yesterday: nothing recorded in the sandbox)" || fail "check_eod rc=$RC"
[[ ! -e "$T2S_RUN_DIR/eod.pending" ]] && pass "eod.pending cleared" || fail "eod.pending still holds: $(cat "$T2S_RUN_DIR/eod.pending")"
stop_wdb; stop_tp

echo ""; echo "==========================================="
if [[ $FAILURES -eq 0 ]]; then echo "Run on demand: all checks passed"; echo "==========================================="; exit 0; fi
echo "Run on demand: $FAILURES failure(s)"; echo "==========================================="; exit 1
