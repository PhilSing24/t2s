#!/bin/bash
# test_tp_replay.sh - replay that seeks (sandboxed, all three tables).
#
# Real kdb/tick/tp.q and wdb.q started through tests/t_lib.sh, checked by
# tests/t_guard.q. Steps live in tests/tp_replay_body.q, tests/tp_dur_body.q
# and tests/wdb_dur_body.q.
#
# Scenarios:
#   1 seek + live TP   WDB away mid-log; on reconnect it seeks via the index
#                      (offset > 8, several segments) and merges exactly the
#                      rows published while it was away; a slowed replay
#                      (test hook) overlaps a timed publisher, every upd to
#                      TP returns promptly; disk == logs afterwards
#   2 failure midway   the log is corrupted inside the region a replay must
#                      read: the replay fails explicitly, counted, health =
#                      error, the checkpoint does not move
#   3 across midnight  checkpoint in yesterday's log, rows in both logs:
#                      replay spans two logs in order, zero missing, zero
#                      duplicates, rows in their own date partitions
#
# Exit code 0 if every scenario passes.

set -u

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=t_lib.sh
source "$SCRIPT_DIR/t_lib.sh"
cd "$T2S_TEST_ROOT"

TP_PID=""; WDB_PID=""; FAILURES=0; SCENARIO=""; TP_LOG=""; WDB_LOG=""; TP_EXTRA=()

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

TODAY=$(date -u +%Y.%m.%d)
D_MINUS_1=$(date -u -d 'yesterday' +%Y.%m.%d)

fail() { echo "  FAIL: $*"; FAILURES=$((FAILURES + 1)); }
rp()  { q "$SCRIPT_DIR/tp_replay_body.q" "$@" -q < /dev/null; local rc=$?; [[ $rc -ne 0 ]] && FAILURES=$((FAILURES + 1)); return $rc; }
tp()  { q "$SCRIPT_DIR/tp_dur_body.q"    "$@" -q < /dev/null; local rc=$?; [[ $rc -ne 0 ]] && FAILURES=$((FAILURES + 1)); return $rc; }
wdb() { q "$SCRIPT_DIR/wdb_dur_body.q"   "$@" -q < /dev/null; local rc=$?; [[ $rc -ne 0 ]] && FAILURES=$((FAILURES + 1)); return $rc; }

start_tp() {
    TP_LOG="$T2S_SANDBOX/tp_${SCENARIO}_$(date +%s%N).log"
    TP_PID=$(t2s_spawn_tp "$T2S_PORT_TP" "$TP_LOG" T2S_TP_INDEX_EVERY=50 "${TP_EXTRA[@]}")
    if ! t2s_wait_port "$T2S_PORT_TP" 6; then fail "TP did not start"; cat "$TP_LOG"; return 1; fi
    t2s_guard tp "$T2S_PORT_TP" > "$T2S_SANDBOX/guard_tp.log" || { fail "TP guard"; cat "$T2S_SANDBOX/guard_tp.log"; return 1; }
    return 0
}
# start_wdb [EXTRA=value ...]; waits for the port only (replay may still be running)
start_wdb_nowait() {
    WDB_LOG="$T2S_SANDBOX/wdb_${SCENARIO}_$(date +%s%N).log"
    WDB_PID=$(t2s_spawn_wdb "$T2S_PORT_WDB" "$T2S_PORT_TP" "$WDB_LOG" T2S_WDB_ROLL_GRACE_SEC=0 "$@")
    if ! t2s_wait_port "$T2S_PORT_WDB" 6; then fail "WDB did not start"; cat "$WDB_LOG"; return 1; fi
    return 0
}
start_wdb() {
    start_wdb_nowait "$@" || return 1
    t2s_guard wdb "$T2S_PORT_WDB" > "$T2S_SANDBOX/guard_wdb.log" || { fail "WDB guard"; cat "$T2S_SANDBOX/guard_wdb.log"; return 1; }
    sleep 1
}
stop_tp_term() { [[ -n "$TP_PID" ]] && kill -TERM "$TP_PID" 2>/dev/null && wait "$TP_PID" 2>/dev/null; TP_PID=""; t2s_kill_port "$T2S_PORT_TP"; echo "  TP stopped (SIGTERM)"; }
kill9_wdb()    { [[ -n "$WDB_PID" ]] && kill -9 "$WDB_PID" 2>/dev/null && wait "$WDB_PID" 2>/dev/null; WDB_PID=""; t2s_kill_port "$T2S_PORT_WDB"; echo "  WDB killed (SIGKILL)"; }
wdb_graceful_stop() {
    wdb -step shutdown || return 1
    for (( i = 0; i < 150; i++ )); do
        if ! lsof -ti:"$T2S_PORT_WDB" >/dev/null 2>&1; then wait "$WDB_PID" 2>/dev/null; WDB_PID=""; return 0; fi
        sleep 0.1
    done
    fail "WDB did not exit after shutdown request"; kill -9 "$WDB_PID" 2>/dev/null; WDB_PID=""; return 1
}
# Wait until WDB's log says the replay finished (either way), up to 60s.
wait_replay_done() {
    for (( i = 0; i < 600; i++ )); do
        if grep -qE "replay complete|REPLAY FAILED" "$WDB_LOG"; then return 0; fi
        sleep 0.1
    done
    fail "WDB replay did not finish within 60s"; return 1
}
begin_scenario() {
    SCENARIO=$1; shift
    echo ""; echo "=== scenario $SCENARIO: $* ==="
    t2s_sandbox_reset; rm -f "$T2S_SANDBOX/fhseq" "$T2S_SANDBOX"/cp_*
    TP_EXTRA=()
    start_tp || return 1
}
end_scenario() {
    [[ -n "$WDB_PID" ]] && { kill -9 "$WDB_PID" 2>/dev/null; wait "$WDB_PID" 2>/dev/null; WDB_PID=""; }
    [[ -n "$TP_PID" ]]  && { kill -TERM "$TP_PID" 2>/dev/null; wait "$TP_PID" 2>/dev/null; TP_PID=""; }
    t2s_kill_port "$T2S_PORT_TP"; t2s_kill_port "$T2S_PORT_WDB"
}
all_tables_match_logs() {
    wdb -step assert_vs_tplog -table trade_binance
    wdb -step assert_vs_tplog -table quote_binance
    wdb -step assert_vs_tplog -table trade_binance_fut
}

# ============================================================================
# Scenario 1: replay seeks, returns exactly the missing rows, TP stays live
# ============================================================================
begin_scenario 1 "seek via the index; exactly the missing rows; TP live during replay" && {
    start_wdb
    tp -step publish -table trade_binance     -rows 500 -date "$TODAY" -session 1001
    tp -step publish -table quote_binance     -rows 200 -date "$TODAY" -session 1002
    tp -step publish -table trade_binance_fut -rows 100 -date "$TODAY" -session 1003
    sleep 1
    wdb -step flush
    rp  -step assert_index -date "$TODAY" -min 16
    kill9_wdb
    # Rows WDB will have to replay (index step is 50, so many segments)
    tp -step publish -table trade_binance     -rows 300 -date "$TODAY" -session 1001
    tp -step publish -table quote_binance     -rows 120 -date "$TODAY" -session 1002
    tp -step publish -table trade_binance_fut -rows 60  -date "$TODAY" -session 1003
    # Restart with a slowed replay (300 ms per segment) and publish to TP
    # meanwhile, timing every call. TP must not notice the replay at all.
    start_wdb_nowait T2S_WDB_REPLAY_DELAY_MS=300
    rp -step publish_timed -table trade_binance -rows 100 -date "$TODAY" -session 1001 -maxms 500
    rp -step assert_tp_live -value ok
    wait_replay_done
    grep -q "replay complete" "$WDB_LOG" || fail "WDB replay did not complete"
    grep -qE "replayed $TODAY from offset [0-9]+ to" "$WDB_LOG" || fail "WDB log lacks the seek line"
    t2s_guard wdb "$T2S_PORT_WDB" > /dev/null || fail "WDB guard"
    sleep 1
    rp  -step assert_replay -logs 1 -seeked 1 -minsegs 5 -failures 0
    wdb -step assert_status -key replayRowsApplied -value 480
    wdb -step assert_status -key duplicatesDropped -value 0
    wdb -step assert_status -key status -value ok
    wdb_graceful_stop
    all_tables_match_logs
    wdb -step assert_tmp -table trade_binance     -date "$TODAY" -rows 900
    wdb -step assert_tmp -table quote_binance     -date "$TODAY" -rows 320
    wdb -step assert_tmp -table trade_binance_fut -date "$TODAY" -rows 160
}
end_scenario

# ============================================================================
# Scenario 2: corruption inside the replay region -> explicit failure
# ============================================================================
begin_scenario 2 "corrupt log: replay fails explicitly, checkpoint untouched" && {
    start_wdb
    tp -step publish -table trade_binance     -rows 300 -date "$TODAY" -session 2001
    tp -step publish -table quote_binance     -rows 100 -date "$TODAY" -session 2002
    tp -step publish -table trade_binance_fut -rows 50  -date "$TODAY" -session 2003
    sleep 1
    wdb -step flush
    rp  -step checkpoint_save -name before
    kill9_wdb
    tp -step publish -table trade_binance     -rows 400 -date "$TODAY" -session 2001
    tp -step publish -table quote_binance     -rows 100 -date "$TODAY" -session 2002
    # Damage the log well inside the rows WDB must replay
    rp -step corrupt_log -date "$TODAY" -tailbytes 4000
    start_wdb_nowait
    wait_replay_done
    grep -q "REPLAY FAILED" "$WDB_LOG" || fail "WDB log lacks REPLAY FAILED"
    grep -q "corrupt segment" "$WDB_LOG" || fail "WDB log lacks the corruption reason"
    sleep 1
    rp  -step assert_replay -failures 1
    wdb -step assert_status -key status -value error
    wdb -step assert_status -key replayRowsApplied -value 0
    wdb -step assert_status -key connState -value disconnected
    rp  -step checkpoint_same -name before
    # Still nothing merged after a retry on the timer
    sleep 6
    rp  -step assert_replay -failures 2
    rp  -step checkpoint_same -name before
    wdb -step assert_tmp -table trade_binance -date "$TODAY" -rows 300
}
end_scenario

# ============================================================================
# Scenario 3: replay across midnight, two logs in order
# ============================================================================
begin_scenario 3 "checkpoint in yesterday's log: replay spans both logs" && {
    stop_tp_term
    TP_EXTRA=("T2S_TP_FAKE_DATE=$D_MINUS_1"); start_tp
    start_wdb "T2S_WDB_FAKE_DATE=$D_MINUS_1"
    tp -step publish -table trade_binance     -rows 200 -date "$D_MINUS_1" -session 3001
    tp -step publish -table quote_binance     -rows 80  -date "$D_MINUS_1" -session 3002
    tp -step publish -table trade_binance_fut -rows 40  -date "$D_MINUS_1" -session 3003
    sleep 1
    wdb -step flush
    wdb_graceful_stop
    # More rows land in yesterday's log after WDB is gone ...
    tp -step publish -table trade_binance     -rows 150 -date "$D_MINUS_1" -session 3001
    tp -step publish -table quote_binance     -rows 60  -date "$D_MINUS_1" -session 3002
    tp -step publish -table trade_binance_fut -rows 30  -date "$D_MINUS_1" -session 3003
    stop_tp_term
    # ... then the day changes and TP keeps logging into today's log
    TP_EXTRA=(); start_tp
    tp -step publish -table trade_binance     -rows 100 -date "$TODAY" -session 3001
    tp -step publish -table quote_binance     -rows 40  -date "$TODAY" -session 3002
    tp -step publish -table trade_binance_fut -rows 20  -date "$TODAY" -session 3003
    start_wdb_nowait
    wait_replay_done
    grep -q "replay complete" "$WDB_LOG" || fail "WDB replay did not complete"
    grep -qE "over log\(s\) $D_MINUS_1 $TODAY" "$WDB_LOG" || fail "WDB did not replay both logs in order"
    t2s_guard wdb "$T2S_PORT_WDB" > /dev/null || fail "WDB guard"
    sleep 1
    rp  -step assert_replay -logs 2 -seeked 1 -failures 0
    wdb -step assert_status -key replayRowsApplied -value 400
    wdb -step assert_status -key duplicatesDropped -value 0
    wdb -step assert_status -key halted -value 0
    # Startup roll ran after the replay: yesterday is in the HDB (both
    # batches), today's rows in tmp.today
    wdb -step assert_partition -table trade_binance     -date "$D_MINUS_1" -rows 350
    wdb -step assert_partition -table quote_binance     -date "$D_MINUS_1" -rows 140
    wdb -step assert_partition -table trade_binance_fut -date "$D_MINUS_1" -rows 70
    wdb -step assert_status -key lateRows -value 0
    wdb_graceful_stop
    wdb -step assert_tmp -table trade_binance     -date "$TODAY" -rows 100
    wdb -step assert_tmp -table quote_binance     -date "$TODAY" -rows 40
    wdb -step assert_tmp -table trade_binance_fut -date "$TODAY" -rows 20
    all_tables_match_logs
    tp -step assert_log_monotone
}
end_scenario

echo ""; echo "==========================================="
if [[ $FAILURES -eq 0 ]]; then echo "TP replay: all scenarios passed"; echo "==========================================="; exit 0; fi
echo "TP replay: $FAILURES failure(s)"; echo "==========================================="; exit 1
