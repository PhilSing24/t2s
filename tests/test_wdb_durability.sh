#!/bin/bash
# test_wdb_durability.sh - WDB durability scenarios (sandboxed).
#
# Every process is the real kdb/tick/tp.q / wdb.q started through
# tests/t_lib.sh with all paths and ports pointed at tests/sandbox, and
# checked by tests/t_guard.q after start-up. q-side steps and assertions
# live in tests/wdb_dur_body.q.
#
# Scenarios:
#   1 graceful stop      .wdb.shutdownAndExit[] flushes the buffer to
#                        tmp.<date> and writes the checkpoint
#   2 restart after stop restart replays nothing, new rows append, zero
#                        duplicate tpSeqNo, disk == TP log
#   3 kill -9 mid-run    small MAXROWS so several flushes happen, kill -9,
#                        restart (replay from checkpoint), publish more,
#                        disk == TP log, zero duplicates. Then rewind the
#                        checkpoint by hand (a crash between write and
#                        checkpoint): the next start raises it back from
#                        the tmp dirs and replays nothing; a resent row
#                        with an old tpSeqNo is dropped at receipt
#   4 missed midnight    WDB disconnected while rows for two different
#                        dates are published; after restart + clock advance
#                        each date lands in its own partition, nothing
#                        mixed; a row for an already-rolled date is kept in
#                        a fresh tmp dir and counted as late
#   5 TP restart on a    TP restarted on a day with no log (fake date =
#     new day            tomorrow) keeps its tpSeqNo from the reservation
#                        file: WDB stays healthy, no halt, no rows dropped,
#                        everything on disk
#   6 counter backwards  TP started with BOTH its reservation file and its
#                        logs removed hands out numbers below WDB's
#                        checkpoint: WDB halts, health = error, rows dropped
#                        and counted, nothing written
#
# Exit code 0 if every scenario passes.

set -u

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=t_lib.sh
source "$SCRIPT_DIR/t_lib.sh"
cd "$T2S_TEST_ROOT"

TP_PID=""
WDB_PID=""
FAILURES=0
SCENARIO=""

# -------------------- cleanup --------------------
cleanup() {
    local rc=$?
    [[ -n "$WDB_PID" ]] && kill -9 "$WDB_PID" 2>/dev/null
    [[ -n "$TP_PID" ]]  && kill -9 "$TP_PID"  2>/dev/null
    t2s_kill_port "$T2S_PORT_TP"
    t2s_kill_port "$T2S_PORT_WDB"
    if [[ $rc -eq 0 ]]; then
        t2s_sandbox_remove
    else
        echo "  Sandbox preserved at: $T2S_SANDBOX (for inspection)"
    fi
    exit $rc
}
trap cleanup EXIT INT TERM

for port in "$T2S_PORT_TP" "$T2S_PORT_WDB"; do
    if lsof -ti:"$port" >/dev/null 2>&1; then
        echo "WARN: Killing stale process on test port $port"
        t2s_kill_port "$port"; sleep 0.2
    fi
done

# -------------------- helpers --------------------
export TEST_TP_PORT=$T2S_PORT_TP
export TEST_WDB_PORT=$T2S_PORT_WDB
export SANDBOX_TMP_PATH=$T2S_SB_TMP
export SANDBOX_HDB_PATH=$T2S_SB_HDB
export SANDBOX_TPLOG_PATH=$T2S_SB_TPLOGS
export SANDBOX_CHECKPOINT=$T2S_SB_CHECKPOINT

TODAY=$(date -u +%Y.%m.%d)
D_MINUS_1=$(date -u -d 'yesterday' +%Y.%m.%d)
D_MINUS_2=$(date -u -d '2 days ago' +%Y.%m.%d)
D_PLUS_1=$(date -u -d 'tomorrow' +%Y.%m.%d)
TP_EXTRA=()

fail() { echo "  FAIL: $*"; FAILURES=$((FAILURES + 1)); }

# Run one q step; count a failure if it exits non-zero.
step() {
    q "$SCRIPT_DIR/wdb_dur_body.q" "$@" -q < /dev/null
    local rc=$?
    [[ $rc -ne 0 ]] && FAILURES=$((FAILURES + 1))
    return $rc
}

start_tp() {
    TP_PID=$(t2s_spawn_tp "$T2S_PORT_TP" "$T2S_SANDBOX/tp_${SCENARIO}.log" "${TP_EXTRA[@]}")
    if ! t2s_wait_port "$T2S_PORT_TP" 6; then fail "TP did not start"; cat "$T2S_SANDBOX/tp_${SCENARIO}.log"; return 1; fi
    t2s_guard tp "$T2S_PORT_TP" > /dev/null || { fail "TP guard"; return 1; }
    return 0
}

# start_wdb [EXTRA=value ...]
WDB_LOG=""
start_wdb() {
    WDB_LOG="$T2S_SANDBOX/wdb_${SCENARIO}_$(date +%s%N).log"
    WDB_PID=$(t2s_spawn_wdb "$T2S_PORT_WDB" "$T2S_PORT_TP" "$WDB_LOG" "$@")
    if ! t2s_wait_port "$T2S_PORT_WDB" 6; then fail "WDB did not start"; cat "$WDB_LOG"; return 1; fi
    t2s_guard wdb "$T2S_PORT_WDB" > /dev/null || { fail "WDB guard"; return 1; }
    # Let the WDB finish subscribe + replay before the scenario publishes
    sleep 1
    return 0
}

# Wait for the WDB port to close after a graceful shutdown request.
wait_wdb_exit() {
    for (( i = 0; i < 150; i++ )); do
        if ! lsof -ti:"$T2S_PORT_WDB" >/dev/null 2>&1; then
            wait "$WDB_PID" 2>/dev/null; WDB_PID=""; return 0
        fi
        sleep 0.1
    done
    return 1
}

graceful_stop() {
    step -step shutdown || return 1
    if wait_wdb_exit; then
        echo "  stop path: graceful (IPC shutdown, port closed)"
        grep -q "shutdown flush complete" "$WDB_LOG" || fail "WDB log lacks 'shutdown flush complete'"
        return 0
    fi
    fail "WDB did not exit after shutdown request"
    kill -9 "$WDB_PID" 2>/dev/null; WDB_PID=""
    return 1
}

hard_kill_wdb() {
    kill -9 "$WDB_PID" 2>/dev/null; wait "$WDB_PID" 2>/dev/null; WDB_PID=""
    t2s_kill_port "$T2S_PORT_WDB"
    echo "  WDB killed with SIGKILL"
}

stop_tp() {
    [[ -n "$TP_PID" ]] && kill -TERM "$TP_PID" 2>/dev/null && wait "$TP_PID" 2>/dev/null; TP_PID=""
    t2s_kill_port "$T2S_PORT_TP"
}

# Fresh sandbox + TP for a scenario (resets the fhSeqNo counter too).
begin_scenario() {
    SCENARIO=$1; shift
    echo ""
    echo "=== scenario $SCENARIO: $* ==="
    t2s_sandbox_reset
    rm -f "$T2S_SANDBOX/fhseq"
    start_tp || return 1
}

end_scenario() {
    [[ -n "$WDB_PID" ]] && hard_kill_wdb > /dev/null
    stop_tp
}

# Wait up to N seconds for the WDB timer to run a roll (5s timer, grace 0).
wait_roll() { sleep "${1:-7}"; }

# ============================================================================
# Scenario 1: graceful stop flushes the buffer and writes the checkpoint
# ============================================================================
begin_scenario 1 "graceful stop flushes and checkpoints" && {
    start_wdb T2S_WDB_ROLL_GRACE_SEC=0
    step -step publish -table trade_binance     -rows 100 -date "$TODAY"
    step -step publish -table quote_binance     -rows 50  -date "$TODAY"
    step -step publish -table trade_binance_fut -rows 30  -date "$TODAY"
    sleep 1
    step -step assert_status -key bufferTrades -value 100
    step -step assert_status -key bufferAggTrades -value 30
    graceful_stop
    step -step assert_tmp -table trade_binance     -date "$TODAY" -rows 100
    step -step assert_tmp -table quote_binance     -date "$TODAY" -rows 50
    step -step assert_tmp -table trade_binance_fut -date "$TODAY" -rows 30
    step -step assert_checkpoint -table trade_binance     -date "$TODAY"
    step -step assert_checkpoint -table quote_binance     -date "$TODAY"
    step -step assert_checkpoint -table trade_binance_fut -date "$TODAY"

    # ------------------------------------------------------------------
    # Scenario 2 (continues in the same sandbox): restart after clean stop
    # ------------------------------------------------------------------
    echo ""
    echo "=== scenario 2: restart after clean stop, zero duplicates ==="
    SCENARIO=2
    start_wdb T2S_WDB_ROLL_GRACE_SEC=0
    step -step assert_status -key replayRowsApplied -value 0
    step -step assert_status -key duplicatesDropped -value 0
    # With a non-zero checkpoint, every table must still accept live rows
    # (regression: a wrong per-table tpSeqNo index drops quotes/futures here)
    step -step publish -table trade_binance     -rows 100 -date "$TODAY"
    step -step publish -table quote_binance     -rows 50  -date "$TODAY"
    step -step publish -table trade_binance_fut -rows 30  -date "$TODAY"
    sleep 1
    step -step assert_status -key duplicatesDropped -value 0
    step -step assert_status -key quotesRecv -value 50
    step -step assert_status -key aggTradesRecv -value 30
    graceful_stop
    step -step assert_tmp -table trade_binance     -date "$TODAY" -rows 200
    step -step assert_tmp -table quote_binance     -date "$TODAY" -rows 100
    step -step assert_tmp -table trade_binance_fut -date "$TODAY" -rows 60
    step -step assert_vs_tplog -table trade_binance
    step -step assert_vs_tplog -table quote_binance
    step -step assert_vs_tplog -table trade_binance_fut
}
end_scenario

# ============================================================================
# Scenario 3: kill -9 mid-run, restart, no loss, no duplicates
# ============================================================================
begin_scenario 3 "kill -9 mid-run, restart, overlap replay deduped" && {
    start_wdb T2S_WDB_ROLL_GRACE_SEC=0 T2S_WDB_MAXROWS=25
    step -step publish -table trade_binance     -rows 100 -date "$TODAY"
    step -step publish -table quote_binance     -rows 40  -date "$TODAY"
    step -step publish -table trade_binance_fut -rows 30  -date "$TODAY"
    sleep 1
    hard_kill_wdb
    # Restart: replay from the checkpoint written by the last interval flush
    start_wdb T2S_WDB_ROLL_GRACE_SEC=0 T2S_WDB_MAXROWS=25
    step -step assert_status -key halted -value 0
    step -step publish -table trade_binance     -rows 50 -date "$TODAY"
    step -step publish -table quote_binance     -rows 20 -date "$TODAY"
    step -step publish -table trade_binance_fut -rows 10 -date "$TODAY"
    sleep 1
    graceful_stop
    step -step assert_vs_tplog -table trade_binance
    step -step assert_vs_tplog -table quote_binance
    step -step assert_vs_tplog -table trade_binance_fut
    step -step assert_tmp -table trade_binance     -date "$TODAY" -rows 150
    step -step assert_tmp -table quote_binance     -date "$TODAY" -rows 60
    step -step assert_tmp -table trade_binance_fut -date "$TODAY" -rows 40
    # Simulate a crash between the tmp write and the checkpoint save: rewind
    # the checkpoint below what is on disk. The next start must raise it back
    # from the tmp dirs and replay nothing, so no duplicates are written.
    step -step rewind_checkpoint -table trade_binance -by 30
    step -step rewind_checkpoint -table quote_binance -by 10
    start_wdb T2S_WDB_ROLL_GRACE_SEC=0 T2S_WDB_MAXROWS=25
    grep -q "checkpoint BEHIND disk" "$WDB_LOG" || fail "WDB log lacks 'checkpoint BEHIND disk'"
    step -step assert_status -key checkpointBehindDisk -value 1
    step -step assert_status -key replayRowsApplied -value 0
    step -step assert_status -key duplicatesDropped -value 0
    step -step assert_status -key lastTpSeqNoTrade -value 220
    # A resend of an already-persisted row (tpSeqNo below the floor) must be
    # dropped at receipt, logged and counted.
    step -step inject_dup -table trade_binance     -seq 150 -date "$TODAY"
    step -step inject_dup -table quote_binance     -seq 150 -date "$TODAY"
    step -step inject_dup -table trade_binance_fut -seq 150 -date "$TODAY"
    sleep 1
    step -step assert_status -key duplicatesDropped -value 3
    step -step assert_status -key bufferTrades -value 0
    step -step assert_status -key bufferQuotes -value 0
    step -step assert_status -key bufferAggTrades -value 0
    grep -q "DUPLICATE dropped - trade_binance tpSeqNo=150" "$WDB_LOG" || fail "WDB log lacks DUPLICATE line (trade)"
    grep -q "DUPLICATE dropped - quote_binance tpSeqNo=150" "$WDB_LOG" || fail "WDB log lacks DUPLICATE line (quote)"
    grep -q "DUPLICATE dropped - trade_binance_fut tpSeqNo=150" "$WDB_LOG" || fail "WDB log lacks DUPLICATE line (fut)"
    step -step publish -table trade_binance -rows 10 -date "$TODAY"
    sleep 1
    graceful_stop
    step -step assert_vs_tplog -table trade_binance
    step -step assert_vs_tplog -table quote_binance
    step -step assert_vs_tplog -table trade_binance_fut
    step -step assert_tmp -table trade_binance -date "$TODAY" -rows 160
}
end_scenario

# ============================================================================
# Scenario 4: WDB disconnected across midnight; partition by row time
# ============================================================================
begin_scenario 4 "missed midnight: rows land in their own date partitions" && {
    start_wdb T2S_WDB_ROLL_GRACE_SEC=0 "T2S_WDB_FAKE_DATE=$D_MINUS_2"
    step -step publish -table trade_binance -rows 40 -date "$D_MINUS_2"
    sleep 1
    hard_kill_wdb
    # Day changes while WDB is away; TP keeps logging. No endofday reaches WDB.
    step -step publish -table trade_binance -rows 30 -date "$D_MINUS_1"
    step -step publish -table quote_binance -rows 20 -date "$D_MINUS_1"
    start_wdb T2S_WDB_ROLL_GRACE_SEC=0 "T2S_WDB_FAKE_DATE=$D_MINUS_1"
    step -step assert_status -key replayRowsApplied -value 90
    # The startup roll runs right after the first replay: everything is
    # flushed by date, and D-2 (a past date for a WDB whose today is D-1)
    # is already in the HDB; D-1 rows sit in tmp.D-1.
    step -step assert_partition -table trade_binance -date "$D_MINUS_2" -rows 40
    step -step assert_tmp -table trade_binance -date "$D_MINUS_1" -rows 30
    step -step assert_tmp -table quote_binance -date "$D_MINUS_1" -rows 20
    # WDB's own clock passes midnight: roll D-1
    step -step set_clock -date "$TODAY"
    wait_roll 7
    step -step assert_partition -table trade_binance -date "$D_MINUS_2" -rows 40
    step -step assert_partition -table trade_binance -date "$D_MINUS_1" -rows 30
    step -step assert_partition -table quote_binance -date "$D_MINUS_1" -rows 20
    step -step assert_no_tmp -date "$D_MINUS_2"
    step -step assert_no_tmp -date "$D_MINUS_1"
    step -step assert_status -key lastRollDate -value "$D_MINUS_1"
    step -step assert_status -key lateRows -value 0
    step -step assert_status -key status -value ok
    # A straggler for an already-rolled date: kept, counted, never merged
    step -step publish -table trade_binance -rows 5 -date "$D_MINUS_1"
    sleep 1
    step -step flush
    step -step assert_status -key lateRows -value 5
    step -step assert_status -key status -value degraded
    step -step assert_tmp -table trade_binance -date "$D_MINUS_1" -rows 5
    step -step assert_partition -table trade_binance -date "$D_MINUS_1" -rows 30
    graceful_stop
    step -step assert_vs_tplog -table trade_binance
    step -step assert_vs_tplog -table quote_binance
}
end_scenario

# ============================================================================
# Scenario 5: TP restart on a new day with no log -> tpSeqNo continues
# ============================================================================
begin_scenario 5 "TP restart on a new day with no log: counter continues, WDB healthy" && {
    start_wdb T2S_WDB_ROLL_GRACE_SEC=0 "T2S_WDB_FAKE_DATE=$D_MINUS_1"
    step -step publish -table trade_binance     -rows 50 -date "$D_MINUS_1"
    step -step publish -table quote_binance     -rows 20 -date "$D_MINUS_1"
    step -step publish -table trade_binance_fut -rows 10 -date "$D_MINUS_1"
    sleep 1
    step -step flush
    step -step assert_checkpoint -table trade_binance -date "$D_MINUS_1"
    graceful_stop
    # "Next day": TP restarted with a fake date for which no log exists. The
    # reservation file keeps tpSeqNo monotonic.
    stop_tp
    TP_EXTRA=("T2S_TP_FAKE_DATE=$D_PLUS_1")
    start_tp
    grep -q "T2S_TP_FAKE_DATE is set" "$T2S_SANDBOX/tp_${SCENARIO}.log" || fail "TP log lacks the fake-date warning"
    grep -q "no log for today" "$T2S_SANDBOX/tp_${SCENARIO}.log" || fail "TP did not report a missing log for its (fake) today"
    step -step publish -table trade_binance     -rows 10 -date "$TODAY"
    step -step publish -table quote_binance     -rows 5  -date "$TODAY"
    step -step publish -table trade_binance_fut -rows 5  -date "$TODAY"
    start_wdb T2S_WDB_ROLL_GRACE_SEC=0
    step -step assert_status -key halted -value 0
    step -step assert_status -key status -value ok
    step -step assert_status -key duplicatesDropped -value 0
    step -step assert_status -key replayRowsApplied -value 20
    # yesterday's tmp dir was rolled right after the first replay (it is a
    # past date now); today's replayed rows were flushed by the same roll
    step -step assert_partition -table trade_binance -date "$D_MINUS_1" -rows 50
    step -step assert_tmp -table trade_binance -date "$TODAY" -rows 10
    graceful_stop
    step -step assert_tmp -table trade_binance -date "$TODAY" -rows 10
    step -step assert_checkpoint -table trade_binance -date "$TODAY"
    step -step assert_vs_tplog -table trade_binance
    step -step assert_vs_tplog -table quote_binance
    step -step assert_vs_tplog -table trade_binance_fut
    TP_EXTRA=()
}
end_scenario

# ============================================================================
# Scenario 6: tpSeqNo really goes backwards -> WDB halts (safety net)
# ============================================================================
begin_scenario 6 "TP counter goes backwards (seq file and logs removed): halt" && {
    start_wdb T2S_WDB_ROLL_GRACE_SEC=0
    step -step publish -table trade_binance -rows 50 -date "$TODAY"
    sleep 1
    step -step flush
    step -step assert_checkpoint -table trade_binance -date "$TODAY"
    graceful_stop
    stop_tp
    # Remove the reservation file AND every log: TP can only seed from 0.
    rm -f "$T2S_SB_TPLOGS"/*.log "$T2S_SB_TPSEQ" "$T2S_SANDBOX/fhseq"
    start_tp
    grep -q "BELOW WDB's persisted checkpoint" "$T2S_SANDBOX/tp_${SCENARIO}.log" || fail "TP did not warn that its seed is below WDB's checkpoint"
    step -step publish -table trade_binance -rows 10 -date "$TODAY"
    start_wdb T2S_WDB_ROLL_GRACE_SEC=0
    step -step assert_status -key halted -value 1
    step -step assert_status -key status -value error
    step -step assert_status -key replayRowsApplied -value 0
    step -step publish -table trade_binance -rows 5 -date "$TODAY"
    sleep 1
    step -step assert_status -key haltedRowsDropped -value 5
    grep -q "HALTED" "$WDB_LOG" || fail "WDB log lacks HALTED message"
    graceful_stop
    # Nothing new was written: still the 50 rows from before the halt
    step -step assert_tmp -table trade_binance -date "$TODAY" -rows 50
}
end_scenario

echo ""
echo "==========================================="
if [[ $FAILURES -eq 0 ]]; then
    echo "WDB durability: all scenarios passed"
    echo "==========================================="
    exit 0
fi
echo "WDB durability: $FAILURES failure(s)"
echo "==========================================="
exit 1
