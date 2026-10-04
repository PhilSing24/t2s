#!/bin/bash
# test_tp_durability.sh - TP durability scenarios (sandboxed).
#
# Every process is the real kdb/tick/tp.q / wdb.q started through
# tests/t_lib.sh with all paths and ports pointed at tests/sandbox, checked
# by tests/t_guard.q after start-up. q-side steps live in
# tests/tp_dur_body.q (TP side) and tests/wdb_dur_body.q (WDB side); rows
# for all three tables (spot, futures, quotes) are realistic.
#
# Scenarios:
#   1 schema           TP runs on kdb/schemas.q; a handler announcing the
#                      wrong row width is refused at registration (counted),
#                      a row with the wrong width is rejected (counted, not
#                      logged); correct rows for all tables flow to WDB
#   2 TP restart,      SIGTERM + restart: tpSeqNo continues above the old
#     same day         value, handler sessions continue with no restart and
#                      no missed rows (even when an earlier session left
#                      higher fhSeqNo in the log), WDB healthy, disk == logs
#   3 TP restart on a  restart with a fake date for which no log exists:
#     new day          counter continues from the reservation file; then a
#                      first start WITHOUT the reservation file on yet
#                      another day seeds from the NEWEST log (not today's),
#                      stays above WDB's checkpoint, WDB healthy
#   4 kill -9 TP       mid-publish: after restart no tpSeqNo is handed out
#                      twice, logs strictly increasing, WDB healthy
#   5 handler restart  a new session id early in the day: zero rows lost,
#                      restart logged and counted; an out-of-order resend
#                      inside a session is accepted and counted; rows from
#                      an unregistered publisher are accepted and counted;
#                      a reconnect of the same session is counted
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
TP_LOG=""
WDB_LOG=""
TP_EXTRA=()

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

export TEST_TP_PORT=$T2S_PORT_TP
export TEST_WDB_PORT=$T2S_PORT_WDB
export SANDBOX_TMP_PATH=$T2S_SB_TMP
export SANDBOX_HDB_PATH=$T2S_SB_HDB
export SANDBOX_TPLOG_PATH=$T2S_SB_TPLOGS
export SANDBOX_CHECKPOINT=$T2S_SB_CHECKPOINT

TODAY=$(date -u +%Y.%m.%d)
D_PLUS_1=$(date -u -d 'tomorrow' +%Y.%m.%d)
D_PLUS_2=$(date -u -d '2 days' +%Y.%m.%d)

fail() { echo "  FAIL: $*"; FAILURES=$((FAILURES + 1)); }

tp()  { q "$SCRIPT_DIR/tp_dur_body.q"  "$@" -q < /dev/null; local rc=$?; [[ $rc -ne 0 ]] && FAILURES=$((FAILURES + 1)); return $rc; }
wdb() { q "$SCRIPT_DIR/wdb_dur_body.q" "$@" -q < /dev/null; local rc=$?; [[ $rc -ne 0 ]] && FAILURES=$((FAILURES + 1)); return $rc; }

start_tp() {
    TP_LOG="$T2S_SANDBOX/tp_${SCENARIO}_$(date +%s%N).log"
    TP_PID=$(t2s_spawn_tp "$T2S_PORT_TP" "$TP_LOG" "${TP_EXTRA[@]}")
    if ! t2s_wait_port "$T2S_PORT_TP" 6; then fail "TP did not start"; cat "$TP_LOG"; return 1; fi
    t2s_guard tp "$T2S_PORT_TP" > "$T2S_SANDBOX/guard_tp.log" || { fail "TP guard"; cat "$T2S_SANDBOX/guard_tp.log"; return 1; }
    return 0
}

start_wdb() {
    WDB_LOG="$T2S_SANDBOX/wdb_${SCENARIO}_$(date +%s%N).log"
    WDB_PID=$(t2s_spawn_wdb "$T2S_PORT_WDB" "$T2S_PORT_TP" "$WDB_LOG" T2S_WDB_ROLL_GRACE_SEC=0 "$@")
    if ! t2s_wait_port "$T2S_PORT_WDB" 6; then fail "WDB did not start"; cat "$WDB_LOG"; return 1; fi
    t2s_guard wdb "$T2S_PORT_WDB" > "$T2S_SANDBOX/guard_wdb.log" || { fail "WDB guard"; cat "$T2S_SANDBOX/guard_wdb.log"; return 1; }
    sleep 1
    return 0
}

stop_tp_term() {
    [[ -n "$TP_PID" ]] && kill -TERM "$TP_PID" 2>/dev/null && wait "$TP_PID" 2>/dev/null; TP_PID=""
    t2s_kill_port "$T2S_PORT_TP"
    echo "  TP stopped (SIGTERM)"
}

kill9_tp() {
    [[ -n "$TP_PID" ]] && kill -9 "$TP_PID" 2>/dev/null && wait "$TP_PID" 2>/dev/null; TP_PID=""
    t2s_kill_port "$T2S_PORT_TP"
    echo "  TP killed (SIGKILL)"
}

wdb_graceful_stop() {
    wdb -step shutdown || return 1
    for (( i = 0; i < 150; i++ )); do
        if ! lsof -ti:"$T2S_PORT_WDB" >/dev/null 2>&1; then wait "$WDB_PID" 2>/dev/null; WDB_PID=""; return 0; fi
        sleep 0.1
    done
    fail "WDB did not exit after shutdown request"; kill -9 "$WDB_PID" 2>/dev/null; WDB_PID=""; return 1
}

# Wait for WDB to reconnect to a restarted TP (its timer is 5s).
wait_wdb_reconnect() { sleep 7; }

begin_scenario() {
    SCENARIO=$1; shift
    echo ""
    echo "=== scenario $SCENARIO: $* ==="
    t2s_sandbox_reset
    rm -f "$T2S_SANDBOX/fhseq" "$T2S_SANDBOX"/tpseq_*
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
    wdb -step assert_vs_tplog -table quote_binance_fut
}

# ============================================================================
# Scenario 1: schema from schemas.q, width mismatches refused
# ============================================================================
begin_scenario 1 "schemas.q loaded; wrong row width refused at registration and at upd" && {
    grep -q "Schema: kdb/schemas.q" "$TP_LOG" || fail "TP did not report loading kdb/schemas.q"
    start_wdb
    tp -step register_bad -table trade_binance     -width 13
    tp -step register_bad -table quote_binance     -width 12
    # the futures trade layout before qtyExRpi (14 columns) is refused now that it has 15
    tp -step register_bad -table trade_binance_fut -width 14
    # a spot-layout quote row (30 columns) must not get into the futures quote table (32)
    tp -step register_bad -table quote_binance_fut -width 30
    tp -step tp_status -key rejectedRegistrations -value 4
    grep -q "REJECTED registration" "$TP_LOG" || fail "TP log lacks REJECTED registration line"
    tp -step publish_bad_width -table trade_binance -date "$TODAY"
    tp -step publish_bad_width -table quote_binance -date "$TODAY"
    sleep 0.5
    tp -step tp_status -key schemaMismatch -value 2
    tp -step tp_status -key logChunks -value 0
    tp -step tp_status -key status -value degraded
    grep -q "SCHEMA MISMATCH - rejected" "$TP_LOG" || fail "TP log lacks SCHEMA MISMATCH line"
    # Correct widths flow for every table
    tp -step publish -table trade_binance     -rows 20 -date "$TODAY" -session 1001
    tp -step publish -table quote_binance     -rows 10 -date "$TODAY" -session 1002
    tp -step publish -table trade_binance_fut -rows 5  -date "$TODAY" -session 1003
    tp -step publish -table quote_binance_fut -rows 7  -date "$TODAY" -session 1004
    sleep 1
    tp -step tp_status -key logChunks -value 42
    tp -step tp_status -key unregisteredRows -value 0
    tp -step fh_stats
    wdb -step assert_status -key tradesRecv -value 20
    wdb -step assert_status -key quotesRecv -value 10
    wdb -step assert_status -key aggTradesRecv -value 5
    wdb -step assert_status -key quotesFutRecv -value 7
    wdb_graceful_stop
    all_tables_match_logs
}
end_scenario

# ============================================================================
# Scenario 2: TP restart on the same day
# ============================================================================
begin_scenario 2 "TP SIGTERM + restart same day: tpSeqNo continues, sessions continue" && {
    start_wdb
    # An earlier spot session leaves HIGH fhSeqNo (1..100) in the log; the
    # handler then restarts and the live session is at a lower fhSeqNo when
    # TP restarts. TP must compare against the LAST logged row, not the
    # day's maximum, or it misreports the reconnect as another restart.
    tp -step publish -table trade_binance     -rows 100 -date "$TODAY" -session 2000
    tp -step fhseq_reset -table trade_binance
    tp -step publish -table trade_binance     -rows 50 -date "$TODAY" -session 2001
    tp -step publish -table quote_binance     -rows 20 -date "$TODAY" -session 2002
    tp -step publish -table trade_binance_fut -rows 10 -date "$TODAY" -session 2003
    sleep 1
    tp -step tp_status -key tradeRestarts -value 1
    tp -step tp_seq_save -name before_restart
    stop_tp_term
    start_tp
    grep -q "tpSeqNo resumed from reservation file" "$TP_LOG" || fail "TP did not resume tpSeqNo from the reservation file"
    grep -q "recovered fhSeqNo of the last logged row" "$TP_LOG" || fail "TP did not recover fhSeqNo from today's log"
    tp -step tp_seq_assert_gt -name before_restart
    wait_wdb_reconnect
    wdb -step assert_status -key halted -value 0
    wdb -step assert_status -key status -value ok
    # Same sessions continue: no restart, nothing missed
    tp -step publish -table trade_binance     -rows 30 -date "$TODAY" -session 2001
    tp -step publish -table quote_binance     -rows 10 -date "$TODAY" -session 2002
    tp -step publish -table trade_binance_fut -rows 5  -date "$TODAY" -session 2003
    sleep 1
    grep -q "continues for trade_binance after TP restart" "$TP_LOG" || fail "TP log lacks session-continues line"
    if grep -q "RESTART detected" "$TP_LOG"; then fail "TP misreported a reconnect after its own restart as a handler restart"; fi
    tp -step tp_status -key tradeRestarts -value 0
    tp -step tp_status -key tradeMissed -value 0
    tp -step tp_status -key missed -value 0
    tp -step tp_status -key restarts -value 0
    wdb -step assert_status -key duplicatesDropped -value 0
    wdb -step assert_status -key halted -value 0
    wdb_graceful_stop
    tp -step assert_log_monotone
    tp -step assert_log_rows -table trade_binance -rows 180
    all_tables_match_logs
}
end_scenario

# ============================================================================
# Scenario 3: TP restart on a new day with no log; migration after midnight
# ============================================================================
begin_scenario 3 "new day with no log: counter continues; migration seeds from the newest log" && {
    start_wdb
    tp -step publish -table trade_binance     -rows 40 -date "$TODAY" -session 3001
    tp -step publish -table quote_binance     -rows 20 -date "$TODAY" -session 3002
    tp -step publish -table trade_binance_fut -rows 10 -date "$TODAY" -session 3003
    sleep 1
    wdb -step flush
    tp -step tp_seq_save -name day0
    stop_tp_term
    # --- new day, no log for it, reservation file present
    TP_EXTRA=("T2S_TP_FAKE_DATE=$D_PLUS_1")
    start_tp
    grep -q "T2S_TP_FAKE_DATE is set" "$TP_LOG" || fail "TP log lacks the fake-date warning"
    grep -q "no log for today" "$TP_LOG" || fail "TP did not report a missing log for its (fake) today"
    tp -step tp_seq_assert_gt -name day0
    wait_wdb_reconnect
    wdb -step assert_status -key halted -value 0
    tp -step publish -table trade_binance     -rows 30 -date "$TODAY" -session 3001
    tp -step publish -table quote_binance     -rows 10 -date "$TODAY" -session 3002
    tp -step publish -table trade_binance_fut -rows 5  -date "$TODAY" -session 3003
    sleep 1
    wdb -step flush
    tp -step tp_seq_save -name day1
    stop_tp_term
    # --- migration: first start WITHOUT a reservation file, on yet another
    #     day. The seed must come from the newest log (day1), not from a
    #     missing today's log, and stay above WDB's checkpoint.
    rm -f "$T2S_SB_TPSEQ"
    TP_EXTRA=("T2S_TP_FAKE_DATE=$D_PLUS_2")
    start_tp
    grep -q "migrating: scanning newest log" "$TP_LOG" || fail "TP did not migrate from a log"
    grep -q "migration seed from .*${D_PLUS_1}.log" "$TP_LOG" || fail "TP did not seed from the NEWEST log (${D_PLUS_1})"
    if grep -q "BELOW WDB's persisted checkpoint" "$TP_LOG"; then fail "TP seed fell below WDB's checkpoint"; fi
    # The migrated counter equals the last number in the newest log; the
    # next number handed out is above it.
    tp -step tp_seq_assert_ge -name day1
    wait_wdb_reconnect
    wdb -step assert_status -key halted -value 0
    wdb -step assert_status -key status -value ok
    tp -step publish -table trade_binance -rows 10 -date "$TODAY" -session 3001
    sleep 1
    wdb -step assert_status -key duplicatesDropped -value 0
    wdb_graceful_stop
    tp -step assert_log_monotone
    tp -step assert_log_rows -table trade_binance -rows 80
    all_tables_match_logs
    TP_EXTRA=()
}
end_scenario

# ============================================================================
# Scenario 4: kill -9 of TP mid-run
# ============================================================================
begin_scenario 4 "kill -9 TP mid-publish: no tpSeqNo handed out twice" && {
    start_wdb
    tp -step publish -table trade_binance_fut -rows 10 -date "$TODAY" -session 4003
    # Long synchronous publish in the background; TP dies under it.
    ( q "$SCRIPT_DIR/tp_dur_body.q" -step publish -table trade_binance -rows 2000 -date "$TODAY" -session 4001 -q < /dev/null > "$T2S_SANDBOX/bg_publish.log" 2>&1 ) &
    BG=$!
    sleep 0.5
    kill9_tp
    wait $BG 2>/dev/null
    echo "  background publish ended ($(grep -c . "$T2S_SANDBOX/bg_publish.log") log lines)"
    start_tp
    grep -q "tpSeqNo resumed from reservation file" "$TP_LOG" || fail "TP did not resume tpSeqNo from the reservation file after kill -9"
    wait_wdb_reconnect
    # The publisher's session was cut mid-stream; a new handler session
    # takes over for trades, which TP must report as a restart, not a gap.
    tp -step fhseq_reset -table trade_binance
    tp -step publish -table trade_binance     -rows 50 -date "$TODAY" -session 4002
    tp -step publish -table quote_binance     -rows 20 -date "$TODAY" -session 4004
    tp -step publish -table trade_binance_fut -rows 5  -date "$TODAY" -session 4003
    sleep 1
    tp -step tp_status -key tradeRestarts -value 1
    wdb -step assert_status -key halted -value 0
    wdb -step assert_status -key duplicatesDropped -value 0
    wdb_graceful_stop
    tp -step assert_log_monotone
    all_tables_match_logs
}
end_scenario

# ============================================================================
# Scenario 5: handler restart, out-of-order resend, unregistered publisher
# ============================================================================
begin_scenario 5 "handler restart early in the day: nothing lost, logged and counted" && {
    start_wdb
    tp -step publish -table trade_binance -rows 50 -date "$TODAY" -session 5001
    tp -step publish -table quote_binance -rows 10 -date "$TODAY" -session 5010
    # The spot handler restarts: new session id, fhSeqNo back to 1
    tp -step fhseq_reset -table trade_binance
    tp -step publish -table trade_binance -rows 30 -date "$TODAY" -session 5002
    sleep 1
    grep -q "FH RESTART detected for trade_binance: session 5001 -> 5002" "$TP_LOG" || fail "TP log lacks the FH RESTART line"
    tp -step tp_status -key tradeRestarts -value 1
    tp -step tp_status -key tradeMissed -value 0
    tp -step tp_status -key tradeGaps -value 0
    tp -step assert_log_rows -table trade_binance -rows 80
    tp -step assert_fh_sessions -table trade_binance -restarts 1
    # Reconnect of the same session (new handle, same id): counted, no gap
    tp -step publish -table trade_binance -rows 10 -date "$TODAY" -session 5002
    sleep 0.5
    tp -step tp_status -key tradeReconnects -value 1
    tp -step tp_status -key tradeMissed -value 0
    # Out-of-order resend inside the session (another reconnect of 5002):
    # accepted and counted
    tp -step publish_seq -table trade_binance -seq 10 -date "$TODAY" -session 5002
    sleep 0.5
    grep -q "OUT OF ORDER fhSeqNo 10" "$TP_LOG" || fail "TP log lacks the OUT OF ORDER line"
    tp -step tp_status -key tradeOutOfOrder -value 1
    tp -step tp_status -key tradeReconnects -value 2
    tp -step assert_log_rows -table trade_binance -rows 91
    # Unregistered publisher: accepted and counted
    tp -step publish -table trade_binance_fut -rows 7 -date "$TODAY"
    sleep 0.5
    grep -q "without a session registration" "$TP_LOG" || fail "TP log lacks the unregistered-publisher line"
    tp -step tp_status -key unregisteredRows -value 7
    tp -step tp_status -key restarts -value 1
    tp -step tp_status -key reconnects -value 2
    tp -step tp_status -key outOfOrder -value 1
    wdb -step assert_status -key duplicatesDropped -value 0
    wdb_graceful_stop
    tp -step assert_log_monotone
    all_tables_match_logs
}
end_scenario

echo ""
echo "==========================================="
if [[ $FAILURES -eq 0 ]]; then
    echo "TP durability: all scenarios passed"
    echo "==========================================="
    exit 0
fi
echo "TP durability: $FAILURES failure(s)"
echo "==========================================="
exit 1
