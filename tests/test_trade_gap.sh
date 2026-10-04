#!/bin/bash
# test_trade_gap.sh - gaps in the exchange's trade ids are recorded (sandboxed).
#
# build/sim_trade_publisher publishes synthetic trades through the handlers'
# own TpPublisher, TradeIdTracker and GapEventQueue.
#   1. a jump in the trade ids becomes a `detected` row in trade_gap, with
#      the exact missing range, in TP's log and in WDB's copy
#   2. a gap event raised while TP is down is not lost: it stays queued and
#      is logged once TP is back
#   3. TP refuses a malformed event and an unknown event table
#   4. trades missed while the handler was not running: a restarted handler
#      takes the last logged id per symbol from TP and records the gap
#   5. the same with TP killed in between (and its session file removed):
#      TP rebuilds the ids and the open gaps from its log
#   6. the same on a new day before any row is logged: the session file
#      carries the state
#   7. backfill: the missing trades are fetched (from a fake exchange) and
#      published; every missing id is on disk exactly once, marked by a null
#      exchEventTimeMs; the gap goes detected -> partial -> recovered
#   8. REST failures are retried without loss or duplication
#   9. unrecoverable gaps carry their reason: tooLarge, notServed, restFailed
#  10. a handler killed mid-backfill: the next run resumes from what TP has
#      logged, without fetching or publishing any id twice
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
gap() { q "$SCRIPT_DIR/trade_gap_body.q" "$@" -q < /dev/null; local rc=$?; [[ $rc -ne 0 ]] && FAILURES=$((FAILURES + 1)); return $rc; }
wdb() { q "$SCRIPT_DIR/wdb_dur_body.q" "$@" -q < /dev/null; local rc=$?; [[ $rc -ne 0 ]] && FAILURES=$((FAILURES + 1)); return $rc; }
start_tp() {
    TP_LOG="$T2S_SANDBOX/tp_${SCENARIO}_$(date +%s%N).log"
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
sim_bg() { "$SIM" --port "$T2S_PORT_TP" "$@" > "$T2S_SANDBOX/sim_${SCENARIO}.log" 2>&1 < /dev/null & SIM_PID=$!; }
sim_wait() { wait "$SIM_PID"; SIM_RC=$?; SIM_PID=""; SIM_LINE=$(grep "^SIM " "$T2S_SANDBOX/sim_${SCENARIO}.log" | tail -1); echo "  $SIM_LINE"; }
sim_field() { echo "$SIM_LINE" | sed -n "s/.*$1=\([0-9]*\).*/\1/p"; }
qtp() { q -q -p 0 < /dev/null <<QEOF
h:hopen (\`\$":localhost:$T2S_PORT_TP"; 3000); -1 .Q.s1 h "$1"; hclose h; exit 0
QEOF
}

echo ""; SCENARIO=1
echo "=== scenario 1: a jump in the trade ids is recorded ==="
t2s_sandbox_reset; rm -f "$T2S_SANDBOX/fhseq"
start_tp; start_wdb
# ids 1000..1199, then 40 ids skipped, then 1240..1539
sim_bg --rows 500 --rate 2000 --session 9101 --first-id 1000 --gap-at 200 --gap-size 40 --sym ETHUSDT; sim_wait
[[ $SIM_RC -eq 0 ]] && pass "publisher finished" || fail "publisher rc=$SIM_RC"
[[ "$(sim_field gaps)" == "1" && "$(sim_field gapEventsAcked)" == "1" && "$(sim_field gapEventsPending)" == "0" ]] && pass "one gap detected, its event acknowledged by TP" || fail "sim: $SIM_LINE"
gap -step assert_events -count 1 -statuses detected
gap -step assert_gap -status detected -first 1200 -last 1239 -recovered 0 -src trade_binance -sym ETHUSDT
[[ "$(qtp '.tp.ctr.events`trade_gap')" == "1" ]] && pass "TP counted one trade_gap event" || fail "TP event counter $(qtp '.tp.ctr.events`trade_gap')"
[[ "$(qtp '.tp.ctr.missed`trade_binance')" == "0" ]] && pass "the id gap is not a TP-hop gap: TP missed = 0" || fail "TP missed"
wdb_graceful_stop
gap -step assert_disk -count 1
wdb -step assert_vs_tplog -table trade_binance
stop_tp

echo ""; SCENARIO=2
echo "=== scenario 2: a gap event raised while TP is down is kept and logged later ==="
t2s_sandbox_reset; rm -f "$T2S_SANDBOX/fhseq"
# TP is NOT running yet: the publisher queues the event and keeps trying to connect
sim_bg --rows 200 --rate 2000 --session 9102 --first-id 5000 --pending-gap 4900:4999
sleep 1.5
kill -0 "$SIM_PID" 2>/dev/null && pass "publisher is waiting for TP with the event queued" || fail "publisher exited early"
start_tp; start_wdb
sim_wait
[[ $SIM_RC -eq 0 ]] && pass "publisher finished once TP was up" || fail "publisher rc=$SIM_RC"
[[ "$(sim_field gapEventsAcked)" == "1" && "$(sim_field gapEventsPending)" == "0" ]] && pass "the queued event was acknowledged" || fail "sim: $SIM_LINE"
gap -step assert_events -count 1 -statuses detected
gap -step assert_gap -status detected -first 4900 -last 4999 -src trade_binance
wdb_graceful_stop
gap -step assert_disk -count 1

echo ""; SCENARIO=3
echo "=== scenario 3: TP refuses malformed events ==="
[[ "$(qtp '@[{.tp.event[`trade_gap; 1 2 3]}; 0; {x}]')" == *"event width mismatch"* ]] && pass "wrong width refused" || fail "wrong width accepted"
[[ "$(qtp '@[{.tp.event[`trade_binance; 1 2 3]}; 0; {x}]')" == *"not an event table"* ]] && pass "a data table is not an event table" || fail "data table accepted as event table"
gap -step assert_events -count 1
stop_tp

restart_gap_checks() {   # after publisher B: gap 1300..1349 left by the "downtime"
    [[ $SIM_RC -eq 0 ]] && pass "restarted publisher finished" || fail "publisher rc=$SIM_RC"
    [[ "$(sim_field seeded)" -ge 1 ]] && pass "it took the last logged id from TP" || fail "not seeded: $SIM_LINE"
    [[ "$(sim_field gaps)" == "1" && "$(sim_field gapEventsAcked)" == "1" ]] && pass "the downtime gap was detected and acknowledged" || fail "sim: $SIM_LINE"
    gap -step assert_gap -status detected -first 1300 -last 1349 -reason handlerRestart -src trade_binance
    [[ "$(qtp 'exec count i from .tp.gapStatus[] where status in `detected`partial')" == "1" ]] && pass "TP lists it as an open gap" || fail "TP open gaps: $(qtp '.tp.gapStatus[]')"
    [[ "$(qtp '(.tp.tradeState `trade_binance) 3 4')" == "(,1300;,1349)" ]] && pass "TP would hand the open gap to the next handler start" || fail "tradeState: $(qtp '.tp.tradeState `trade_binance')"
}

echo ""; SCENARIO=4
echo "=== scenario 4: trades missed while the handler was not running ==="
t2s_sandbox_reset; rm -f "$T2S_SANDBOX/fhseq"
start_tp
sim_bg --rows 300 --rate 3000 --session 9201 --first-id 1000; sim_wait          # ids 1000..1299, then the handler "dies"
[[ "$(qtp '.tp.lastId[`trade_binance;`BTCUSDT]')" == "1299" ]] && pass "TP tracks the last logged trade id per symbol" || fail "TP lastId $(qtp '.tp.lastId')"
sim_bg --rows 100 --rate 3000 --session 9202 --first-id 1350 --seed-from-tp 1; sim_wait   # first live id after the restart: 1350
restart_gap_checks
gap -step assert_events -count 1 -statuses detected
stop_tp

echo ""; SCENARIO=5
echo "=== scenario 5: the same with TP killed in between (state rebuilt from the log) ==="
t2s_sandbox_reset; rm -f "$T2S_SANDBOX/fhseq"
start_tp
sim_bg --rows 300 --rate 3000 --session 9301 --first-id 1000; sim_wait
kill -9 "$TP_PID" 2>/dev/null; sleep 0.3; TP_PID=""; t2s_kill_port "$T2S_PORT_TP"
rm -f "$T2S_SB_TPLOGS/tp.sessions"            # worst case: no session file either
start_tp
[[ "$(qtp '.tp.lastId[`trade_binance;`BTCUSDT]')" == "1299" ]] && pass "restarted TP recovered the last trade id from its log" || fail "TP lastId after restart $(qtp '.tp.lastId')"
sim_bg --rows 100 --rate 3000 --session 9302 --first-id 1350 --seed-from-tp 1; sim_wait
restart_gap_checks
# and the open gap itself survives another TP restart
kill -9 "$TP_PID" 2>/dev/null; sleep 0.3; TP_PID=""; t2s_kill_port "$T2S_PORT_TP"
start_tp
[[ "$(qtp 'exec count i from .tp.gapStatus[] where status = `detected')" == "1" ]] && pass "the open gap is still known after a second kill -9 (from the log)" || fail "gap lost across TP restart"
stop_tp

echo ""; SCENARIO=6
echo "=== scenario 6: a new day with no log yet (state carried by the session file) ==="
t2s_sandbox_reset; rm -f "$T2S_SANDBOX/fhseq"
D1=$(date -u -d '3 days ago' +%Y.%m.%d); D2=$(date -u -d '2 days ago' +%Y.%m.%d)
start_tp "T2S_TP_FAKE_DATE=$D1"
sim_bg --rows 300 --rate 3000 --session 9401 --first-id 1000; sim_wait
stop_tp
start_tp "T2S_TP_FAKE_DATE=$D2"
[[ ! -e "$T2S_SB_TPLOGS/$D2.log" || ! -s "$T2S_SB_TPLOGS/$D2.log" || $(stat -c %s "$T2S_SB_TPLOGS/$D2.log") -le 8 ]] && pass "no rows logged yet on the new day" || fail "unexpected rows in $D2.log"
[[ "$(qtp '.tp.lastId[`trade_binance;`BTCUSDT]')" == "1299" ]] && pass "last trade id carried over by the session file" || fail "TP lastId on the new day $(qtp '.tp.lastId')"
sim_bg --rows 100 --rate 3000 --session 9402 --first-id 1350 --seed-from-tp 1; sim_wait
restart_gap_checks
stop_tp

TODAY=$(date -u +%Y.%m.%d)

echo ""; SCENARIO=7
echo "=== scenario 7: a gap is backfilled - every missing id lands on disk once ==="
t2s_sandbox_reset; rm -f "$T2S_SANDBOX/fhseq"
start_tp; start_wdb
# ids 1000..1199 live, 1200..1449 missing (250 ids = 3 pages of 100), then 1450.. live
sim_bg --rows 600 --rate 2000 --session 9501 --first-id 1000 --gap-at 200 --gap-size 250 --backfill 1 --backfill-page 100; sim_wait
[[ $SIM_RC -eq 0 ]] && pass "publisher finished" || fail "publisher rc=$SIM_RC"
[[ "$(sim_field backfilled)" == "250" && "$(sim_field gapsRecovered)" == "1" && "$(sim_field gapsStillOpen)" == "0" ]] && pass "250 trades backfilled, gap recovered" || fail "sim: $SIM_LINE"
gap -step assert_events -count 4 -statuses detected,partial,partial,recovered
gap -step assert_gap -status recovered -first 1200 -last 1449 -recovered 250 -src trade_binance
[[ "$(qtp 'exec count i from .tp.gapStatus[] where status in `detected`partial')" == "0" ]] && pass "TP has no open gap left" || fail "TP still lists an open gap"
[[ "$(qtp '.tp.ctr.missed`trade_binance')" == "0" && "$(qtp '.tp.ctr.outOfOrder`trade_binance')" == "0" ]] && pass "TP missed = 0, outOfOrder = 0: backfilled rows are ordinary rows of the session" || fail "TP counters"
wdb_graceful_stop
gap -step assert_ids -table trade_binance -first 1200 -last 1449 -backfilled 1
gap -step assert_ids -table trade_binance -first 1000 -last 1849
wdb -step assert_vs_tplog -table trade_binance
gap -step assert_disk -count 4
stop_tp

echo ""; SCENARIO=8
echo "=== scenario 8: REST fails twice, then works - nothing lost, nothing twice ==="
t2s_sandbox_reset; rm -f "$T2S_SANDBOX/fhseq"
start_tp; start_wdb
sim_bg --rows 400 --rate 2000 --session 9601 --first-id 1000 --gap-at 100 --gap-size 60 --backfill 1 --backfill-fail 2; sim_wait
[[ "$(sim_field backfilled)" == "60" && "$(sim_field gapsRecovered)" == "1" ]] && pass "recovered after two failed requests" || fail "sim: $SIM_LINE"
gap -step assert_events -count 2 -statuses detected,recovered
wdb_graceful_stop
gap -step assert_ids -table trade_binance -first 1100 -last 1159 -backfilled 1
stop_tp

echo ""; SCENARIO=9
echo "=== scenario 9: unrecoverable gaps are recorded with their reason ==="
t2s_sandbox_reset; rm -f "$T2S_SANDBOX/fhseq"
start_tp; start_wdb
# tooLarge: 5000 missing ids with a cap of 1000
sim_bg --rows 300 --rate 3000 --session 9701 --first-id 1000 --gap-at 100 --gap-size 5000 --backfill 1 --backfill-max-gap 1000; sim_wait
gap -step assert_gap -status unrecoverable -first 1100 -last 6099 -recovered 0 -reason tooLarge
# notServed: the exchange only serves ids from 20000 on, the gap is 9100..9159
sim_bg --rows 300 --rate 3000 --session 9702 --first-id 9000 --gap-at 100 --gap-size 60 --backfill 1 --backfill-served-from 20000 --sym ETHUSDT; sim_wait
gap -step assert_gap -status unrecoverable -first 9100 -last 9159 -recovered 0 -reason notServed -sym ETHUSDT
# restFailed: the exchange never answers
sim_bg --rows 300 --rate 3000 --session 9703 --first-id 500 --gap-at 100 --gap-size 10 --backfill 1 --backfill-fail 100000 --sym SOLUSDT; sim_wait
gap -step assert_gap -status unrecoverable -first 600 -last 609 -recovered 0 -reason restFailed -sym SOLUSDT
[[ "$(qtp 'exec count i from .tp.gapStatus[] where status = `unrecoverable')" == "3" ]] && pass "TP lists the three unrecoverable gaps" || fail "TP: $(qtp '.tp.gapStatus[]')"
wdb_graceful_stop
gap -step assert_ids -table trade_binance -first 1000 -last 1099
stop_tp

echo ""; SCENARIO=10
echo "=== scenario 10: the handler dies mid-backfill; the next run resumes without duplicates ==="
t2s_sandbox_reset; rm -f "$T2S_SANDBOX/fhseq"
start_tp; start_wdb
# gap 1200..1699 (500 ids, pages of 100); the process is killed right after its 250th backfilled row
sim_bg --rows 600 --rate 2000 --session 9801 --first-id 1000 --gap-at 200 --gap-size 500 --backfill 1 --backfill-page 100 --die-after-backfilled 250
wait "$SIM_PID"; SIM_RC=$?; SIM_PID=""
[[ $SIM_RC -eq 9 ]] && pass "publisher died mid-backfill (exit 9)" || fail "publisher rc=$SIM_RC (expected 9)"
sleep 0.5
[[ "$(qtp 'exec first recoveredThroughId from .tp.gapStatus[] where firstMissingId = 1200')" == "1449" ]] && pass "TP knows from the rows it logged that ids through 1449 are in" || fail "TP progress: $(qtp '.tp.gapStatus[]')"
[[ "$(qtp '(.tp.tradeState `trade_binance) 3 4 6')" == "(,1200;,1699;,1449)" ]] && pass "the open gap and its exact progress are handed to the next run" || fail "tradeState: $(qtp '.tp.tradeState `trade_binance')"
# the restarted handler: live ids continue later; it resumes the open gap AND records the new downtime gap
LAST=$(qtp '.tp.lastId[`trade_binance;`BTCUSDT]')
sim_bg --rows 200 --rate 2000 --session 9802 --first-id $((LAST + 31)) --seed-from-tp 1 --backfill 1 --backfill-page 100; sim_wait
[[ $SIM_RC -eq 0 ]] && pass "restarted publisher finished" || fail "restarted publisher rc=$SIM_RC"
[[ "$(sim_field openGaps)" == "1" ]] && pass "it picked up the open gap" || fail "sim: $SIM_LINE"
[[ "$(sim_field backfilled)" == "280" ]] && pass "it fetched only what was missing: 250 of the old gap + 30 of its own downtime" || fail "backfilled=$(sim_field backfilled) (expected 280)"
gap -step assert_gap -status recovered -first 1200 -last 1699 -recovered 500
gap -step assert_gap -status recovered -first $((LAST + 1)) -last $((LAST + 30)) -recovered 30
[[ "$(qtp 'exec count i from .tp.gapStatus[] where status in `detected`partial')" == "0" ]] && pass "no open gap left" || fail "open gaps remain"
wdb_graceful_stop
gap -step assert_ids -table trade_binance -first 1200 -last 1699
gap -step assert_ids -table trade_binance -first $((LAST + 1)) -last $((LAST + 30))
wdb -step assert_vs_tplog -table trade_binance
stop_tp

echo ""; echo "==========================================="
if [[ $FAILURES -eq 0 ]]; then echo "Trade gap: all scenarios passed"; echo "==========================================="; exit 0; fi
echo "Trade gap: $FAILURES failure(s)"; echo "==========================================="; exit 1
