#!/bin/bash
# test_midnight.sh - the daily roll with nobody watching (sandboxed).
#
# Real kdb/tick/tp.q and wdb.q on fake dates (tests/t_lib.sh, tests/t_guard.q):
#   1. rows for day D on all three tables, nothing flushed by hand
#   2. TP's clock passes midnight: it rotates its log to D+1 and broadcasts
#      endofday; then WDB's clock passes midnight and it rolls day D
#   3. rows for D+1 keep flowing into the new log and the new tmp dir
#   4. ./check_eod.sh D, run against the sandbox, must confirm the day:
#      partition present, every logged row for D in it, no tmp.D left
#   5. the same check for a day that is NOT complete must fail
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

fail() { echo "  FAIL: $*"; FAILURES=$((FAILURES + 1)); }
tp()  { q "$SCRIPT_DIR/tp_dur_body.q"  "$@" -q < /dev/null; local rc=$?; [[ $rc -ne 0 ]] && FAILURES=$((FAILURES + 1)); return $rc; }
wdb() { q "$SCRIPT_DIR/wdb_dur_body.q" "$@" -q < /dev/null; local rc=$?; [[ $rc -ne 0 ]] && FAILURES=$((FAILURES + 1)); return $rc; }
qtp() {  # evaluate on the sandbox TP
    q -q -p 0 < /dev/null <<QEOF
h:hopen (\`\$":localhost:$T2S_PORT_TP"; 3000); r:h "$1"; hclose h; -1 .Q.s1 r; system "sleep 0.05"; exit 0
QEOF
}

echo ""
echo "=== midnight roll, unattended ==="
t2s_sandbox_reset; rm -f "$T2S_SANDBOX/fhseq"
TP_LOG="$T2S_SANDBOX/tp.log"
TP_PID=$(t2s_spawn_tp "$T2S_PORT_TP" "$TP_LOG" "T2S_TP_FAKE_DATE=$D0")
t2s_wait_port "$T2S_PORT_TP" 6 || { fail "TP did not start"; cat "$TP_LOG"; exit 1; }
t2s_guard tp "$T2S_PORT_TP" > /dev/null || { fail "TP guard"; exit 1; }
WDB_LOG="$T2S_SANDBOX/wdb.log"
WDB_PID=$(t2s_spawn_wdb "$T2S_PORT_WDB" "$T2S_PORT_TP" "$WDB_LOG" T2S_WDB_ROLL_GRACE_SEC=0 "T2S_WDB_FAKE_DATE=$D0")
t2s_wait_port "$T2S_PORT_WDB" 6 || { fail "WDB did not start"; cat "$WDB_LOG"; exit 1; }
t2s_guard wdb "$T2S_PORT_WDB" > /dev/null || { fail "WDB guard"; exit 1; }
sleep 1

# Day D0: rows on every table, left in WDB's memory (no manual flush)
tp -step publish -table trade_binance     -rows 120 -date "$D0" -session 9001
tp -step publish -table quote_binance     -rows 60  -date "$D0" -session 9002
tp -step publish -table trade_binance_fut -rows 30  -date "$D0" -session 9003
sleep 1
wdb -step assert_status -key bufferTrades -value 120

# Midnight at TP: log rotates, endofday broadcast
echo "  TP clock -> $D1"
qtp ".tp.clock.set[$D1]" > /dev/null
sleep 2
grep -q "Midnight UTC detected" "$TP_LOG" || fail "TP did not detect midnight"
grep -q "endofday received from TP" "$WDB_LOG" || fail "WDB did not receive endofday"
[[ -f "$T2S_SB_TPLOGS/$D1.log" ]] && echo "  PASS: TP rotated to $D1.log" || fail "TP did not open $D1.log"

# Midnight at WDB: roll day D0 (its own clock, independent of the message)
echo "  WDB clock -> $D1"
wdb -step set_clock -date "$D1"
sleep 7
grep -q "HDB partition created" "$WDB_LOG" || fail "WDB did not create the partition"
wdb -step assert_partition -table trade_binance     -date "$D0" -rows 120
wdb -step assert_partition -table quote_binance     -date "$D0" -rows 60
wdb -step assert_partition -table trade_binance_fut -date "$D0" -rows 30
wdb -step assert_no_tmp -date "$D0"
wdb -step assert_status -key lastRollDate -value "$D0"
wdb -step assert_status -key status -value ok

# The new day keeps flowing into the new log and the new tmp dir
tp -step publish -table trade_binance     -rows 40 -date "$D1" -session 9001
tp -step publish -table quote_binance     -rows 20 -date "$D1" -session 9002
tp -step publish -table trade_binance_fut -rows 10 -date "$D1" -session 9003
sleep 1
wdb -step flush
wdb -step assert_tmp -table trade_binance -date "$D1" -rows 40
tp  -step assert_log_rows -table trade_binance -rows 160
tp  -step assert_log_monotone
wdb -step assert_status -key lateRows -value 0
wdb -step assert_status -key status -value ok
qtp ".health[]\`status" | grep -q "ok" && echo "  PASS: TP healthy after rollover" || fail "TP not healthy after rollover"

# check_eod.sh against the sandbox: D0 complete, D1 (still open) not
echo "  --- ./check_eod.sh $D0 (sandbox)"
OUT=$(T2S_TP_LOG_DIR="$T2S_SB_TPLOGS" T2S_HDB_DIR="$T2S_SB_HDB" T2S_TMP_DIR="$T2S_SB_TMP" ./check_eod.sh "$D0" 2>&1); RC=$?
echo "$OUT" | sed 's/^/    /'
[[ $RC -eq 0 ]] && echo "  PASS: check_eod.sh confirms $D0" || fail "check_eod.sh did not confirm $D0"
echo "  --- ./check_eod.sh $D1 (sandbox, day still open: must report incomplete)"
OUT=$(T2S_TP_LOG_DIR="$T2S_SB_TPLOGS" T2S_HDB_DIR="$T2S_SB_HDB" T2S_TMP_DIR="$T2S_SB_TMP" ./check_eod.sh "$D1" 2>&1); RC=$?
echo "$OUT" | sed 's/^/    /'
[[ $RC -ne 0 ]] && echo "  PASS: check_eod.sh reports $D1 as needing attention" || fail "check_eod.sh wrongly confirmed the open day $D1"

# ----------------------------------------------------------------------------
# Rows received just after midnight but logged before TP rotated: they sit in
# D0's log and belong to D1. Seen on the first real midnight under traffic
# (12 such rows): check_eod must not call D0 incomplete because of them.
# ----------------------------------------------------------------------------
echo ""
echo "=== rows dated the next day in a day's log ==="
kill -9 "$WDB_PID" "$TP_PID" 2>/dev/null; WDB_PID=""; TP_PID=""
t2s_kill_port "$T2S_PORT_TP"; t2s_kill_port "$T2S_PORT_WDB"
t2s_sandbox_reset; rm -f "$T2S_SANDBOX/fhseq"
TP_PID=$(t2s_spawn_tp "$T2S_PORT_TP" "$TP_LOG" "T2S_TP_FAKE_DATE=$D0")
t2s_wait_port "$T2S_PORT_TP" 6 || { fail "TP did not start"; exit 1; }
WDB_PID=$(t2s_spawn_wdb "$T2S_PORT_WDB" "$T2S_PORT_TP" "$WDB_LOG" T2S_WDB_ROLL_GRACE_SEC=0 "T2S_WDB_FAKE_DATE=$D0")
t2s_wait_port "$T2S_PORT_WDB" 6 || { fail "WDB did not start"; exit 1; }
sleep 1
tp -step publish -table trade_binance -rows 50 -date "$D0" -session 9101
tp -step publish -table quote_binance -rows 20 -date "$D0" -session 9102
sleep 1
# WDB's clock passes midnight first (it rolls D0); TP has not rotated yet
wdb -step set_clock -date "$D1"; sleep 7
tp -step publish -table trade_binance -rows 3 -date "$D1" -session 9101     # in D0's log, dated D1
tp -step publish -table quote_binance -rows 2 -date "$D1" -session 9102
sleep 1
qtp ".tp.clock.set[$D1]" > /dev/null; sleep 2
wdb -step assert_partition -table trade_binance -date "$D0" -rows 50
wdb -step shutdown; sleep 2; WDB_PID=""
OUT=$(T2S_TP_LOG_DIR="$T2S_SB_TPLOGS" T2S_HDB_DIR="$T2S_SB_HDB" T2S_TMP_DIR="$T2S_SB_TMP" ./check_eod.sh "$D0" 2>&1); RC=$?
echo "$OUT" | grep -E "check-eod|belong to|rows," | sed 's/^/    /' | cut -c1-230
[[ $RC -eq 0 ]] && echo "  PASS: check_eod.sh confirms $D0 although its log holds rows of $D1" || fail "check_eod.sh rejected $D0 (rc=$RC)"
echo "$OUT" | grep -q "5 row(s) in this log were received just after midnight and belong to $D1 (5 already on disk in tmp.$D1" && echo "  PASS: the five next-day rows are reported, and found on disk in tmp.$D1" || fail "next-day rows not reported"
echo "$OUT" | grep -q "trade_binance:53 rows, 0 missing (+3 for the next day)" && echo "  PASS: per-table detail separates them from missing rows" || fail "per-table detail"
SUM=$(T2S_TP_LOG_DIR="$T2S_SB_TPLOGS" T2S_HDB_DIR="$T2S_SB_HDB" T2S_TMP_DIR="$T2S_SB_TMP" T2S_LOG_RETENTION_DAYS=0 q kdb/utils/logmgr.q -retention < /dev/null 2>&1)
echo "$SUM" | grep "^$D0" | grep -q " keep " && echo "  PASS: retention stays strict: $D0's log is kept until those rows are in the HDB too" || { fail "retention would delete $D0's log"; echo "$SUM" | grep "^$D0"; }
# a row of D0 that really is missing must still fail the check
rm -rf "$T2S_SB_HDB/$D0/quote_binance"
T2S_TP_LOG_DIR="$T2S_SB_TPLOGS" T2S_HDB_DIR="$T2S_SB_HDB" T2S_TMP_DIR="$T2S_SB_TMP" ./check_eod.sh "$D0" > /dev/null 2>&1 && fail "check_eod confirmed a day with a table missing" || echo "  PASS: rows of $D0 that are really missing still fail the check"

echo ""; echo "==========================================="
if [[ $FAILURES -eq 0 ]]; then echo "Midnight roll: all checks passed"; echo "==========================================="; exit 0; fi
echo "Midnight roll: $FAILURES failure(s)"; echo "==========================================="; exit 1
