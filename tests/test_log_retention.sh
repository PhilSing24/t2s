#!/bin/bash
# test_log_retention.sh - log retention policy (sandboxed).
#
# Builds several dated logs and HDB partitions with the real TP and WDB on
# fake dates (tests/t_lib.sh, tests/t_guard.q), then runs
# kdb/utils/logmgr.q against the sandbox log dir and HDB:
#
#   D-12  complete partition, older than retention   -> deletable
#   D-11  complete, older, but protected              -> kept
#   D-10  partition missing                           -> kept
#   D-9   partition present but one table removed     -> kept (rows missing)
#   D-2   complete but younger than retention         -> kept
#   today complete so far, but today's log            -> kept
#
# The dry run must list exactly D-12 as deletable with the right reasons for
# the rest; the apply run must delete only D-12's log and index, and leave
# tp.tpSeqNo alone.
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

TODAY=$(date -u +%Y.%m.%d)
d_ago() { date -u -d "$1 days ago" +%Y.%m.%d; }
D12=$(d_ago 12); D11=$(d_ago 11); D10=$(d_ago 10); D9=$(d_ago 9); D2=$(d_ago 2)

fail() { echo "  FAIL: $*"; FAILURES=$((FAILURES + 1)); }
tp()  { q "$SCRIPT_DIR/tp_dur_body.q"  "$@" -q < /dev/null; local rc=$?; [[ $rc -ne 0 ]] && FAILURES=$((FAILURES + 1)); return $rc; }
wdb() { q "$SCRIPT_DIR/wdb_dur_body.q" "$@" -q < /dev/null; local rc=$?; [[ $rc -ne 0 ]] && FAILURES=$((FAILURES + 1)); return $rc; }

# One "day": TP and WDB on a fake date, rows for all three tables, graceful
# WDB stop (flush + checkpoint), TP stop. Leaves tmp.<date> on disk.
build_day() {
    local d=$1 rows=$2 log="$T2S_SANDBOX/tp_$d.log"
    TP_PID=$(t2s_spawn_tp "$T2S_PORT_TP" "$log" "T2S_TP_FAKE_DATE=$d")
    t2s_wait_port "$T2S_PORT_TP" 6 || { fail "TP did not start for $d"; cat "$log"; return 1; }
    t2s_guard tp "$T2S_PORT_TP" > /dev/null || { fail "TP guard $d"; return 1; }
    WDB_PID=$(t2s_spawn_wdb "$T2S_PORT_WDB" "$T2S_PORT_TP" "$T2S_SANDBOX/wdb_$d.log" T2S_WDB_ROLL_GRACE_SEC=0 "T2S_WDB_FAKE_DATE=$d")
    t2s_wait_port "$T2S_PORT_WDB" 6 || { fail "WDB did not start for $d"; return 1; }
    t2s_guard wdb "$T2S_PORT_WDB" > /dev/null || { fail "WDB guard $d"; return 1; }
    sleep 1
    tp -step publish -table trade_binance     -rows "$rows" -date "$d" -session 7001
    tp -step publish -table quote_binance     -rows "$rows" -date "$d" -session 7002
    tp -step publish -table trade_binance_fut -rows "$rows" -date "$d" -session 7003
    sleep 1
    wdb -step shutdown
    for (( i = 0; i < 100; i++ )); do lsof -ti:"$T2S_PORT_WDB" >/dev/null 2>&1 || break; sleep 0.1; done
    wait "$WDB_PID" 2>/dev/null; WDB_PID=""
    kill -TERM "$TP_PID" 2>/dev/null; wait "$TP_PID" 2>/dev/null; TP_PID=""
    t2s_kill_port "$T2S_PORT_TP"; t2s_kill_port "$T2S_PORT_WDB"
    echo "  built $d ($rows rows per table)"
}

echo ""
echo "=== building dated logs and partitions ==="
t2s_sandbox_reset; rm -f "$T2S_SANDBOX/fhseq"
for d in "$D12" "$D11" "$D10" "$D9" "$D2"; do
    rm -f "$T2S_SANDBOX/fhseq"
    build_day "$d" 40 || exit 1
done
# Roll the past dates into the HDB: a WDB on the real clock connects to a
# TP on the real clock (replay finds nothing newer) and rolls after that.
TP_PID=$(t2s_spawn_tp "$T2S_PORT_TP" "$T2S_SANDBOX/tp_today.log")
t2s_wait_port "$T2S_PORT_TP" 6 || { fail "TP (today) did not start"; exit 1; }
WDB_PID=$(t2s_spawn_wdb "$T2S_PORT_WDB" "$T2S_PORT_TP" "$T2S_SANDBOX/wdb_today.log" T2S_WDB_ROLL_GRACE_SEC=0)
t2s_wait_port "$T2S_PORT_WDB" 6 || { fail "WDB (today) did not start"; exit 1; }
sleep 2
rm -f "$T2S_SANDBOX/fhseq"
tp -step publish -table trade_binance -rows 20 -date "$TODAY" -session 7004
sleep 1
wdb -step flush
for d in "$D12" "$D11" "$D10" "$D9" "$D2"; do
    wdb -step assert_partition -table trade_binance -date "$d" -rows 40
done
wdb -step shutdown; sleep 2; wait "$WDB_PID" 2>/dev/null; WDB_PID=""
kill -TERM "$TP_PID" 2>/dev/null; wait "$TP_PID" 2>/dev/null; TP_PID=""
t2s_kill_port "$T2S_PORT_TP"; t2s_kill_port "$T2S_PORT_WDB"

# Break two days on purpose
rm -rf "$T2S_SB_HDB/$D10"
rm -rf "$T2S_SB_HDB/$D9/trade_binance"
echo "  removed partition $D10 and $D9/trade_binance"
ls "$T2S_SB_TPLOGS" | tr '\n' ' '; echo

run_logmgr() {
    env T2S_TP_LOG_DIR="$T2S_SB_TPLOGS" T2S_HDB_DIR="$T2S_SB_HDB" T2S_LOG_RETENTION_DAYS=7 T2S_LOG_PROTECTED="$D11" \
        q "$T2S_TEST_ROOT/kdb/utils/logmgr.q" "$@" < /dev/null 2>&1
}

echo ""
echo "=== retention dry run ==="
OUT="$T2S_SANDBOX/retention_dry.txt"
run_logmgr -retention | tee "$OUT"
expect_line() {  # date status reason-fragment
    if grep -E "^$1 .* $2 " "$OUT" | grep -q -- "$3"; then echo "  PASS: $1 -> $2 ($3)"; else fail "$1: expected $2 with reason '$3'"; fi
}
expect_line "$D12" delete "complete in HDB, 12 days old"
expect_line "$D11" keep   "protected"
expect_line "$D10" keep   "HDB partition $D10 missing"
expect_line "$D9"  keep   "logged rows not found"
expect_line "$D2"  keep   "only 2 day(s) old"
expect_line "$TODAY" keep "today's log"
grep -q "1 log(s) deletable" "$OUT" || fail "dry run should find exactly 1 deletable log"
grep -q "dry run - nothing deleted" "$OUT" || fail "dry run should say nothing was deleted"
for d in "$D12" "$D11" "$D10" "$D9" "$D2" "$TODAY"; do
    [[ -f "$T2S_SB_TPLOGS/$d.log" ]] || fail "dry run deleted $d.log"
done

echo ""
echo "=== retention apply ==="
OUT2="$T2S_SANDBOX/retention_apply.txt"
run_logmgr -retention -apply | tee "$OUT2" | grep -E "LOG:|delete" | head -8
[[ ! -f "$T2S_SB_TPLOGS/$D12.log" ]] && echo "  PASS: $D12.log deleted" || fail "$D12.log still present after apply"
[[ ! -f "$T2S_SB_TPLOGS/$D12.idx" ]] && echo "  PASS: $D12.idx deleted" || fail "$D12.idx still present after apply"
for d in "$D11" "$D10" "$D9" "$D2" "$TODAY"; do
    [[ -f "$T2S_SB_TPLOGS/$d.log" ]] && echo "  PASS: $d.log kept" || fail "$d.log was deleted"
done
[[ -f "$T2S_SB_TPSEQ" ]] && echo "  PASS: tp.tpSeqNo untouched" || fail "tp.tpSeqNo was deleted"

echo ""; echo "==========================================="
if [[ $FAILURES -eq 0 ]]; then echo "Log retention: all checks passed"; echo "==========================================="; exit 0; fi
echo "Log retention: $FAILURES failure(s)"; echo "==========================================="; exit 1
