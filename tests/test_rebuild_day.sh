#!/bin/bash
# test_rebuild_day.sh - rebuilding a day's partition from the TP logs (sandboxed).
#
# Builds one day D with the real TP and WDB on fake dates (rows for all
# three tables, rolled into the sandbox HDB), then:
#   1. report: the partition matches the logs
#   2. the partition is damaged (one table removed) and the report shows it
#   3. -build writes and verifies hdb/.rebuild/D
#   4. -swap moves the damaged partition to a backup and the staging dir in;
#      check_eod.sh confirms the day; the backup exists under hdb/.rebuild
#   5. the rebuilt rows carry a null wdbRecvTimeUtcNs; a straggler row for
#      D that sits in D+1's log is included
#   6. rebuilding today is refused
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
TODAY=$(date -u +%Y.%m.%d)

fail() { echo "  FAIL: $*"; FAILURES=$((FAILURES + 1)); }
tp()  { q "$SCRIPT_DIR/tp_dur_body.q"  "$@" -q < /dev/null; local rc=$?; [[ $rc -ne 0 ]] && FAILURES=$((FAILURES + 1)); return $rc; }
wdb() { q "$SCRIPT_DIR/wdb_dur_body.q" "$@" -q < /dev/null; local rc=$?; [[ $rc -ne 0 ]] && FAILURES=$((FAILURES + 1)); return $rc; }
rebuild() { T2S_TP_LOG_DIR="$T2S_SB_TPLOGS" T2S_HDB_DIR="$T2S_SB_HDB" q "$T2S_TEST_ROOT/kdb/utils/rebuild_day.q" "$@" < /dev/null 2>&1; }
check_eod() { T2S_TP_LOG_DIR="$T2S_SB_TPLOGS" T2S_HDB_DIR="$T2S_SB_HDB" T2S_TMP_DIR="$T2S_SB_TMP" ./check_eod.sh "$@" 2>&1; }
qtp() { q -q -p 0 < /dev/null <<QEOF
h:hopen (\`\$":localhost:$T2S_PORT_TP"; 3000); r:h "$1"; hclose h; system "sleep 0.05"; exit 0
QEOF
}

echo ""
echo "=== build day $D0 with the real pipeline ==="
t2s_sandbox_reset; rm -f "$T2S_SANDBOX/fhseq"
TP_PID=$(t2s_spawn_tp "$T2S_PORT_TP" "$T2S_SANDBOX/tp.log" "T2S_TP_FAKE_DATE=$D0")
t2s_wait_port "$T2S_PORT_TP" 6 || { fail "TP did not start"; exit 1; }
t2s_guard tp "$T2S_PORT_TP" > /dev/null || { fail "TP guard"; exit 1; }
WDB_PID=$(t2s_spawn_wdb "$T2S_PORT_WDB" "$T2S_PORT_TP" "$T2S_SANDBOX/wdb.log" T2S_WDB_ROLL_GRACE_SEC=0 "T2S_WDB_FAKE_DATE=$D0")
t2s_wait_port "$T2S_PORT_WDB" 6 || { fail "WDB did not start"; exit 1; }
t2s_guard wdb "$T2S_PORT_WDB" > /dev/null || { fail "WDB guard"; exit 1; }
sleep 1
tp -step publish -table trade_binance     -rows 150 -date "$D0" -session 8001
tp -step publish -table quote_binance     -rows 80  -date "$D0" -session 8002
tp -step publish -table trade_binance_fut -rows 40  -date "$D0" -session 8003
tp -step publish -table quote_binance_fut -rows 30  -date "$D0" -session 8004
sleep 1
# Midnight: TP rotates, then one straggler dated D0 lands in D1's log
qtp ".tp.clock.set[$D1]"; sleep 2
tp -step publish -table trade_binance -rows 1 -date "$D0" -session 8001
sleep 1
wdb -step set_clock -date "$D1"; sleep 7
wdb -step assert_partition -table trade_binance     -date "$D0" -rows 151
wdb -step assert_partition -table quote_binance     -date "$D0" -rows 80
wdb -step assert_partition -table trade_binance_fut -date "$D0" -rows 40
wdb -step assert_partition -table quote_binance_fut -date "$D0" -rows 30
wdb -step shutdown; sleep 2; wait "$WDB_PID" 2>/dev/null; WDB_PID=""
kill -TERM "$TP_PID" 2>/dev/null; wait "$TP_PID" 2>/dev/null; TP_PID=""
t2s_kill_port "$T2S_PORT_TP"; t2s_kill_port "$T2S_PORT_WDB"

echo ""
echo "=== 1. report on an intact partition ==="
OUT=$(rebuild -date "$D0"); echo "$OUT" | sed 's/^/    /' | grep -E "rows dated|existing partition|trade_binance:|report only"
echo "$OUT" | grep -q "rows dated $D0: trade_binance=151, trade_binance_fut=40, quote_binance=80, quote_binance_fut=30" && echo "  PASS: logs hold 151/40/80/30 rows for $D0 (straggler from $D1's log included)" || fail "report row counts"
echo "$OUT" | grep -qE "trade_binance: rows 151 +distinct 151 +logs-only 0 +dir-only 0" && echo "  PASS: existing partition matches the logs" || fail "intact partition not reported as matching"

echo ""
echo "=== 2. damage the partition, report shows it ==="
rm -rf "$T2S_SB_HDB/$D0/trade_binance"
OUT=$(rebuild -date "$D0"); echo "$OUT" | grep -E "trade_binance:" | sed 's/^/    /'
echo "$OUT" | grep -qE "trade_binance: rows 0 +distinct 0 +logs-only 151" && echo "  PASS: missing table reported" || fail "damage not reported"
check_eod "$D0" > /dev/null && fail "check_eod should fail on the damaged day" || echo "  PASS: check_eod reports the damaged day"

echo ""
echo "=== 3. -build writes and verifies staging ==="
OUT=$(rebuild -date "$D0" -build); echo "$OUT" | grep -E "verify|staging" | sed 's/^/    /'
echo "$OUT" | grep -q "staging verified against the logs" && echo "  PASS: staging verified" || fail "staging not verified"
[[ -d "$T2S_SB_HDB/.rebuild/$D0/trade_binance" ]] && echo "  PASS: staging under hdb/.rebuild" || fail "staging dir missing"
[[ -d "$T2S_SB_HDB/$D0" && ! -d "$T2S_SB_HDB/$D0/trade_binance" ]] && echo "  PASS: live partition untouched by -build" || fail "-build touched the live partition"

echo ""
echo "=== 4. -swap ==="
OUT=$(rebuild -date "$D0" -swap); echo "$OUT" | grep -E "moved|swap complete|verify" | sed 's/^/    /'
echo "$OUT" | grep -q "swap complete" && echo "  PASS: swap complete" || fail "swap did not complete"
ls -d "$T2S_SB_HDB/.rebuild/$D0.bak."* > /dev/null 2>&1 && echo "  PASS: old partition kept as backup under hdb/.rebuild" || fail "no backup of the old partition"
[[ ! -d "$T2S_SB_HDB/.rebuild/$D0" ]] && echo "  PASS: staging dir consumed" || fail "staging dir still present"
wdb -step assert_partition -table trade_binance     -date "$D0" -rows 151
wdb -step assert_partition -table quote_binance     -date "$D0" -rows 80
wdb -step assert_partition -table trade_binance_fut -date "$D0" -rows 40
wdb -step assert_partition -table quote_binance_fut -date "$D0" -rows 30
check_eod "$D0" | sed 's/^/    /'
check_eod "$D0" > /dev/null && echo "  PASS: check_eod confirms the rebuilt day" || fail "check_eod does not confirm the rebuilt day"

echo ""
echo "=== 5. rebuilt rows are marked: wdbRecvTimeUtcNs null; schema identical ==="
if REBUILT_TABLE="$T2S_SB_HDB/$D0/trade_binance" q "$SCRIPT_DIR/rebuild_check.q" -q < /dev/null 2>&1 | sed 's/^/    /'; then
    :
fi
if REBUILT_TABLE="$T2S_SB_HDB/$D0/trade_binance" q "$SCRIPT_DIR/rebuild_check.q" -q < /dev/null > /dev/null 2>&1; then
    echo "  PASS: null wdbRecvTimeUtcNs, same column order, parted sym"
else
    fail "rebuilt partition shape"
fi

echo ""
echo "=== 6. today is refused ==="
rebuild -date "$TODAY" | grep -q "refusing to rebuild today" && echo "  PASS: today refused" || fail "today was not refused"

echo ""; echo "==========================================="
if [[ $FAILURES -eq 0 ]]; then echo "Rebuild day: all checks passed"; echo "==========================================="; exit 0; fi
echo "Rebuild day: $FAILURES failure(s)"; echo "==========================================="; exit 1
