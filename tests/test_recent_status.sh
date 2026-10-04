#!/bin/bash
# test_recent_status.sh - status flags recent problems, not old totals (sandboxed).
#
# TP and WDB run with T2S_ALERT_WINDOW_SEC=4.
#   1. a jump in fhSeqNo: TP's .health[] is degraded and status.q lists the
#      missed rows; a few seconds later both are clean again, while the
#      cumulative counter still shows the total
#   2. handler counters reported to TP: only the INCREASE of a problem
#      counter is an incident; a repeat of the same total is not; a counter
#      that went down (handler restart) counts its new value
#   3. WDB: a problem within the window degrades its health, then clears
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

fail() { echo "  FAIL: $*"; FAILURES=$((FAILURES + 1)); }
pass() { echo "  PASS: $*"; }
tp()  { q "$SCRIPT_DIR/tp_dur_body.q" "$@" -q < /dev/null; local rc=$?; [[ $rc -ne 0 ]] && FAILURES=$((FAILURES + 1)); return $rc; }
qp() { q -q -p 0 < /dev/null <<QEOF
h:hopen (\`\$":localhost:$1"; 3000); -1 .Q.s1 h "$2"; hclose h; exit 0
QEOF
}
qtp() { qp "$T2S_PORT_TP" "$1"; }
qwdb() { qp "$T2S_PORT_WDB" "$1"; }
status() { T2S_STATUS_MARKETS=spot T2S_TP_PORT=$T2S_PORT_TP T2S_WDB_PORT=$T2S_PORT_WDB T2S_TMP_DIR="$T2S_SB_TMP" q kdb/utils/status.q < /dev/null 2>&1; }

t2s_sandbox_reset; rm -f "$T2S_SANDBOX/fhseq"
TP_PID=$(t2s_spawn_tp "$T2S_PORT_TP" "$T2S_SANDBOX/tp.log" T2S_ALERT_WINDOW_SEC=4)
t2s_wait_port "$T2S_PORT_TP" 6 || { fail "TP did not start"; cat "$T2S_SANDBOX/tp.log"; exit 1; }
t2s_guard tp "$T2S_PORT_TP" > /dev/null || { fail "TP guard"; exit 1; }
WDB_PID=$(t2s_spawn_wdb "$T2S_PORT_WDB" "$T2S_PORT_TP" "$T2S_SANDBOX/wdb.log" T2S_ALERT_WINDOW_SEC=4)
t2s_wait_port "$T2S_PORT_WDB" 6 || { fail "WDB did not start"; cat "$T2S_SANDBOX/wdb.log"; exit 1; }
t2s_guard wdb "$T2S_PORT_WDB" > /dev/null || { fail "WDB guard"; exit 1; }
sleep 1

echo ""
echo "=== 1. a problem at TP is flagged while recent, then clears ==="
[[ "$(qtp '.health[]`alertWindowSec')" == "4" ]] && pass "alert window is 4 s (T2S_ALERT_WINDOW_SEC)" || fail "window $(qtp '.health[]`alertWindowSec')"
tp -step publish -table trade_binance -rows 5 -date "$TODAY" -session 7001
[[ "$(qtp '.health[]`status')" == '`ok' ]] && pass "healthy to begin with" || fail "status $(qtp '.health[]`status')"
tp -step publish_seq -table trade_binance -seq 9 -date "$TODAY" -session 7001      # fhSeqNo 6, 7, 8 never arrive
[[ "$(qtp '.health[]`status')" == '`degraded' ]] && pass "TP is degraded right after the jump" || fail "status $(qtp '.health[]`status')"
[[ "$(qtp '.health[][`recent]`trade_binance.missed')" == "3" ]] && pass ".health[] recent: trade_binance.missed = 3" || fail "recent $(qtp '.health[]`recent')"
OUT=$(status)
echo "$OUT" | grep -E "^(RECENT|ATTN|  - )" | sed 's/^/    /'
echo "$OUT" | grep -q "RECENT (4 s): trade_binance missed +3" && pass "status.q RECENT line shows it" || fail "RECENT line"
echo "$OUT" | grep -q "trade_binance: missed +3 in the last 4 s" && pass "status.q raises it as attention" || fail "attention note missing"
sleep 5
[[ "$(qtp '.health[]`status')" == '`ok' ]] && pass "five seconds later TP is ok again" || fail "status $(qtp '.health[]`status')"
[[ "$(qtp 'count .health[]`recent')" == "0" ]] && pass "nothing recent any more" || fail "recent $(qtp '.health[]`recent')"
[[ "$(qtp '.health[]`missed')" == "3" ]] && pass "the cumulative counter still says 3 missed" || fail "total missed $(qtp '.health[]`missed')"
OUT=$(status)
echo "$OUT" | grep -q "RECENT (4 s): nothing" && pass "status.q: RECENT nothing" || fail "RECENT line after expiry"
echo "$OUT" | grep -q "missed +3" && fail "the old problem is still flagged" || pass "the old problem is no longer flagged"
echo "$OUT" | grep -q "missed 3" && pass "the total is still displayed" || fail "total not displayed"

echo ""
echo "=== 2. handler counters: only an increase is an incident ==="
qtp '.tp.fhStats[`quote_binance; `msgsReceived`rowsPublished`bookGaps`resyncs`rateLimitPauses; 10 10 0 0 0]' > /dev/null
[[ "$(qtp 'count .tp.incidents[]')" == "0" ]] && pass "zeros: no incident" || fail "incident from zeros"
qtp '.tp.fhStats[`quote_binance; `msgsReceived`rowsPublished`bookGaps`resyncs`rateLimitPauses; 20 20 2 5 0]' > /dev/null
[[ "$(qtp '.health[][`recent]`quote_binance.bookGaps')" == "2" ]] && pass "bookGaps 0 -> 2: incident +2" || fail "recent $(qtp '.health[]`recent')"
[[ "$(qtp '`quote_binance.resyncs in key .health[]`recent')" == "0b" ]] && pass "resyncs is a total, not an alert counter" || fail "resyncs flagged"
qtp '.tp.fhStats[`quote_binance; `msgsReceived`rowsPublished`bookGaps`resyncs`rateLimitPauses; 30 30 2 5 0]' > /dev/null
[[ "$(qtp '.health[][`recent]`quote_binance.bookGaps')" == "2" ]] && pass "same total reported again: still +2" || fail "repeat counted"
qtp '.tp.fhStats[`quote_binance; `msgsReceived`rowsPublished`bookGaps`resyncs`rateLimitPauses; 5 5 1 0 1]' > /dev/null
[[ "$(qtp '.health[][`recent]`quote_binance.bookGaps')" == "3" ]] && pass "counter went down (handler restarted) with 1: +1 more" || fail "restart delta $(qtp '.health[]`recent')"
[[ "$(qtp '.health[][`recent]`quote_binance.rateLimitPauses')" == "1" ]] && pass "rateLimitPauses +1" || fail "rateLimitPauses"
status | grep -q "quote_binance: bookGaps +3 in the last 4 s" && pass "status.q flags the handler incident" || fail "handler incident not flagged"
sleep 5
OUT=$(status)
echo "$OUT" | grep -q "bookGaps +" && fail "handler incident still flagged after the window" || pass "after the window it is no longer flagged"
echo "$OUT" | grep -q "FH   : quote_binance.*bookGaps 1" && pass "the FH line still shows the handler's total" || { fail "FH total"; echo "$OUT" | grep "^FH"; }

echo ""
echo "=== 3. WDB: recent problems degrade its health, then clear ==="
[[ "$(qwdb '.health[]`status')" == '`ok' ]] && pass "WDB ok" || fail "WDB status $(qwdb '.health[]`status')"
qwdb '.inc.add[`trade_binance; `lateRows; 2]' > /dev/null
[[ "$(qwdb '.health[]`status')" == '`degraded' ]] && pass "late rows within the window: degraded" || fail "WDB status $(qwdb '.health[]`status')"
status | grep -q "2 late rows in the last 4 s (trade_binance)" && pass "status.q flags them" || fail "late rows not flagged"
sleep 5
[[ "$(qwdb '.health[]`status')" == '`ok' ]] && pass "after the window: ok" || fail "WDB status $(qwdb '.health[]`status')"
status | grep -q "late rows in the last" && fail "late rows still flagged" || pass "no longer flagged"

echo ""; echo "==========================================="
if [[ $FAILURES -eq 0 ]]; then echo "Recent status: all checks passed"; echo "==========================================="; exit 0; fi
echo "Recent status: $FAILURES failure(s)"; echo "==========================================="; exit 1
