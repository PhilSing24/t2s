#!/bin/bash
# test_quote_seq.sh - kdb/utils/check_quote_seq.q on synthetic partitions (sandboxed).
#
#   1. clean data (contiguous spot ranges with a heartbeat and an overlap;
#      a futures pu chain with a heartbeat): no break, exit 0
#   2. breaks: a spot gap marked by an invalid row and a futures handler
#      restart are reported as marked; a silent spot jump and a silent
#      futures pu mismatch are reported as UNMARKED, with their time; exit 1
#   3. rows without update ids (stored before the columns existed) are
#      reported as not checkable, not as fine
#   4. -dir checks a directory outside the HDB layout; usage errors exit 2
#
# Exit code 0 on success.

set -u

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=t_lib.sh
source "$SCRIPT_DIR/t_lib.sh"
cd "$T2S_TEST_ROOT"

FAILURES=0
cleanup() { local rc=$?; if [[ $rc -eq 0 ]]; then t2s_sandbox_remove; else echo "  Sandbox preserved at: $T2S_SANDBOX (for inspection)"; fi; exit $rc; }
trap cleanup EXIT INT TERM
fail() { echo "  FAIL: $*"; FAILURES=$((FAILURES + 1)); }
pass() { echo "  PASS: $*"; }
body() { SANDBOX_HDB_PATH="$T2S_SB_HDB" q "$SCRIPT_DIR/quote_seq_body.q" "$@" -q < /dev/null 2>&1 || FAILURES=$((FAILURES + 1)); }
qseq() { T2S_HDB_DIR="$T2S_SB_HDB" q kdb/utils/check_quote_seq.q "$@" < /dev/null 2>&1; }

t2s_sandbox_reset
body -step clean; body -step breaks; body -step noids

echo ""
echo "=== 1. clean data ==="
OUT=$(qseq -date 2026.01.05); rc=$?
echo "$OUT" | sed 's/^/    /'
[[ $rc -eq 0 ]] && pass "exit 0" || fail "rc=$rc"
echo "$OUT" | grep -q "quote_binance: 7 rows (7 valid, 0 invalid); 7 rows with update ids checked; breaks 0 (0 marked, 0 UNMARKED)" && pass "spot: heartbeat and overlap are not breaks" || fail "spot clean line"
echo "$OUT" | grep -q "quote_binance_fut: 6 rows (6 valid, 0 invalid); 6 rows with update ids checked; breaks 0 (0 marked, 0 UNMARKED)" && pass "futures: non-consecutive ids chained by pu, heartbeat ok" || fail "futures clean line"
echo "$OUT" | grep -q "OK - no unmarked break" && pass "verdict OK" || fail "verdict"

echo ""
echo "=== 2. breaks: marked and unmarked ==="
OUT=$(qseq -date 2026.01.06); rc=$?
echo "$OUT" | sed 's/^/    /' | cut -c1-200
[[ $rc -eq 1 ]] && pass "exit 1" || fail "rc=$rc (expected 1)"
echo "$OUT" | grep -q "quote_binance: 8 rows (7 valid, 1 invalid); 7 rows with update ids checked; breaks 2 (1 marked, 1 UNMARKED)" && pass "spot: one marked, one unmarked" || fail "spot break counts"
echo "$OUT" | grep -q "quote_binance_fut: 7 rows (7 valid, 0 invalid); 7 rows with update ids checked; breaks 2 (1 marked, 1 UNMARKED)" && pass "futures: one marked, one unmarked" || fail "futures break counts"
echo "$OUT" | grep -E "ETHUSDT +marked +invalidRow" | grep -qE " 505 +900 +910" && pass "spot gap behind an invalid row is 'marked', with both ids" || fail "ETH spot break line"
echo "$OUT" | grep -E "SOLUSDT +UNMARKED" | grep -qE " 205 +300 +301" && pass "silent spot jump 205 -> 300 is UNMARKED" || fail "SOL spot break line"
echo "$OUT" | grep -E "ETHUSDT +marked +handlerRestart" | grep -qE " 44 +700 +720 +690" && pass "futures handler restart is 'marked'" || fail "ETH futures break line"
echo "$OUT" | grep -E "SOLUSDT +UNMARKED" | grep -qE " 66 +90 +95 +80" && pass "futures pu 80 after u 66 is UNMARKED" || fail "SOL futures break line"
echo "$OUT" | grep -E "SOLUSDT +UNMARKED" | grep -q "2026.01.06D\|2026.01.05D" && pass "each break carries its time" || fail "break time missing"
echo "$OUT" | grep -q "FAILED - 2 unmarked break(s)" && pass "verdict FAILED with the count" || fail "verdict"

echo ""
echo "=== 3. rows without update ids ==="
OUT=$(qseq -date 2026.01.07); rc=$?
echo "$OUT" | sed 's/^/    /'
echo "$OUT" | grep -q "2 valid rows have no update ids (stored before the columns existed): not checkable" && pass "said to be not checkable" || fail "no-ids wording"
echo "$OUT" | grep -q "NOT CHECKED: no row carries update ids" && pass "NOT CHECKED stated" || fail "NOT CHECKED missing"
echo "$OUT" | grep -q "quote_binance_fut: not present" && pass "a missing table is reported" || fail "missing table"
echo "$OUT" | grep -q "NOTHING CHECKED - no row carries update ids" && pass "verdict is NOTHING CHECKED, not OK" || fail "verdict for unverifiable data"
[[ $rc -eq 0 ]] && pass "exit 0 (nothing proven wrong)" || fail "rc=$rc"

echo ""
echo "=== 4. -dir and usage ==="
mkdir -p "$T2S_SB_TMP"; cp -r "$T2S_SB_HDB/2026.01.05" "${T2S_SB_TMP}tmp.2026.01.05"
OUT=$(qseq -dir "${T2S_SB_TMP}tmp.2026.01.05"); rc=$?
[[ $rc -eq 0 ]] && echo "$OUT" | grep -q "OK - no unmarked break" && pass "-dir checks a tmp dir" || fail "-dir: rc=$rc"
qseq > /dev/null; [[ $? -eq 2 ]] && pass "no argument: exit 2" || fail "usage rc"
qseq -date 1999.01.01 > /dev/null; [[ $? -eq 2 ]] && pass "unknown date: exit 2" || fail "unknown date rc"

echo ""; echo "==========================================="
if [[ $FAILURES -eq 0 ]]; then echo "Quote sequence: all checks passed"; echo "==========================================="; exit 0; fi
echo "Quote sequence: $FAILURES failure(s)"; echo "==========================================="; exit 1
