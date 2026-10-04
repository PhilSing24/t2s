#!/bin/bash
# test_hdb_migrate.sh - layout guard and kdb/utils/hdb_migrate.q (sandboxed).
#
# Builds a sandbox HDB the way the pipeline wrote it BEFORE columns were
# added to the schema (quotes without update ids, futures trades without
# qtyExRpi, one partition without the futures quote table) plus an old
# tmp.<date> dir, then:
#   1. TP and WDB refuse to start over it and name the migration tool
#   2. the dry run lists the work and changes nothing
#   3. -apply adds the columns as typed nulls and creates the missing table;
#      every file that existed before is byte-identical afterwards
#   4. the HDB loads and HDB-wide queries on the new columns work
#   5. TP and WDB start; a second run finds nothing to do
#   6. a table of another quote depth, or with a column the schema does not
#      know, is refused and nothing is changed
#
# Exit code 0 on success.

set -u

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=t_lib.sh
source "$SCRIPT_DIR/t_lib.sh"
cd "$T2S_TEST_ROOT"

TP_PID=""; WDB_PID=""; FAILURES=0
TP_LOG="$T2S_SANDBOX/tp.log"; WDB_LOG="$T2S_SANDBOX/wdb.log"

cleanup() {
    local rc=$?
    [[ -n "$WDB_PID" ]] && kill -9 "$WDB_PID" 2>/dev/null
    [[ -n "$TP_PID" ]]  && kill -9 "$TP_PID"  2>/dev/null
    t2s_kill_port "$T2S_PORT_TP"; t2s_kill_port "$T2S_PORT_WDB"
    if [[ $rc -eq 0 ]]; then t2s_sandbox_remove; else echo "  Sandbox preserved at: $T2S_SANDBOX (for inspection)"; fi
    exit $rc
}
trap cleanup EXIT INT TERM

fail() { echo "  FAIL: $*"; FAILURES=$((FAILURES + 1)); }
pass() { echo "  PASS: $*"; }
gone() { local i; for ((i = 0; i < $2 * 10; i++)); do kill -0 "$1" 2>/dev/null || return 0; sleep 0.1; done; return 1; }
stop_all() {
    [[ -n "$WDB_PID" ]] && { kill -TERM "$WDB_PID" 2>/dev/null; gone "$WDB_PID" 5; WDB_PID=""; }
    [[ -n "$TP_PID" ]]  && { kill -TERM "$TP_PID"  2>/dev/null; gone "$TP_PID" 5;  TP_PID=""; }
    t2s_kill_port "$T2S_PORT_TP"; t2s_kill_port "$T2S_PORT_WDB"
}
migrate() { T2S_HDB_DIR="$T2S_SB_HDB" T2S_TMP_DIR="$T2S_SB_TMP" q kdb/utils/hdb_migrate.q "$@" < /dev/null 2>&1; }
fingerprint() { (cd "$T2S_SANDBOX" && find hdb tmp -type f ! -name '.d' | sort | xargs md5sum); }

for port in "$T2S_PORT_TP" "$T2S_PORT_WDB"; do
    if lsof -ti:"$port" >/dev/null 2>&1; then echo "WARN: Killing stale process on test port $port"; t2s_kill_port "$port"; sleep 0.2; fi
done

body() { SANDBOX_HDB_PATH="$T2S_SB_HDB" SANDBOX_TMP_PATH="$T2S_SB_TMP" q "$SCRIPT_DIR/hdb_migrate_body.q" "$@" -q < /dev/null 2>&1; local rc=$?; [[ $rc -ne 0 ]] && FAILURES=$((FAILURES + 1)); return $rc; }
# Old-layout data: quotes without update ids, futures trades without qtyExRpi
build_old() { body -step build; }

echo ""
echo "=== 1. TP and WDB refuse to start over old-layout data ==="
t2s_sandbox_reset
build_old
TP_PID=$(t2s_spawn_tp "$T2S_PORT_TP" "$TP_LOG")
gone "$TP_PID" 10 && pass "TP exited" || fail "TP is still running"; TP_PID=""
grep -q "TP: REFUSING TO START - 7 existing table dir(s) do not have the schema's columns" "$TP_LOG" && pass "TP counts the seven old table dirs" || { fail "TP refusal"; tail -8 "$TP_LOG"; }
grep -q "2026.01.05/quote_binance: lacks exchFirstUpdateId exchUpdateId" "$TP_LOG" && pass "TP names the missing columns" || fail "missing columns not named"
grep -q "q kdb/utils/hdb_migrate.q -apply" "$TP_LOG" && pass "TP points to the migration tool" || fail "tool not mentioned"
[[ -z "$(ls "$T2S_SB_TPLOGS" 2>/dev/null)" ]] && pass "TP wrote nothing" || fail "TP wrote: $(ls "$T2S_SB_TPLOGS")"
WDB_PID=$(t2s_spawn_wdb "$T2S_PORT_WDB" "$T2S_PORT_TP" "$WDB_LOG")
gone "$WDB_PID" 10 && pass "WDB exited" || fail "WDB is still running"; WDB_PID=""
grep -q "WDB: REFUSING TO START" "$WDB_LOG" && grep -q "tmp.2026.01.07/quote_binance_fut: lacks exchFirstUpdateId exchUpdateId exchPrevUpdateId" "$WDB_LOG" && pass "WDB refuses and names the tmp dir" || { fail "WDB refusal"; tail -8 "$WDB_LOG"; }
stop_all

echo ""
echo "=== 2. dry run: lists the work, changes nothing ==="
BEFORE=$(fingerprint); DBEFORE=$(cd "$T2S_SANDBOX" && find hdb tmp -name '.d' | sort | xargs md5sum)
OUT=$(migrate); rc=$?
echo "$OUT" | sed 's/^/    /' | cut -c1-170
[[ $rc -eq 0 ]] && pass "dry run exit 0" || fail "dry run rc=$rc"
echo "$OUT" | grep -q "7 table dir(s) get null columns (14 new column files, 37 rows covered); 3 empty table(s) created" && pass "summary: 7 dirs, 14 column files, 37 rows, 3 empty tables" || fail "summary line"
echo "$OUT" | grep -q "dry run - nothing written" && pass "says nothing was written" || fail "dry run wording"
[[ "$BEFORE" == "$(fingerprint)" && "$DBEFORE" == "$(cd "$T2S_SANDBOX" && find hdb tmp -name '.d' | sort | xargs md5sum)" ]] && pass "no file changed" || fail "dry run changed files"
[[ ! -d "$T2S_SB_HDB/2026.01.05/quote_binance_fut" ]] && pass "no table created" || fail "dry run created a table"

echo ""
echo "=== 3. apply: null columns added, missing table created, old files untouched ==="
OUT=$(migrate -apply); rc=$?
echo "$OUT" | grep -E "applied|re-scan|ERROR" | sed 's/^/    /'
[[ $rc -eq 0 ]] && pass "apply exit 0" || fail "apply rc=$rc"
echo "$OUT" | grep -q "re-scan: every stored table now matches the schema" && pass "re-scan clean" || fail "re-scan"
AFTER_OLD=$(fingerprint | grep -v -E "/(exchFirstUpdateId|exchUpdateId|exchPrevUpdateId|qtyExRpi)$" | grep -v "2026.01.05/quote_binance_fut/" | grep -v "/trade_gap/")
[[ "$BEFORE" == "$AFTER_OLD" ]] && pass "every pre-existing column file is byte-identical" || fail "an existing file changed"
body -step verify

echo ""
echo "=== 4. the HDB loads; HDB-wide queries on the new columns work ==="
body -step query

echo ""
echo "=== 5. TP and WDB start after the migration; nothing left to do ==="
TP_PID=$(t2s_spawn_tp "$T2S_PORT_TP" "$TP_LOG")
t2s_wait_port "$T2S_PORT_TP" 6 && pass "TP started" || { fail "TP did not start"; tail -5 "$TP_LOG"; }
WDB_PID=$(t2s_spawn_wdb "$T2S_PORT_WDB" "$T2S_PORT_TP" "$WDB_LOG")
t2s_wait_port "$T2S_PORT_WDB" 6 && pass "WDB started" || { fail "WDB did not start"; tail -5 "$WDB_LOG"; }
stop_all
OUT=$(migrate -apply); rc=$?
[[ $rc -eq 0 ]] && echo "$OUT" | grep -q "nothing to do" && pass "second run: nothing to do" || fail "second run: rc=$rc $OUT"

echo ""
echo "=== 6. what cannot be fixed in place is refused, untouched ==="
t2s_sandbox_reset
build_old
body -step bad
BEFORE=$(cd "$T2S_SANDBOX" && find hdb tmp -type f | sort | xargs md5sum)
OUT=$(migrate -apply); rc=$?
echo "$OUT" | grep -E "CANNOT" | sed 's/^/    /' | cut -c1-170
[[ $rc -eq 1 ]] && pass "exit 1" || fail "rc=$rc (expected 1)"
echo "$OUT" | grep -q "quote depth 2, schema has 5" && pass "another quote depth is refused" || fail "depth not refused"
echo "$OUT" | grep -q "columns the schema does not know" && pass "an unknown column is refused" || fail "unknown column not refused"
echo "$OUT" | grep -q "2 table dir(s) CANNOT be migrated (see the note column) - nothing was changed" && pass "says nothing was changed" || fail "wording"
[[ "$BEFORE" == "$(cd "$T2S_SANDBOX" && find hdb tmp -type f | sort | xargs md5sum)" ]] && pass "no file changed, not even the migratable ones" || fail "files changed"

echo ""; echo "==========================================="
if [[ $FAILURES -eq 0 ]]; then echo "HDB migrate: all checks passed"; echo "==========================================="; exit 0; fi
echo "HDB migrate: $FAILURES failure(s)"; echo "==========================================="; exit 1
