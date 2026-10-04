#!/bin/bash
# test_depth_config.sh - the shared quote depth is explicit and guarded (sandboxed).
#
# config/shared.json holds the symbols and quote_depth for all handlers and
# for the q schemas. This test uses sandbox copies of it (T2S_SHARED_CONFIG):
#   1. depth 3: TP builds a 22-column quote schema; a registration announcing
#      30 columns (depth 5) is refused, 22 is accepted
#   2. the real quote handler binary configured for depth 5 is refused by a
#      depth-3 TP and exits with code 2 before touching the network
#   3. an HDB partition written at depth 5: TP and WDB configured for depth 3
#      refuse to start, and nothing on disk is modified
#   4. the same for a tmp.<date> dir left at another depth
#   5. matching depth: both start
#   6. a shared config with a bad depth or no symbols stops q processes and
#      the handler binaries at start-up
#   7. each quote binary refuses the other market's config
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

for port in "$T2S_PORT_TP" "$T2S_PORT_WDB"; do
    if lsof -ti:"$port" >/dev/null 2>&1; then echo "WARN: Killing stale process on test port $port"; t2s_kill_port "$port"; sleep 0.2; fi
done

shared() {   # shared <file> <depth-json> [symbols-json]
    printf '{ "symbols": %s, "quote_depth": %s }\n' "${3:-[\"btcusdt\", \"ethusdt\"]}" "$2" > "$1"
}
qtp() { q -q -p 0 < /dev/null <<QEOF
h:hopen (\`\$":localhost:$T2S_PORT_TP"; 3000); -1 .Q.s1 h "$1"; hclose h; exit 0
QEOF
}
# Wait until a process is gone (it refused to start). Args: pid seconds
gone() { local i; for ((i = 0; i < $2 * 10; i++)); do kill -0 "$1" 2>/dev/null || return 0; sleep 0.1; done; return 1; }
stop_all() {
    [[ -n "$WDB_PID" ]] && { kill -TERM "$WDB_PID" 2>/dev/null; gone "$WDB_PID" 5; WDB_PID=""; }
    [[ -n "$TP_PID" ]]  && { kill -TERM "$TP_PID"  2>/dev/null; gone "$TP_PID" 5;  TP_PID=""; }
    t2s_kill_port "$T2S_PORT_TP"; t2s_kill_port "$T2S_PORT_WDB"
}
# Write an empty splayed quote_binance of the repo's depth (5) under a dir
mk_quote_dir() {   # mk_quote_dir <root dir, e.g. $T2S_SB_HDB/2026.01.05>
    ROOT="$1" HDB="$T2S_SB_HDB" q -q < /dev/null <<'QEOF'
\l kdb/schemas.q
t:.schema.extend[.schema.quote; `tpRecvTimeUtcNs`tpSeqNo`wdbRecvTimeUtcNs];
(hsym `$ raze (getenv `ROOT; "/quote_binance/")) set .Q.en[hsym `$getenv `HDB] t;
exit 0
QEOF
}
D3="$T2S_SANDBOX/shared_depth3.json"; D5="$T2S_SANDBOX/shared_depth5.json"

echo ""
echo "=== 1. depth 3: schema, widths and registration follow the shared config ==="
t2s_sandbox_reset; shared "$D3" 3; shared "$D5" 5
TP_PID=$(t2s_spawn_tp "$T2S_PORT_TP" "$TP_LOG" "T2S_SHARED_CONFIG=$D3")
t2s_wait_port "$T2S_PORT_TP" 6 || { fail "TP did not start at depth 3"; cat "$TP_LOG"; exit 1; }
t2s_guard tp "$T2S_PORT_TP" > /dev/null || { fail "TP guard"; exit 1; }
[[ "$(qtp '.schema.depth')" == "3" ]] && pass "TP runs at depth 3" || fail "TP depth is $(qtp '.schema.depth')"
[[ "$(qtp '.tp.fhWidth`quote_binance')" == "22" ]] && pass "quote row width is 22 (10 + 4*3)" || fail "quote width $(qtp '.tp.fhWidth`quote_binance')"
[[ "$(qtp '.schema.symbols')" == '`BTCUSDT`ETHUSDT' ]] && pass "symbols come from the shared file" || fail "symbols $(qtp '.schema.symbols')"
[[ "$(qtp '@[{.tp.registerSession[`quote_binance;1;1;30]}; 0; {x}]')" == *"width mismatch"* ]] && pass "a depth-5 width (30) is refused at registration" || fail "width 30 was not refused"
[[ "$(qtp '.tp.registerSession[`quote_binance;1;1;22]')" == '-1' ]] && pass "the depth-3 width (22) registers" || fail "width 22 did not register"

echo ""
echo "=== 2. the real quote handler at depth 5 is refused by the depth-3 TP ==="
if [[ -x build/quote_feed_handler ]]; then
    python3 - "$T2S_SANDBOX/qfh.json" "$T2S_PORT_TP" <<'PYEOF'
import json, sys
c = json.load(open('config/quote_feed_handler.json'))
c['tickerplant']['port'] = int(sys.argv[2]); c['logging']['file'] = ''
json.dump(c, open(sys.argv[1], 'w'))
PYEOF
    T2S_SHARED_CONFIG="$D5" timeout 20 ./build/quote_feed_handler "$T2S_SANDBOX/qfh.json" > "$T2S_SANDBOX/qfh.log" 2>&1 < /dev/null
    rc=$?
    [[ $rc -eq 2 ]] && pass "handler exited with code 2" || { fail "handler exit code $rc (expected 2)"; tail -5 "$T2S_SANDBOX/qfh.log"; }
    grep -q "TP REJECTED session registration for quote_binance" "$T2S_SANDBOX/qfh.log" && pass "handler log names the rejection" || fail "handler log lacks the rejection"
    grep -q "handler sends 30 columns, schema expects 22" "$T2S_SANDBOX/qfh.log" && pass "the message gives both widths" || fail "widths not in the message"
    grep -q "Connecting to Binance" "$T2S_SANDBOX/qfh.log" && fail "handler reached the network" || pass "handler never connected to the exchange"
else
    echo "  SKIP: build/quote_feed_handler not built"
fi
stop_all

echo ""
echo "=== 3. HDB partition at depth 5, config at depth 3: TP and WDB refuse ==="
t2s_sandbox_reset; shared "$D3" 3; shared "$D5" 5
mk_quote_dir "$T2S_SB_HDB/2026.01.05"
BEFORE=$(find "$T2S_SB_HDB" -type f | sort | xargs md5sum | md5sum)
TP_PID=$(t2s_spawn_tp "$T2S_PORT_TP" "$TP_LOG" "T2S_SHARED_CONFIG=$D3")
gone "$TP_PID" 10 && pass "TP exited" || fail "TP is still running"
TP_PID=""
grep -q "TP: REFUSING TO START - quote_depth is 3" "$TP_LOG" && pass "TP says why" || { fail "TP log lacks the refusal"; tail -5 "$TP_LOG"; }
grep -q "2026.01.05/quote_binance has depth 5" "$TP_LOG" && pass "TP names the partition and its depth" || fail "partition not named"
lsof -ti:"$T2S_PORT_TP" -sTCP:LISTEN > /dev/null 2>&1 && fail "TP port is listening" || pass "TP port never opened"
[[ -z "$(ls "$T2S_SB_TPLOGS" 2>/dev/null)" ]] && pass "TP wrote no log and no seq file" || fail "TP wrote: $(ls "$T2S_SB_TPLOGS")"
WDB_PID=$(t2s_spawn_wdb "$T2S_PORT_WDB" "$T2S_PORT_TP" "$WDB_LOG" "T2S_SHARED_CONFIG=$D3")
gone "$WDB_PID" 10 && pass "WDB exited" || fail "WDB is still running"
WDB_PID=""
grep -q "WDB: REFUSING TO START - quote_depth is 3" "$WDB_LOG" && pass "WDB says why" || { fail "WDB log lacks the refusal"; tail -5 "$WDB_LOG"; }
AFTER=$(find "$T2S_SB_HDB" -type f | sort | xargs md5sum | md5sum)
[[ "$BEFORE" == "$AFTER" ]] && pass "HDB unchanged" || fail "HDB was modified"
stop_all

echo ""
echo "=== 4. tmp.<date> dir at depth 5, config at depth 3: refused ==="
t2s_sandbox_reset; shared "$D3" 3; shared "$D5" 5
mk_quote_dir "${T2S_SB_TMP}tmp.2026.01.06"
WDB_PID=$(t2s_spawn_wdb "$T2S_PORT_WDB" "$T2S_PORT_TP" "$WDB_LOG" "T2S_SHARED_CONFIG=$D3")
gone "$WDB_PID" 10 && pass "WDB exited" || fail "WDB is still running"
WDB_PID=""
grep -q "tmp.2026.01.06/quote_binance has depth 5" "$WDB_LOG" && pass "WDB names the tmp dir" || { fail "tmp dir not named"; tail -5 "$WDB_LOG"; }
TP_PID=$(t2s_spawn_tp "$T2S_PORT_TP" "$TP_LOG" "T2S_SHARED_CONFIG=$D3")
gone "$TP_PID" 10 && pass "TP exited too" || fail "TP is still running"
TP_PID=""
stop_all

echo ""
echo "=== 5. matching depth: both start ==="
mk_quote_dir "$T2S_SB_HDB/2026.01.05"
TP_PID=$(t2s_spawn_tp "$T2S_PORT_TP" "$TP_LOG" "T2S_SHARED_CONFIG=$D5")
t2s_wait_port "$T2S_PORT_TP" 6 && pass "TP started at depth 5 over depth-5 data" || { fail "TP did not start"; tail -5 "$TP_LOG"; }
WDB_PID=$(t2s_spawn_wdb "$T2S_PORT_WDB" "$T2S_PORT_TP" "$WDB_LOG" "T2S_SHARED_CONFIG=$D5")
t2s_wait_port "$T2S_PORT_WDB" 6 && pass "WDB started at depth 5 over depth-5 data" || { fail "WDB did not start"; tail -5 "$WDB_LOG"; }
grep -q "quote depth 5" "$TP_LOG" && pass "TP banner shows the depth and the shared file" || fail "TP banner lacks the depth"
stop_all

echo ""
echo "=== 6. a bad shared config stops everything at start-up ==="
t2s_sandbox_reset
BAD="$T2S_SANDBOX/shared_bad.json"
for spec in '0|depth 0' '51|depth 51' '2.5|fractional depth' '"five"|non-numeric depth'; do
    shared "$BAD" "${spec%%|*}"
    OUT=$(T2S_SHARED_CONFIG="$BAD" q kdb/schemas.q < /dev/null 2>&1); rc=$?
    [[ $rc -eq 1 && "$OUT" == *"quote_depth"* ]] && pass "q refuses ${spec##*|}" || fail "q accepted ${spec##*|} (rc=$rc: $OUT)"
done
shared "$BAD" 5 '[]'
OUT=$(T2S_SHARED_CONFIG="$BAD" q kdb/schemas.q < /dev/null 2>&1); rc=$?
[[ $rc -eq 1 && "$OUT" == *"symbols is empty"* ]] && pass "q refuses an empty symbol list" || fail "q accepted empty symbols (rc=$rc: $OUT)"
OUT=$(T2S_SHARED_CONFIG="$T2S_SANDBOX/missing.json" q kdb/schemas.q < /dev/null 2>&1); rc=$?
[[ $rc -eq 1 && "$OUT" == *"cannot read"* ]] && pass "q refuses a missing shared file" || fail "q accepted a missing file (rc=$rc: $OUT)"
if [[ -x build/quote_feed_handler && -x build/trade_feed_handler ]]; then
    shared "$BAD" 0
    T2S_SHARED_CONFIG="$BAD" ./build/quote_feed_handler config/quote_feed_handler.json > "$T2S_SANDBOX/h.log" 2>&1 < /dev/null; rc=$?
    [[ $rc -eq 1 ]] && grep -q "quote_depth 0 is outside 1..50" "$T2S_SANDBOX/h.log" && pass "quote handler refuses depth 0" || fail "quote handler rc=$rc"
    shared "$BAD" 5 '[]'
    T2S_SHARED_CONFIG="$BAD" ./build/trade_feed_handler config/trade_feed_handler.json > "$T2S_SANDBOX/h.log" 2>&1 < /dev/null; rc=$?
    [[ $rc -eq 1 ]] && grep -q "non-empty array" "$T2S_SANDBOX/h.log" && pass "trade handler refuses an empty symbol list" || fail "trade handler rc=$rc"
    printf '{"symbols": ["btcusdt"], "tickerplant": {"port": %s}}\n' "$T2S_PORT_UNREACHABLE" > "$T2S_SANDBOX/old.json"
    ./build/trade_feed_handler "$T2S_SANDBOX/old.json" > "$T2S_SANDBOX/h.log" 2>&1 < /dev/null; rc=$?
    [[ $rc -eq 1 ]] && grep -q "Symbols are now shared" "$T2S_SANDBOX/h.log" && pass "a handler config with its own symbols list is refused" || fail "old-style config rc=$rc"
else
    echo "  SKIP: handler binaries not built"
fi

echo ""
echo "=== 7. each quote binary accepts only its own market ==="
if [[ -x build/quote_feed_handler && -x build/quote_feed_handler_fut ]]; then
    # Sandbox copies that log to the console, so the refusals below do not
    # land in the real handlers' log files under logs/.
    mkdir -p "$T2S_SANDBOX/cfg"
    python3 - "$T2S_SANDBOX/cfg" <<'CFGEOF'
import json, sys
for name in ('quote_feed_handler.json', 'quote_feed_handler_fut.json'):
    c = json.load(open('config/' + name)); c['logging']['file'] = ''
    json.dump(c, open(sys.argv[1] + '/' + name, 'w'))
CFGEOF
    export T2S_SHARED_CONFIG="$T2S_TEST_ROOT/config/shared.json"
    ./build/quote_feed_handler "$T2S_SANDBOX/cfg/quote_feed_handler_fut.json" > "$T2S_SANDBOX/h.log" 2>&1 < /dev/null; rc=$?
    [[ $rc -eq 1 ]] && grep -q 'handles market.schema "spot_depth"' "$T2S_SANDBOX/h.log" && pass "spot binary refuses the futures config" || fail "spot binary with futures config rc=$rc"
    ./build/quote_feed_handler_fut "$T2S_SANDBOX/cfg/quote_feed_handler.json" > "$T2S_SANDBOX/h.log" 2>&1 < /dev/null; rc=$?
    unset T2S_SHARED_CONFIG
    [[ $rc -eq 1 ]] && grep -q 'handles market.schema "futures_depth"' "$T2S_SANDBOX/h.log" && pass "futures binary refuses the spot config" || fail "futures binary with spot config rc=$rc"
    # TP's expected widths for the two quote layouts at depth 5
    t2s_sandbox_reset; shared "$D5" 5
    TP_PID=$(t2s_spawn_tp "$T2S_PORT_TP" "$TP_LOG" "T2S_SHARED_CONFIG=$D5")
    t2s_wait_port "$T2S_PORT_TP" 6 || { fail "TP did not start"; exit 1; }
    [[ "$(qtp '.tp.fhWidth`quote_binance`quote_binance_fut')" == "30 32" ]] && pass "TP expects 30 and 32 columns for the two quote tables" || fail "widths $(qtp '.tp.fhWidth`quote_binance`quote_binance_fut')"
    stop_all
else
    echo "  SKIP: quote handler binaries not built"
fi

echo ""; echo "==========================================="
if [[ $FAILURES -eq 0 ]]; then echo "Depth config: all checks passed"; echo "==========================================="; exit 0; fi
echo "Depth config: $FAILURES failure(s)"; echo "==========================================="; exit 1
