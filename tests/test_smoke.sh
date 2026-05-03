#!/bin/bash
# test_smoke.sh - Smoke test for each q process.
#
# For each q process (tp, ctp, rdb, wdb, sig, pnl, rte, tel):
#   1. Copy its .q file into a sandbox with patched test ports
#   2. Start it
#   3. Wait for its listening port to come up
#   4. Connect via IPC and call .health[]
#   5. Assert the response has a `status` key with a sane value
#   6. Kill it cleanly
#
# Each process is exercised independently. Upstream connections will be
# in `disconnected` state because we don't start the upstream - that is
# acceptable for a smoke test (we are checking that the file loads
# without errors, not that the full pipeline works).

set -u

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PROJECT_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)
cd "$PROJECT_ROOT"

SANDBOX="$PROJECT_ROOT/tests/sandbox"

# Listen ports = production + 10000
# Upstream ports = production + 20000 (intentionally unreachable -> degraded mode)
declare -A LISTEN_PORT
LISTEN_PORT[tp]=15010
LISTEN_PORT[wdb]=15011
LISTEN_PORT[sig]=15012
LISTEN_PORT[ctp]=15014
LISTEN_PORT[rte]=15015
LISTEN_PORT[tel]=15016
LISTEN_PORT[rdb]=15017
LISTEN_PORT[pnl]=15018

# Where each .q file lives, relative to project root
declare -A SRC_PATH
SRC_PATH[tp]="kdb/tick/tp.q"
SRC_PATH[ctp]="kdb/tick/chained_tp.q"
SRC_PATH[rdb]="kdb/tick/rdb.q"
SRC_PATH[wdb]="kdb/tick/wdb.q"
SRC_PATH[sig]="kdb/analytics/sig.q"
SRC_PATH[pnl]="kdb/analytics/pnl.q"
SRC_PATH[rte]="kdb/analytics/rte.q"
SRC_PATH[tel]="kdb/analytics/tel.q"

# Order matters only for readability; each is independent
PROCESSES=(tp ctp rdb wdb sig pnl rte tel)

# -------------------- cleanup --------------------
CHILD_PID=""
cleanup() {
    local rc=$?
    if [[ -n "$CHILD_PID" ]]; then
        kill -TERM "$CHILD_PID" 2>/dev/null || true
        wait "$CHILD_PID" 2>/dev/null || true
    fi
    # Belt-and-braces: kill anything still on test ports
    for proc in "${PROCESSES[@]}"; do
        local port=${LISTEN_PORT[$proc]}
        local pid
        pid=$(lsof -ti:$port 2>/dev/null || true)
        [[ -n "$pid" ]] && kill -9 "$pid" 2>/dev/null || true
    done
    if [[ $rc -eq 0 ]]; then
        rm -rf "$SANDBOX"
    fi
    exit $rc
}
trap cleanup EXIT INT TERM

# -------------------- pre-flight --------------------
# Clean up stale test ports
for proc in "${PROCESSES[@]}"; do
    port=${LISTEN_PORT[$proc]}
    pid=$(lsof -ti:$port 2>/dev/null || true)
    if [[ -n "$pid" ]]; then
        echo "WARN: Killing stale process on port $port (pid $pid)"
        kill -9 "$pid" 2>/dev/null || true
        sleep 0.1
    fi
done

# -------------------- sandbox setup --------------------
rm -rf "$SANDBOX"
mkdir -p "$SANDBOX/kdb/pubsub" "$SANDBOX/kdb/tick" "$SANDBOX/kdb/analytics" "$SANDBOX/logs"

# Shared infrastructure: schemas + pubsub module
cp "$PROJECT_ROOT/kdb/schemas.q"       "$SANDBOX/kdb/schemas.q"
cp "$PROJECT_ROOT/kdb/pubsub/init.q"   "$SANDBOX/kdb/pubsub/init.q"
cp "$PROJECT_ROOT/kdb/pubsub/pubsub.q" "$SANDBOX/kdb/pubsub/pubsub.q"

# -------------------- per-process smoke --------------------
TOTAL=0
PASSED=0
FAILED_PROCS=()

run_smoke() {
    local proc=$1
    local listen_port=${LISTEN_PORT[$proc]}
    local src="$PROJECT_ROOT/${SRC_PATH[$proc]}"
    local dst="$SANDBOX/${SRC_PATH[$proc]}"
    local logfile="$SANDBOX/${proc}.log"

    TOTAL=$((TOTAL + 1))
    echo ""
    echo "--- smoke: $proc ---"

    if [[ ! -f "$src" ]]; then
        echo "  FAIL: source missing at $src"
        FAILED_PROCS+=("$proc (source missing)")
        return 1
    fi

    # Patch the listen port. Each process uses its own .X.cfg.port pattern.
    # We sed all known listening-port lines and the most common upstream-port
    # lines to point at unreachable test ports so they stay in degraded mode.
    # Each line matches at most one process; the rest are no-ops.
    sed -e "s|^\.tp\.cfg\.port:.*$|.tp.cfg.port:${listen_port};|" \
        -e "s|^\.ctp\.cfg\.port:.*$|.ctp.cfg.port:${listen_port};|" \
        -e "s|^\.rdb\.cfg\.port:.*$|.rdb.cfg.port:${listen_port};|" \
        -e "s|^\.wdb\.cfg\.port:.*$|.wdb.cfg.port:${listen_port};|" \
        -e "s|^\.sig\.cfg\.port:.*$|.sig.cfg.port:${listen_port};|" \
        -e "s|^\.pnl\.cfg\.port:.*$|.pnl.cfg.port:${listen_port};|" \
        -e "s|^\.rte\.cfg\.port:.*$|.rte.cfg.port:${listen_port};|" \
        -e "s|^\.tel\.cfg\.port:.*$|.tel.cfg.port:${listen_port};|" \
        -e "s|^\.wdb\.cfg\.tpPort:.*$|.wdb.cfg.tpPort:25010;|" \
        -e "s|^\.sig\.cfg\.tpPort:.*$|.sig.cfg.tpPort:25010;|" \
        -e "s|^\.rte\.cfg\.tpPort:.*$|.rte.cfg.tpPort:25010;|" \
        -e "s|^\.tel\.cfg\.tpPort:.*$|.tel.cfg.tpPort:25010;|" \
        -e "s|^\.rdb\.cfg\.tpPort:.*$|.rdb.cfg.tpPort:25010;|" \
        -e "s|^\.pnl\.cfg\.tpPort:.*$|.pnl.cfg.tpPort:25010;|" \
        -e "s|^\.ctp\.cfg\.primaryTP:.*$|.ctp.cfg.primaryTP:25010;|" \
        -e "s|^\.tp\.cfg\.logDir:.*$|.tp.cfg.logDir:\"${SANDBOX}/logs\";|" \
        "$src" > "$dst"

    # Start the process. Run from its source directory so relative paths
    # like ../schemas.q and ../pubsub/init.q resolve correctly.
    local rundir
    rundir=$(dirname "$dst")
    ( cd "$rundir" && q "$(basename "$dst")" ) > "$logfile" 2>&1 &
    CHILD_PID=$!

    # Wait for listening port (up to 5 seconds)
    local listening=0
    for i in {1..50}; do
        if lsof -ti:$listen_port >/dev/null 2>&1; then
            listening=1
            break
        fi
        sleep 0.1
    done

    if [[ $listening -eq 0 ]]; then
        echo "  FAIL: $proc did not start listening on $listen_port"
        echo "  --- log ---"
        sed 's/^/    /' "$logfile"
        kill -9 "$CHILD_PID" 2>/dev/null || true
        CHILD_PID=""
        FAILED_PROCS+=("$proc (did not listen)")
        return 1
    fi

    # Run the q assertion: connect, call .health[], check status key
    local result
    result=$(q -q -p 0 < /dev/null <<EOF 2>&1
h:@[hopen; (\`\$":localhost:${listen_port}"; 3000); {[err] -1 "OPEN_FAILED:",err; 0}];
if[h <= 0; -1 "FAIL: could not connect"; exit 2];
res:@[h; ".health[]"; {[err] -1 "EXEC_FAILED:",err; 0N}];
hclose h;
if[null res; -1 "FAIL: .health[] errored"; exit 3];
if[not 99h = type res; -1 "FAIL: .health[] returned non-dict"; exit 4];
if[not \`status in key res; -1 "FAIL: .health[] missing status key"; exit 5];
st:res \`status;
if[not st in \`ok\`degraded\`disconnected\`error;
  -1 "FAIL: status value '",string[st],"' not in {ok,degraded,disconnected,error}";
  exit 6];
-1 "OK: status=",string[st];
exit 0;
EOF
)
    local rc=$?

    # Kill the spawned process before evaluating result
    kill -TERM "$CHILD_PID" 2>/dev/null || true
    wait "$CHILD_PID" 2>/dev/null || true
    CHILD_PID=""

    if [[ $rc -eq 0 ]]; then
        echo "  PASS: $proc - $(echo "$result" | grep '^OK:')"
        PASSED=$((PASSED + 1))
    else
        echo "  FAIL: $proc (rc=$rc)"
        echo "$result" | sed 's/^/    /'
        FAILED_PROCS+=("$proc (assertion failed rc=$rc)")
    fi
}

for proc in "${PROCESSES[@]}"; do
    run_smoke "$proc"
done

echo ""
echo "==========================================="
echo "Smoke summary: $PASSED / $TOTAL passed"
echo "==========================================="
if [[ ${#FAILED_PROCS[@]} -gt 0 ]]; then
    echo "Failed:"
    for p in "${FAILED_PROCS[@]}"; do
        echo "  - $p"
    done
    exit 1
fi
exit 0
