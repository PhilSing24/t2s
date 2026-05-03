#!/bin/bash
# test_wdb_eod.sh - End-to-end test of WDB EOD persistence.
#
# Spawns TP and WDB on test ports (15010, 15011) inside a sandbox.
# Runs test_wdb_eod_body.q which publishes synthetic data, triggers EOD,
# and asserts a partition was correctly written to the sandbox HDB.
#
# Cleanup is guaranteed via trap, even on test failure.

set -u

# Resolve project root (tests/ -> project root)
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PROJECT_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)
cd "$PROJECT_ROOT"

SANDBOX="$PROJECT_ROOT/tests/sandbox"
SANDBOX_KDB="$SANDBOX/kdb"
SANDBOX_TICK="$SANDBOX_KDB/tick"
SANDBOX_HDB="$SANDBOX/hdb"
SANDBOX_LOG="$SANDBOX/test.log"

# Test ports = production + 10000
PORT_TP=15010
PORT_WDB=15011

# PIDs of background q processes (filled in as we spawn)
TP_PID=""
WDB_PID=""

# -------------------- cleanup --------------------
cleanup() {
    local rc=$?
    [[ -n "$TP_PID" ]]  && kill -TERM "$TP_PID"  2>/dev/null && wait "$TP_PID"  2>/dev/null
    [[ -n "$WDB_PID" ]] && kill -TERM "$WDB_PID" 2>/dev/null && wait "$WDB_PID" 2>/dev/null
    # Also kill anything lingering on the test ports as a belt-and-braces
    for port in $PORT_TP $PORT_WDB; do
        local pid
        pid=$(lsof -ti:$port 2>/dev/null || true)
        [[ -n "$pid" ]] && kill -9 "$pid" 2>/dev/null || true
    done
    # Sandbox is left in place on failure so user can inspect; cleaned on success
    if [[ $rc -eq 0 ]]; then
        rm -rf "$SANDBOX"
    else
        echo "  Sandbox preserved at: $SANDBOX (for inspection)"
    fi
    exit $rc
}
trap cleanup EXIT INT TERM

# -------------------- pre-flight --------------------
# Clean up any lingering state from a previous failed run
for port in $PORT_TP $PORT_WDB; do
    pid=$(lsof -ti:$port 2>/dev/null || true)
    if [[ -n "$pid" ]]; then
        echo "WARN: Killing stale process on port $port (pid $pid)"
        kill -9 "$pid" 2>/dev/null || true
        sleep 0.2
    fi
done

# -------------------- sandbox setup --------------------
rm -rf "$SANDBOX"
mkdir -p "$SANDBOX_TICK" "$SANDBOX_HDB"

# Copy production tp.q + wdb.q into sandbox, with port + path overrides.
# We override:
#  - listening port
#  - WDB hdbDir (point at sandbox HDB)
#  - WDB tpPort (talk to test TP, not production)
#  - TP log directory (so we don't pollute kdb/tick/logs)

# Copy schemas.q and pubsub module to where the sandbox processes will look.
mkdir -p "$SANDBOX_KDB/pubsub"
cp "$PROJECT_ROOT/kdb/schemas.q"          "$SANDBOX_KDB/schemas.q"
cp "$PROJECT_ROOT/kdb/pubsub/init.q"      "$SANDBOX_KDB/pubsub/init.q"
cp "$PROJECT_ROOT/kdb/pubsub/pubsub.q"    "$SANDBOX_KDB/pubsub/pubsub.q"

# Patched tp.q: change cfg.port and cfg.logDir
sed -e "s|^\.tp\.cfg\.port:.*$|.tp.cfg.port:${PORT_TP};|" \
    -e "s|^\.tp\.cfg\.logDir:.*$|.tp.cfg.logDir:\"${SANDBOX}/tplogs\";|" \
    "$PROJECT_ROOT/kdb/tick/tp.q" > "$SANDBOX_TICK/tp.q"

# Patched wdb.q: change cfg.port, cfg.tpPort, cfg.hdbDir
sed -e "s|^\.wdb\.cfg\.port:.*$|.wdb.cfg.port:${PORT_WDB};|" \
    -e "s|^\.wdb\.cfg\.tpPort:.*$|.wdb.cfg.tpPort:${PORT_TP};|" \
    -e "s|^\.wdb\.cfg\.hdbDir:.*$|.wdb.cfg.hdbDir:\`\$\":${SANDBOX_HDB}\";|" \
    "$PROJECT_ROOT/kdb/tick/wdb.q" > "$SANDBOX_TICK/wdb.q"

# WDB's TMPSAVE uses a relative path "../tmp.PID.DATE" - that resolves to
# $SANDBOX_KDB/tmp.* when WDB runs from $SANDBOX_TICK. Good - inside the sandbox.

mkdir -p "$SANDBOX/tplogs"

# -------------------- spawn TP --------------------
echo "Starting test TP on port $PORT_TP..."
( cd "$SANDBOX_TICK" && q tp.q ) > "$SANDBOX/tp.log" 2>&1 &
TP_PID=$!

# Wait for TP to listen
for i in {1..30}; do
    if lsof -ti:$PORT_TP >/dev/null 2>&1; then break; fi
    sleep 0.2
done
if ! lsof -ti:$PORT_TP >/dev/null 2>&1; then
    echo "ERROR: TP failed to start - log:"
    cat "$SANDBOX/tp.log"
    exit 1
fi

# -------------------- spawn WDB --------------------
echo "Starting test WDB on port $PORT_WDB..."
( cd "$SANDBOX_TICK" && q wdb.q ) > "$SANDBOX/wdb.log" 2>&1 &
WDB_PID=$!

for i in {1..30}; do
    if lsof -ti:$PORT_WDB >/dev/null 2>&1; then break; fi
    sleep 0.2
done
if ! lsof -ti:$PORT_WDB >/dev/null 2>&1; then
    echo "ERROR: WDB failed to start - log:"
    cat "$SANDBOX/wdb.log"
    exit 1
fi

# Give WDB a moment to subscribe to TP
sleep 1

# -------------------- run the q test body --------------------
echo "Running test body..."
SANDBOX_HDB_PATH="$SANDBOX_HDB" \
TEST_TP_PORT=$PORT_TP \
TEST_WDB_PORT=$PORT_WDB \
q "$SCRIPT_DIR/wdb_eod_body.q"
TEST_RC=$?

# Show subprocess logs on failure for easier debugging
if [[ $TEST_RC -ne 0 ]]; then
    echo ""
    echo "--- TP log ---"
    tail -30 "$SANDBOX/tp.log"
    echo ""
    echo "--- WDB log ---"
    tail -50 "$SANDBOX/wdb.log"
fi

exit $TEST_RC
