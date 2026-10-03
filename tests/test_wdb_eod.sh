#!/bin/bash
# test_wdb_eod.sh - End-to-end test of WDB EOD persistence.
#
# Spawns the REAL kdb/tick/tp.q and kdb/tick/wdb.q on test ports with every
# path (TP log dir, WDB tmp dir, checkpoint file, HDB dir) pointed at
# tests/sandbox via environment variables (see tests/t_lib.sh). No source
# is copied or patched. Each process is checked by the isolation guard
# before the test body runs.
#
# Runs wdb_eod_body.q which publishes synthetic data, triggers EOD, and
# asserts a partition was correctly written to the sandbox HDB.
#
# Cleanup is guaranteed via trap, even on test failure.

set -u

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=t_lib.sh
source "$SCRIPT_DIR/t_lib.sh"
cd "$T2S_TEST_ROOT"

# PIDs of background q processes (filled in as we spawn)
TP_PID=""
WDB_PID=""

# -------------------- cleanup --------------------
cleanup() {
    local rc=$?
    [[ -n "$WDB_PID" ]] && kill -TERM "$WDB_PID" 2>/dev/null && wait "$WDB_PID" 2>/dev/null
    [[ -n "$TP_PID" ]]  && kill -TERM "$TP_PID"  2>/dev/null && wait "$TP_PID"  2>/dev/null
    # Belt-and-braces: anything lingering on the test ports (test range only)
    t2s_kill_port "$T2S_PORT_TP"
    t2s_kill_port "$T2S_PORT_WDB"
    # Sandbox is left in place on failure so user can inspect; cleaned on success
    if [[ $rc -eq 0 ]]; then
        t2s_sandbox_remove
    else
        echo "  Sandbox preserved at: $T2S_SANDBOX (for inspection)"
    fi
    exit $rc
}
trap cleanup EXIT INT TERM

# -------------------- pre-flight --------------------
for port in "$T2S_PORT_TP" "$T2S_PORT_WDB"; do
    if lsof -ti:"$port" >/dev/null 2>&1; then
        echo "WARN: Killing stale process on test port $port"
        t2s_kill_port "$port"
        sleep 0.2
    fi
done

t2s_sandbox_reset

# -------------------- spawn TP --------------------
echo "Starting test TP on port $T2S_PORT_TP..."
TP_PID=$(t2s_spawn_tp "$T2S_PORT_TP" "$T2S_SANDBOX/tp.log")
if ! t2s_wait_port "$T2S_PORT_TP" 6; then
    echo "ERROR: TP failed to start - log:"
    cat "$T2S_SANDBOX/tp.log"
    exit 1
fi
if ! t2s_guard tp "$T2S_PORT_TP"; then
    echo "ERROR: sandboxed TP is not isolated - aborting"
    exit 1
fi

# -------------------- spawn WDB --------------------
echo "Starting test WDB on port $T2S_PORT_WDB..."
WDB_PID=$(t2s_spawn_wdb "$T2S_PORT_WDB" "$T2S_PORT_TP" "$T2S_SANDBOX/wdb.log")
if ! t2s_wait_port "$T2S_PORT_WDB" 6; then
    echo "ERROR: WDB failed to start - log:"
    cat "$T2S_SANDBOX/wdb.log"
    exit 1
fi
if ! t2s_guard wdb "$T2S_PORT_WDB"; then
    echo "ERROR: sandboxed WDB is not isolated - aborting"
    exit 1
fi

# Give WDB a moment to subscribe to TP
sleep 1

# -------------------- run the q test body --------------------
echo "Running test body..."
SANDBOX_HDB_PATH="$T2S_SB_HDB" \
SANDBOX_TMP_PATH="$T2S_SB_TMP" \
TEST_TP_PORT=$T2S_PORT_TP \
TEST_WDB_PORT=$T2S_PORT_WDB \
q "$SCRIPT_DIR/wdb_eod_body.q" < /dev/null
TEST_RC=$?

# Show subprocess logs on failure for easier debugging
if [[ $TEST_RC -ne 0 ]]; then
    echo ""
    echo "--- TP log ---"
    tail -30 "$T2S_SANDBOX/tp.log"
    echo ""
    echo "--- WDB log ---"
    tail -50 "$T2S_SANDBOX/wdb.log"
fi

exit $TEST_RC
