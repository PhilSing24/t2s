#!/bin/bash
# test_smoke.sh - Smoke test for each q process (tp, wdb).
#
# For each process:
#   1. Start the REAL kdb/tick/<proc>.q with every path and port pointed at
#      tests/sandbox and the test port range (see tests/t_lib.sh). No
#      source is copied or patched.
#   2. Wait for its listening port to come up
#   3. Run the isolation guard (tests/t_guard.q): the process reports the
#      config it actually resolved; any path outside the sandbox or port
#      outside the test range fails the test
#   4. Connect via IPC and call .health[]
#   5. Assert the response has a `status` key with a sane value
#   6. Kill it cleanly
#
# Each process is exercised independently. WDB's upstream TP port is set to
# a test-range port nothing listens on, so it sits in `disconnected` state;
# that is acceptable for a smoke test (we are checking that the file loads
# without errors, not that the full pipeline works).

set -u

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=t_lib.sh
source "$SCRIPT_DIR/t_lib.sh"
cd "$T2S_TEST_ROOT"

PROCESSES=(tp wdb)

# -------------------- cleanup --------------------
CHILD_PID=""
cleanup() {
    local rc=$?
    if [[ -n "$CHILD_PID" ]]; then
        kill -TERM "$CHILD_PID" 2>/dev/null || true
        wait "$CHILD_PID" 2>/dev/null || true
    fi
    # Belt-and-braces: kill anything still on the test ports (test range only)
    t2s_kill_port "$T2S_PORT_TP"
    t2s_kill_port "$T2S_PORT_WDB"
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
        sleep 0.1
    fi
done

t2s_sandbox_reset

# -------------------- per-process smoke --------------------
TOTAL=0
PASSED=0
FAILED_PROCS=()

run_smoke() {
    local proc=$1
    local listen_port logfile
    logfile="$T2S_SANDBOX/${proc}.log"

    TOTAL=$((TOTAL + 1))
    echo ""
    echo "--- smoke: $proc ---"

    case "$proc" in
        tp)
            listen_port=$T2S_PORT_TP
            CHILD_PID=$(t2s_spawn_tp "$listen_port" "$logfile")
            ;;
        wdb)
            listen_port=$T2S_PORT_WDB
            CHILD_PID=$(t2s_spawn_wdb "$listen_port" "$T2S_PORT_UNREACHABLE" "$logfile")
            ;;
        *)
            echo "  FAIL: unknown process $proc"
            FAILED_PROCS+=("$proc (unknown)")
            return 1
            ;;
    esac

    if ! t2s_wait_port "$listen_port" 5; then
        echo "  FAIL: $proc did not start listening on $listen_port"
        echo "  --- log ---"
        sed 's/^/    /' "$logfile"
        kill -9 "$CHILD_PID" 2>/dev/null || true
        CHILD_PID=""
        FAILED_PROCS+=("$proc (did not listen)")
        return 1
    fi

    # Isolation guard: read back the resolved config from the live process
    if ! t2s_guard "$proc" "$listen_port"; then
        echo "  FAIL: $proc resolved a path or port outside the sandbox"
        kill -TERM "$CHILD_PID" 2>/dev/null || true
        wait "$CHILD_PID" 2>/dev/null || true
        CHILD_PID=""
        FAILED_PROCS+=("$proc (isolation guard)")
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
