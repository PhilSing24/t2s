#!/bin/bash
# t_lib.sh - shared bash helpers for the sandboxed integration tests.
#
# Source this from tests/test_*.sh. It gives every test the same sandbox
# root, the same test port range, and spawn helpers that start the REAL
# kdb/tick/tp.q and kdb/tick/wdb.q with every path and port set explicitly
# through environment variables. Nothing inherited from the caller's shell
# (e.g. T2S_TMP_DIR or T2S_HDB_DIR exported in .bashrc) can reach a
# sandboxed process, because every variable those processes read is set
# here, not merely unset.
#
# Isolation is enforced twice:
#   1. by construction here (explicit env for every spawn), and
#   2. by t2s_guard, which connects to the running process, reads back the
#      config it actually resolved, and fails if any path is outside the
#      sandbox or any port is outside the test range.

T2S_TEST_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
T2S_SANDBOX="$T2S_TEST_ROOT/tests/sandbox"

# Test port range: production + 10000, and nothing else.
T2S_TEST_PORT_MIN=15000
T2S_TEST_PORT_MAX=15999
T2S_PORT_TP=15010
T2S_PORT_WDB=15011
# A port inside the range that nothing listens on, for tests that want a
# process to sit in degraded mode with its upstream unreachable.
T2S_PORT_UNREACHABLE=15999

# Sandbox layout. Everything a sandboxed TP or WDB can write lands here.
T2S_SB_TPLOGS="$T2S_SANDBOX/tplogs"
T2S_SB_HDB="$T2S_SANDBOX/hdb"
T2S_SB_TMP="$T2S_SANDBOX/tmp/"            # trailing slash: wdb.q concatenates
T2S_SB_CHECKPOINT="$T2S_SANDBOX/tmp/wdb.lastTpSeqNo"

t2s_sandbox_reset() {
    rm -rf "$T2S_SANDBOX"
    mkdir -p "$T2S_SB_TPLOGS" "$T2S_SB_HDB" "$T2S_SB_TMP"
}

t2s_sandbox_remove() {
    rm -rf "$T2S_SANDBOX"
}

t2s_port_in_test_range() {
    local p=$1
    [[ "$p" =~ ^[0-9]+$ ]] && (( p >= T2S_TEST_PORT_MIN && p <= T2S_TEST_PORT_MAX ))
}

# Kill whatever listens on a port, but only if the port is in the test
# range. Refuses (and fails) for anything else so a typo can never reach
# a production listener.
t2s_kill_port() {
    local port=$1
    if ! t2s_port_in_test_range "$port"; then
        echo "t_lib: REFUSING to touch port $port (outside ${T2S_TEST_PORT_MIN}-${T2S_TEST_PORT_MAX})" >&2
        return 1
    fi
    local pid
    pid=$(lsof -ti:"$port" 2>/dev/null || true)
    [[ -n "$pid" ]] && kill -9 $pid 2>/dev/null || true
    return 0
}

# Wait until something listens on a port. Args: port [timeout_sec]
t2s_wait_port() {
    local port=$1 timeout=${2:-5}
    local tries=$(( timeout * 10 ))
    for (( i = 0; i < tries; i++ )); do
        if lsof -ti:"$port" >/dev/null 2>&1; then return 0; fi
        sleep 0.1
    done
    return 1
}

# Spawn the real tp.q from its own directory with every path/port it reads
# set explicitly. Args: port logfile. Prints the PID.
t2s_spawn_tp() {
    local port=$1 logfile=$2
    t2s_port_in_test_range "$port" || { echo "t_lib: tp port $port outside test range" >&2; return 1; }
    (
        cd "$T2S_TEST_ROOT/kdb/tick" && exec env \
            T2S_TP_PORT="$port" \
            T2S_TP_LOG_DIR="$T2S_SB_TPLOGS" \
            q tp.q
    ) > "$logfile" 2>&1 < /dev/null &
    echo $!
}

# Spawn the real wdb.q from its own directory with every path/port it reads
# set explicitly. Args: port tpport logfile [EXTRA=value ...]. Extra
# assignments (e.g. T2S_WDB_MAXROWS=20, T2S_WDB_FAKE_DATE=2026.01.01,
# T2S_WDB_ROLL_GRACE_SEC=0) are passed through to the process environment.
# Prints the PID.
t2s_spawn_wdb() {
    local port=$1 tpport=$2 logfile=$3
    shift 3
    t2s_port_in_test_range "$port"   || { echo "t_lib: wdb port $port outside test range" >&2; return 1; }
    t2s_port_in_test_range "$tpport" || { echo "t_lib: wdb tpPort $tpport outside test range" >&2; return 1; }
    (
        cd "$T2S_TEST_ROOT/kdb/tick" && exec env \
            T2S_WDB_PORT="$port" \
            T2S_WDB_TP_PORT="$tpport" \
            T2S_HDB_DIR="$T2S_SB_HDB" \
            T2S_TMP_DIR="$T2S_SB_TMP" \
            T2S_WDB_CHECKPOINT="$T2S_SB_CHECKPOINT" \
            "$@" \
            q wdb.q
    ) > "$logfile" 2>&1 < /dev/null &
    echo $!
}

# Connect to a running sandboxed process and verify the config it actually
# resolved: every path under the sandbox, every port in the test range.
# Args: proc (tp|wdb) port. Returns non-zero (and prints why) on violation.
t2s_guard() {
    local proc=$1 port=$2
    env T2S_GUARD_PROC="$proc" \
        T2S_GUARD_PORT="$port" \
        T2S_GUARD_SANDBOX="$T2S_SANDBOX" \
        T2S_GUARD_PORT_MIN="$T2S_TEST_PORT_MIN" \
        T2S_GUARD_PORT_MAX="$T2S_TEST_PORT_MAX" \
        q "$T2S_TEST_ROOT/tests/t_guard.q" -q -p 0 < /dev/null
}
