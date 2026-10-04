#!/bin/bash
# t2s - Stop market data pipeline
# Usage: ./stop.sh [-f|--force]
#
# Shutdown order (so WDB persists everything TP published and nothing has
# to rely on replay after a normal stop):
#   1. feed handlers      SIGTERM (they close their sockets cleanly), then
#                         SIGKILL after a timeout
#   2. WDB                graceful: .wdb.shutdownAndExit[] over IPC flushes
#                         every buffer to disk and writes the checkpoint;
#                         fallback SIGTERM (runs the same flush in .z.exit);
#                         last resort SIGKILL
#   3. TP                 SIGTERM, then SIGKILL after a timeout
#   4. tmux session       killed last. Killing it first sends SIGHUP, which
#                         q does NOT treat as an exit, so WDB's flush would
#                         never run (that is how a live buffer was lost).
#
# -f / --force skips the graceful paths and goes straight to SIGKILL.
# Every stop reports which path was taken.

SESSION="t2s"
BASEDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

PORT_TP=5010
PORT_WDB=5011

# Seconds to wait for each graceful step before escalating
WDB_IPC_TIMEOUT=30
TERM_TIMEOUT=10

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m'

FORCE=false
[[ "${1:-}" == "-f" || "${1:-}" == "--force" ]] && FORCE=true

log()  { echo -e "  $*"; }
warn() { echo -e "  ${YELLOW}$*${NC}"; }
err()  { echo -e "  ${RED}$*${NC}"; }

# Wait until none of the given PIDs are alive. Args: timeout_sec pid...
wait_pids() {
    local timeout=$1; shift
    local deadline=$(( $(date +%s) + timeout ))
    while (( $(date +%s) < deadline )); do
        local alive=0
        for pid in "$@"; do
            ps -p "$pid" >/dev/null 2>&1 && alive=1
        done
        [[ $alive -eq 0 ]] && return 0
        sleep 0.2
    done
    return 1
}

# Stop a set of PIDs: SIGTERM, wait, SIGKILL. Args: name pid...
stop_pids() {
    local name=$1; shift
    [[ $# -eq 0 ]] && { log "$name: not running"; return 0; }
    if [[ "$FORCE" == true ]]; then
        kill -9 "$@" 2>/dev/null
        warn "$name: SIGKILL (force)"
        return 0
    fi
    kill -15 "$@" 2>/dev/null
    if wait_pids "$TERM_TIMEOUT" "$@"; then
        log "$name: stopped on SIGTERM (PIDs $*)"
        return 0
    fi
    kill -9 "$@" 2>/dev/null
    warn "$name: did not exit within ${TERM_TIMEOUT}s, SIGKILL sent (PIDs $*)"
    return 0
}

# Feed handlers: match the START of the command line (the binary path as
# start.sh launches it). An unanchored pgrep -f also hits any shell or
# editor whose arguments mention the binary (and once killed the operator's
# own session mid-stop); pgrep -x cannot be used because the kernel
# truncates process names to 15 characters ("trade_feed_hand").
fh_pids() { pgrep -f '^(\./)?build/(trade_feed_handler|trade_feed_handler_fut|quote_feed_handler|quote_feed_handler_fut)( |$)' 2>/dev/null || true; }
# The process LISTENING on a port, not its clients (lsof -ti:PORT alone
# also returns every process connected to it, e.g. the handlers on TP's port).
pid_by_port() { lsof -ti TCP:"$1" -sTCP:LISTEN 2>/dev/null || true; }

echo "Stopping t2s pipeline..."

# ---------------------------------------------------------------------------
# 1. Feed handlers
# ---------------------------------------------------------------------------
FH_PIDS=$(fh_pids | sort -u)
# shellcheck disable=SC2086
stop_pids "feed handlers" $FH_PIDS

# ---------------------------------------------------------------------------
# 2. WDB - graceful flush + checkpoint over IPC, then escalate
# ---------------------------------------------------------------------------
WDB_PID=$(pid_by_port "$PORT_WDB")
if [[ -z "$WDB_PID" ]]; then
    log "WDB: not running"
elif [[ "$FORCE" == true ]]; then
    kill -9 $WDB_PID 2>/dev/null
    warn "WDB: SIGKILL (force) - buffer NOT flushed"
else
    if command -v q >/dev/null 2>&1; then
        q -q -p 0 < /dev/null > /dev/null 2>&1 <<EOF
h:@[hopen; (\`\$":localhost:${PORT_WDB}"; 5000); {0N}];
if[not null h; neg[h] ".wdb.shutdownAndExit[]"; neg[h] (::); hclose h];
exit 0
EOF
        if wait_pids "$WDB_IPC_TIMEOUT" $WDB_PID; then
            echo -e "  ${GREEN}WDB: stopped gracefully via IPC (buffers flushed, checkpoint written)${NC}"
        else
            warn "WDB: no exit within ${WDB_IPC_TIMEOUT}s after IPC shutdown - sending SIGTERM"
            kill -15 $WDB_PID 2>/dev/null
            if wait_pids "$TERM_TIMEOUT" $WDB_PID; then
                warn "WDB: stopped on SIGTERM (exit flush ran)"
            else
                kill -9 $WDB_PID 2>/dev/null
                err "WDB: SIGKILL after ${TERM_TIMEOUT}s - buffer may NOT be flushed; check tmp/ and the TP log"
            fi
        fi
    else
        warn "WDB: q not on PATH, cannot request IPC shutdown - using SIGTERM"
        stop_pids "WDB" $WDB_PID
    fi
fi

# ---------------------------------------------------------------------------
# 3. TP
# ---------------------------------------------------------------------------
TP_PID=$(pid_by_port "$PORT_TP")
# shellcheck disable=SC2086
stop_pids "TP" $TP_PID

# ---------------------------------------------------------------------------
# 4. tmux session, then any strays
# ---------------------------------------------------------------------------
if tmux has-session -t $SESSION 2>/dev/null; then
    tmux kill-session -t $SESSION 2>/dev/null
    log "tmux session '$SESSION' killed"
fi

for script in tp.q wdb.q; do
    # q processes running our scripts: command line is exactly "q <script>"
    STRAY=$(pgrep -fx "q $script" 2>/dev/null || true)
    if [[ -n "$STRAY" ]]; then
        warn "stray q $script (PIDs $STRAY) - SIGKILL"
        kill -9 $STRAY 2>/dev/null
    fi
done
for port in $PORT_TP $PORT_WDB; do
    pid=$(pid_by_port "$port")
    if [[ -n "$pid" ]]; then
        warn "process still on port $port (PID $pid) - SIGKILL"
        kill -9 $pid 2>/dev/null
    fi
done

echo -e "${GREEN}✓ Done${NC}"
