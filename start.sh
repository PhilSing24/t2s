#!/bin/bash
# t2s - Start market data pipeline
#
# Usage:
#   ./start.sh                     - default: spot trade FH + quote FH
#   ./start.sh --markets spot      - spot trade FH + quote FH (same as default)
#   ./start.sh --markets futures   - futures trade FH + quote FH (no spot)
#   ./start.sh --markets spot,futures - both spot and futures trade FH + quote FH
#   ./start.sh --headless ...      - start and return without attaching to tmux
#                                    (for scheduled / unattended starts)
#
# Start-up is health-based, not timed: TP must answer .health[] ok before
# WDB starts; WDB must be connected with its replay complete before the feed
# handlers start; each handler must have registered its session with TP.
# Any step that does not happen within its timeout fails the start with
# the process's log tail.
#
# The --markets flag controls which trade feed handlers are launched. The
# rest of the pipeline (TP, WDB, quote FH) is unconditional and
# market-agnostic. See ADR-013.
set -e  # Exit on error
SESSION="t2s"
# Resolve the project root from the script's own location so this works
# regardless of where the repo is cloned (previously hardcoded $HOME/t2s,
# which silently launched the wrong copy if you had multiple checkouts).
BASEDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# --------------------------------------------------------------------------
# Parse --markets flag
# --------------------------------------------------------------------------
MARKETS="spot"  # default: spot trade FH only
HEADLESS=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --markets)
            MARKETS="$2"
            shift 2
            ;;
        --markets=*)
            MARKETS="${1#*=}"
            shift
            ;;
        --headless)
            HEADLESS=1
            shift
            ;;
        -h|--help)
            sed -n '3,11p' "${BASH_SOURCE[0]}"
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown argument: $1${NC}"
            echo "Usage: $0 [--markets spot|futures|spot,futures]"
            exit 1
            ;;
    esac
done

# Validate MARKETS value
LAUNCH_SPOT=0
LAUNCH_FUT=0
case "$MARKETS" in
    spot)           LAUNCH_SPOT=1 ;;
    futures)        LAUNCH_FUT=1 ;;
    spot,futures|futures,spot) LAUNCH_SPOT=1; LAUNCH_FUT=1 ;;
    *)
        echo -e "${RED}Invalid --markets value: '$MARKETS'${NC}"
        echo "Allowed: spot, futures, spot,futures"
        exit 1
        ;;
esac

# Refuse to start production with a test-only clock override in the
# environment. These make TP/WDB believe it is another date; they exist for
# the sandboxed tests only (tests/t_lib.sh sets them per process).
for var in T2S_TP_FAKE_DATE T2S_WDB_FAKE_DATE; do
    if [[ -n "${!var:-}" ]]; then
        echo -e "${RED}Error: $var is set in the environment (${!var}). Refusing to start the live pipeline with a fake date.${NC}"
        exit 1
    fi
done

# Dependency checks
command -v tmux >/dev/null 2>&1 || { echo -e "${RED}Error: tmux not installed${NC}"; exit 1; }
command -v q >/dev/null 2>&1 || { echo -e "${RED}Error: q (kdb+) not installed${NC}"; exit 1; }
# Check if session already exists
if tmux has-session -t $SESSION 2>/dev/null; then
    echo -e "${RED}Session '$SESSION' already running. Run ./stop.sh first.${NC}"
    exit 1
fi
# Check critical ports
PORTS=(5010 5011)
for port in "${PORTS[@]}"; do
    if lsof -ti:$port >/dev/null 2>&1; then
        echo -e "${RED}Error: Port $port already in use${NC}"
        exit 1
    fi
done

# Check binaries exist for the markets we plan to launch
if [[ $LAUNCH_SPOT -eq 1 && ! -x "$BASEDIR/build/trade_feed_handler" ]]; then
    echo -e "${RED}Error: spot binary build/trade_feed_handler is missing or not executable${NC}"
    echo "  Build with: cmake --build build"
    exit 1
fi
if [[ $LAUNCH_FUT -eq 1 && ! -x "$BASEDIR/build/trade_feed_handler_fut" ]]; then
    echo -e "${RED}Error: futures binary build/trade_feed_handler_fut is missing or not executable${NC}"
    echo "  Build with: cmake --build build"
    exit 1
fi

# Record active markets for health monitoring / runbooks
mkdir -p "$BASEDIR/run"
echo "$MARKETS" > "$BASEDIR/run/markets.active"

echo "Starting t2s pipeline (markets=$MARKETS)..."

# --------------------------------------------------------------------------
# Health-based waits
# --------------------------------------------------------------------------
PORT_TP=5010
PORT_WDB=5011

# Evaluate a q expression against a process over IPC; prints the result as
# a string, or nothing if the process cannot be reached.
q_eval() {  # port expr
    q -q -p 0 < /dev/null 2>/dev/null <<QEOF
h:@[hopen; (\`\$":localhost:$1"; 2000); {0N}];
if[null h; exit 1];
r:@[h; "$2"; {\`error}]; hclose h;
-1 \$[10h = type r; r; -11h = type r; string r; .Q.s1 r];
system "sleep 0.05"; exit 0
QEOF
}

fail_start() {  # window message
    echo -e "${RED}Start failed: $2${NC}"
    echo "--- last lines of tmux window '$1' ---"
    tmux capture-pane -t "$SESSION:$1" -p 2>/dev/null | grep -v '^$' | tail -15
    echo "---"
    echo "The session '$SESSION' is left running for inspection; ./stop.sh to tear it down."
    exit 1
}

# wait_for "label" window timeout_sec check_command [args]
wait_for() {
    local label=$1 window=$2 timeout=$3; shift 3
    local deadline=$(( $(date +%s) + timeout ))
    while (( $(date +%s) < deadline )); do
        if "$@"; then echo -e "  ${GREEN}✓${NC} $label"; return 0; fi
        sleep 0.5
    done
    fail_start "$window" "$label did not happen within ${timeout}s"
}

tp_ok()         { [[ "$(q_eval $PORT_TP '.health[]`status')" == "ok" ]]; }
wdb_ready()     { [[ "$(q_eval $PORT_WDB '(.wdb.conn.state = `connected) and not .wdb.replayMode')" == "1b" ]]; }
wdb_healthy()   { local s; s=$(q_eval $PORT_WDB '.health[]`status'); [[ "$s" == "ok" || "$s" == "degraded" ]]; }
# A handler has registered when TP has a session for its table
fh_registered() { [[ "$(q_eval $PORT_TP "not null .tp.session.id\`$1")" == "1b" ]]; }

# Window 0: Tickerplant (primary) - port 5010
tmux new-session -d -s $SESSION -n "tp"
tmux send-keys -t $SESSION:tp "cd $BASEDIR/kdb/tick && q tp.q" C-m
wait_for "TP listening and healthy on $PORT_TP" tp 30 tp_ok

# Window 1: WDB (write-only -> HDB) - port 5011. It connects to TP and
# replays before accepting live rows; a long replay (after a long outage)
# is normal, so the timeout is generous.
tmux new-window -t $SESSION -n "wdb"
tmux send-keys -t $SESSION:wdb "cd $BASEDIR/kdb/tick && q wdb.q" C-m
wait_for "WDB connected to TP with replay complete" wdb 600 wdb_ready
wait_for "WDB healthy" wdb 10 wdb_healthy

# Feed handlers. Each registers a session with TP once it has connected;
# a missing registration within the timeout usually means no route to
# Binance or a schema mismatch (see the handler's window).
if [[ $LAUNCH_SPOT -eq 1 ]]; then
    tmux new-window -t $SESSION -n "trade-fh"
    tmux send-keys -t $SESSION:trade-fh "cd $BASEDIR && ./build/trade_feed_handler" C-m
fi
if [[ $LAUNCH_FUT -eq 1 ]]; then
    tmux new-window -t $SESSION -n "trade-fh-fut"
    tmux send-keys -t $SESSION:trade-fh-fut "cd $BASEDIR && ./build/trade_feed_handler_fut" C-m
fi
# Quote feed handler (unconditional; spot only for now - futures L5 is a follow-up ADR)
tmux new-window -t $SESSION -n "quote-fh"
tmux send-keys -t $SESSION:quote-fh "cd $BASEDIR && ./build/quote_feed_handler" C-m

if [[ $LAUNCH_SPOT -eq 1 ]]; then
    wait_for "spot trade handler registered with TP" trade-fh 60 fh_registered trade_binance
fi
if [[ $LAUNCH_FUT -eq 1 ]]; then
    wait_for "futures trade handler registered with TP" trade-fh-fut 60 fh_registered trade_binance_fut
fi
wait_for "quote handler registered with TP" quote-fh 60 fh_registered quote_binance

# Select first window
tmux select-window -t $SESSION:tp
echo -e "${GREEN}✓ Pipeline up (markets=$MARKETS)${NC}"
echo ""
echo "Architecture:"
echo "  Primary TP:5010 -> WDB:5011 -> HDB"
if [[ $LAUNCH_SPOT -eq 1 ]]; then
    echo "  trade_feed_handler     -> TP:5010 (trade_binance)"
fi
if [[ $LAUNCH_FUT -eq 1 ]]; then
    echo "  trade_feed_handler_fut -> TP:5010 (trade_binance_fut)"
fi
echo "  quote_feed_handler     -> TP:5010 (quote_binance)"
echo ""
echo "Navigation:"
echo "  Ctrl+B N       next window"
echo "  Ctrl+B P       previous window"
echo "  Ctrl+B 0-9     jump to window"
echo "  Ctrl+B D       detach (keeps running)"
echo ""
echo "Reattach: tmux attach -t $SESSION"
echo "Status:   ./status.sh"
echo ""
if [[ $HEADLESS -eq 1 ]]; then
    echo "Headless start: not attaching."
    exit 0
fi
tmux attach -t $SESSION
