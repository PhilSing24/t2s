#!/bin/bash
# t2s - Start market data pipeline
#
# Usage:
#   ./start.sh                     - default: spot trade FH + quote FH
#   ./start.sh --markets spot      - spot trade FH + quote FH (same as default)
#   ./start.sh --markets futures   - futures trade FH + quote FH (no spot)
#   ./start.sh --markets spot,futures - both spot and futures trade FH + quote FH
#
# The --markets flag controls which trade feed handlers are launched. The
# rest of the pipeline (TP, CTP, WDB, RDB, RTE, SIG, TEL, PNL, quote FH)
# is unconditional and market-agnostic. See ADR-013.
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

# Dependency checks
command -v tmux >/dev/null 2>&1 || { echo -e "${RED}Error: tmux not installed${NC}"; exit 1; }
command -v q >/dev/null 2>&1 || { echo -e "${RED}Error: q (kdb+) not installed${NC}"; exit 1; }
# Check if session already exists
if tmux has-session -t $SESSION 2>/dev/null; then
    echo -e "${RED}Session '$SESSION' already running. Run ./stop.sh first.${NC}"
    exit 1
fi
# Check critical ports
PORTS=(5010 5011 5012 5014 5015 5016 5017 5018)
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

# Window 0: Tickerplant (primary) - port 5010
tmux new-session -d -s $SESSION -n "tp"
tmux send-keys -t $SESSION:tp "cd $BASEDIR/kdb/tick && q tp.q" C-m
# Window 1: WDB (write-only -> HDB) - port 5011
tmux new-window -t $SESSION -n "wdb"
tmux send-keys -t $SESSION:wdb "sleep 2 && cd $BASEDIR/kdb/tick && q wdb.q" C-m
# Window 2: Chained TP (batched publisher) - port 5014
# Must start before SIG (SIG publishes positions to CTP)
tmux new-window -t $SESSION -n "ctp"
tmux send-keys -t $SESSION:ctp "sleep 3 && cd $BASEDIR/kdb/tick && q chained_tp.q" C-m
# Window 3: SIG (signal generator) - port 5012
# Subscribes to TP, publishes to CTP
tmux new-window -t $SESSION -n "sig"
tmux send-keys -t $SESSION:sig "sleep 5 && cd $BASEDIR/kdb/analytics && q sig.q" C-m
# Window 4: RTE (real-time analytics) - port 5015
tmux new-window -t $SESSION -n "rte"
tmux send-keys -t $SESSION:rte "sleep 5 && cd $BASEDIR/kdb/analytics && q rte.q" C-m
# Window 5: TEL (telemetry) - port 5016
tmux new-window -t $SESSION -n "tel"
tmux send-keys -t $SESSION:tel "sleep 5 && cd $BASEDIR/kdb/analytics && q tel.q" C-m
# Window 6: RDB (user queries) - port 5017
tmux new-window -t $SESSION -n "rdb"
tmux send-keys -t $SESSION:rdb "sleep 6 && cd $BASEDIR/kdb/tick && q rdb.q" C-m
# Window 7: PNL (P&L monitoring) - port 5018
tmux new-window -t $SESSION -n "pnl"
tmux send-keys -t $SESSION:pnl "sleep 6 && cd $BASEDIR/kdb/analytics && q pnl.q" C-m

# Spot trade feed handler (conditional)
if [[ $LAUNCH_SPOT -eq 1 ]]; then
    tmux new-window -t $SESSION -n "trade-fh"
    tmux send-keys -t $SESSION:trade-fh "sleep 8 && cd $BASEDIR && ./build/trade_feed_handler" C-m
fi

# Futures trade feed handler (conditional)
if [[ $LAUNCH_FUT -eq 1 ]]; then
    tmux new-window -t $SESSION -n "trade-fh-fut"
    tmux send-keys -t $SESSION:trade-fh-fut "sleep 8 && cd $BASEDIR && ./build/trade_feed_handler_fut" C-m
fi

# Quote feed handler (unconditional; spot only for now - futures L5 is a follow-up ADR)
tmux new-window -t $SESSION -n "quote-fh"
tmux send-keys -t $SESSION:quote-fh "sleep 9 && cd $BASEDIR && ./build/quote_feed_handler" C-m

# Select first window
tmux select-window -t $SESSION:tp
echo -e "${GREEN}✓ Pipeline starting (markets=$MARKETS)${NC}"
echo ""
echo "Architecture:"
echo "  Primary TP:5010 -> WDB:5011, SIG:5012"
echo "  Primary TP:5010 -> Chained TP:5014 -> RTE:5015, TEL:5016, RDB:5017, PNL:5018"
echo "  SIG:5012 -> Chained TP:5014 (positions)"
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
echo ""
tmux attach -t $SESSION
