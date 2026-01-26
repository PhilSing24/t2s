#!/bin/bash
# t2s - Start market data pipeline
# Usage: ./start.sh

set -e  # Exit on error

SESSION="t2s"
BASEDIR="$HOME/t2s"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

# Dependency checks
command -v tmux >/dev/null 2>&1 || { echo -e "${RED}Error: tmux not installed${NC}"; exit 1; }
command -v q >/dev/null 2>&1 || { echo -e "${RED}Error: q (kdb+) not installed${NC}"; exit 1; }

# Check if session already exists
if tmux has-session -t $SESSION 2>/dev/null; then
    echo -e "${RED}Session '$SESSION' already running. Run ./stop.sh first.${NC}"
    exit 1
fi

# Check critical ports
PORTS=(5010 5012 5013 5014 5015 5020 5021)
for port in "${PORTS[@]}"; do
    if lsof -ti:$port >/dev/null 2>&1; then
        echo -e "${RED}Error: Port $port already in use${NC}"
        exit 1
    fi
done

echo "Starting t2s pipeline..."

# Window 0: Tickerplant (primary)
tmux new-session -d -s $SESSION -n "tp"
tmux send-keys -t $SESSION:tp "cd $BASEDIR/kdb && q tp.q" C-m

# Window 1: WDB (write-only -> HDB)
tmux new-window -t $SESSION -n "wdb"
tmux send-keys -t $SESSION:wdb "sleep 2 && cd $BASEDIR/kdb && q wdb.q" C-m

# Window 2: MLE (ML features)
tmux new-window -t $SESSION -n "mle"
tmux send-keys -t $SESSION:mle "sleep 5 && cd $BASEDIR/kdb && q mle.q" C-m

# Window 3: Chained TP (batched publisher)
tmux new-window -t $SESSION -n "ctp"
tmux send-keys -t $SESSION:ctp "sleep 6 && cd $BASEDIR/kdb && q chained_tp.q" C-m

# Window 4: RTE (real-time analytics)
tmux new-window -t $SESSION -n "rte"
tmux send-keys -t $SESSION:rte "sleep 5 && cd $BASEDIR/kdb && q rte.q" C-m

# Window 5: TEL (telemetry)
tmux new-window -t $SESSION -n "tel"
tmux send-keys -t $SESSION:tel "sleep 5 && cd $BASEDIR/kdb && q tel.q" C-m

# Window 6: RDB (user queries)
tmux new-window -t $SESSION -n "rdb"
tmux send-keys -t $SESSION:rdb "sleep 7 && cd $BASEDIR/kdb && q rdb.q" C-m

# Window 7: Trade Feed Handler
tmux new-window -t $SESSION -n "trade-fh"
tmux send-keys -t $SESSION:trade-fh "sleep 8 && cd $BASEDIR && ./build/trade_feed_handler" C-m

# Window 8: Quote Feed Handler
tmux new-window -t $SESSION -n "quote-fh"
tmux send-keys -t $SESSION:quote-fh "sleep 9 && cd $BASEDIR && ./build/quote_feed_handler" C-m

# Select first window
tmux select-window -t $SESSION:tp

echo -e "${GREEN}✓ Pipeline starting${NC}"
echo ""
echo "Architecture:"
echo "  TP:5010 -> WDB:5012, RTE:5013, MLE:5015, TEL:5014"
echo "  TP:5010 -> Chained TP:5020 -> RDB:5021"
echo ""
echo "Navigation:"
echo "  Ctrl+B N       next window"
echo "  Ctrl+B P       previous window"
echo "  Ctrl+B 0-8     jump to window"
echo "  Ctrl+B D       detach (keeps running)"
echo ""
echo "Reattach: tmux attach -t $SESSION"
echo ""

tmux attach -t $SESSION
