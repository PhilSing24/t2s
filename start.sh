#!/bin/bash
# t2s - Start market data pipeline
# Usage: ./start.sh

set -e  # Exit on error

SESSION="t2s"
# Resolve the project root from the script's own location so this works
# regardless of where the repo is cloned (previously hardcoded $HOME/t2s,
# which silently launched the wrong copy if you had multiple checkouts).
BASEDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

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
PORTS=(5010 5011 5012 5014 5015 5016 5017 5018)
for port in "${PORTS[@]}"; do
    if lsof -ti:$port >/dev/null 2>&1; then
        echo -e "${RED}Error: Port $port already in use${NC}"
        exit 1
    fi
done

echo "Starting t2s pipeline..."

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

# Window 8: Trade Feed Handler
tmux new-window -t $SESSION -n "trade-fh"
tmux send-keys -t $SESSION:trade-fh "sleep 8 && cd $BASEDIR && ./build/trade_feed_handler" C-m

# Window 9: Quote Feed Handler
tmux new-window -t $SESSION -n "quote-fh"
tmux send-keys -t $SESSION:quote-fh "sleep 9 && cd $BASEDIR && ./build/quote_feed_handler" C-m

# Select first window
tmux select-window -t $SESSION:tp

echo -e "${GREEN}✓ Pipeline starting${NC}"
echo ""
echo "Architecture:"
echo "  Primary TP:5010 -> WDB:5011, SIG:5012"
echo "  Primary TP:5010 -> Chained TP:5014 -> RTE:5015, TEL:5016, RDB:5017, PNL:5018"
echo "  SIG:5012 -> Chained TP:5014 (positions)"
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
