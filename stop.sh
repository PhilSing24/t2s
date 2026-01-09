#!/bin/bash
# stop.sh - Stop all market data pipeline components
#
# Usage: ./stop.sh

echo "Stopping market data pipeline..."

# Kill tmux session (stops all processes in panes)
tmux kill-session -t market-data 2>/dev/null

# Kill by PID files if they exist
for pidfile in tp rdb rte tel logmgr trade_fh quote_fh; do
    if [ -f ~/tick-to-signal/logs/processes/${pidfile}.pid ]; then
        pid=$(cat ~/tick-to-signal/logs/processes/${pidfile}.pid)
        kill -9 $pid 2>/dev/null
        rm -f ~/tick-to-signal/logs/processes/${pidfile}.pid
    fi
done

# Kill any stray processes
pkill -f "trade_feed_handler" 2>/dev/null
pkill -f "quote_feed_handler" 2>/dev/null
pkill -f "q kdb/" 2>/dev/null

echo "Done."
