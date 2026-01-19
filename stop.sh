#!/bin/bash
# stop.sh - Stop all market data pipeline components
# Uses graceful shutdown (SIGTERM) first, then forced (SIGKILL)
#
# Usage: ./stop.sh

echo "Stopping market data pipeline (graceful)..."

# Kill tmux session (stops all processes in panes)
tmux kill-session -t market-data 2>/dev/null

# Kill by PID files if they exist (graceful then forced)
for pidfile in tp wdb rte tel mle chained_tp rdb trade_fh quote_fh; do
    if [ -f ~/new-tick-to-signal/logs/processes/${pidfile}.pid ]; then
        pid=$(cat ~/new-tick-to-signal/logs/processes/${pidfile}.pid)
        # Graceful first
        kill -15 $pid 2>/dev/null
    fi
done

# Wait for graceful shutdown
sleep 1

# Force kill any remaining by PID
for pidfile in tp wdb rte tel mle chained_tp rdb trade_fh quote_fh; do
    if [ -f ~/new-tick-to-signal/logs/processes/${pidfile}.pid ]; then
        pid=$(cat ~/new-tick-to-signal/logs/processes/${pidfile}.pid)
        kill -9 $pid 2>/dev/null
        rm -f ~/new-tick-to-signal/logs/processes/${pidfile}.pid
    fi
done

# Kill any stray feed handler processes (graceful then forced)
pkill -15 -f "trade_feed_handler" 2>/dev/null
pkill -15 -f "quote_feed_handler" 2>/dev/null
sleep 1
pkill -9 -f "trade_feed_handler" 2>/dev/null
pkill -9 -f "quote_feed_handler" 2>/dev/null

# Kill q processes by specific script names - graceful then forced
pkill -15 -f "q tp.q" 2>/dev/null
pkill -15 -f "q wdb.q" 2>/dev/null
pkill -15 -f "q rte.q" 2>/dev/null
pkill -15 -f "q tel.q" 2>/dev/null
pkill -15 -f "q mle.q" 2>/dev/null
pkill -15 -f "q chained_tp.q" 2>/dev/null
pkill -15 -f "q rdb.q" 2>/dev/null
sleep 1
pkill -9 -f "q tp.q" 2>/dev/null
pkill -9 -f "q wdb.q" 2>/dev/null
pkill -9 -f "q rte.q" 2>/dev/null
pkill -9 -f "q tel.q" 2>/dev/null
pkill -9 -f "q mle.q" 2>/dev/null
pkill -9 -f "q chained_tp.q" 2>/dev/null
pkill -9 -f "q rdb.q" 2>/dev/null

# Kill by ports - graceful then forced
for port in 5010 5011 5012 5014 5015 5016 5017; do
    lsof -ti:$port 2>/dev/null | xargs kill -15 2>/dev/null
done
sleep 1
for port in 5010 5011 5012 5014 5015 5016 5017; do
    lsof -ti:$port 2>/dev/null | xargs kill -9 2>/dev/null
done

echo "Done."
