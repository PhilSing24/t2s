#!/bin/bash
# start_bg.sh - Start all processes in background (for dashboard control)
# Unlike start.sh, this doesn't use tmux
# Each q process runs from kdb/ directory so \l u.q works correctly

cd ~/tick-to-signal

# Kill any existing processes first
./stop.sh 2>/dev/null

# Create log directory
mkdir -p logs/processes

# Start TP (run from kdb/ directory)
cd ~/tick-to-signal/kdb
nohup q tp.q > ../logs/processes/tp.log 2>&1 &
echo $! > ../logs/processes/tp.pid
sleep 2

# Start RDB (run from kdb/ directory)
cd ~/tick-to-signal/kdb
nohup q rdb.q > ../logs/processes/rdb.log 2>&1 &
echo $! > ../logs/processes/rdb.pid
sleep 1

# Start RTE (run from kdb/ directory)
cd ~/tick-to-signal/kdb
nohup q rte.q > ../logs/processes/rte.log 2>&1 &
echo $! > ../logs/processes/rte.pid
sleep 1

# Start TEL (run from kdb/ directory)
cd ~/tick-to-signal/kdb
nohup q tel.q > ../logs/processes/tel.log 2>&1 &
echo $! > ../logs/processes/tel.pid
sleep 1

# Start LOG Manager (run from kdb/ directory)
cd ~/tick-to-signal/kdb
nohup q logmgr.q > ../logs/processes/logmgr.log 2>&1 &
echo $! > ../logs/processes/logmgr.pid
sleep 1

# Start Trade FH (run from project root)
cd ~/tick-to-signal
nohup ./build/trade_feed_handler config/trade_feed_handler.json > logs/processes/trade_fh.log 2>&1 &
echo $! > logs/processes/trade_fh.pid
sleep 1

# Start Quote FH (run from project root)
cd ~/tick-to-signal
nohup ./build/quote_feed_handler > logs/processes/quote_fh.log 2>&1 &
echo $! > logs/processes/quote_fh.pid

echo "All processes started"
