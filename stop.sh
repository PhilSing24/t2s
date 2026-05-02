#!/bin/bash
# t2s - Stop market data pipeline
# Usage: ./stop.sh [-f|--force]

SESSION="t2s"
BASEDIR="$HOME/t2s"
PIDDIR="$BASEDIR/kdb/logs/processes"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m'

# Force mode (skip graceful shutdown)
FORCE=false
[[ "$1" == "-f" || "$1" == "--force" ]] && FORCE=true

echo "Stopping t2s pipeline..."

# 1. Kill tmux session
if tmux has-session -t $SESSION 2>/dev/null; then
    echo -e "  ${YELLOW}Killing tmux session${NC}"
    tmux kill-session -t $SESSION 2>/dev/null
fi

# 2. Kill by PID files
COMPONENTS=(tp wdb rte tel sig pnl chained_tp rdb trade_fh quote_fh)
for comp in "${COMPONENTS[@]}"; do
    pidfile="$PIDDIR/${comp}.pid"
    if [[ -f "$pidfile" ]]; then
        pid=$(cat "$pidfile")
        if ps -p $pid >/dev/null 2>&1; then
            echo -e "  ${YELLOW}Stopping $comp (PID $pid)${NC}"
            if [[ "$FORCE" == false ]]; then
                kill -15 $pid 2>/dev/null
                sleep 0.5
            fi
            kill -9 $pid 2>/dev/null
        fi
        rm -f "$pidfile"
    fi
done

# 3. Kill stray processes by name (updated paths)
Q_SCRIPTS=(tick/tp.q tick/wdb.q tick/rdb.q tick/chained_tp.q analytics/rte.q analytics/tel.q analytics/sig.q analytics/pnl.q)
FH_PROCS=(trade_feed_handler quote_feed_handler)

for script in "${Q_SCRIPTS[@]}"; do
    if pgrep -f "q $script" >/dev/null 2>&1; then
        echo -e "  ${YELLOW}Killing q $script${NC}"
        [[ "$FORCE" == false ]] && pkill -15 -f "q $script" 2>/dev/null && sleep 0.3
        pkill -9 -f "q $script" 2>/dev/null
    fi
done

# Also check for just the filename (in case working dir differs)
Q_FILES=(tp.q wdb.q rdb.q chained_tp.q rte.q tel.q sig.q pnl.q)
for script in "${Q_FILES[@]}"; do
    if pgrep -f "q $script" >/dev/null 2>&1; then
        echo -e "  ${YELLOW}Killing q $script${NC}"
        [[ "$FORCE" == false ]] && pkill -15 -f "q $script" 2>/dev/null && sleep 0.3
        pkill -9 -f "q $script" 2>/dev/null
    fi
done

for proc in "${FH_PROCS[@]}"; do
    if pgrep -f "$proc" >/dev/null 2>&1; then
        echo -e "  ${YELLOW}Killing $proc${NC}"
        [[ "$FORCE" == false ]] && pkill -15 -f "$proc" 2>/dev/null && sleep 0.3
        pkill -9 -f "$proc" 2>/dev/null
    fi
done

# 4. Kill by ports (last resort)
PORTS=(5010 5011 5012 5014 5015 5016 5017 5018)
for port in "${PORTS[@]}"; do
    pid=$(lsof -ti:$port 2>/dev/null)
    if [[ -n "$pid" ]]; then
        echo -e "  ${YELLOW}Killing process on port $port${NC}"
        [[ "$FORCE" == false ]] && kill -15 $pid 2>/dev/null && sleep 0.3
        kill -9 $pid 2>/dev/null
    fi
done

echo -e "${GREEN}✓ Done${NC}"
