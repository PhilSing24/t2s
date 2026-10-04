#!/bin/bash
# wait-port.sh PORT SECONDS - return 0 once something listens on PORT.
# ExecStartPost of TP and WDB, so "started" means "listening".
port=$1; deadline=$(( $(date +%s) + ${2:-30} ))
while (( $(date +%s) < deadline )); do
    if lsof -ti TCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then exit 0; fi
    sleep 0.2
done
echo "t2s: nothing listening on port $port after ${2:-30}s" >&2
exit 1
