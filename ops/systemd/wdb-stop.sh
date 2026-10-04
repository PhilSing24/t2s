#!/bin/bash
# wdb-stop.sh PORT SECONDS - ExecStop of t2s-wdb.service: ask WDB over IPC
# to flush, checkpoint and exit, and wait for it. Always exits 0: if WDB is
# still there afterwards systemd sends SIGTERM, on which wdb.q flushes too.
port=$1; timeout=${2:-45}
pid=$(lsof -ti TCP:"$port" -sTCP:LISTEN 2>/dev/null | head -1)
[[ -z "$pid" ]] && exit 0
q -q -p 0 < /dev/null > /dev/null 2>&1 <<QEOF
h:@[hopen; (\`\$":localhost:${port}"; 5000); {0N}];
if[not null h; neg[h] ".wdb.shutdownAndExit[]"; neg[h] (::); hclose h];
exit 0
QEOF
deadline=$(( $(date +%s) + timeout ))
while (( $(date +%s) < deadline )); do
    kill -0 "$pid" 2>/dev/null || { echo "t2s: WDB flushed and exited"; exit 0; }
    sleep 0.2
done
echo "t2s: WDB still running ${timeout}s after the IPC shutdown request - leaving it to SIGTERM" >&2
exit 0
