#!/bin/bash
# Run after midnight UTC to verify EOD persistence
echo "=== HDB partitions ==="
ls -la ~/t2s/hdb/

echo ""
echo "=== Yesterday's partition ==="
YESTERDAY=$(date -u -d 'yesterday' +%Y.%m.%d)
ls -la ~/t2s/hdb/$YESTERDAY/ 2>/dev/null || echo "Missing — EOD did not persist"

echo ""
echo "=== WDB EOD logs ==="
tmux capture-pane -t t2s:wdb -p 2>/dev/null | grep -E "EOD|ERROR|Moving|HDB partition" | tail -20

echo ""
echo "=== Lingering temp dirs (should be empty if EOD succeeded) ==="
find ~/t2s -maxdepth 2 -name "tmp.*" -type d 2>/dev/null
