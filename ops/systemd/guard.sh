#!/bin/bash
# guard.sh - ExecStartPre of every t2s unit: refuse to start while a
# tmux-mode pipeline is running, so the two ways of running it can never
# overlap. Exit 3 tells systemd not to retry (RestartPreventExitStatus=3).
if tmux has-session -t t2s 2>/dev/null; then
    echo "t2s: a tmux session 't2s' exists - the pipeline is running in tmux mode. Stop it with ./stop.sh before starting the systemd units." >&2
    exit 3
fi
exit 0
