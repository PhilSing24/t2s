#!/bin/bash
# clock_check.sh - is the WSL clock still right, and fix it if allowed.
#
# WSL2's clock can fall behind after the laptop sleeps. Partitions are dated
# by this clock, so drift matters. The VM's hardware clock (RTC) follows the
# Windows clock; /sys/class/rtc/rtc0/since_epoch reads it without root.
#
#   drift = RTC - system clock, in seconds
#   |drift| <= T2S_CLOCK_MAX_DRIFT_SEC (default 2): one "ok" line per hour
#   otherwise: sudo -n /usr/sbin/hwclock -s  (re-reads the RTC)
#
# The fix needs this sudoers line (not installed by the repo):
#   philippe ALL=(root) NOPASSWD: /usr/sbin/hwclock -s
# Without it the drift is reported and the script exits 1, which shows as a
# failed unit and in ./status.sh.
#
# Usage: ops/clock_check.sh [--check]   (--check: report only, never fix)
# Prints one line. Exit 0 ok or fixed, 1 drift not fixed, 2 RTC unreadable.

MAX=${T2S_CLOCK_MAX_DRIFT_SEC:-2}
RTC=${T2S_CLOCK_RTC_FILE:-/sys/class/rtc/rtc0/since_epoch}
stamp() { date -u +%Y-%m-%dT%H:%M:%SZ; }

if [[ ! -r "$RTC" ]]; then echo "$(stamp) clock: cannot read $RTC"; exit 2; fi
rtc=$(cat "$RTC"); sys=$(date +%s); drift=$(( rtc - sys )); abs=${drift#-}

if (( abs <= MAX )); then
    # quiet when fine: one line per hour is enough in the log
    if [[ "${1:-}" == "--check" || $(( $(date +%-M) / 5 )) -eq 0 ]]; then echo "$(stamp) clock: ok (drift ${drift}s)"; fi
    exit 0
fi
if [[ "${1:-}" == "--check" ]]; then
    echo "$(stamp) clock: DRIFT ${drift}s (system clock is $([[ $drift -gt 0 ]] && echo behind || echo ahead))"
    exit 1
fi
if sudo -n /usr/sbin/hwclock -s 2>/dev/null; then
    rtc=$(cat "$RTC"); sys=$(date +%s)
    echo "$(stamp) clock: DRIFT ${drift}s corrected with hwclock -s (now $(( rtc - sys ))s)"
    exit 0
fi
echo "$(stamp) clock: DRIFT ${drift}s NOT corrected - sudo -n hwclock -s is not permitted (see the sudoers line in ops/RUNNING.md)"
exit 1
