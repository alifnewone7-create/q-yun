#!/usr/bin/env bash
# Restarts quotex-backend when it is running but frozen (/health not answering 3 checks in a row).
URL="${HEALTH_URL:-http://127.0.0.1:8000/health}"
STATE=/run/quotex-backend-health.fails

systemctl is-active --quiet quotex-backend || exit 0

# Skip the first 3 minutes after (re)start: login + market subscribe take time.
started=$(systemctl show -p ActiveEnterTimestampMonotonic --value quotex-backend)
now=$(awk '{printf "%d", $1 * 1000000}' /proc/uptime)
[ $(( (now - started) / 1000000 )) -lt 180 ] && exit 0

if curl -fsS -m 10 "$URL" >/dev/null; then
    rm -f "$STATE"
    exit 0
fi

fails=$(( $(cat "$STATE" 2>/dev/null || echo 0) + 1 ))
echo "$fails" > "$STATE"
echo "health check failed ($fails/3)"
if [ "$fails" -ge 3 ]; then
    rm -f "$STATE"
    echo "backend frozen - restarting quotex-backend"
    systemctl restart quotex-backend
fi
