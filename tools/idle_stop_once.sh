#!/usr/bin/env bash
set -euo pipefail

# One-shot idle stopper for host cron/systemd timer.
# It does not run as a permanent process. Run it once per minute from the host:
#   IDLE_SECONDS=900 tools/idle_stop_once.sh
#
# By default PostgreSQL is kept running to avoid unnecessary DB restart churn.
# Set STOP_DB=1 if the deployment must stop the database too.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ACTIVITY_FILE="${ACTIVITY_FILE:-$ROOT_DIR/.runtime/last_activity}"
IDLE_SECONDS="${IDLE_SECONDS:-900}"
SERVICES="${SERVICES:-telegram-bot api ocr-service web}"

if [[ "${STOP_DB:-0}" == "1" ]]; then
  SERVICES="$SERVICES db"
fi

if [[ ! -f "$ACTIVITY_FILE" ]]; then
  exit 0
fi

if last_ts="$(stat -f %m "$ACTIVITY_FILE" 2>/dev/null)"; then
  :
elif last_ts="$(stat -c %Y "$ACTIVITY_FILE" 2>/dev/null)"; then
  :
else
  exit 0
fi

now_ts="$(date +%s)"
idle_for=$((now_ts - last_ts))

if (( idle_for < IDLE_SECONDS )); then
  exit 0
fi

cd "$ROOT_DIR"
echo "No activity for ${idle_for}s; stopping services: $SERVICES"
docker compose stop $SERVICES
