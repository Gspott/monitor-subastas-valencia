#!/bin/zsh

set -u

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
START_SCRIPT="$PROJECT_ROOT/scripts/start_local_monitor_services.sh"
WEB_APP_URL="http://127.0.0.1:8765"

timestamp() {
  /bin/date '+%Y-%m-%d %H:%M:%S'
}

log_info() {
  echo "[$(timestamp)] [INFO] $1"
}

[[ -x "$START_SCRIPT" ]] || {
  echo "Start script not found or not executable: $START_SCRIPT" >&2
  exit 1
}

log_info "Starting local monitor services if needed..."
"$START_SCRIPT" || exit 1

log_info "Waiting briefly for the web app to become reachable..."
for _ in 1 2 3 4 5 6 7 8 9 10; do
  if /usr/bin/curl --silent --show-error --max-time 2 --output /dev/null "$WEB_APP_URL"; then
    break
  fi
  /bin/sleep 1
done

log_info "Opening browser at $WEB_APP_URL"
/usr/bin/open "$WEB_APP_URL"
