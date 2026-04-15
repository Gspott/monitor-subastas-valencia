#!/bin/zsh

set -u

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
VENV_DIR="$PROJECT_ROOT/.venv"
PYTHON_BIN="$VENV_DIR/bin/python"
STREAMLIT_BIN="$VENV_DIR/bin/streamlit"
WEB_APP_SCRIPT="$PROJECT_ROOT/apps/monitor_runner_web.py"
DASHBOARD_SCRIPT="$PROJECT_ROOT/scripts/monitor_dashboard.py"
LOG_DIR="$PROJECT_ROOT/logs/local_services"
WEB_APP_LOG="$LOG_DIR/web_app.log"
DASHBOARD_LOG="$LOG_DIR/dashboard.log"
WEB_APP_URL="http://127.0.0.1:8765"
DASHBOARD_URL="http://127.0.0.1:8501"

timestamp() {
  /bin/date '+%Y-%m-%d %H:%M:%S'
}

log_info() {
  echo "[$(timestamp)] [INFO] $1"
}

log_error() {
  echo "[$(timestamp)] [ERROR] $1" >&2
}

fail() {
  log_error "$1"
  exit 1
}

is_http_ready() {
  local url="$1"
  /usr/bin/curl --silent --show-error --max-time 2 --output /dev/null "$url"
}

start_background_service() {
  local name="$1"
  local log_file="$2"
  shift 2

  log_info "Starting $name..."
  (
    cd "$PROJECT_ROOT" || exit 1
    export PATH="$VENV_DIR/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
    export PYTHONUNBUFFERED=1
    nohup "$@" >>"$log_file" 2>&1 &
  )
}

[[ -d "$PROJECT_ROOT" ]] || fail "Project root not found: $PROJECT_ROOT"
[[ -x "$PYTHON_BIN" ]] || fail "Virtualenv Python not found: $PYTHON_BIN"
[[ -x "$STREAMLIT_BIN" ]] || fail "Streamlit not found: $STREAMLIT_BIN"
[[ -f "$WEB_APP_SCRIPT" ]] || fail "Web app script not found: $WEB_APP_SCRIPT"
[[ -f "$DASHBOARD_SCRIPT" ]] || fail "Dashboard script not found: $DASHBOARD_SCRIPT"

/bin/mkdir -p "$LOG_DIR" || fail "Failed to create log directory: $LOG_DIR"

if is_http_ready "$WEB_APP_URL"; then
  log_info "Web app already responding at $WEB_APP_URL"
else
  start_background_service \
    "web app" \
    "$WEB_APP_LOG" \
    "$PYTHON_BIN" \
    "$WEB_APP_SCRIPT"
fi

if is_http_ready "$DASHBOARD_URL"; then
  log_info "Dashboard already responding at $DASHBOARD_URL"
else
  start_background_service \
    "dashboard" \
    "$DASHBOARD_LOG" \
    "$STREAMLIT_BIN" \
    run \
    "$DASHBOARD_SCRIPT" \
    --server.headless \
    true
fi

log_info "Local services startup check completed."
