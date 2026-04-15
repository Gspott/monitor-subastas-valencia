#!/bin/zsh

set -u

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RUN_SCRIPT="$PROJECT_ROOT/scripts/run_weekly.sh"
PLIST_SOURCE="$PROJECT_ROOT/scripts/launchd/com.carlosblanco.monitor-subastas-valencia.weekly.plist"
ENV_FILE="$PROJECT_ROOT/.env.launchd"
PYTHON_BIN="$PROJECT_ROOT/.venv/bin/python"
LAUNCH_AGENTS_DIR="$HOME/Library/LaunchAgents"
PLIST_TARGET="$LAUNCH_AGENTS_DIR/com.carlosblanco.monitor-subastas-valencia.weekly.plist"
JOB_LABEL="com.carlosblanco.monitor-subastas-valencia.weekly"
GUI_DOMAIN="gui/$(id -u)"
JOB_ID="$GUI_DOMAIN/$JOB_LABEL"
LOG_DIR="$PROJECT_ROOT/logs"
WEEKLY_LOG_DIR="$LOG_DIR/weekly"

timestamp() {
  /bin/date '+%Y-%m-%d %H:%M:%S'
}

log_info() {
  echo "[$(timestamp)] [INFO] $1"
}

log_warn() {
  echo "[$(timestamp)] [WARN] $1"
}

log_error() {
  echo "[$(timestamp)] [ERROR] $1" >&2
}

fail() {
  log_error "$1"
  exit 1
}

log_info "Starting launchd weekly install/reload"

[[ -d "$PROJECT_ROOT" ]] || fail "Project root not found: $PROJECT_ROOT"
[[ -f "$RUN_SCRIPT" ]] || fail "Wrapper script not found: $RUN_SCRIPT"
[[ -f "$PLIST_SOURCE" ]] || fail "launchd plist not found: $PLIST_SOURCE"
[[ -x "$PYTHON_BIN" ]] || fail "Virtualenv Python not found or not executable: $PYTHON_BIN"

log_info "Validating weekly wrapper syntax"
/bin/zsh -n "$RUN_SCRIPT" || fail "zsh syntax validation failed for $RUN_SCRIPT"

log_info "Validating launchd plist"
/usr/bin/plutil -lint "$PLIST_SOURCE" >/dev/null || fail "plist validation failed for $PLIST_SOURCE"

if [[ ! -f "$ENV_FILE" ]]; then
  log_warn ".env.launchd not found at $ENV_FILE"
  log_warn "Telegram and launchd-specific environment variables will not be loaded by run_weekly.sh"
fi

log_info "Creating required directories"
/bin/mkdir -p "$LAUNCH_AGENTS_DIR" "$LOG_DIR" "$WEEKLY_LOG_DIR" || fail "Failed to create required directories"

log_info "Ensuring wrapper is executable"
/bin/chmod +x "$RUN_SCRIPT" || fail "Failed to chmod +x $RUN_SCRIPT"

log_info "Rendering plist into LaunchAgents"
/usr/bin/sed "s|__PROJECT_ROOT__|$PROJECT_ROOT|g" "$PLIST_SOURCE" > "$PLIST_TARGET" \
  || fail "Failed to render plist to $PLIST_TARGET"

log_info "Booting out existing job if already loaded"
if /bin/launchctl print "$JOB_ID" >/dev/null 2>&1; then
  /bin/launchctl bootout "$GUI_DOMAIN" "$PLIST_TARGET" || fail "launchctl bootout failed for $JOB_LABEL"
else
  log_info "Job not currently loaded"
fi

log_info "Bootstrapping weekly job"
/bin/launchctl bootstrap "$GUI_DOMAIN" "$PLIST_TARGET" || fail "launchctl bootstrap failed for $JOB_LABEL"

if /bin/launchctl print "$JOB_ID" >/dev/null 2>&1; then
  log_info "Job loaded successfully: $JOB_ID"
else
  fail "Job bootstrap completed but launchctl print could not find $JOB_ID"
fi

cat <<EOF

Launchd weekly job is installed.

Suggested next commands:
  Manual wrapper run:
    $RUN_SCRIPT

  Manual launchd kickstart:
    launchctl kickstart -k "$JOB_ID"

  Inspect loaded job:
    launchctl print "$JOB_ID"
    launchctl list | rg "$JOB_LABEL"

  Review logs:
    tail -n 100 "$WEEKLY_LOG_DIR/run_weekly.stdout.log"
    tail -n 100 "$WEEKLY_LOG_DIR/run_weekly.stderr.log"
    tail -n 100 "$LOG_DIR/launchd.stdout.log"
    tail -n 100 "$LOG_DIR/launchd.stderr.log"
EOF
