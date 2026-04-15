#!/bin/zsh

set -u

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LAUNCH_AGENTS_DIR="$HOME/Library/LaunchAgents"
PLIST_TARGET="$LAUNCH_AGENTS_DIR/com.carlosblanco.monitor-subastas-valencia.weekly.plist"
JOB_LABEL="com.carlosblanco.monitor-subastas-valencia.weekly"
GUI_DOMAIN="gui/$(id -u)"
JOB_ID="$GUI_DOMAIN/$JOB_LABEL"

timestamp() {
  /bin/date '+%Y-%m-%d %H:%M:%S'
}

log_info() {
  echo "[$(timestamp)] [INFO] $1"
}

log_warn() {
  echo "[$(timestamp)] [WARN] $1"
}

fail() {
  echo "[$(timestamp)] [ERROR] $1" >&2
  exit 1
}

[[ -d "$PROJECT_ROOT" ]] || fail "Project root not found: $PROJECT_ROOT"

log_info "Starting launchd weekly uninstall"

if [[ -f "$PLIST_TARGET" ]] || /bin/launchctl print "$JOB_ID" >/dev/null 2>&1; then
  if /bin/launchctl print "$JOB_ID" >/dev/null 2>&1; then
    log_info "Booting out loaded job"
    /bin/launchctl bootout "$GUI_DOMAIN" "$PLIST_TARGET" || fail "launchctl bootout failed for $JOB_LABEL"
  else
    log_info "Job is not loaded"
  fi

  if [[ -f "$PLIST_TARGET" ]]; then
    log_info "Removing plist from LaunchAgents"
    /bin/rm -f "$PLIST_TARGET" || fail "Failed to remove $PLIST_TARGET"
  else
    log_warn "No plist file found at $PLIST_TARGET"
  fi
else
  log_warn "Nothing to uninstall for $JOB_LABEL"
fi

cat <<EOF

Launchd weekly job is uninstalled.

You can confirm with:
  launchctl print "$JOB_ID"
  launchctl list | rg "$JOB_LABEL"
EOF
