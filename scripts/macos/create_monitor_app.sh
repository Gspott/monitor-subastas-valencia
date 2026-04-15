#!/bin/zsh

set -u

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
LAUNCHER_SCRIPT="$PROJECT_ROOT/scripts/open_monitor_app.command"
APPLESCRIPT_TEMPLATE="$PROJECT_ROOT/scripts/macos/Monitor Subastas Valencia.applescript"
OUTPUT_DIR="$PROJECT_ROOT/macos"
OUTPUT_APP="$OUTPUT_DIR/Monitor Subastas Valencia.app"
TMP_SCRIPT="$(mktemp /tmp/monitor_subastas_valencia_app.XXXXXX.applescript)"

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
  [[ -f "$TMP_SCRIPT" ]] && /bin/rm -f "$TMP_SCRIPT"
  exit 1
}

cleanup() {
  [[ -f "$TMP_SCRIPT" ]] && /bin/rm -f "$TMP_SCRIPT"
}

trap cleanup EXIT

[[ -x "$LAUNCHER_SCRIPT" ]] || fail "Launcher script not found or not executable: $LAUNCHER_SCRIPT"
[[ -f "$APPLESCRIPT_TEMPLATE" ]] || fail "AppleScript template not found: $APPLESCRIPT_TEMPLATE"
[[ -x "/usr/bin/osacompile" ]] || fail "osacompile not found. This script must run on macOS."

/bin/mkdir -p "$OUTPUT_DIR" || fail "Failed to create output directory: $OUTPUT_DIR"

/usr/bin/sed "s|__PROJECT_ROOT__|$PROJECT_ROOT|g" "$APPLESCRIPT_TEMPLATE" > "$TMP_SCRIPT" \
  || fail "Failed to render AppleScript template"

if [[ -d "$OUTPUT_APP" ]]; then
  log_info "Replacing existing app bundle at $OUTPUT_APP"
  /bin/rm -rf "$OUTPUT_APP" || fail "Failed to remove previous app bundle"
fi

log_info "Compiling macOS app bundle..."
/usr/bin/osacompile -o "$OUTPUT_APP" "$TMP_SCRIPT" || fail "osacompile failed"

log_info "App bundle created at: $OUTPUT_APP"
log_info "You can now double-click it from Finder."
