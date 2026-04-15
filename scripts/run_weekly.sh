#!/bin/zsh

set -u

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
VENV_DIR="$PROJECT_ROOT/.venv"
PYTHON_BIN="$VENV_DIR/bin/python"
MAIN_MODULE="scripts.run_full_monitor_cycle"
ENV_FILE="$PROJECT_ROOT/.env.launchd"
LOG_DIR="$PROJECT_ROOT/logs"
RUN_LOG_DIR="$LOG_DIR/weekly"
STDOUT_LOG="$RUN_LOG_DIR/run_weekly.stdout.log"
STDERR_LOG="$RUN_LOG_DIR/run_weekly.stderr.log"
LOCK_DIR="$RUN_LOG_DIR/run_weekly.lock"

umask 022
mkdir -p "$RUN_LOG_DIR"

# Duplicar la salida a logs y a la terminal cuando exista una sesion interactiva.
exec > >(/usr/bin/tee -a "$STDOUT_LOG") 2> >(/usr/bin/tee -a "$STDERR_LOG" >&2)

timestamp() {
  /bin/date '+%Y-%m-%d %H:%M:%S'
}

cleanup() {
  local exit_code="$?"
  if [[ -d "$LOCK_DIR" ]]; then
    /bin/rmdir "$LOCK_DIR" 2>/dev/null || true
  fi
  echo "[$(timestamp)] [END] run_weekly.sh exit_code=$exit_code"
}

trap cleanup EXIT INT TERM

echo "[$(timestamp)] [START] run_weekly.sh"

# Evitar ejecuciones solapadas si launchd dispara una nueva corrida.
if ! /bin/mkdir "$LOCK_DIR" 2>/dev/null; then
  echo "[$(timestamp)] [WARN] Another weekly run is already in progress. Exiting."
  exit 0
fi

if [[ ! -d "$PROJECT_ROOT" ]]; then
  echo "[$(timestamp)] [ERROR] Project root not found: $PROJECT_ROOT"
  exit 1
fi

cd "$PROJECT_ROOT" || exit 1

if [[ -f "$ENV_FILE" ]]; then
  echo "[$(timestamp)] [INFO] Loading environment file: $ENV_FILE"
  set -a
  . "$ENV_FILE"
  set +a
else
  echo "[$(timestamp)] [INFO] No .env.launchd file found. Using current environment only."
fi

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "[$(timestamp)] [ERROR] Python interpreter not found in virtualenv: $PYTHON_BIN"
  exit 1
fi

# Activar el venv mantiene compatibilidad con scripts que esperen variables del entorno.
if [[ -f "$VENV_DIR/bin/activate" ]]; then
  . "$VENV_DIR/bin/activate"
fi

export PATH="$VENV_DIR/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
export PYTHONUNBUFFERED=1

echo "[$(timestamp)] [INFO] Working directory: $PROJECT_ROOT"
echo "[$(timestamp)] [INFO] Using python: $PYTHON_BIN"
echo "[$(timestamp)] [INFO] Running module: $MAIN_MODULE"

"$PYTHON_BIN" -m "$MAIN_MODULE"
