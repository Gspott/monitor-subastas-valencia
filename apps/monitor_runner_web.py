"""Local web UI to run the full monitor cycle from a browser."""

from __future__ import annotations

import json
import os
import queue
import subprocess
import sys
import threading
from collections import deque
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    from flask import Flask, Response, abort, jsonify, render_template_string, request
except ModuleNotFoundError as exc:  # pragma: no cover - mensaje de arranque
    raise SystemExit(
        "Flask is not installed. Install it with: .venv/bin/pip install -e '.[web]'"
    ) from exc


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from monitor.runner_progress import (  # noqa: E402
    STAGE_ERROR,
    STAGE_FINISHED,
    STAGE_IDLE,
    parse_runner_progress_line,
)


APP_TITLE = "Monitor Subastas Valencia"
STATE_PATH = REPO_ROOT / "output" / "monitor_runner_web_state.json"
RUN_LOG_DIR = REPO_ROOT / "logs" / "web_runner"
DASHBOARD_URL = os.environ.get("MONITOR_DASHBOARD_URL", "http://127.0.0.1:8501")
MAX_LOG_LINES = 4000
MAX_HISTORY_ITEMS = 10
RUN_MODE_PARTIAL = "Partial"
RUN_MODE_FULL = "Full"


@dataclass(slots=True)
class ExecutionHistoryItem:
    """Compact persisted summary for one finished run."""

    started_at: str
    finished_at: str
    exit_code: int | None
    status: str
    mode: str
    log_path: str


class MonitorRunManager:
    """Singleton in-memory manager for the current subprocess and UI state."""

    def __init__(self) -> None:
        self.project_root = REPO_ROOT
        self.python_bin = self.project_root / ".venv" / "bin" / "python"
        self.streamlit_bin = self.project_root / ".venv" / "bin" / "streamlit"
        self.dashboard_script = self.project_root / "scripts" / "monitor_dashboard.py"
        self.env_file = self.project_root / ".env.launchd"
        self.logs_dir = RUN_LOG_DIR
        self.state_path = STATE_PATH
        self.lock = threading.Lock()
        self.process: subprocess.Popen[str] | None = None
        self.dashboard_process: subprocess.Popen[str] | None = None
        self.current_stage = STAGE_IDLE
        self.status_text = "Idle"
        self.detail_text = "Ready to run"
        self.general_progress = 0
        self.started_at: str | None = None
        self.finished_at: str | None = None
        self.exit_code: int | None = None
        self.cancel_requested = False
        self.current_mode = RUN_MODE_PARTIAL
        self.dashboard_started_at: str | None = None
        self.dashboard_pid: int | None = None
        self.dashboard_log_path: Path | None = None
        self.dashboard_log_handle = None
        self.current_log_path: Path | None = None
        self.current_log_lines: deque[str] = deque(maxlen=MAX_LOG_LINES)
        self.current_log_handle = None
        self.history: list[ExecutionHistoryItem] = self._load_history()
        self.process_queue: queue.Queue[tuple[str, str]] = queue.Queue()

    def start(self, mode: str = RUN_MODE_PARTIAL) -> tuple[bool, str]:
        """Start a new monitor subprocess unless one is already running."""
        with self.lock:
            if self.process is not None:
                return False, "A monitor run is already in progress."

            if not self.python_bin.exists():
                return False, f"Virtualenv Python not found: {self.python_bin}"

            normalized_mode = self._normalize_mode(mode)
            if normalized_mode is None:
                return False, f"Unsupported run mode: {mode}"

            self.logs_dir.mkdir(parents=True, exist_ok=True)
            started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            log_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            self.current_log_path = self.logs_dir / f"run_{log_timestamp}.log"
            self.current_log_handle = self.current_log_path.open("a", encoding="utf-8")
            self.current_log_lines.clear()
            self.current_stage = STAGE_IDLE
            self.status_text = "Starting monitor"
            self.detail_text = "Launching subprocess"
            self.general_progress = 0
            self.started_at = started_at
            self.finished_at = None
            self.exit_code = None
            self.cancel_requested = False
            self.current_mode = normalized_mode

            env = os.environ.copy()
            env.update(self._load_launchd_env())
            env["PATH"] = (
                f"{self.project_root / '.venv' / 'bin'}:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
            )
            env["PYTHONUNBUFFERED"] = "1"

            command = [str(self.python_bin), "-m", "scripts.run_full_monitor_cycle"]
            if normalized_mode == RUN_MODE_FULL:
                command.append("--completed-full-refresh")
            try:
                self.process = subprocess.Popen(
                    command,
                    cwd=self.project_root,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    bufsize=1,
                    env=env,
                )
            except OSError as exc:
                self._append_log_line(f"[SYSTEM] Failed to launch process: {exc}\n")
                self.status_text = "Error"
                self.detail_text = f"Failed to launch process: {exc}"
                self.general_progress = 100
                self.exit_code = None
                self._close_log_handle()
                self.process = None
                return False, self.detail_text

            self._append_log_line(f"[SYSTEM] Monitor run launched in {normalized_mode} mode\n")
            assert self.process.stdout is not None
            assert self.process.stderr is not None
            threading.Thread(
                target=self._stream_reader,
                args=(self.process.stdout, "stdout"),
                daemon=True,
            ).start()
            threading.Thread(
                target=self._stream_reader,
                args=(self.process.stderr, "stderr"),
                daemon=True,
            ).start()
            threading.Thread(
                target=self._wait_for_process,
                daemon=True,
            ).start()
            threading.Thread(
                target=self._drain_queue_loop,
                daemon=True,
            ).start()
            return True, "Monitor run started."

    def stop(self) -> tuple[bool, str]:
        """Request a cautious stop for the current subprocess."""
        with self.lock:
            if self.process is None:
                return False, "No monitor run is active."

            self.cancel_requested = True
            self.detail_text = "Stopping process..."
            self._append_log_line("[SYSTEM] Stop requested by user\n")
            try:
                self.process.terminate()
            except OSError:
                return False, "Unable to terminate process."
            threading.Thread(target=self._force_kill_after_timeout, daemon=True).start()
            return True, "Stop requested."

    def open_logs_folder(self) -> tuple[bool, str]:
        """Open the web runner logs folder in Finder."""
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        try:
            subprocess.Popen(["open", str(self.logs_dir)])
        except OSError as exc:
            return False, f"Failed to open logs folder: {exc}"
        return True, str(self.logs_dir)

    def start_dashboard(self) -> tuple[bool, str]:
        """Start the Streamlit dashboard unless one is already running."""
        with self.lock:
            self._refresh_dashboard_process_state()
            if self.dashboard_process is not None:
                return False, "The dashboard is already running."

            if not self.streamlit_bin.exists():
                return False, f"Streamlit not found: {self.streamlit_bin}"

            if not self.dashboard_script.exists():
                return False, f"Dashboard script not found: {self.dashboard_script}"

            self.logs_dir.mkdir(parents=True, exist_ok=True)
            started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            log_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            self.dashboard_log_path = self.logs_dir / f"dashboard_{log_timestamp}.log"
            self.dashboard_log_handle = self.dashboard_log_path.open("a", encoding="utf-8")

            env = os.environ.copy()
            env.update(self._load_launchd_env())
            env["PATH"] = (
                f"{self.project_root / '.venv' / 'bin'}:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
            )
            env["PYTHONUNBUFFERED"] = "1"

            command = [
                str(self.streamlit_bin),
                "run",
                str(self.dashboard_script),
            ]
            try:
                self.dashboard_process = subprocess.Popen(
                    command,
                    cwd=self.project_root,
                    stdout=self.dashboard_log_handle,
                    stderr=subprocess.STDOUT,
                    text=True,
                    env=env,
                )
            except OSError as exc:
                self._close_dashboard_log_handle()
                self.dashboard_log_path = None
                return False, f"Failed to launch dashboard: {exc}"

            self.dashboard_started_at = started_at
            self.dashboard_pid = self.dashboard_process.pid
            return True, "Dashboard started."

    def stop_dashboard(self) -> tuple[bool, str]:
        """Stop the Streamlit dashboard if it is running."""
        with self.lock:
            self._refresh_dashboard_process_state()
            if self.dashboard_process is None:
                return False, "The dashboard is not running."

            process = self.dashboard_process
            try:
                process.terminate()
            except OSError:
                return False, "Unable to terminate dashboard process."

        threading.Thread(
            target=self._force_stop_dashboard_after_timeout,
            args=(process,),
            daemon=True,
        ).start()
        return True, "Dashboard stop requested."

    def snapshot(self) -> dict[str, Any]:
        """Return the current UI state for polling clients."""
        with self.lock:
            running = self.process is not None
            self._refresh_dashboard_process_state()
            current_log_path = self._current_or_latest_log_path()
            return {
                "title": APP_TITLE,
                "running": running,
                "status": self.status_text,
                "detail": self.detail_text,
                "general_progress": self.general_progress,
                "started_at": self.started_at,
                "finished_at": self.finished_at,
                "exit_code": self.exit_code,
                "mode": self.current_mode,
                "log_text": "".join(self.current_log_lines),
                "log_path": str(self.current_log_path) if self.current_log_path is not None else "",
                "current_or_latest_log_path": (
                    str(current_log_path) if current_log_path is not None else ""
                ),
                "current_log_name": current_log_path.name if current_log_path is not None else "",
                "current_log_url": "/logs/current" if current_log_path is not None else "",
                "dashboard_url": DASHBOARD_URL,
                "dashboard_running": self.dashboard_process is not None,
                "dashboard_status": "running" if self.dashboard_process is not None else "not running",
                "dashboard_started_at": self.dashboard_started_at,
                "dashboard_pid": self.dashboard_pid,
                "dashboard_log_path": (
                    str(self.dashboard_log_path) if self.dashboard_log_path is not None else ""
                ),
                "history": [asdict(item) for item in self.history],
            }

    def _stream_reader(self, stream, stream_name: str) -> None:
        for line in iter(stream.readline, ""):
            self.process_queue.put((stream_name, line))
        stream.close()

    def _wait_for_process(self) -> None:
        process = self.process
        if process is None:
            return
        return_code = process.wait()
        self.process_queue.put(("process_exit", str(return_code)))

    def _drain_queue_loop(self) -> None:
        while True:
            try:
                source, payload = self.process_queue.get(timeout=0.2)
            except queue.Empty:
                with self.lock:
                    if self.process is None:
                        return
                continue

            if source == "process_exit":
                self._handle_process_exit(int(payload))
                return

            self._handle_process_line(payload)

    def _handle_process_line(self, line: str) -> None:
        with self.lock:
            self._append_log_line(line)
            progress = parse_runner_progress_line(line, current_stage=self.current_stage)
            if progress is None:
                return
            self.current_stage = progress.stage
            self.status_text = progress.status_text
            self.detail_text = progress.detail_text
            self.general_progress = progress.general_progress

    def _handle_process_exit(self, return_code: int) -> None:
        with self.lock:
            self.exit_code = return_code
            self.finished_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            if self.cancel_requested and return_code != 0:
                self.current_stage = STAGE_ERROR
                self.status_text = "Cancelled"
                self.detail_text = "Process stopped by user"
                self.general_progress = 100
                self._append_log_line("[SYSTEM] Monitor process cancelled by user\n")
            elif return_code == 0 and self.current_stage != STAGE_ERROR:
                self.current_stage = STAGE_FINISHED
                self.status_text = "Finished"
                self.detail_text = "Monitor cycle completed"
                self.general_progress = 100
                self._append_log_line("[SYSTEM] Monitor process finished successfully\n")
            else:
                self.current_stage = STAGE_ERROR
                self.status_text = "Error"
                self.detail_text = "Monitor process failed"
                self.general_progress = 100
                self._append_log_line("[SYSTEM] Monitor process finished with an error\n")

            self._remember_finished_run()
            self._close_log_handle()
            self.process = None
            self.cancel_requested = False

    def _force_kill_after_timeout(self) -> None:
        process = self.process
        if process is None:
            return
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self._append_log_line("[SYSTEM] Process did not stop in time. Killing it.\n")
            try:
                process.kill()
            except OSError:
                return

    def _append_log_line(self, line: str) -> None:
        self.current_log_lines.append(line)
        if self.current_log_handle is not None:
            self.current_log_handle.write(line)
            self.current_log_handle.flush()

    def _remember_finished_run(self) -> None:
        if self.started_at is None or self.finished_at is None or self.current_log_path is None:
            return
        self.history.insert(
            0,
            ExecutionHistoryItem(
                started_at=self.started_at,
                finished_at=self.finished_at,
                exit_code=self.exit_code,
                status=self.status_text,
                mode=self.current_mode,
                log_path=str(self.current_log_path),
            ),
        )
        self.history = self.history[:MAX_HISTORY_ITEMS]
        self._save_history()

    def _save_history(self) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"history": [asdict(item) for item in self.history]}
        self.state_path.write_text(
            json.dumps(payload, indent=2, ensure_ascii=True),
            encoding="utf-8",
        )

    def _load_history(self) -> list[ExecutionHistoryItem]:
        if not self.state_path.exists():
            return []
        try:
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return []
        history = payload.get("history", [])
        if not isinstance(history, list):
            return []
        items: list[ExecutionHistoryItem] = []
        for raw_item in history[:MAX_HISTORY_ITEMS]:
            if not isinstance(raw_item, dict):
                continue
            try:
                items.append(
                    ExecutionHistoryItem(
                        started_at=str(raw_item.get("started_at", "-")),
                        finished_at=str(raw_item.get("finished_at", "-")),
                        exit_code=raw_item.get("exit_code"),
                        status=str(raw_item.get("status", "Unknown")),
                        mode=self._normalize_mode(str(raw_item.get("mode", RUN_MODE_PARTIAL)))
                        or RUN_MODE_PARTIAL,
                        log_path=str(raw_item.get("log_path", "")),
                    )
                )
            except Exception:
                continue
        return items

    def _load_launchd_env(self) -> dict[str, str]:
        """Read .env.launchd using a small KEY=VALUE parser."""
        if not self.env_file.exists():
            return {}
        loaded_env: dict[str, str] = {}
        for raw_line in self.env_file.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", maxsplit=1)
            loaded_env[key.strip()] = value.strip()
        return loaded_env

    def _close_log_handle(self) -> None:
        if self.current_log_handle is not None:
            self.current_log_handle.close()
            self.current_log_handle = None

    def _close_dashboard_log_handle(self) -> None:
        if self.dashboard_log_handle is not None:
            self.dashboard_log_handle.close()
            self.dashboard_log_handle = None

    def _refresh_dashboard_process_state(self) -> None:
        if self.dashboard_process is None:
            return
        if self.dashboard_process.poll() is None:
            return
        self.dashboard_process = None
        self.dashboard_pid = None
        self.dashboard_started_at = None
        self._close_dashboard_log_handle()

    def _force_stop_dashboard_after_timeout(self, process: subprocess.Popen[str]) -> None:
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            try:
                process.kill()
            except OSError:
                return
        finally:
            with self.lock:
                if self.dashboard_process is process:
                    self.dashboard_process = None
                    self.dashboard_pid = None
                    self.dashboard_started_at = None
                    self._close_dashboard_log_handle()

    def _normalize_mode(self, mode: str | None) -> str | None:
        if mode is None:
            return RUN_MODE_PARTIAL
        normalized = mode.strip().casefold()
        if normalized == "partial":
            return RUN_MODE_PARTIAL
        if normalized == "full":
            return RUN_MODE_FULL
        return None

    def current_or_latest_log_path(self) -> Path | None:
        """Return the active log path, or the latest known run log."""
        with self.lock:
            return self._current_or_latest_log_path()

    def _current_or_latest_log_path(self) -> Path | None:
        if self.current_log_path is not None and self.current_log_path.exists():
            return self.current_log_path

        for item in self.history:
            log_path = Path(item.log_path)
            if log_path.exists():
                return log_path
        return None


manager = MonitorRunManager()
app = Flask(__name__)


PAGE_HTML = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Monitor Subastas Valencia Runner</title>
  <style>
    :root {
      --bg: #f5efe5;
      --card: #fffaf3;
      --line: #d9cfbf;
      --ink: #1c1a17;
      --muted: #6d6458;
      --accent: #0a6c8e;
      --accent-soft: #d9edf5;
      --good: #156f3b;
      --warn: #9d3d23;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: linear-gradient(180deg, #efe7d9 0%, var(--bg) 100%);
      color: var(--ink);
    }
    .shell {
      max-width: 1120px;
      margin: 0 auto;
      padding: 20px 16px 32px;
    }
    .topbar {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      flex-wrap: wrap;
      align-items: flex-end;
      margin-bottom: 16px;
    }
    h1 {
      margin: 0;
      font-size: 34px;
      line-height: 1.05;
    }
    .lead {
      margin: 6px 0 0;
      color: var(--muted);
      font-size: 16px;
    }
    .card {
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 16px;
      box-shadow: 0 8px 20px rgba(34, 29, 23, 0.05);
      margin-bottom: 14px;
    }
    .controls {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
    }
    .mode-picker {
      display: flex;
      align-items: center;
      gap: 10px;
      flex-wrap: wrap;
      margin-bottom: 10px;
    }
    .mode-group {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
    }
    .mode-option {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 8px 10px;
      border-radius: 999px;
      background: #ece4d7;
      color: var(--ink);
      font-size: 14px;
      font-weight: 700;
    }
    .mode-option input {
      margin: 0;
    }
    button {
      border: 0;
      border-radius: 12px;
      padding: 12px 16px;
      font-size: 15px;
      font-weight: 700;
      cursor: pointer;
      background: var(--accent);
      color: white;
    }
    button.secondary {
      background: #3d4e57;
    }
    button.ghost {
      background: #ece4d7;
      color: var(--ink);
    }
    a.button-link {
      display: inline-flex;
      align-items: center;
      border-radius: 12px;
      padding: 12px 16px;
      font-size: 15px;
      font-weight: 700;
      text-decoration: none;
      background: #ece4d7;
      color: var(--ink);
    }
    a.button-link.disabled {
      opacity: 0.5;
      pointer-events: none;
    }
    button:disabled {
      opacity: 0.5;
      cursor: not-allowed;
    }
    .grid {
      display: grid;
      grid-template-columns: 1.2fr 0.8fr;
      gap: 14px;
    }
    .meta-row {
      display: grid;
      grid-template-columns: 140px 1fr;
      gap: 8px;
      margin-bottom: 8px;
      align-items: start;
    }
    .label {
      color: var(--muted);
      font-size: 13px;
      text-transform: uppercase;
      letter-spacing: 0.06em;
    }
    .value {
      font-size: 16px;
    }
    .progress-shell {
      width: 100%;
      height: 14px;
      border-radius: 999px;
      background: #e7dece;
      overflow: hidden;
      margin: 8px 0 4px;
    }
    .progress-bar {
      height: 100%;
      width: 0%;
      background: linear-gradient(90deg, #0a6c8e, #6ba8be);
      transition: width 0.3s ease;
    }
    .log-box {
      min-height: 360px;
      max-height: 560px;
      overflow: auto;
      white-space: pre-wrap;
      word-break: break-word;
      font-family: "SF Mono", Menlo, monospace;
      font-size: 13px;
      line-height: 1.45;
      background: #161514;
      color: #ece7de;
      border-radius: 14px;
      padding: 14px;
    }
    .history-item {
      border-top: 1px solid var(--line);
      padding-top: 10px;
      margin-top: 10px;
      font-size: 14px;
    }
    .tag {
      display: inline-block;
      padding: 4px 8px;
      border-radius: 999px;
      background: var(--accent-soft);
      color: var(--accent);
      font-size: 12px;
      font-weight: 700;
    }
    .muted {
      color: var(--muted);
    }
    .footer-note {
      margin-top: 8px;
      font-size: 13px;
      color: var(--muted);
    }
    @media (max-width: 840px) {
      .grid {
        grid-template-columns: 1fr;
      }
    }
  </style>
</head>
<body>
  <div class="shell">
    <div class="topbar">
      <div>
        <h1>Monitor Runner</h1>
        <p class="lead">Manual local runner for the full monitor cycle.</p>
      </div>
      <div>
        <div class="mode-picker">
          <div class="label">Run mode</div>
          <div class="mode-group">
            <label class="mode-option"><input type="radio" name="run-mode" value="Partial" checked>Partial</label>
            <label class="mode-option"><input type="radio" name="run-mode" value="Full">Full</label>
          </div>
        </div>
        <div class="controls">
          <button id="run-btn" onclick="startRun()">Run monitor</button>
          <button id="stop-btn" class="secondary" onclick="stopRun()" disabled>Stop</button>
          <button id="start-dashboard-btn" class="ghost" onclick="startDashboard()">Start dashboard</button>
          <button id="stop-dashboard-btn" class="ghost" onclick="stopDashboard()" disabled>Stop dashboard</button>
          <button class="ghost" onclick="openLogsFolder()">Open logs folder</button>
          <a id="dashboard-link" class="button-link" href="http://127.0.0.1:8501" target="_blank" rel="noopener noreferrer">Open monitor</a>
          <a id="current-log-link" class="button-link disabled" href="#" target="_blank" rel="noopener noreferrer">Open current log</a>
        </div>
      </div>
    </div>

    <div class="grid">
      <div>
        <div class="card">
          <div class="meta-row"><div class="label">Status</div><div class="value" id="status-text">Idle</div></div>
          <div class="meta-row"><div class="label">Mode</div><div class="value" id="mode-text">Partial</div></div>
          <div class="meta-row"><div class="label">Detail</div><div class="value" id="detail-text">Ready to run</div></div>
          <div class="meta-row"><div class="label">Last run</div><div class="value" id="started-at">-</div></div>
          <div class="meta-row"><div class="label">Finished</div><div class="value" id="finished-at">-</div></div>
          <div class="meta-row"><div class="label">Exit code</div><div class="value" id="exit-code">-</div></div>
          <div class="meta-row"><div class="label">Current log</div><div class="value" id="log-path">-</div></div>
          <div class="meta-row"><div class="label">Dashboard status</div><div class="value" id="dashboard-status">not running</div></div>
          <div class="meta-row"><div class="label">Dashboard PID</div><div class="value" id="dashboard-pid">-</div></div>
          <div class="meta-row"><div class="label">Dashboard started</div><div class="value" id="dashboard-started-at">-</div></div>
          <div class="meta-row"><div class="label">Dashboard URL</div><div class="value" id="dashboard-url">http://127.0.0.1:8501</div></div>
          <div class="progress-shell"><div id="progress-bar" class="progress-bar"></div></div>
          <div class="footer-note" id="progress-note">General progress: 0%</div>
        </div>

        <div class="card">
          <div class="label" style="margin-bottom: 8px;">Live log</div>
          <div id="log-box" class="log-box">Waiting for the first run...</div>
        </div>
      </div>

      <div>
        <div class="card">
          <div class="label" style="margin-bottom: 8px;">Current state</div>
          <div class="tag" id="running-tag">Idle</div>
          <p class="footer-note">The page refreshes its state automatically every second.</p>
        </div>

        <div class="card">
          <div class="label" style="margin-bottom: 8px;">Recent runs</div>
          <div id="history-list" class="muted">No runs yet.</div>
        </div>
      </div>
    </div>
  </div>

  <script>
    let lastLogLength = 0;

    function getSelectedMode() {
      const selected = document.querySelector('input[name="run-mode"]:checked');
      return selected ? selected.value : "Partial";
    }

    async function postJson(url, body = null) {
      const response = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: body ? JSON.stringify(body) : null,
      });
      return response.json();
    }

    async function startRun() {
      const payload = await postJson("/api/run", { mode: getSelectedMode() });
      if (!payload.ok) {
        alert(payload.message);
      }
      await refreshState();
    }

    async function stopRun() {
      const payload = await postJson("/api/stop");
      if (!payload.ok) {
        alert(payload.message);
      }
      await refreshState();
    }

    async function openLogsFolder() {
      const payload = await postJson("/api/open-logs-folder");
      if (!payload.ok) {
        alert(payload.message);
      }
    }

    async function startDashboard() {
      const payload = await postJson("/api/start-dashboard");
      if (!payload.ok) {
        alert(payload.message);
      }
      await refreshState();
    }

    async function stopDashboard() {
      const payload = await postJson("/api/stop-dashboard");
      if (!payload.ok) {
        alert(payload.message);
      }
      await refreshState();
    }

    function renderHistory(history) {
      const container = document.getElementById("history-list");
      if (!history || history.length === 0) {
        container.innerHTML = '<div class="muted">No runs yet.</div>';
        return;
      }
      container.innerHTML = history.map((item) => `
        <div class="history-item">
          <div><strong>${item.status}</strong></div>
          <div>Mode: ${item.mode || "-"}</div>
          <div>Started: ${item.started_at || "-"}</div>
          <div>Finished: ${item.finished_at || "-"}</div>
          <div>Exit code: ${item.exit_code === null ? "-" : item.exit_code}</div>
          <div class="muted">${item.log_path || "-"}</div>
        </div>
      `).join("");
    }

    async function refreshState() {
      const response = await fetch("/api/state");
      const state = await response.json();

      document.getElementById("status-text").textContent = state.status || "Idle";
      document.getElementById("mode-text").textContent = state.mode || "Partial";
      document.getElementById("detail-text").textContent = state.detail || "-";
      document.getElementById("started-at").textContent = state.started_at || "-";
      document.getElementById("finished-at").textContent = state.finished_at || "-";
      document.getElementById("exit-code").textContent = state.exit_code === null ? "-" : state.exit_code;
      document.getElementById("log-path").textContent = state.current_or_latest_log_path || state.log_path || "-";
      document.getElementById("dashboard-status").textContent = state.dashboard_status || "not running";
      document.getElementById("dashboard-pid").textContent = state.dashboard_pid === null ? "-" : state.dashboard_pid;
      document.getElementById("dashboard-started-at").textContent = state.dashboard_started_at || "-";
      document.getElementById("dashboard-url").textContent = state.dashboard_url || "-";
      document.getElementById("running-tag").textContent = state.running ? "Running" : state.status;
      document.getElementById("progress-bar").style.width = `${state.general_progress || 0}%`;
      document.getElementById("progress-note").textContent = `General progress: ${state.general_progress || 0}%`;

      const runButton = document.getElementById("run-btn");
      const stopButton = document.getElementById("stop-btn");
      const startDashboardButton = document.getElementById("start-dashboard-btn");
      const stopDashboardButton = document.getElementById("stop-dashboard-btn");
      const dashboardLink = document.getElementById("dashboard-link");
      const currentLogLink = document.getElementById("current-log-link");
      const modeInputs = document.querySelectorAll('input[name="run-mode"]');
      runButton.disabled = Boolean(state.running);
      stopButton.disabled = !state.running;
      startDashboardButton.disabled = Boolean(state.dashboard_running);
      stopDashboardButton.disabled = !state.dashboard_running;
      modeInputs.forEach((input) => {
        input.disabled = Boolean(state.running);
        input.checked = input.value === (state.mode || "Partial");
      });
      dashboardLink.href = state.dashboard_url || "http://127.0.0.1:8501";

      if (state.current_log_url) {
        currentLogLink.href = state.current_log_url;
        currentLogLink.classList.remove("disabled");
      } else {
        currentLogLink.href = "#";
        currentLogLink.classList.add("disabled");
      }

      const logBox = document.getElementById("log-box");
      const nextLogText = state.log_text || "";
      const shouldStickToBottom = logBox.scrollTop + logBox.clientHeight >= logBox.scrollHeight - 16;
      if (nextLogText.length !== lastLogLength) {
        logBox.textContent = nextLogText || "Waiting for the first run...";
        lastLogLength = nextLogText.length;
        if (shouldStickToBottom) {
          logBox.scrollTop = logBox.scrollHeight;
        }
      }

      renderHistory(state.history || []);
    }

    refreshState();
    setInterval(refreshState, 1000);
  </script>
</body>
</html>
"""


@app.get("/")
def index():
    return render_template_string(PAGE_HTML)


@app.get("/api/state")
def api_state():
    return jsonify(manager.snapshot())


@app.post("/api/run")
def api_run():
    payload = request.get_json(silent=True) or {}
    mode = str(payload.get("mode", RUN_MODE_PARTIAL))
    ok, message = manager.start(mode)
    return jsonify({"ok": ok, "message": message})


@app.post("/api/stop")
def api_stop():
    ok, message = manager.stop()
    return jsonify({"ok": ok, "message": message})


@app.post("/api/open-logs-folder")
def api_open_logs_folder():
    ok, message = manager.open_logs_folder()
    return jsonify({"ok": ok, "message": message})


@app.post("/api/start-dashboard")
def api_start_dashboard():
    ok, message = manager.start_dashboard()
    return jsonify({"ok": ok, "message": message})


@app.post("/api/stop-dashboard")
def api_stop_dashboard():
    ok, message = manager.stop_dashboard()
    return jsonify({"ok": ok, "message": message})


@app.get("/logs/current")
def current_log():
    log_path = manager.current_or_latest_log_path()
    if log_path is None:
        abort(404)

    resolved_log_path = log_path.resolve()
    resolved_logs_dir = RUN_LOG_DIR.resolve()
    if resolved_logs_dir not in resolved_log_path.parents:
        abort(403)

    return Response(
        resolved_log_path.read_text(encoding="utf-8"),
        mimetype="text/plain; charset=utf-8",
        headers={
            "Content-Disposition": f'inline; filename="{resolved_log_path.name}"',
        },
    )


def main() -> None:
    app.run(host="127.0.0.1", port=8765, debug=False, threaded=True)


if __name__ == "__main__":
    main()
