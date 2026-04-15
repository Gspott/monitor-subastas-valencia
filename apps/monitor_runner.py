"""Simple macOS desktop runner for the full monitor cycle."""

from __future__ import annotations

import os
import queue
import subprocess
import sys
import threading
import tkinter as tk
from datetime import datetime
from pathlib import Path
from tkinter import filedialog, messagebox, ttk


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from monitor.runner_progress import (
    STAGE_ERROR,
    STAGE_FINISHED,
    STAGE_IDLE,
    parse_runner_progress_line,
    stage_status_text,
)


class MonitorRunnerApp:
    """Minimal desktop UI to launch and observe the monitor cycle."""

    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("Monitor Subastas Valencia")
        self.root.geometry("980x700")
        self.root.minsize(860, 580)

        self.project_root = REPO_ROOT
        self.python_bin = self.project_root / ".venv" / "bin" / "python"
        self.env_file = self.project_root / ".env.launchd"
        self.logs_dir = self.project_root / "logs"
        self.process: subprocess.Popen[str] | None = None
        self.process_queue: queue.Queue[tuple[str, str]] = queue.Queue()
        self.current_stage = STAGE_IDLE
        self.current_log_lines: list[str] = []
        self.cancel_requested = False

        self.status_var = tk.StringVar(value="Idle")
        self.detail_var = tk.StringVar(value="Ready to run")
        self.last_run_var = tk.StringVar(value="Last run: -")
        self.exit_code_var = tk.StringVar(value="Exit code: -")
        self.progress_var = tk.DoubleVar(value=0.0)

        self._build_ui()
        self.root.after(100, self._poll_process_queue)
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

    def _build_ui(self) -> None:
        container = ttk.Frame(self.root, padding=14)
        container.pack(fill="both", expand=True)

        title = ttk.Label(
            container,
            text="Monitor Subastas Valencia",
            font=("SF Pro Text", 20, "bold"),
        )
        title.pack(anchor="w")

        subtitle = ttk.Label(
            container,
            text="Manual runner for the full monitor cycle with live output.",
        )
        subtitle.pack(anchor="w", pady=(2, 12))

        controls = ttk.Frame(container)
        controls.pack(fill="x", pady=(0, 10))

        self.run_button = ttk.Button(
            controls,
            text="Run monitor",
            command=self.start_monitor,
        )
        self.run_button.pack(side="left")

        self.stop_button = ttk.Button(
            controls,
            text="Stop",
            command=self.stop_monitor,
            state="disabled",
        )
        self.stop_button.pack(side="left", padx=(8, 0))

        self.save_button = ttk.Button(
            controls,
            text="Save log",
            command=self.save_log,
        )
        self.save_button.pack(side="left", padx=(8, 0))

        self.open_logs_button = ttk.Button(
            controls,
            text="Open logs folder",
            command=self.open_logs_folder,
        )
        self.open_logs_button.pack(side="left", padx=(8, 0))

        status_frame = ttk.Frame(container)
        status_frame.pack(fill="x", pady=(0, 10))

        ttk.Label(status_frame, text="Status:").grid(row=0, column=0, sticky="w")
        ttk.Label(status_frame, textvariable=self.status_var).grid(row=0, column=1, sticky="w", padx=(6, 0))

        ttk.Label(status_frame, text="Detail:").grid(row=1, column=0, sticky="w", pady=(6, 0))
        ttk.Label(status_frame, textvariable=self.detail_var).grid(row=1, column=1, sticky="w", padx=(6, 0), pady=(6, 0))

        ttk.Label(status_frame, textvariable=self.last_run_var).grid(row=0, column=2, sticky="e", padx=(20, 0))
        ttk.Label(status_frame, textvariable=self.exit_code_var).grid(row=1, column=2, sticky="e", padx=(20, 0), pady=(6, 0))
        status_frame.columnconfigure(1, weight=1)
        status_frame.columnconfigure(2, weight=1)

        self.progress_bar = ttk.Progressbar(
            container,
            mode="determinate",
            maximum=100,
            variable=self.progress_var,
        )
        self.progress_bar.pack(fill="x", pady=(0, 10))

        log_frame = ttk.Frame(container)
        log_frame.pack(fill="both", expand=True)

        self.log_text = tk.Text(
            log_frame,
            wrap="word",
            state="disabled",
            font=("SF Mono", 12),
            background="#faf7f0",
        )
        self.log_text.pack(side="left", fill="both", expand=True)

        scrollbar = ttk.Scrollbar(log_frame, orient="vertical", command=self.log_text.yview)
        scrollbar.pack(side="right", fill="y")
        self.log_text.configure(yscrollcommand=scrollbar.set)
        self.log_text.tag_configure("stderr", foreground="#9d3d23")
        self.log_text.tag_configure("stdout", foreground="#1b1a17")
        self.log_text.tag_configure("system", foreground="#0d5e8c")

    def start_monitor(self) -> None:
        """Launch the monitor subprocess and begin reading live output."""
        if self.process is not None:
            return

        if not self.python_bin.exists():
            messagebox.showerror(
                "Missing Python",
                f"Virtualenv Python not found:\n{self.python_bin}",
            )
            return

        self.cancel_requested = False
        self.current_stage = STAGE_IDLE
        self.current_log_lines.clear()
        self._set_status("Starting monitor", "Launching subprocess", 0)
        self.last_run_var.set(f"Last run: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.exit_code_var.set("Exit code: running")
        self._clear_log()
        self._append_log("[SYSTEM] Launching monitor process\n", tag="system")

        env = os.environ.copy()
        env.update(self._load_launchd_env())
        env["PATH"] = (
            f"{self.project_root / '.venv' / 'bin'}:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
        )
        env["PYTHONUNBUFFERED"] = "1"

        command = [str(self.python_bin), "-m", "scripts.run_full_monitor_cycle"]
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
            self.process = None
            self._set_status("Error", f"Failed to launch process: {exc}", 100)
            self.exit_code_var.set("Exit code: launch failed")
            self._append_log(f"[SYSTEM] Failed to launch process: {exc}\n", tag="system")
            return

        self.run_button.configure(state="disabled")
        self.stop_button.configure(state="normal")

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

    def stop_monitor(self) -> None:
        """Request a graceful stop of the current subprocess."""
        if self.process is None:
            return

        self.cancel_requested = True
        self._append_log("[SYSTEM] Stop requested by user\n", tag="system")
        self.detail_var.set("Stopping process...")
        self.stop_button.configure(state="disabled")

        try:
            self.process.terminate()
        except OSError:
            return

        threading.Thread(target=self._force_kill_after_timeout, daemon=True).start()

    def save_log(self) -> None:
        """Allow the user to save the current visible log to a file."""
        if not self.current_log_lines:
            messagebox.showinfo("No log", "There is no log content to save yet.")
            return

        file_path = filedialog.asksaveasfilename(
            title="Save monitor log",
            defaultextension=".log",
            filetypes=[("Log files", "*.log"), ("Text files", "*.txt"), ("All files", "*.*")],
            initialfile=f"monitor_runner_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
        )
        if not file_path:
            return

        Path(file_path).write_text("".join(self.current_log_lines), encoding="utf-8")
        self._append_log(f"[SYSTEM] Log saved to {file_path}\n", tag="system")

    def open_logs_folder(self) -> None:
        """Open the project logs folder in Finder."""
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        subprocess.Popen(["open", str(self.logs_dir)])

    def _stream_reader(self, stream, stream_name: str) -> None:
        for line in iter(stream.readline, ""):
            self.process_queue.put(("line", f"{stream_name}\t{line}"))
        stream.close()

    def _wait_for_process(self) -> None:
        if self.process is None:
            return
        return_code = self.process.wait()
        self.process_queue.put(("process_exit", str(return_code)))

    def _force_kill_after_timeout(self) -> None:
        """Escalar a kill si terminate no cierra el proceso a tiempo."""
        process = self.process
        if process is None:
            return
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process_queue.put(("line", "system\t[SYSTEM] Process did not stop in time. Killing it.\n"))
            try:
                process.kill()
            except OSError:
                return

    def _poll_process_queue(self) -> None:
        while True:
            try:
                event_type, payload = self.process_queue.get_nowait()
            except queue.Empty:
                break

            if event_type == "line":
                stream_name, line = payload.split("\t", maxsplit=1)
                self._handle_process_line(line=line, stream_name=stream_name)
            elif event_type == "process_exit":
                self._handle_process_exit(int(payload))

        self.root.after(100, self._poll_process_queue)

    def _handle_process_line(self, *, line: str, stream_name: str) -> None:
        tag = "stderr" if stream_name == "stderr" else "stdout"
        self._append_log(line, tag=tag)

        progress = parse_runner_progress_line(line, current_stage=self.current_stage)
        if progress is None:
            return

        self.current_stage = progress.stage
        self._set_status(
            progress.status_text,
            progress.detail_text,
            progress.general_progress,
        )

    def _handle_process_exit(self, return_code: int) -> None:
        self.exit_code_var.set(f"Exit code: {return_code}")
        self.run_button.configure(state="normal")
        self.stop_button.configure(state="disabled")
        self.process = None

        if self.cancel_requested and return_code != 0:
            self._set_status("Cancelled", "Process stopped by user", self.progress_var.get())
            self._append_log("[SYSTEM] Monitor process cancelled by user\n", tag="system")
            return

        if return_code == 0 and self.current_stage != STAGE_ERROR:
            self.current_stage = STAGE_FINISHED
            self._set_status("Finished", "Monitor cycle completed", 100)
            self._append_log("[SYSTEM] Monitor process finished successfully\n", tag="system")
            return

        self.current_stage = STAGE_ERROR
        self._set_status("Error", "Monitor process failed", 100)
        self._append_log("[SYSTEM] Monitor process finished with an error\n", tag="system")

    def _set_status(self, status: str, detail: str, progress: float) -> None:
        self.status_var.set(status)
        self.detail_var.set(detail)
        self.progress_var.set(progress)

    def _append_log(self, text: str, *, tag: str) -> None:
        self.current_log_lines.append(text)
        self.log_text.configure(state="normal")
        self.log_text.insert("end", text, tag)
        self.log_text.see("end")
        self.log_text.configure(state="disabled")

    def _clear_log(self) -> None:
        self.log_text.configure(state="normal")
        self.log_text.delete("1.0", "end")
        self.log_text.configure(state="disabled")

    def _load_launchd_env(self) -> dict[str, str]:
        """Leer .env.launchd con reglas simples de KEY=VALUE."""
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

    def _on_close(self) -> None:
        if self.process is not None:
            if not messagebox.askyesno(
                "Monitor running",
                "The monitor is still running. Stop it and close the window?",
            ):
                return
            self.stop_monitor()
        self.root.after(200, self.root.destroy)


def main() -> None:
    root = tk.Tk()
    style = ttk.Style(root)
    try:
        style.theme_use("clam")
    except tk.TclError:
        pass
    app = MonitorRunnerApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
