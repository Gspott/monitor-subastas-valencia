# Monitor Subastas Valencia

`monitor-subastas-valencia` is a local BOE auction monitor focused on Valencia/València. It fetches public auction data, normalizes it, stores it in SQLite, exposes a Streamlit dashboard, and includes lightweight local automation for manual and weekly runs.

The repository is currently optimized for local use on macOS.

## Requirements

- macOS
- Python 3.10+

## Installation

Create a local virtual environment and install the project:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[web]"
```

If you also want the test tools:

```bash
pip install -e ".[web,dev]"
```

## Configuration

Create your local environment file from the example:

```bash
cp .env.example .env.launchd
```

Available variables:

```dotenv
TELEGRAM_BOT_TOKEN=your_token_here
TELEGRAM_CHAT_ID=your_chat_id_here
TELEGRAM_ONLY_TOP_OPPORTUNITIES=true
TELEGRAM_SEND_IF_NO_CHANGES=false
MONITOR_DASHBOARD_URL=http://127.0.0.1:8501
```

Telegram is optional for local exploration, but required if you want alert delivery.

Do not commit `.env`, `.env.launchd`, logs, or runtime outputs.

## Usage

### Recommended macOS flow

Double-click:

```text
scripts/open_monitor_app.command
```

This launcher:

- starts the local Flask web app if needed;
- starts the Streamlit dashboard if needed;
- opens the browser at `http://127.0.0.1:8765`.

### Manual alternative

Start the local web app directly:

```bash
.venv/bin/python apps/monitor_runner_web.py
```

Then open:

```text
http://127.0.0.1:8765
```

## Dashboard

The dashboard is a Streamlit app served locally on:

```text
http://127.0.0.1:8501
```

It can be launched manually with:

```bash
.venv/bin/streamlit run scripts/monitor_dashboard.py
```

## Main monitor cycle

The full local monitor cycle runs with:

```bash
.venv/bin/python -m scripts.run_full_monitor_cycle
```

The web runner also supports:

- `Partial`: normal incremental execution
- `Full`: forces a full refresh of completed auctions

## Automation (optional)

macOS `launchd` support is included for weekly runs.

Useful files:

- `scripts/run_weekly.sh`
- `scripts/install_launchd_weekly.sh`
- `scripts/launchd/com.carlosblanco.monitor-subastas-valencia.weekly.plist`

The launchd plist inside the repository is a template. `scripts/install_launchd_weekly.sh` renders it with the real local project path before installing it.

## Notes

- This project is currently designed primarily for macOS.
- Python 3.10+ is recommended.
- The BOE integration is conservative and evolves incrementally.
- Do not upload `.env`, `.env.launchd`, `logs/`, `output/`, SQLite files, or generated app bundles.

## Development

Run tests:

```bash
.venv/bin/python -m pytest -q
```

Compile key modules:

```bash
.venv/bin/python -m compileall src scripts apps tests
```
