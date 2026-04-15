"""Helpers to infer runner progress from monitor console output."""

from __future__ import annotations

import re
from dataclasses import dataclass


STAGE_IDLE = "idle"
STAGE_ACTIVE = "active"
STAGE_UPCOMING = "upcoming"
STAGE_COMPLETED = "completed"
STAGE_TELEGRAM = "telegram"
STAGE_FINISHED = "finished"
STAGE_ERROR = "error"

STAGE_PROGRESS = {
    STAGE_IDLE: 0,
    STAGE_ACTIVE: 20,
    STAGE_UPCOMING: 40,
    STAGE_COMPLETED: 85,
    STAGE_TELEGRAM: 95,
    STAGE_FINISHED: 100,
    STAGE_ERROR: 100,
}

ACTIVE_DETAIL_RE = re.compile(r"Processing detail (\d+)/(\d+):")
UPCOMING_DETAIL_RE = re.compile(r"Processing upcoming detail (\d+)/(\d+):")
COMPLETED_DETAIL_RE = re.compile(r"Processing completed detail (\d+)/(\d+):")


@dataclass(slots=True, frozen=True)
class RunnerProgress:
    """Structured progress snapshot inferred from one output line."""

    stage: str
    status_text: str
    general_progress: int
    detail_text: str
    is_finished: bool = False
    is_error: bool = False


def parse_runner_progress_line(line: str, *, current_stage: str) -> RunnerProgress | None:
    """Infer UI-friendly progress updates from one monitor output line."""
    stripped = line.strip()
    if not stripped:
        return None

    if stripped.startswith("[START]"):
        return RunnerProgress(
            stage=STAGE_IDLE,
            status_text="Starting monitor",
            general_progress=STAGE_PROGRESS[STAGE_IDLE],
            detail_text="Initializing process",
        )

    if "[STEP] Refreshing active auctions..." in stripped:
        return _stage_progress(STAGE_ACTIVE, "Running active auctions")

    if "[STEP] Refreshing upcoming auctions..." in stripped:
        return _stage_progress(STAGE_UPCOMING, "Running upcoming auctions")

    if "[STEP] Refreshing completed auctions..." in stripped:
        return _stage_progress(STAGE_COMPLETED, "Running completed auctions")

    if "[STEP] Sending Telegram top opportunities..." in stripped:
        return _stage_progress(STAGE_TELEGRAM, "Sending Telegram")

    if stripped.startswith("[END] Full monitor cycle finished at"):
        return RunnerProgress(
            stage=STAGE_FINISHED,
            status_text="Finished",
            general_progress=STAGE_PROGRESS[STAGE_FINISHED],
            detail_text="Monitor cycle completed",
            is_finished=True,
        )

    if stripped.startswith("[END] Full monitor cycle finished with Telegram error."):
        return RunnerProgress(
            stage=STAGE_ERROR,
            status_text="Error",
            general_progress=STAGE_PROGRESS[STAGE_ERROR],
            detail_text="Finished with Telegram error",
            is_error=True,
        )

    if stripped.startswith("[END] Full monitor cycle stopped before Telegram."):
        return RunnerProgress(
            stage=STAGE_ERROR,
            status_text="Error",
            general_progress=STAGE_PROGRESS[STAGE_ERROR],
            detail_text="Stopped before Telegram",
            is_error=True,
        )

    if stripped.startswith("[ERROR]"):
        return RunnerProgress(
            stage=STAGE_ERROR,
            status_text="Error",
            general_progress=STAGE_PROGRESS[STAGE_ERROR],
            detail_text=stripped,
            is_error=True,
        )

    upcoming_match = UPCOMING_DETAIL_RE.search(stripped)
    if upcoming_match is not None:
        return RunnerProgress(
            stage=STAGE_UPCOMING,
            status_text="Running upcoming auctions",
            general_progress=STAGE_PROGRESS[STAGE_UPCOMING],
            detail_text=f"Upcoming auctions: {upcoming_match.group(1)}/{upcoming_match.group(2)}",
        )

    completed_match = COMPLETED_DETAIL_RE.search(stripped)
    if completed_match is not None:
        return RunnerProgress(
            stage=STAGE_COMPLETED,
            status_text="Running completed auctions",
            general_progress=STAGE_PROGRESS[STAGE_COMPLETED],
            detail_text=f"Completed auctions: {completed_match.group(1)}/{completed_match.group(2)}",
        )

    active_match = ACTIVE_DETAIL_RE.search(stripped)
    if active_match is not None:
        active_stage = current_stage if current_stage in {
            STAGE_ACTIVE,
            STAGE_UPCOMING,
            STAGE_COMPLETED,
        } else STAGE_ACTIVE
        return RunnerProgress(
            stage=active_stage,
            status_text=stage_status_text(active_stage),
            general_progress=STAGE_PROGRESS[active_stage],
            detail_text=f"{stage_detail_prefix(active_stage)}: {active_match.group(1)}/{active_match.group(2)}",
        )

    return None


def stage_status_text(stage: str) -> str:
    """Return a user-facing status line for one known stage."""
    mapping = {
        STAGE_IDLE: "Idle",
        STAGE_ACTIVE: "Running active auctions",
        STAGE_UPCOMING: "Running upcoming auctions",
        STAGE_COMPLETED: "Running completed auctions",
        STAGE_TELEGRAM: "Sending Telegram",
        STAGE_FINISHED: "Finished",
        STAGE_ERROR: "Error",
    }
    return mapping.get(stage, "Running monitor")


def stage_detail_prefix(stage: str) -> str:
    """Return a user-facing detail prefix for one stage."""
    mapping = {
        STAGE_ACTIVE: "Active auctions",
        STAGE_UPCOMING: "Upcoming auctions",
        STAGE_COMPLETED: "Completed auctions",
    }
    return mapping.get(stage, "Current step")


def _stage_progress(stage: str, status_text: str) -> RunnerProgress:
    return RunnerProgress(
        stage=stage,
        status_text=status_text,
        general_progress=STAGE_PROGRESS[stage],
        detail_text="Waiting for detailed progress",
    )
