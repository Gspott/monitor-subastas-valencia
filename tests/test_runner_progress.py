"""Tests for manual monitor runner progress parsing."""

from __future__ import annotations

from monitor.runner_progress import (
    STAGE_ACTIVE,
    STAGE_COMPLETED,
    STAGE_ERROR,
    STAGE_FINISHED,
    STAGE_TELEGRAM,
    STAGE_UPCOMING,
    parse_runner_progress_line,
)


def test_parse_runner_progress_line_detects_top_level_steps() -> None:
    """Debe traducir las fases principales a estado y progreso general."""
    active = parse_runner_progress_line(
        "[STEP] Refreshing active auctions...",
        current_stage="idle",
    )
    upcoming = parse_runner_progress_line(
        "[STEP] Refreshing upcoming auctions...",
        current_stage=STAGE_ACTIVE,
    )
    completed = parse_runner_progress_line(
        "[STEP] Refreshing completed auctions...",
        current_stage=STAGE_UPCOMING,
    )
    telegram = parse_runner_progress_line(
        "[STEP] Sending Telegram top opportunities...",
        current_stage=STAGE_COMPLETED,
    )

    assert active is not None and active.stage == STAGE_ACTIVE and active.general_progress == 20
    assert upcoming is not None and upcoming.stage == STAGE_UPCOMING and upcoming.general_progress == 40
    assert completed is not None and completed.stage == STAGE_COMPLETED and completed.general_progress == 85
    assert telegram is not None and telegram.stage == STAGE_TELEGRAM and telegram.general_progress == 95


def test_parse_runner_progress_line_extracts_fine_grained_completed_progress() -> None:
    """Debe convertir el progreso interno de completed a texto visible."""
    progress = parse_runner_progress_line(
        "Processing completed detail 143/392: SUB-TEST",
        current_stage=STAGE_COMPLETED,
    )

    assert progress is not None
    assert progress.stage == STAGE_COMPLETED
    assert progress.detail_text == "Completed auctions: 143/392"


def test_parse_runner_progress_line_uses_current_stage_for_generic_detail_lines() -> None:
    """Debe atribuir los detalles genericos a la fase actual."""
    progress = parse_runner_progress_line(
        "Processing detail 23/392: SUB-TEST",
        current_stage=STAGE_ACTIVE,
    )

    assert progress is not None
    assert progress.stage == STAGE_ACTIVE
    assert progress.detail_text == "Active auctions: 23/392"


def test_parse_runner_progress_line_marks_finish_and_error_lines() -> None:
    """Debe distinguir final correcto y error."""
    finished = parse_runner_progress_line(
        "[END] Full monitor cycle finished at 2026-04-15T12:00:00",
        current_stage=STAGE_TELEGRAM,
    )
    errored = parse_runner_progress_line(
        "[ERROR] Telegram alerting failed: boom",
        current_stage=STAGE_TELEGRAM,
    )

    assert finished is not None and finished.stage == STAGE_FINISHED and finished.is_finished is True
    assert errored is not None and errored.stage == STAGE_ERROR and errored.is_error is True
