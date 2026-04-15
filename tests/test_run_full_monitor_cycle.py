"""Tests for the full local monitor cycle orchestration."""

from __future__ import annotations

import pytest

from scripts import run_full_monitor_cycle


def test_main_runs_all_steps_in_order(monkeypatch) -> None:
    """Debe ejecutar active, upcoming, completed y Telegram en ese orden."""
    calls: list[str] = []
    completed_calls: list[bool] = []

    monkeypatch.setattr(run_full_monitor_cycle.load_sample_boe_data, "main", lambda: calls.append("active"))
    monkeypatch.setattr(run_full_monitor_cycle.load_upcoming_boe_data, "main", lambda: calls.append("upcoming"))
    monkeypatch.setattr(
        run_full_monitor_cycle.load_completed_boe_data,
        "main",
        lambda *, full_refresh=False: (
            completed_calls.append(full_refresh),
            calls.append("completed"),
        )[-1],
    )
    monkeypatch.setattr(run_full_monitor_cycle.send_opportunities_telegram, "main", lambda: calls.append("telegram"))

    run_full_monitor_cycle.main()

    assert calls == ["active", "upcoming", "completed", "telegram"]
    assert completed_calls == [False]


def test_main_stops_before_telegram_if_one_loader_fails(monkeypatch) -> None:
    """Debe detener el ciclo si falla una carga anterior a Telegram."""
    calls: list[str] = []

    monkeypatch.setattr(run_full_monitor_cycle.load_sample_boe_data, "main", lambda: calls.append("active"))
    monkeypatch.setattr(
        run_full_monitor_cycle.load_upcoming_boe_data,
        "main",
        lambda: (_ for _ in ()).throw(RuntimeError("upcoming failed")),
    )
    monkeypatch.setattr(
        run_full_monitor_cycle.load_completed_boe_data,
        "main",
        lambda *, full_refresh=False: calls.append("completed"),
    )
    monkeypatch.setattr(run_full_monitor_cycle.send_opportunities_telegram, "main", lambda: calls.append("telegram"))

    with pytest.raises(RuntimeError, match="upcoming failed"):
        run_full_monitor_cycle.main()

    assert calls == ["active"]


def test_main_can_force_completed_full_refresh(monkeypatch) -> None:
    """Debe propagar el full refresh solo al paso de completed."""
    calls: list[str] = []
    completed_calls: list[bool] = []

    monkeypatch.setattr(run_full_monitor_cycle.load_sample_boe_data, "main", lambda: calls.append("active"))
    monkeypatch.setattr(run_full_monitor_cycle.load_upcoming_boe_data, "main", lambda: calls.append("upcoming"))
    monkeypatch.setattr(
        run_full_monitor_cycle.load_completed_boe_data,
        "main",
        lambda *, full_refresh=False: (
            completed_calls.append(full_refresh),
            calls.append("completed"),
        )[-1],
    )
    monkeypatch.setattr(run_full_monitor_cycle.send_opportunities_telegram, "main", lambda: calls.append("telegram"))

    run_full_monitor_cycle.main(completed_full_refresh=True)

    assert calls == ["active", "upcoming", "completed", "telegram"]
    assert completed_calls == [True]
