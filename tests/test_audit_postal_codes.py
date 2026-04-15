"""Tests for postal-code audit helpers."""

from scripts.audit_postal_codes import (
    build_lot_postal_index,
    classify_postal_code_state,
    normalize_postal_code,
    refine_postal_code_state,
)
from monitor.domain.models import AuctionRecord


def make_record(**overrides) -> AuctionRecord:
    """Construye un record minimo para tests de auditoria."""
    payload = {
        "auction_id": "SUB-TEST",
        "title": "Test auction",
        "asset_type": "real_estate",
        "asset_subtype": "residential",
        "municipality": None,
        "province": None,
        "postal_code": None,
    }
    payload.update(overrides)
    return AuctionRecord(**payload)


def test_classify_postal_code_state_returns_ok_for_valencia_range() -> None:
    """Debe aceptar CP de cinco cifras dentro del rango esperado de Valencia."""
    state = classify_postal_code_state(
        postal_code="46001",
        municipality="Valencia",
        province="Valencia",
    )

    assert state == "OK"


def test_classify_postal_code_state_detects_missing_and_suspicious_values() -> None:
    """Debe distinguir ausente frente a formato sospechoso."""
    assert classify_postal_code_state(
        postal_code=None,
        municipality="Valencia",
        province="Valencia",
    ) == "MISSING"
    assert classify_postal_code_state(
        postal_code="46-001",
        municipality="Valencia",
        province="Valencia",
    ) == "SUSPICIOUS_FORMAT"


def test_classify_postal_code_state_detects_outside_expected_valencia_range() -> None:
    """Debe marcar CP válidos pero fuera del 46XXX en el foco Valencia."""
    state = classify_postal_code_state(
        postal_code="28001",
        municipality="Valencia",
        province="Valencia",
    )

    assert state == "OUTSIDE_EXPECTED_VALENCIA_RANGE"


def test_normalize_postal_code_trims_empty_values() -> None:
    """Debe normalizar espacios sin inventar valores."""
    assert normalize_postal_code(" 46001 ") == "46001"
    assert normalize_postal_code("   ") is None


def test_refine_postal_code_state_detects_missing_parent_covered_by_lot() -> None:
    """Debe distinguir padres sin CP cuando algun lote ya aporta un CP valido."""
    parent = make_record(auction_id="SUB-123")
    lot = make_record(
        auction_id="SUB-123::lot:1",
        municipality="Valencia",
        province="Valencia",
        postal_code="46001",
    )

    lots_have_postal_by_parent_id = build_lot_postal_index([parent, lot])

    state = refine_postal_code_state(
        base_state="MISSING",
        auction_id=parent.auction_id,
        lots_have_postal_by_parent_id=lots_have_postal_by_parent_id,
    )

    assert state == "MISSING_PARENT_BUT_LOTS_HAVE_POSTAL"


def test_refine_postal_code_state_keeps_true_missing_without_useful_lots() -> None:
    """Debe mantener missing verdadero si no hay lotes con CP valido."""
    parent = make_record(auction_id="SUB-456")
    lot = make_record(
        auction_id="SUB-456::lot:1",
        municipality="Valencia",
        province="Valencia",
        postal_code=None,
    )

    lots_have_postal_by_parent_id = build_lot_postal_index([parent, lot])

    state = refine_postal_code_state(
        base_state="MISSING",
        auction_id=parent.auction_id,
        lots_have_postal_by_parent_id=lots_have_postal_by_parent_id,
    )

    assert state == "MISSING_TRUE"


def test_refine_postal_code_state_does_not_change_ok_rows() -> None:
    """No debe alterar filas ya clasificadas como correctas."""
    state = refine_postal_code_state(
        base_state="OK",
        auction_id="SUB-789",
        lots_have_postal_by_parent_id={"SUB-789": True},
    )

    assert state == "OK"
