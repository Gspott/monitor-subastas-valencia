"""Tests for auction status semantics."""

from monitor.status import is_active_status, normalize_status


def test_is_active_status_handles_known_variants_conservatively() -> None:
    """Debe decidir actividad con variantes comunes en castellano."""
    assert is_active_status("abierta") is True
    assert is_active_status("  abierta con pujas  ") is True
    assert is_active_status("Celebrándose con pujas") is True
    assert is_active_status("En tramitación") is True
    assert is_active_status("suspendida") is False
    assert is_active_status("cancelada") is False
    assert is_active_status("adjudicada") is False
    assert is_active_status("finalizada") is False
    assert is_active_status("cerrada") is False


def test_is_active_status_treats_unknown_or_missing_status_as_inactive() -> None:
    """Debe excluir estados ambiguos o ausentes por prudencia."""
    assert is_active_status(None) is False
    assert is_active_status("") is False
    assert is_active_status("pendiente de revisión") is False


def test_normalize_status_maps_variants_to_canonical_taxonomy() -> None:
    """Debe mapear variantes observadas a estados canónicos."""
    assert normalize_status("abierta") == "abierta"
    assert normalize_status("  ABIERTA con pujas ") == "abierta con pujas"
    assert normalize_status("Celebrándose") == "abierta"
    assert normalize_status("Celebrándose con pujas") == "abierta con pujas"
    assert normalize_status("En tramitación") == "en tramitacion"
    assert normalize_status("suspendida") == "suspendida"
    assert normalize_status("cancelada") == "cancelada"
    assert normalize_status("adjudicada") == "adjudicada"
    assert normalize_status("finalizada") == "finalizada"
    assert normalize_status("cerrada") == "cerrada"


def test_normalize_status_keeps_prudent_normalized_text_for_unknown_values() -> None:
    """Debe conservar una forma limpia cuando el estado no encaja aún en la taxonomía."""
    assert normalize_status("  Pendiente de revisión ") == "pendiente de revision"
