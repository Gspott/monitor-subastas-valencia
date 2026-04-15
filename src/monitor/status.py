"""Status helpers for auction lifecycle decisions."""

from __future__ import annotations

import unicodedata


ACTIVE_STATUS_TOKENS = {
    "abierta",
    "abierta con pujas",
    "en tramitacion",
}
INACTIVE_STATUS_TOKENS = {
    "suspendida",
    "cancelada",
    "adjudicada",
    "finalizada",
    "cerrada",
    "desierta",
}


def is_active_status(status: str | None) -> bool:
    """Return whether an auction status should be treated as active."""
    normalized = normalize_status(status)
    if normalized is None:
        return False

    if normalized in ACTIVE_STATUS_TOKENS:
        return True

    if normalized in INACTIVE_STATUS_TOKENS:
        return False

    # Ante estados no reconocidos, optar por excluirlos hasta validarlos mejor.
    return False


def normalize_status(status: str | None) -> str | None:
    """Normalize a raw status string into a canonical stored status."""
    if status is None:
        return None

    cleaned = " ".join(status.split()).strip()
    if not cleaned:
        return None

    folded = _fold_text(cleaned)
    folded = folded.replace("/", " ").replace("-", " ")
    folded = " ".join(folded.split())

    # Reducir variantes observadas a una taxonomía mínima para guardar el estado.
    if "abierta" in folded and "pujas" in folded:
        return "abierta con pujas"
    if "celebrandose" in folded and "pujas" in folded:
        return "abierta con pujas"
    if "abierta" in folded:
        return "abierta"
    if "celebrandose" in folded:
        return "abierta"
    if "en tramitacion" in folded:
        return "en tramitacion"
    if "suspendida" in folded:
        return "suspendida"
    if "cancelada" in folded:
        return "cancelada"
    if "adjudicada" in folded:
        return "adjudicada"
    if "finalizada" in folded:
        return "finalizada"
    if "cerrada" in folded:
        return "cerrada"
    if "desierta" in folded:
        return "desierta"

    # Conservar una forma prudente y normalizada para estados aún no clasificados.
    return folded


def normalize_status_text(status: str | None) -> str | None:
    """Backward-compatible alias for normalized status text."""
    return normalize_status(status)


def _fold_text(value: str) -> str:
    """Remove accents and case distinctions for robust status matching."""
    normalized = unicodedata.normalize("NFKD", value)
    without_marks = "".join(char for char in normalized if not unicodedata.combining(char))
    return without_marks.casefold()
