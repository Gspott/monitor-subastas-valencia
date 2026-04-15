"""Helpers for conservative location normalization."""

from __future__ import annotations

import re
import unicodedata


WHITESPACE_RE = re.compile(r"\s+")
AMBIGUOUS_PLAYA_GANDIA_POSTAL_CODE = "46730"
LOWERCASE_MUNICIPALITY_TOKENS = {"de", "del", "la", "las", "el", "els", "les", "los", "y", "i"}
VALENCIA_POSTAL_CODE_TO_MUNICIPALITY = {
    "46001": "Valencia",
    "46002": "Valencia",
    "46370": "Chiva",
    "46410": "Sueca",
    "46430": "Sollana",
    "46500": "Sagunto",
    "46520": "Puerto de Sagunto",
    "46680": "Algemesi",
    "46701": "Gandia",
    "46710": "Daimus",
    "46900": "Torrent",
    "46980": "Paterna",
}
GENERIC_GANDIA_FOLDED = "gandia"
SPECIAL_46730_MUNICIPALITY_ALIASES = {
    "playa de gandia": "Playa de Gandia",
    "grao de gandia": "Grao de Gandia",
}


def normalize_municipality_with_postal_code(
    municipality: str,
    *,
    postal_code: str | None,
    description: str | None = None,
) -> str:
    """Normalize municipality using a trusted postal-code map when possible."""
    cleaned_municipality = clean_text(municipality)
    if clean_text(postal_code or "") == AMBIGUOUS_PLAYA_GANDIA_POSTAL_CODE:
        resolved_municipality = _normalize_ambiguous_46730_municipality(
            cleaned_municipality,
            description=description,
        )
        return normalize_municipality_name(resolved_municipality)

    canonical_municipality = resolve_municipality_from_postal_code(postal_code)
    if canonical_municipality is None:
        return normalize_municipality_name(cleaned_municipality)
    if not cleaned_municipality:
        return normalize_municipality_name(canonical_municipality)
    if fold_text(cleaned_municipality) == fold_text(canonical_municipality):
        return normalize_municipality_name(canonical_municipality)
    return normalize_municipality_name(canonical_municipality)


def resolve_municipality_from_postal_code(postal_code: str | None) -> str | None:
    """Return a canonical municipality only for explicitly supported postal codes."""
    if postal_code is None:
        return None
    return VALENCIA_POSTAL_CODE_TO_MUNICIPALITY.get(clean_text(postal_code))


def clean_text(value: str) -> str:
    """Collapse whitespace to keep matching conservative and stable."""
    normalized = unicodedata.normalize("NFKC", value)
    normalized = normalized.replace("\xa0", " ")
    normalized = normalized.replace("\r", " ").replace("\n", " ").replace("\t", " ")
    normalized = WHITESPACE_RE.sub(" ", normalized)
    return normalized.strip(" ;,|-")


def normalize_municipality_name(municipality: str) -> str:
    """Normalize one municipality display name to an accent-free stable form."""
    cleaned_municipality = clean_text(municipality)
    if not cleaned_municipality:
        return ""

    without_accents = strip_accents(cleaned_municipality)
    parts = re.split(r"([ -])", without_accents.casefold())
    normalized_parts: list[str] = []
    is_first_word = True
    for part in parts:
        if part in {" ", "-"}:
            normalized_parts.append(part)
            continue
        if not part:
            continue
        if not is_first_word and part in LOWERCASE_MUNICIPALITY_TOKENS:
            normalized_parts.append(part)
        else:
            normalized_parts.append(part.capitalize())
        is_first_word = False
    return "".join(normalized_parts)


def fold_text(value: str) -> str:
    """Fold accents and case for conservative equality checks."""
    normalized = unicodedata.normalize("NFKD", value)
    without_marks = "".join(char for char in normalized if not unicodedata.combining(char))
    return without_marks.casefold()


def strip_accents(value: str) -> str:
    """Remove accents while preserving the base characters."""
    normalized = unicodedata.normalize("NFKD", value)
    return "".join(char for char in normalized if not unicodedata.combining(char))


def _normalize_ambiguous_46730_municipality(
    municipality: str,
    *,
    description: str | None,
) -> str:
    """Resolve 46730 conservatively without collapsing Playa and Grao into Gandia."""
    folded_municipality = fold_text(municipality)
    canonical_from_municipality = SPECIAL_46730_MUNICIPALITY_ALIASES.get(folded_municipality)
    if canonical_from_municipality is not None:
        return canonical_from_municipality

    if not municipality:
        return municipality

    if folded_municipality != GENERIC_GANDIA_FOLDED:
        return municipality.title()

    canonical_from_description = _resolve_46730_municipality_from_description(description)
    if canonical_from_description is None:
        return municipality.title()

    return canonical_from_description


def _resolve_46730_municipality_from_description(description: str | None) -> str | None:
    """Read explicit Playa or Grao references from description when 46730 is generic."""
    if description is None:
        return None

    folded_description = fold_text(clean_text(description))
    matched_canonical_values = {
        canonical
        for alias, canonical in SPECIAL_46730_MUNICIPALITY_ALIASES.items()
        if alias in folded_description
    }
    if len(matched_canonical_values) != 1:
        return None

    return next(iter(matched_canonical_values))


def classify_46730_municipality_variant(municipality: str) -> str:
    """Classify the current municipality value for 46730 without inventing a replacement."""
    cleaned_municipality = clean_text(municipality)
    if not cleaned_municipality:
        return "empty"

    folded_municipality = fold_text(cleaned_municipality)
    if folded_municipality == GENERIC_GANDIA_FOLDED:
        return "generic"
    if folded_municipality in SPECIAL_46730_MUNICIPALITY_ALIASES:
        return "specific"
    return "other"
