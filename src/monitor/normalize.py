"""Normalization helpers for auction data."""

from __future__ import annotations

import re
import unicodedata
from typing import Iterable

from .location import normalize_municipality_with_postal_code
from .models import Auction
from .status import normalize_status


WHITESPACE_RE = re.compile(r"\s+")
POSTAL_CODE_RE = re.compile(r"^\d{5}$")
EMAIL_RE = re.compile(r"\b[\w.\-+]+@[\w.\-]+\.\w+\b")
PHONE_RE = re.compile(r"(?<!\w)(?:\+?\d[\d .-]{7,}\d)(?!\w)")
DOCUMENT_ID_RE = re.compile(r"\b(?:\d{8}[A-Z]|[XYZ]\d{7}[A-Z])\b", re.IGNORECASE)
SENSITIVE_LABELS = (
    "dni",
    "nif",
    "nie",
    "domicilio",
    "deudor",
    "deudora",
    "ejecutado",
    "ejecutada",
    "demandado",
    "demandada",
    "nombre",
    "apellidos",
    "titular",
)

PROVINCE_ALIASES = {
    "valencia": "Valencia",
    "valència": "Valencia",
    "valencia/valència": "Valencia",
    "alicante": "Alicante",
    "alacant": "Alicante",
    "alicante/alacant": "Alicante",
    "castellon": "Castellon",
    "castellón": "Castellon",
    "castello": "Castellon",
    "castelló": "Castellon",
    "castellon/castello": "Castellon",
    "castellón/castelló": "Castellon",
}

MUNICIPALITY_ALIASES = {
    "valencia": "Valencia",
    "valència": "Valencia",
    "gandia": "Gandia",
    "torrent": "Torrent",
    "paterna": "Paterna",
    "sagunto": "Sagunto",
    "sagunt": "Sagunto",
}

ASSET_CLASS_ALIASES = {
    "real_estate": "real_estate",
    "real estate": "real_estate",
    "inmueble": "real_estate",
    "property": "real_estate",
    "other_non_vehicle_asset": "other_non_vehicle_asset",
    "other non vehicle asset": "other_non_vehicle_asset",
    "other asset": "other_non_vehicle_asset",
}

ASSET_SUBCLASS_ALIASES = {
    "inmueble_vivienda": "residential_property",
    "vivienda": "residential_property",
    "piso": "residential_property",
    "casa": "residential_property",
    "inmueble_local": "commercial_property",
    "local": "commercial_property",
    "local_comercial": "commercial_property",
    "garaje": "garage",
    "trastero": "storage_room",
    "solar": "land",
    "parcela": "land",
    "finca": "land",
    "nave": "industrial_property",
}


def normalize_auctions(auctions: Iterable[Auction]) -> list[Auction]:
    """Normalize a collection of Auction objects."""
    return [normalize_auction(auction) for auction in auctions]


def normalize_auction(auction: Auction) -> Auction:
    """Normalize one Auction object."""
    payload = auction.model_dump()

    # Limpiar y homogeneizar los campos de texto antes de guardar.
    payload["source"] = clean_text(payload["source"])
    payload["external_id"] = clean_optional_text(payload["external_id"])
    payload["title"] = clean_text(payload["title"])
    payload["province"] = normalize_province(payload["province"])
    payload["postal_code"] = normalize_postal_code(payload.get("postal_code"))
    payload["description"] = sanitize_public_text(payload["description"])
    payload["municipality"] = normalize_municipality(
        payload["municipality"],
        postal_code=payload["postal_code"],
        description=payload["description"],
    )
    payload["asset_class"] = normalize_asset_class(payload["asset_class"])
    payload["asset_subclass"] = normalize_asset_subclass(payload["asset_subclass"])
    # Guardar una forma canónica cuando el estado sea reconocible. Si no lo es,
    # conservar una versión prudente y normalizada para no perder información útil.
    payload["official_status"] = normalize_status(payload["official_status"]) or clean_text(payload["official_status"]).casefold()
    payload["occupancy_status"] = sanitize_public_text(payload["occupancy_status"])
    payload["encumbrances_summary"] = sanitize_public_text(payload["encumbrances_summary"])
    payload["official_url"] = clean_optional_text(payload["official_url"])

    return Auction(**payload)


def clean_optional_text(value: str | None) -> str | None:
    """Clean optional text while preserving missing values."""
    if value is None:
        return None

    cleaned = clean_text(value)
    return cleaned or None


def clean_text(value: str) -> str:
    """Collapse dirty whitespace and trim noisy text."""
    normalized = unicodedata.normalize("NFKC", value)
    normalized = normalized.replace("\xa0", " ")
    normalized = normalized.replace("\r", " ").replace("\n", " ").replace("\t", " ")
    normalized = WHITESPACE_RE.sub(" ", normalized)
    return normalized.strip(" ;,|-")


def normalize_postal_code(value: str | None) -> str | None:
    """Normalize postal codes conservatively and drop suspicious values."""
    if value is None:
        return None

    cleaned = clean_text(value)
    if not cleaned:
        return None
    if not POSTAL_CODE_RE.fullmatch(cleaned):
        return None
    return cleaned


def sanitize_public_text(value: str | None) -> str | None:
    """Sanitize free text to reduce the risk of persisting personal data."""
    if value is None:
        return None

    cleaned = clean_text(value)
    if not cleaned:
        return None

    folded = _fold_text(cleaned)
    if any(label in folded for label in SENSITIVE_LABELS):
        # Bloquear textos que parezcan describir personas o identificadores.
        return None

    sanitized = EMAIL_RE.sub("[redacted-email]", cleaned)
    sanitized = PHONE_RE.sub("[redacted-phone]", sanitized)
    sanitized = DOCUMENT_ID_RE.sub("[redacted-id]", sanitized)
    return sanitized or None


def normalize_province(value: str) -> str:
    """Normalize province names to a stable display form."""
    cleaned = clean_text(value)
    canonical = PROVINCE_ALIASES.get(_fold_text(cleaned))
    return canonical or cleaned.title()


def normalize_municipality(
    value: str,
    *,
    postal_code: str | None = None,
    description: str | None = None,
) -> str:
    """Normalize municipality names when a safe alias exists."""
    cleaned = clean_text(value)
    canonical = MUNICIPALITY_ALIASES.get(_fold_text(cleaned))
    normalized = canonical or cleaned.title()
    return normalize_municipality_with_postal_code(
        normalized,
        postal_code=postal_code,
        description=description,
    )


def normalize_asset_class(value: str) -> str:
    """Normalize asset class to controlled internal values."""
    cleaned = clean_text(value)
    folded = _fold_text(cleaned).replace("-", " ").replace("_", " ")
    return ASSET_CLASS_ALIASES.get(folded, folded.replace(" ", "_"))


def normalize_asset_subclass(value: str) -> str:
    """Normalize asset subclass to controlled internal values when possible."""
    cleaned = clean_text(value)
    folded = _fold_text(cleaned).replace("-", "_").replace(" ", "_")
    return ASSET_SUBCLASS_ALIASES.get(folded, folded)


def _fold_text(value: str) -> str:
    """Remove accents and case distinctions for safe matching."""
    normalized = unicodedata.normalize("NFKD", value)
    without_marks = "".join(char for char in normalized if not unicodedata.combining(char))
    return without_marks.casefold()
