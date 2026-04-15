"""Derived-field helpers for business records."""

from __future__ import annotations

import re
from decimal import Decimal

from .models import AuctionRecord, RecordDerivations


TARGET_PROVINCES = {"valencia", "valencia/valencia", "valencia/valencia"}
TARGET_MUNICIPALITIES = {
    "valencia",
    "torrent",
    "paterna",
    "gandia",
    "sagunto",
    "sagunt",
}
PROPERTY_ASSET_TYPES = {"real_estate", "property", "inmueble"}
RESIDENTIAL_HINTS = (
    "residential",
    "vivienda",
    "piso",
    "casa",
    "duplex",
    "dúplex",
)
UNKNOWN_CHARGES_HINTS = (
    "unknown",
    "pendiente",
    "no consta",
    "sin informacion",
    "sin información",
    "desconoc",
)
POSTAL_CODE_RE = re.compile(r"\b\d{5}\b")
POSTAL_CODE_WITH_LABEL_RE = re.compile(
    r"(?:codigo\s+postal|c\.?\s*p\.?)\D{0,12}((?:46\d{3})|\d{5})",
    re.IGNORECASE,
)
POSTAL_CODE_WITH_LOCALITY_RE = re.compile(
    r"(?<![\d/])((?:46\d{3})|\d{5})(?![\d/])\s*[-,)]\s*[A-ZÁÉÍÓÚÜÑ]",
    re.IGNORECASE,
)
VALENCIA_POSTAL_CODE_RE = re.compile(r"(?<![\d/])(46\d{3})(?![\d/])")
LOT_COUNT_RE = re.compile(r"\((\d+)\s+lotes?\)", re.IGNORECASE)


def build_record_derivations(record: AuctionRecord) -> RecordDerivations:
    """Build explicit derived fields without making a final judgment."""
    has_lots = record.has_lots if record.has_lots is not None else _infer_has_lots(record)
    lot_count = record.lot_count if record.lot_count is not None else _infer_lot_count(record)
    minimum_location = bool(record.municipality or record.province or record.postal_code)
    has_invalid_appraisal = _has_invalid_appraisal(record)
    has_complex_lot_structure = bool(has_lots)

    return RecordDerivations(
        opening_bid_ratio=_safe_ratio(record.opening_bid, record.appraisal_value),
        deposit_ratio=_safe_ratio(record.deposit, record.opening_bid),
        has_invalid_appraisal=has_invalid_appraisal,
        has_reference_price_data=_has_reference_price_data(record),
        is_property=_is_property(record),
        is_residential_like=_is_residential_like(record),
        is_in_target_area=_is_in_target_area(record),
        has_unknown_charges=_has_unknown_charges(record),
        has_complex_lot_structure=has_complex_lot_structure,
        has_critical_missing_data=_has_critical_missing_data(
            record,
            has_complex_lot_structure=has_complex_lot_structure,
        ),
        has_minimum_location=minimum_location,
        description_is_poor=_description_is_poor(record),
    )


def infer_postal_code(record: AuctionRecord) -> str | None:
    """Extract a postal code from safe public text only when context is reliable."""
    searchable_text = " ".join(value for value in (record.address_text, record.description) if value)
    if not searchable_text:
        return None

    folded_text = searchable_text.casefold()
    is_valencia_focus = _is_in_target_area(record)

    label_matches = [match.group(1) for match in POSTAL_CODE_WITH_LABEL_RE.finditer(folded_text)]
    locality_matches = [match.group(1) for match in POSTAL_CODE_WITH_LOCALITY_RE.finditer(searchable_text)]

    if is_valencia_focus:
        prioritized_label = next((value for value in label_matches if value.startswith("46")), None)
        if prioritized_label is not None:
            return prioritized_label

        prioritized_locality = next((value for value in locality_matches if value.startswith("46")), None)
        if prioritized_locality is not None:
            return prioritized_locality

        standalone_valencia_match = VALENCIA_POSTAL_CODE_RE.search(searchable_text)
        if standalone_valencia_match is not None:
            return standalone_valencia_match.group(1)

        return None

    if label_matches:
        return label_matches[0]
    if locality_matches:
        return locality_matches[0]
    return None


def _safe_ratio(numerator: Decimal | None, denominator: Decimal | None) -> Decimal | None:
    """Return a direct ratio only when both values are usable."""
    if numerator is None or denominator is None or denominator <= 0:
        return None
    return numerator / denominator


def _is_property(record: AuctionRecord) -> bool:
    """Detect whether the record points to a property-like asset."""
    searchable_text = " ".join(
        value for value in (record.asset_type, record.asset_subtype, record.title, record.description) if value
    ).casefold()
    return any(token in searchable_text for token in PROPERTY_ASSET_TYPES | set(RESIDENTIAL_HINTS))


def _is_residential_like(record: AuctionRecord) -> bool:
    """Detect residential-like assets with simple text hints."""
    searchable_text = " ".join(
        value for value in (record.asset_subtype, record.title, record.description) if value
    ).casefold()
    return any(token in searchable_text for token in RESIDENTIAL_HINTS)


def _is_in_target_area(record: AuctionRecord) -> bool:
    """Detect whether the record is within the current target geography."""
    province = (record.province or "").casefold()
    municipality = (record.municipality or "").casefold()
    return province in TARGET_PROVINCES or municipality in TARGET_MUNICIPALITIES


def _has_unknown_charges(record: AuctionRecord) -> bool:
    """Detect when charges are missing or explicitly unclear."""
    if record.charges_text is None:
        return True

    charges_text = record.charges_text.casefold()
    return any(token in charges_text for token in UNKNOWN_CHARGES_HINTS)


def _infer_has_lots(record: AuctionRecord) -> bool:
    """Infer a lot structure from the public text already available."""
    searchable_text = " ".join(value for value in (record.title, record.description) if value).casefold()
    return "lote" in searchable_text or "lotes" in searchable_text


def _infer_lot_count(record: AuctionRecord) -> int | None:
    """Infer the lot count only from explicit title patterns."""
    searchable_text = " ".join(value for value in (record.title, record.description) if value)
    match = LOT_COUNT_RE.search(searchable_text)
    if match is None:
        return None
    return int(match.group(1))


def _has_invalid_appraisal(record: AuctionRecord) -> bool:
    """Detect appraisal values that are explicit but unusable for valuation ratios."""
    return record.appraisal_value is not None and record.appraisal_value == 0


def _has_reference_price_data(record: AuctionRecord) -> bool:
    """Detect whether at least one direct price signal is available."""
    return record.appraisal_value is not None or record.opening_bid is not None


def _has_critical_missing_data(
    record: AuctionRecord,
    *,
    has_complex_lot_structure: bool,
) -> bool:
    """Mark records that are too incomplete for confident evaluation."""
    missing_price_data = not _has_reference_price_data(record) and not has_complex_lot_structure
    missing_location = not any((record.province, record.municipality, record.postal_code))
    return missing_price_data or missing_location


def _description_is_poor(record: AuctionRecord) -> bool:
    """Detect descriptions that are absent or too thin to help review."""
    description = (record.description or "").strip()
    if not description:
        return True
    return len(description) < 30
