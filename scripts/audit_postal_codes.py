"""Audit postal-code capture quality across one selected dataset."""

from __future__ import annotations

import re
import sys
from collections import Counter
from pathlib import Path
from typing import Literal

import requests


# Permitir ejecutar el script desde la raiz del repositorio sin instalar el paquete.
REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from monitor.pipeline.evaluate import build_auction_record
from monitor.storage import (
    fetch_all_auctions,
    fetch_all_completed_auctions,
    fetch_all_upcoming_auctions,
)


DatasetName = Literal["active", "upcoming", "completed"]
BasePostalCodeState = Literal[
    "OK",
    "MISSING",
    "SUSPICIOUS_FORMAT",
    "OUTSIDE_EXPECTED_VALENCIA_RANGE",
]
PostalCodeState = Literal[
    "OK",
    "MISSING_TRUE",
    "MISSING_PARENT_BUT_LOTS_HAVE_POSTAL",
    "SUSPICIOUS_FORMAT",
    "OUTSIDE_EXPECTED_VALENCIA_RANGE",
]

DATASET: DatasetName = "active"
MAX_ROWS: int | None = 40
CHECK_HTML_FOR_MISSING = True
REQUEST_TIMEOUT_SECONDS = 20
POSTAL_CODE_RE = re.compile(r"\b\d{5}\b")


def main() -> None:
    """Run the postal-code audit for one selected dataset."""
    auctions = load_dataset_auctions(DATASET)
    if MAX_ROWS is not None:
        auctions = auctions[:MAX_ROWS]

    if not auctions:
        print(f"No auctions found for dataset={DATASET}.")
        return

    print(f"Postal code audit for dataset={DATASET}")
    print(f"Rows scanned: {len(auctions)}")
    print("")

    records = [build_auction_record(auction) for auction in auctions]
    lots_have_postal_by_parent_id = build_lot_postal_index(records)
    html_session = requests.Session() if CHECK_HTML_FOR_MISSING else None
    state_counter: Counter[str] = Counter()
    present_in_html_but_not_persisted = 0

    for record in records:
        postal_code = normalize_postal_code(record.postal_code)
        base_state = classify_postal_code_state(
            postal_code=postal_code,
            municipality=record.municipality,
            province=record.province,
        )
        state = refine_postal_code_state(
            base_state=base_state,
            auction_id=record.auction_id,
            lots_have_postal_by_parent_id=lots_have_postal_by_parent_id,
        )
        html_flag = False
        if (
            CHECK_HTML_FOR_MISSING
            and state in {"MISSING_TRUE", "MISSING_PARENT_BUT_LOTS_HAVE_POSTAL"}
            and record.source_url
            and html_session is not None
        ):
            html_postal_code = extract_postal_code_from_html(record.source_url, session=html_session)
            if html_postal_code is not None:
                html_flag = True
                present_in_html_but_not_persisted += 1

        state_counter[state] += 1
        print_record_audit(
            record=record,
            postal_code=postal_code,
            state=state,
            present_in_html_but_not_persisted=html_flag,
        )

    print("Summary")
    print(f"- total rows: {len(auctions)}")
    print(f"- ok: {state_counter['OK']}")
    print(f"- missing_true: {state_counter['MISSING_TRUE']}")
    print(
        "- missing_parent_but_lots_have_postal: "
        f"{state_counter['MISSING_PARENT_BUT_LOTS_HAVE_POSTAL']}"
    )
    print(f"- suspicious_format: {state_counter['SUSPICIOUS_FORMAT']}")
    print(f"- outside_expected_valencia_range: {state_counter['OUTSIDE_EXPECTED_VALENCIA_RANGE']}")
    print(f"- present_in_html_but_not_persisted: {present_in_html_but_not_persisted}")


def load_dataset_auctions(dataset: DatasetName):
    """Load one dataset without mixing storage tables."""
    if dataset == "active":
        return fetch_all_auctions()
    if dataset == "upcoming":
        return fetch_all_upcoming_auctions()
    return fetch_all_completed_auctions()


def normalize_postal_code(postal_code: str | None) -> str | None:
    """Normalize postal codes conservatively for audit purposes."""
    if postal_code is None:
        return None

    normalized = postal_code.strip()
    if not normalized:
        return None
    return normalized


def classify_postal_code_state(
    *,
    postal_code: str | None,
    municipality: str | None,
    province: str | None,
) -> BasePostalCodeState:
    """Classify one postal code against the current Valencia focus rules."""
    if postal_code is None:
        return "MISSING"

    if not POSTAL_CODE_RE.fullmatch(postal_code):
        return "SUSPICIOUS_FORMAT"

    if is_valencia_focus(municipality=municipality, province=province) and not postal_code.startswith("46"):
        return "OUTSIDE_EXPECTED_VALENCIA_RANGE"

    return "OK"


def refine_postal_code_state(
    *,
    base_state: BasePostalCodeState,
    auction_id: str | None,
    lots_have_postal_by_parent_id: dict[str, bool],
) -> PostalCodeState:
    """Split missing cases between true absence and parent rows covered by lots."""
    if base_state != "MISSING":
        return base_state

    if extract_lot_number(auction_id) is not None:
        return "MISSING_TRUE"

    parent_auction_id = extract_base_auction_id(auction_id)
    if lots_have_postal_by_parent_id.get(parent_auction_id, False):
        return "MISSING_PARENT_BUT_LOTS_HAVE_POSTAL"

    return "MISSING_TRUE"


def is_valencia_focus(*, municipality: str | None, province: str | None) -> bool:
    """Detect whether the row falls under the current Valencia-focused scope."""
    province_text = (province or "").casefold()
    municipality_text = (municipality or "").casefold()
    return "valencia" in province_text or "valència" in province_text or municipality_text == "valencia"


def build_auction_lot_id(auction_id: str | None, lot_number: int | None) -> str:
    """Build a readable identifier for auctions and lots."""
    if auction_id is None:
        return ""
    if lot_number is None or auction_id.endswith(f"::lot:{lot_number}"):
        return auction_id
    return f"{auction_id}::lot:{lot_number}"


def extract_base_auction_id(auction_id: str | None) -> str:
    """Return the shared parent auction identifier for parent rows and lots."""
    if auction_id is None:
        return ""
    return auction_id.split("::lot:", maxsplit=1)[0]


def extract_lot_number(auction_id: str | None) -> int | None:
    """Infer lot number from synthetic auction identifiers when present."""
    if auction_id is None or "::lot:" not in auction_id:
        return None

    lot_fragment = auction_id.split("::lot:", maxsplit=1)[1]
    if not lot_fragment.isdigit():
        return None
    return int(lot_fragment)


def build_lot_postal_index(records: list) -> dict[str, bool]:
    """Track which parent auctions already have at least one lot with valid postal code."""
    lots_have_postal_by_parent_id: dict[str, bool] = {}

    for record in records:
        if extract_lot_number(record.auction_id) is None:
            continue

        base_state = classify_postal_code_state(
            postal_code=normalize_postal_code(record.postal_code),
            municipality=record.municipality,
            province=record.province,
        )
        if base_state != "OK":
            continue

        parent_auction_id = extract_base_auction_id(record.auction_id)
        lots_have_postal_by_parent_id[parent_auction_id] = True

    return lots_have_postal_by_parent_id


def extract_postal_code_from_html(source_url: str, *, session: requests.Session) -> str | None:
    """Fetch one detail page and look for a five-digit postal code in the raw HTML."""
    try:
        response = session.get(source_url, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
    except requests.RequestException:
        return None

    match = POSTAL_CODE_RE.search(response.text)
    if match is None:
        return None
    return match.group(0)


def print_record_audit(
    *,
    record,
    postal_code: str | None,
    state: PostalCodeState,
    present_in_html_but_not_persisted: bool,
) -> None:
    """Print one compact audit block for quick manual review."""
    auction_lot_id = build_auction_lot_id(record.auction_id, extract_lot_number(record.auction_id))
    print(f"[{state}]")
    print(auction_lot_id or "-")
    print(f"title={record.title}")
    print(f"municipality={record.municipality or '-'}")
    print(f"province={record.province or '-'}")
    print(f"postal_code={postal_code or '-'}")
    print(f"source_url={record.source_url or '-'}")
    if present_in_html_but_not_persisted:
        print("html_hint=PRESENT_IN_HTML_BUT_NOT_PERSISTED")
    print("")


if __name__ == "__main__":
    main()
