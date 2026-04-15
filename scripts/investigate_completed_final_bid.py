"""Investigate whether BOE completed auctions expose a public final bid or award amount."""

from __future__ import annotations

import re
import sys
from pathlib import Path

import requests
from bs4 import BeautifulSoup, NavigableString, Tag


# Permitir ejecutar el script desde la raiz del repositorio sin instalar el paquete.
REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from monitor.sources.boe import build_lot_detail_url
from monitor.storage import fetch_all_completed_auctions


REQUEST_TIMEOUT_SECONDS = 20
TRACE_LIMIT = 5
TRACE_AUCTION_IDS: list[str] = []
KEYWORDS = (
    "puja",
    "puja final",
    "puja más alta",
    "puja maxima",
    "puja máxima",
    "mejor postura",
    "importe adjudic",
    "adjudic",
    "remate",
    "cesion de remate",
    "cesión de remate",
    "reserva",
    "deposito",
    "depósito",
    "postor",
    "numero de pujas",
    "número de pujas",
    "resultado",
)
AMOUNT_RE = re.compile(r"\b\d{1,3}(?:\.\d{3})*,\d{2}\s*€")


def main() -> None:
    """Inspect several completed auctions and report whether a final amount is publicly exposed."""
    auctions = fetch_all_completed_auctions()
    selected_auctions = select_target_auctions(auctions)

    if not selected_auctions:
        print("No completed auctions available for investigation.")
        return

    with requests.Session() as session:
        for auction in selected_auctions:
            inspect_completed_auction(auction=auction, session=session)


def select_target_auctions(auctions):
    """Pick configured auctions or a small real sample from SQLite."""
    if TRACE_AUCTION_IDS:
        auctions_by_id = {auction.external_id: auction for auction in auctions}
        return [
            auctions_by_id[auction_id]
            for auction_id in TRACE_AUCTION_IDS
            if auction_id in auctions_by_id
        ]
    return auctions[:TRACE_LIMIT]


def inspect_completed_auction(*, auction, session: requests.Session) -> None:
    """Inspect one completed auction across the most relevant BOE views."""
    views = build_candidate_views(auction)

    print("=" * 100)
    print(f"auction_id={auction.external_id or '-'}")
    print(f"official_status={auction.official_status or '-'}")
    print(f"source_url={auction.official_url or '-'}")
    print("")

    found_any_numeric_final_amount = False

    for view_name, view_url in views:
        html = fetch_html(view_url, session=session)
        if html is None:
            print(f"[{view_name}] {view_url}")
            print("- result: request_failed_or_missing")
            print("")
            continue

        findings = extract_keyword_findings(html)
        numeric_final_candidates = [
            finding
            for finding in findings
            if finding["amount"] is not None and is_final_bid_like_label(finding["label"])
        ]
        if numeric_final_candidates:
            found_any_numeric_final_amount = True

        print(f"[{view_name}] {view_url}")
        if not findings:
            print("- result: no_relevant_keyword_blocks_found")
            print("")
            continue

        print(f"- keyword_blocks_found: {len(findings)}")
        for finding in findings[:8]:
            print(f"- label: {finding['label']}")
            print(f"  amount: {finding['amount'] or '-'}")
            print(f"  final_bid_like: {'yes' if is_final_bid_like_label(finding['label']) else 'no'}")
            print(f"  snippet: {finding['snippet']}")
            print(f"  html_block: {finding['html_block']}")
        print("")

    print("Conclusion")
    if found_any_numeric_final_amount:
        print("- A public numeric final-bid-like amount was found in at least one inspected view.")
        print("- The strongest current signal is the BOE `ver=5` view (`Pujas`).")
    else:
        print("- No public numeric final-bid-like amount was found in the inspected BOE views.")
    print("")


def build_candidate_views(auction) -> list[tuple[str, str]]:
    """Build the BOE views that are most likely to expose end-state bidding information."""
    if not auction.official_url:
        return []

    base_url = auction.official_url.split("&ver=", maxsplit=1)[0]
    views = [
        ("base_ver1_general", f"{base_url}&ver=1"),
        ("base_ver2_authority", f"{base_url}&ver=2"),
        ("base_ver3_assets", f"{base_url}&ver=3"),
        ("base_ver5_bids", f"{base_url}&ver=5"),
    ]

    lot_number = extract_lot_number(auction.external_id)
    if lot_number is not None:
        views.extend(
            [
                ("lot_ver1_general", build_lot_detail_url(base_url, lot_number).replace("ver=3", "ver=1")),
                ("lot_ver3_assets", build_lot_detail_url(base_url, lot_number)),
                ("lot_ver5_bids", build_lot_detail_url(base_url, lot_number).replace("ver=3", "ver=5")),
            ]
        )

    return views


def fetch_html(url: str, *, session: requests.Session) -> str | None:
    """Fetch one BOE HTML view safely for investigation."""
    try:
        response = session.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
    except requests.RequestException:
        return None
    return response.text


def extract_keyword_findings(html: str) -> list[dict[str, str | None]]:
    """Extract text blocks that mention bidding, award, or final-result keywords."""
    soup = BeautifulSoup(html, "html.parser")
    findings: list[dict[str, str | None]] = []
    seen_keys: set[tuple[str, str]] = set()

    for text_node in soup.find_all(string=True):
        if not isinstance(text_node, NavigableString):
            continue
        raw_text = str(text_node).strip()
        if not raw_text:
            continue
        folded = raw_text.casefold()
        if not any(keyword.casefold() in folded for keyword in KEYWORDS):
            continue

        parent = text_node.parent
        if not isinstance(parent, Tag):
            continue

        snippet = build_context_snippet(parent)
        html_block = build_context_html(parent)
        label = clean_snippet(raw_text)
        amount = extract_amount_from_text(snippet)
        dedupe_key = (label, snippet)
        if dedupe_key in seen_keys:
            continue
        seen_keys.add(dedupe_key)
        findings.append(
            {
                "label": label,
                "amount": amount,
                "snippet": snippet,
                "html_block": html_block,
            }
        )

    return findings


def build_context_snippet(node: Tag) -> str:
    """Build a wider text context around one keyword node."""
    text_parts = [node.get_text(" ", strip=True)]

    sibling = node.next_sibling
    collected = 0
    while sibling is not None and collected < 3:
        if isinstance(sibling, NavigableString):
            sibling_text = str(sibling).strip()
        elif isinstance(sibling, Tag):
            sibling_text = sibling.get_text(" ", strip=True)
        else:
            sibling_text = ""

        if sibling_text:
            text_parts.append(sibling_text)
            collected += 1
        sibling = sibling.next_sibling

    return clean_snippet(" ".join(text_parts))


def build_context_html(node: Tag) -> str:
    """Build a small HTML excerpt around the keyword node and nearby siblings."""
    html_parts = [str(node)]

    sibling = node.next_sibling
    collected = 0
    while sibling is not None and collected < 3:
        if isinstance(sibling, NavigableString):
            sibling_html = str(sibling).strip()
        elif isinstance(sibling, Tag):
            sibling_html = str(sibling)
        else:
            sibling_html = ""

        if sibling_html:
            html_parts.append(sibling_html)
            collected += 1
        sibling = sibling.next_sibling

    return clean_snippet(" ".join(html_parts)[:800])


def is_final_bid_like_label(label: str) -> bool:
    """Detect whether a keyword block looks like a usable public final amount."""
    lowered = label.casefold()
    return any(
        token in lowered
        for token in (
            "puja máxima",
            "puja maxima",
            "puja más alta",
            "puja mas alta",
            "mejor postura",
            "importe adjudic",
            "adjudic",
            "remate",
        )
    )


def extract_amount_from_text(text: str) -> str | None:
    """Extract the first euro amount from a text block when present."""
    match = AMOUNT_RE.search(text)
    if match is None:
        return None
    return match.group(0)


def extract_lot_number(auction_id: str | None) -> int | None:
    """Infer the lot number from the synthetic auction identifier."""
    if auction_id is None or "::lot:" not in auction_id:
        return None
    lot_fragment = auction_id.split("::lot:", maxsplit=1)[1]
    if not lot_fragment.isdigit():
        return None
    return int(lot_fragment)


def clean_snippet(text: str) -> str:
    """Compact long HTML or text snippets for readable console output."""
    compact = " ".join(text.split())
    return compact[:400]


if __name__ == "__main__":
    main()
