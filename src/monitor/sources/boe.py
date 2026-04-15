"""BOE source adapter."""

from __future__ import annotations

import logging
import re
import unicodedata
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Iterable
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

import requests
from bs4 import BeautifulSoup, Tag

from ..models import Auction


logger = logging.getLogger(__name__)

BOE_SOURCE_NAME = "BOE"
BOE_LISTING_URLS = (
    "https://subastas.boe.es/",
)
BOE_BASE_URL = "https://subastas.boe.es/"
REQUEST_TIMEOUT_SECONDS = 15
TARGET_PROVINCE = "Valencia"
REAL_LISTING_SELECTOR = "div.listadoResult li.resultado-busqueda"
DETAIL_TABLE_SELECTOR = "#idBloqueDatos1 table"
DETAIL_LINK_PATTERN = "detalleSubasta.php"
FIXTURE_REQUIRED_LISTING_FIELDS = ("title", "province", "municipality", "asset_type", "status")
LISTING_META_PREFIXES = ("Expediente:", "Estado:")
VEHICLE_KEYWORDS = (
    "vehiculo",
    "vehículo",
    "vehiculos",
    "vehículos",
    "turismo",
    "todoterreno",
    "coche",
    "coches",
    "moto",
    "motocicleta",
    "motocicletas",
    "camion",
    "camión",
    "camiones",
    "furgoneta",
    "furgonetas",
    "tractor",
    "tractores",
    "remolque",
    "remolques",
    "automovil",
    "automóvil",
    "automoviles",
    "automóviles",
)
REAL_ESTATE_KEYWORDS = (
    "inmueble",
    "vivienda",
    "piso",
    "casa",
    "local",
    "garaje",
    "trastero",
    "parcela",
    "solar",
    "finca",
    "nave",
)
IDENTIFIER_RE = re.compile(r"\bSUB-[A-Z0-9-]+\b")
ISO_DATETIME_RE = re.compile(r"ISO:\s*([0-9T:+-]+)")
SENSITIVE_FREE_TEXT_HINTS = (
    "dni",
    "nif",
    "nie",
    "nombre",
    "apellidos",
    "deudor",
    "deudora",
    "ejecutado",
    "ejecutada",
    "titular",
    "domicilio",
)


@dataclass(slots=True)
class ParsedBoeItem:
    """Intermediate non-personal BOE item."""

    external_id: str | None
    title: str
    province: str
    municipality: str
    asset_class: str
    asset_subclass: str
    official_status: str
    official_url: str | None = None
    description: str | None = None
    appraisal_value: Decimal | None = None
    starting_bid: Decimal | None = None
    current_bid: Decimal | None = None
    deposit: Decimal | None = None
    occupancy_status: str | None = None
    encumbrances_summary: str | None = None
    publication_date: str | None = None
    opening_date: str | None = None
    closing_date: str | None = None
    filter_text: str | None = None


@dataclass(slots=True)
class ParsedBoeDetail:
    """Validated detail fields from a BOE detail page."""

    external_id: str | None
    title: str | None
    opening_date: str | None = None
    closing_date: str | None = None
    appraisal_value: Decimal | None = None
    starting_bid: Decimal | None = None
    current_bid: Decimal | None = None
    deposit: Decimal | None = None
    official_url: str | None = None
    description: str | None = None
    occupancy_status: str | None = None
    encumbrances_summary: str | None = None


@dataclass(slots=True)
class ParsedBoeLot:
    """Validated detail fields for one lot within a BOE multi-lot auction."""

    parent_external_id: str | None
    lot_number: int
    title: str
    description: str | None = None
    asset_class: str = "other_non_vehicle_asset"
    asset_subclass: str = "unknown"
    province: str | None = None
    municipality: str | None = None
    postal_code: str | None = None
    appraisal_value: Decimal | None = None
    starting_bid: Decimal | None = None
    deposit: Decimal | None = None
    occupancy_status: str | None = None
    encumbrances_summary: str | None = None
    official_url: str | None = None


def fetch_listing_pages(
    urls: Iterable[str] = BOE_LISTING_URLS,
    session: requests.Session | None = None,
    timeout: int = REQUEST_TIMEOUT_SECONDS,
) -> list[str]:
    """Fetch BOE listing pages and return raw HTML payloads."""
    http_client = session or requests.Session()
    pages: list[str] = []

    for url in urls:
        try:
            logger.info("Fetching BOE listing page: %s", url)
            response = http_client.get(url, timeout=timeout)
            response.raise_for_status()
        except requests.RequestException as exc:
            logger.warning("Failed to fetch BOE listing page %s: %s", url, exc)
            continue

        pages.append(response.text)

    return pages


def parse_listing_page(html: str) -> list[ParsedBoeItem]:
    """Parse one BOE listing page into intermediate items."""
    soup = BeautifulSoup(html, "html.parser")
    items: list[ParsedBoeItem] = []

    for container in find_listing_candidate_containers(soup):
        item = parse_listing_container(container)
        if item is None:
            continue
        if should_exclude_vehicle(item):
            logger.info("Skipping BOE vehicle item %s.", item.external_id)
            continue
        items.append(item)

    logger.info("Parsed %s BOE listing items from page.", len(items))
    return items


def parse_detail_page(html: str) -> ParsedBoeDetail | None:
    """Parse validated detail fields from a BOE detail page."""
    soup = BeautifulSoup(html, "html.parser")
    table = soup.select_one(DETAIL_TABLE_SELECTOR)
    if not isinstance(table, Tag):
        logger.info("No validated BOE detail table found in page.")
        return None

    row_map = _extract_detail_row_map(table)
    title = _read_heading_text(soup.find("h2"))
    external_id = _extract_identifier(title) or row_map.get("identificador")

    if title is None and external_id is None:
        logger.debug("Skipping BOE detail page due to missing identifier.")
        return None

    value_subasta = parse_amount_text(row_map.get("valor_subasta"))
    tasacion = parse_amount_text(row_map.get("tasacion"))
    puja_minima = parse_amount_text(row_map.get("puja_minima"))

    return ParsedBoeDetail(
        external_id=external_id,
        title=title,
        opening_date=parse_detail_date(row_map.get("fecha_de_inicio")),
        closing_date=parse_detail_date(row_map.get("fecha_de_conclusion")),
        # Usar el valor de subasta como referencia práctica cuando la tasación no aporta
        # un importe fiable o viene a cero en el HTML observado.
        appraisal_value=value_subasta if value_subasta is not None else tasacion,
        # Si la puja mínima no es numérica, usar el valor de subasta como fallback
        # solo cuando el HTML expone un importe general y no un caso por lote.
        starting_bid=puja_minima if puja_minima is not None else value_subasta,
        current_bid=None,
        deposit=parse_amount_text(row_map.get("importe_del_deposito")),
        official_url=None,
        # No persistir texto libre del detalle general hasta validar mejor otras pestañas.
        description=None,
        occupancy_status=None,
        encumbrances_summary=None,
    )


def parse_detail_lots_page(html: str) -> list[ParsedBoeLot]:
    """Parse lot-level data from the BOE lot tab when it is available."""
    soup = BeautifulSoup(html, "html.parser")
    parent_title = _read_heading_text(soup.find("h2"))
    parent_external_id = _extract_identifier(parent_title)
    lot_blocks = [
        block
        for block in soup.select("#idBloqueDatos3 div[id^='idBloqueLote']")
        if isinstance(block, Tag)
    ]

    parsed_lots: list[ParsedBoeLot] = []
    for block in lot_blocks:
        parsed_lot = _parse_lot_block(block, parent_external_id=parent_external_id)
        if parsed_lot is not None:
            parsed_lots.append(parsed_lot)

    return parsed_lots


def parse_detail_lot_general_page(html: str, *, lot_number: int) -> ParsedBoeLot | None:
    """Parse lot-level auction amounts from the BOE general-info tab."""
    soup = BeautifulSoup(html, "html.parser")
    tables = [table for table in soup.select("table") if isinstance(table, Tag)]
    if not tables:
        return None

    row_map: dict[str, str] = {}
    for table in tables:
        row_map.update(_extract_detail_row_map(table))
    title = _read_heading_text(soup.find("h2"))
    parent_external_id = _extract_identifier(title) or row_map.get("identificador")
    value_subasta = parse_amount_text(row_map.get("valor_subasta"))
    puja_minima = parse_amount_text(row_map.get("puja_minima"))
    tasacion = parse_amount_text(row_map.get("tasacion"))

    return ParsedBoeLot(
        parent_external_id=parent_external_id,
        lot_number=lot_number,
        title=f"Lote {lot_number}",
        province=row_map.get("provincia"),
        municipality=row_map.get("localidad"),
        postal_code=row_map.get("codigo_postal"),
        appraisal_value=tasacion,
        starting_bid=puja_minima if puja_minima is not None else value_subasta,
        deposit=parse_amount_text(row_map.get("importe_del_deposito")),
    )


def parse_detail_lot_numbers_page(html: str) -> list[int]:
    """Parse the available lot numbers from the BOE lot tab navigation."""
    soup = BeautifulSoup(html, "html.parser")
    lot_numbers: set[int] = set()

    for link in soup.select("#tabsver a[id^='idTabLote']"):
        if not isinstance(link, Tag):
            continue
        lot_number = _extract_lot_number(link.get("id"))
        if lot_number is not None:
            lot_numbers.add(lot_number)

    for block in soup.select("#idBloqueDatos3 div[id^='idBloqueLote']"):
        if not isinstance(block, Tag):
            continue
        lot_number = _extract_lot_number(block.get("id"))
        if lot_number is not None:
            lot_numbers.add(lot_number)

    return sorted(lot_numbers)


def build_lot_detail_url(official_url: str, lot_number: int | None = None) -> str:
    """Build the BOE lot-tab URL for a given auction detail page."""
    return build_detail_view_url(official_url, view=3, lot_number=lot_number)


def build_detail_view_url(
    official_url: str,
    *,
    view: int | str,
    lot_number: int | None = None,
) -> str:
    """Build a BOE detail URL for a specific tab and optional lot."""
    split_url = urlsplit(official_url)
    query_pairs = dict(parse_qsl(split_url.query, keep_blank_values=True))
    query_pairs["ver"] = str(view)
    if lot_number is None:
        query_pairs.pop("idLote", None)
    else:
        query_pairs["idLote"] = str(lot_number)
    rebuilt_query = urlencode(query_pairs, doseq=True)
    return urlunsplit((split_url.scheme, split_url.netloc, split_url.path, rebuilt_query, split_url.fragment))


def parse_detail_bids_page(html: str, *, lot_number: int | None = None) -> Decimal | None:
    """Parse the public final-bid-like amount from the BOE bids view."""
    soup = BeautifulSoup(html, "html.parser")

    if lot_number is not None:
        lot_bid_map = parse_detail_bids_table_page(html)
        if lot_number in lot_bid_map:
            return lot_bid_map[lot_number]

    for heading in soup.find_all(["h3", "h4"]):
        if not isinstance(heading, Tag):
            continue
        heading_text = heading.get_text(" ", strip=True)
        normalized_heading = slugify_asset_label(heading_text)
        if normalized_heading != "puja_maxima_de_la_subasta":
            continue

        search_nodes: list[Tag] = [heading]
        sibling = heading.next_sibling
        collected = 0
        while sibling is not None and collected < 4:
            if isinstance(sibling, Tag):
                search_nodes.append(sibling)
                collected += 1
            sibling = sibling.next_sibling

        for node in search_nodes:
            amount = parse_amount_text(node.get_text(" ", strip=True))
            if amount is not None:
                return amount

    full_text = soup.get_text(" ", strip=True)
    normalized_text = slugify_asset_label(full_text)
    if "puja_maxima_de_la_subasta" not in normalized_text:
        return None

    match = re.search(
        r"Puja m[áa]xima de la subasta.*?(\d{1,3}(?:\.\d{3})*,\d{2}\s*€)",
        full_text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if match is None:
        return None
    return parse_amount_text(match.group(1))


def parse_detail_bids_table_page(html: str) -> dict[int, Decimal | None]:
    """Parse the BOE `Pujas máximas` table into a lot-to-amount map."""
    soup = BeautifulSoup(html, "html.parser")
    bid_map: dict[int, Decimal | None] = {}

    for table in soup.find_all("table"):
        if not isinstance(table, Tag):
            continue

        headers = [
            slugify_asset_label(header.get_text(" ", strip=True))
            for header in table.find_all("th")
            if isinstance(header, Tag)
        ]
        if "lote" not in headers or "importe_de_la_puja" not in headers:
            continue

        for row in table.find_all("tr"):
            if not isinstance(row, Tag):
                continue
            cells = [cell.get_text(" ", strip=True) for cell in row.find_all(["th", "td"]) if isinstance(cell, Tag)]
            if len(cells) < 2:
                continue
            lot_match = re.search(r"\b(\d+)\b", cells[0])
            if lot_match is None:
                continue
            bid_map[int(lot_match.group(1))] = parse_amount_text(cells[1])

    return bid_map


def map_parsed_items_to_auctions(items: Iterable[ParsedBoeItem]) -> list[Auction]:
    """Map parsed BOE items to validated Auction objects."""
    auctions: list[Auction] = []

    for item in items:
        try:
            auctions.append(
                Auction(
                    source=BOE_SOURCE_NAME,
                    external_id=item.external_id,
                    title=item.title,
                    province=item.province,
                    municipality=item.municipality,
                    postal_code=None,
                    asset_class=item.asset_class,
                    asset_subclass=item.asset_subclass,
                    is_vehicle=False,
                    official_status=item.official_status,
                    publication_date=parse_iso_date(item.publication_date),
                    opening_date=parse_iso_date(item.opening_date),
                    closing_date=parse_iso_date(item.closing_date),
                    appraisal_value=item.appraisal_value,
                    starting_bid=item.starting_bid,
                    current_bid=item.current_bid,
                    deposit=item.deposit,
                    occupancy_status=item.occupancy_status,
                    encumbrances_summary=item.encumbrances_summary,
                    description=item.description,
                    official_url=item.official_url,
                )
            )
        except Exception as exc:  # pragma: no cover - protección defensiva
            logger.warning(
                "Failed to map BOE item %s into Auction: %s",
                item.external_id,
                exc,
            )

    logger.info("Mapped %s BOE items into Auction objects.", len(auctions))
    return auctions


def run_boe_source(
    urls: Iterable[str] = BOE_LISTING_URLS,
    session: requests.Session | None = None,
) -> list[Auction]:
    """Run the BOE source end-to-end."""
    parsed_items: list[ParsedBoeItem] = []

    for html in fetch_listing_pages(urls=urls, session=session):
        parsed_items.extend(parse_listing_page(html))

    return map_parsed_items_to_auctions(parsed_items)


def find_listing_candidate_containers(soup: BeautifulSoup) -> list[Tag]:
    """Locate listing nodes using patterns validated by current fixtures."""
    real_candidates = [
        container
        for container in soup.select(REAL_LISTING_SELECTOR)
        if isinstance(container, Tag)
    ]
    if real_candidates:
        return real_candidates

    # Mantener una ruta de compatibilidad para fixtures controlados ya existentes.
    fallback_candidates: list[Tag] = []
    for selector in ("[data-auction-id]", "[data-auction-item]"):
        fallback_candidates.extend(
            tag for tag in soup.select(selector) if isinstance(tag, Tag)
        )

    return _dedupe_tags(fallback_candidates)


def parse_listing_container(container: Tag) -> ParsedBoeItem | None:
    """Extract a parsed item from a BOE listing container."""
    if _is_real_listing_container(container):
        return _parse_real_listing_container(container)

    return _parse_fixture_listing_container(container)


def should_exclude_vehicle(item: ParsedBoeItem) -> bool:
    """Apply project-level exclusion rules."""
    # El adaptador está orientado a resultados de Valencia; si el HTML no expone
    # provincia por tarjeta, se conserva la inferida del contexto de búsqueda.
    if item.province.casefold() not in {"valencia", "valència"}:
        return True

    searchable_text = " ".join(
        value
        for value in (
            item.title,
            item.filter_text,
            item.description,
            item.occupancy_status,
        )
        if value
    )
    return _contains_vehicle_keyword(searchable_text)


def classify_asset(
    asset_hint: str | None,
    title: str,
    description: str | None,
) -> tuple[str, str]:
    """Derive a coarse asset classification without relying on personal data."""
    searchable_text = " ".join(value for value in (asset_hint, title, description) if value).casefold()

    if any(keyword in searchable_text for keyword in REAL_ESTATE_KEYWORDS):
        return "real_estate", slugify_asset_label(asset_hint or title)

    return "other_non_vehicle_asset", slugify_asset_label(asset_hint or title)


def parse_amount_text(raw_value: str | None) -> Decimal | None:
    """Parse BOE amounts from text when the amount is explicit."""
    if raw_value is None:
        return None

    cleaned = (
        raw_value.replace("EUR", "")
        .replace("€", "")
        .replace(".", "")
        .replace(" ", "")
        .replace(",", ".")
        .strip()
    )
    if not cleaned:
        return None

    if not any(character.isdigit() for character in cleaned):
        return None

    try:
        return Decimal(cleaned)
    except InvalidOperation:
        logger.debug("Failed to parse decimal value from %r", raw_value)
        return None


def parse_detail_date(raw_value: str | None) -> str | None:
    """Extract ISO date text from validated detail rows."""
    if raw_value is None:
        return None

    match = ISO_DATETIME_RE.search(raw_value)
    if match is not None:
        iso_value = match.group(1)
        return iso_value.split("T", maxsplit=1)[0]

    return normalize_date_text(raw_value)


def parse_iso_date(raw_value: str | None):
    """Convert normalized date text into a Python date."""
    if raw_value is None:
        return None

    from datetime import date

    try:
        return date.fromisoformat(raw_value)
    except ValueError:
        logger.debug("Failed to convert %r into ISO date.", raw_value)
        return None


def normalize_date_text(raw_value: str | None) -> str | None:
    """Normalize supported date formats into ISO text for later validation."""
    if raw_value is None:
        return None

    value = raw_value.strip()
    if not value:
        return None

    if len(value) >= 10 and value[4] == "-" and value[7] == "-":
        return value[:10]

    if len(value) >= 10 and value[2] == "/" and value[5] == "/":
        day, month, year = value[:10].split("/")
        return f"{year}-{month}-{day}"

    if len(value) >= 10 and value[2] == "-" and value[5] == "-":
        day, month, year = value[:10].split("-")
        return f"{year}-{month}-{day}"

    logger.debug("Unsupported BOE date format %r", raw_value)
    return None


def slugify_asset_label(value: str) -> str:
    """Normalize free text asset labels into a stable internal token."""
    normalized = unicodedata.normalize("NFKD", value)
    normalized = "".join(char for char in normalized if not unicodedata.combining(char))
    normalized = normalized.casefold().replace("/", " ").replace("-", " ")
    normalized = re.sub(r"[^a-z0-9 ]+", " ", normalized)
    chunks = [chunk for chunk in normalized.split() if chunk]
    return "_".join(chunks) or "unknown"


def _parse_real_listing_container(container: Tag) -> ParsedBoeItem | None:
    """Parse one observed real BOE listing card."""
    title = _read_heading_text(container.find("h3"))
    authority = _read_heading_text(container.find("h4"))
    paragraphs = _read_direct_paragraphs(container)
    state_line = _find_line_with_prefix(paragraphs, "Estado:")
    raw_description = _find_description_line(paragraphs)
    external_id = _extract_identifier(title)
    official_status, closing_date = _parse_status_line(state_line)

    if title is None or authority is None or official_status is None:
        logger.debug("Skipping real BOE listing container due to missing core fields.")
        return None

    province, municipality = _extract_location_from_authority(authority)
    asset_class, asset_subclass = classify_asset(
        asset_hint=raw_description,
        title=title,
        description=raw_description,
    )

    return ParsedBoeItem(
        external_id=external_id,
        title=title,
        province=province,
        municipality=municipality,
        asset_class=asset_class,
        asset_subclass=asset_subclass,
        official_status=official_status,
        official_url=_extract_official_url(container),
        # No persistir descripciones libres del listado real hasta validarlas mejor.
        description=_extract_safe_free_text(raw_description),
        appraisal_value=None,
        starting_bid=None,
        current_bid=None,
        deposit=None,
        occupancy_status=None,
        encumbrances_summary=None,
        publication_date=None,
        opening_date=None,
        closing_date=closing_date,
        filter_text=raw_description,
    )


def _parse_fixture_listing_container(container: Tag) -> ParsedBoeItem | None:
    """Parse the older controlled fixture structure kept for compatibility."""
    field_map = _extract_fixture_field_map(container)
    external_id = _extract_fixture_external_id(container, field_map)
    title = field_map.get("title")
    province = field_map.get("province")
    municipality = field_map.get("municipality")
    asset_type = field_map.get("asset_type")
    status = field_map.get("status")

    if not all((title, province, municipality, asset_type, status)):
        logger.debug("Skipping fixture BOE container due to missing required fields.")
        return None

    description = field_map.get("description")
    asset_class, asset_subclass = classify_asset(asset_type, title, description)

    return ParsedBoeItem(
        external_id=external_id,
        title=title,
        province=province,
        municipality=municipality,
        asset_class=asset_class,
        asset_subclass=asset_subclass,
        official_status=status,
        official_url=_extract_official_url(container),
        description=description,
        appraisal_value=parse_amount_text(field_map.get("appraisal_value")),
        starting_bid=parse_amount_text(field_map.get("starting_bid")),
        current_bid=parse_amount_text(field_map.get("current_bid")),
        deposit=parse_amount_text(field_map.get("deposit")),
        occupancy_status=field_map.get("occupancy_status"),
        encumbrances_summary=field_map.get("encumbrances_summary"),
        publication_date=normalize_date_text(field_map.get("publication_date")),
        opening_date=normalize_date_text(field_map.get("opening_date")),
        closing_date=normalize_date_text(field_map.get("closing_date")),
        filter_text=description,
    )


def _is_real_listing_container(container: Tag) -> bool:
    """Check whether a tag matches the observed real listing card pattern."""
    classes = container.get("class", [])
    return isinstance(classes, list) and "resultado-busqueda" in classes


def _extract_detail_row_map(table: Tag) -> dict[str, str]:
    """Extract the label/value pairs from the validated detail table."""
    row_map: dict[str, str] = {}

    for row in table.find_all("tr"):
        if not isinstance(row, Tag):
            continue
        header = row.find("th")
        value = row.find("td")
        if not isinstance(header, Tag) or not isinstance(value, Tag):
            continue

        key = slugify_asset_label(header.get_text(" ", strip=True))
        text = value.get_text(" ", strip=True)
        if text:
            row_map[key] = text

    return row_map


def _parse_lot_block(block: Tag, *, parent_external_id: str | None) -> ParsedBoeLot | None:
    """Parse one lot block from the BOE lot tab."""
    lot_number = _extract_lot_number(block.get("id"))
    if lot_number is None:
        return None

    tables = [table for table in block.find_all("table") if isinstance(table, Tag)]
    if not tables:
        return None

    auction_row_map: dict[str, str] = {}
    asset_row_map: dict[str, str] = {}
    auction_keys = {"valor_subasta", "puja_minima", "importe_del_deposito", "valor_de_tasacion"}

    for table in tables:
        row_map = _extract_detail_row_map(table)
        if not row_map:
            continue
        if any(key in row_map for key in auction_keys):
            auction_row_map.update(row_map)
        else:
            asset_row_map.update(row_map)

    description = None
    description_box = block.select_one("div.caja")
    if isinstance(description_box, Tag):
        description = description_box.get_text(" ", strip=True) or None

    asset_heading = _read_heading_text(block.find("h4"))
    asset_class, asset_subclass = classify_asset(
        asset_hint=asset_heading,
        title=asset_heading or f"Lote {lot_number}",
        description=description,
    )

    value_subasta = parse_amount_text(auction_row_map.get("valor_subasta"))
    puja_minima = parse_amount_text(auction_row_map.get("puja_minima"))

    return ParsedBoeLot(
        parent_external_id=parent_external_id,
        lot_number=lot_number,
        title=asset_heading or f"Lote {lot_number}",
        description=description or asset_row_map.get("descripcion"),
        asset_class=asset_class,
        asset_subclass=asset_subclass,
        province=asset_row_map.get("provincia"),
        municipality=asset_row_map.get("localidad"),
        postal_code=asset_row_map.get("codigo_postal"),
        appraisal_value=parse_amount_text(auction_row_map.get("valor_de_tasacion")),
        starting_bid=puja_minima if puja_minima is not None else value_subasta,
        deposit=parse_amount_text(auction_row_map.get("importe_del_deposito")),
        occupancy_status=asset_row_map.get("situacion_posesoria"),
        encumbrances_summary=asset_row_map.get("informacion_adicional"),
        official_url=None,
    )


def _extract_lot_number(raw_id: str | None) -> int | None:
    """Extract the lot number from the BOE lot block identifier."""
    if raw_id is None:
        return None

    match = re.search(r"id(?:BloqueLote|TabLote)(\d+)", raw_id)
    if match is None:
        return None
    return int(match.group(1))


def _extract_fixture_field_map(container: Tag) -> dict[str, str]:
    """Read the semantic test fixture fields from data-field markers."""
    field_map: dict[str, str] = {}

    for field_name in (
        "title",
        "province",
        "municipality",
        "asset_type",
        "status",
        "description",
        "appraisal_value",
        "starting_bid",
        "current_bid",
        "deposit",
        "occupancy_status",
        "encumbrances_summary",
        "publication_date",
        "opening_date",
        "closing_date",
        "external_id",
    ):
        value = _read_fixture_field(container, field_name)
        if value is not None:
            field_map[field_name] = value

    return field_map


def _extract_fixture_external_id(container: Tag, field_map: dict[str, str]) -> str | None:
    """Read the fixture identifier from stable semantic attributes."""
    for attribute in ("data-auction-id", "data-id", "id"):
        value = container.get(attribute)
        if isinstance(value, str) and value.strip():
            return value.strip()

    return field_map.get("external_id")


def _extract_official_url(container: Tag) -> str | None:
    """Extract the official detail URL when it can be read reliably."""
    detail_link = container.find("a", href=lambda href: isinstance(href, str) and DETAIL_LINK_PATTERN in href)
    if not isinstance(detail_link, Tag):
        detail_link = container.find("a", href=True)
    if not isinstance(detail_link, Tag):
        return None

    href = detail_link.get("href")
    if not isinstance(href, str) or not href.strip():
        return None

    return urljoin(BOE_BASE_URL, href.strip())


def _extract_identifier(text: str | None) -> str | None:
    """Extract the BOE subasta identifier from a heading or reference text."""
    if text is None:
        return None

    match = IDENTIFIER_RE.search(text)
    if match is None:
        return None

    return match.group(0)


def _extract_location_from_authority(authority_text: str) -> tuple[str, str]:
    """Infer province and municipality from the authority line."""
    cleaned = authority_text.strip()
    if "(" in cleaned:
        cleaned = cleaned.split("(", maxsplit=1)[0].strip()

    if " - " in cleaned:
        municipality = cleaned.rsplit(" - ", maxsplit=1)[-1].strip()
    else:
        municipality = cleaned

    if not municipality:
        municipality = TARGET_PROVINCE

    # Cuando la tarjeta no renderiza provincia explícita, usar el ámbito de búsqueda
    # del adaptador BOE, que está centrado en Valencia/València.
    return TARGET_PROVINCE, municipality.title()


def _parse_status_line(status_line: str | None) -> tuple[str | None, str | None]:
    """Extract status and closing date from the observed BOE listing state line."""
    if status_line is None:
        return None, None

    status_text = status_line.removeprefix("Estado:").strip()
    closing_date: str | None = None

    if "[" in status_text:
        status_text, bracket_text = status_text.split("[", maxsplit=1)
        bracket_text = bracket_text.rstrip("]").strip()
        closing_date = normalize_date_text(_extract_date_fragment(bracket_text))

    normalized_status = status_text.rstrip("- ").strip()
    return normalized_status or None, closing_date


def _extract_date_fragment(text: str) -> str | None:
    """Extract the first day/month/year fragment found in a status sentence."""
    date_match = re.search(r"\d{2}/\d{2}/\d{4}", text)
    if date_match is None:
        return None

    return date_match.group(0)


def _find_description_line(paragraphs: list[str]) -> str | None:
    """Return the first non-meta paragraph from a listing card."""
    for line in paragraphs:
        if any(line.startswith(prefix) for prefix in LISTING_META_PREFIXES):
            continue
        return line

    return None


def _find_line_with_prefix(paragraphs: list[str], prefix: str) -> str | None:
    """Find the first paragraph with the given prefix."""
    for line in paragraphs:
        if line.startswith(prefix):
            return line

    return None


def _read_direct_paragraphs(container: Tag) -> list[str]:
    """Read direct paragraph children from a real listing card."""
    lines: list[str] = []

    for paragraph in container.find_all("p", recursive=False):
        if not isinstance(paragraph, Tag):
            continue
        text = paragraph.get_text(" ", strip=True)
        if text:
            lines.append(text)

    return lines


def _read_heading_text(tag: Tag | None) -> str | None:
    """Read plain text from a heading tag."""
    if not isinstance(tag, Tag):
        return None

    text = tag.get_text(" ", strip=True)
    return text or None


def _read_fixture_field(container: Tag, field_name: str) -> str | None:
    """Read one field using semantic fixture markers."""
    tag = container.find(attrs={"data-field": field_name})
    if not isinstance(tag, Tag):
        return None

    text = tag.get_text(" ", strip=True)
    return text or None


def _extract_safe_free_text(raw_text: str | None) -> str | None:
    """Keep free text only when it looks safely generic."""
    if raw_text is None:
        return None

    cleaned = " ".join(raw_text.split())
    if not cleaned:
        return None

    folded = cleaned.casefold()
    if any(hint in folded for hint in SENSITIVE_FREE_TEXT_HINTS):
        return None

    # El listado real observado mezcla direcciones y posibles referencias personales.
    # Conservar solo textos muy genéricos y cortos; si no, descartarlos.
    if ":" in cleaned or "," in cleaned or len(cleaned) > 120:
        return None

    return cleaned


def _dedupe_tags(tags: Iterable[Tag]) -> list[Tag]:
    """Remove duplicate tag objects while preserving order."""
    unique_tags: list[Tag] = []
    seen_ids: set[int] = set()

    for tag in tags:
        identity = id(tag)
        if identity in seen_ids:
            continue
        seen_ids.add(identity)
        unique_tags.append(tag)

    return unique_tags


def _contains_vehicle_keyword(text: str) -> bool:
    """Check vehicle keywords using token boundaries instead of raw substrings."""
    normalized = slugify_asset_label(text)
    tokens = set(normalized.split("_"))
    vehicle_tokens = {slugify_asset_label(keyword) for keyword in VEHICLE_KEYWORDS}
    return any(token in vehicle_tokens for token in tokens)
