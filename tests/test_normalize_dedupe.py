"""Tests for normalization and deduplication helpers."""

from datetime import date
from decimal import Decimal

from monitor.dedupe import dedupe_auctions, merge_auctions
from monitor.models import Auction
from monitor.normalize import normalize_auction


def test_normalize_auction_cleans_text_and_standardizes_labels() -> None:
    """Debe limpiar textos y unificar etiquetas conocidas."""
    auction = Auction(
        source=" BOE ",
        external_id=" ID-1 ",
        title="  Vivienda \n en   Valencia  ",
        province=" valència ",
        municipality="  valència ",
        asset_class="Real Estate",
        asset_subclass=" inmueble_vivienda ",
        is_vehicle=False,
        official_status=" ABIERTA \n",
        publication_date=None,
        opening_date=None,
        closing_date=None,
        appraisal_value=Decimal("150000.00"),
        starting_bid=None,
        current_bid=None,
        deposit=None,
        occupancy_status="  libre ",
        encumbrances_summary="  sin cargas  ",
        description="  texto \n limpio ",
        official_url=" https://example.test/item ",
    )

    normalized = normalize_auction(auction)

    assert normalized.source == "BOE"
    assert normalized.title == "Vivienda en Valencia"
    assert normalized.province == "Valencia"
    assert normalized.municipality == "Valencia"
    assert normalized.asset_class == "real_estate"
    assert normalized.asset_subclass == "residential_property"
    assert normalized.official_status == "abierta"


def test_normalize_auction_corrects_municipality_from_supported_postal_code() -> None:
    """Debe usar el CP como fuente de verdad cuando haya mapeo conservador."""
    auction = Auction(
        source="BOE",
        external_id="ID-POSTAL-1",
        title="Solar en la costa",
        province="Valencia",
        municipality="Valencia",
        postal_code="46710",
        asset_class="real_estate",
        asset_subclass="land",
        is_vehicle=False,
        official_status="abierta",
        publication_date=None,
        opening_date=None,
        closing_date=None,
        appraisal_value=Decimal("125000.00"),
        starting_bid=None,
        current_bid=None,
        deposit=None,
        occupancy_status=None,
        encumbrances_summary=None,
        description=None,
        official_url=None,
    )

    normalized = normalize_auction(auction)

    assert normalized.municipality == "Daimus"
    assert normalized.postal_code == "46710"


def test_normalize_auction_fills_missing_municipality_from_supported_postal_code() -> None:
    """Debe rellenar municipality si el CP conocido permite hacerlo con seguridad."""
    auction = Auction(
        source="BOE",
        external_id="ID-POSTAL-2",
        title="Vivienda",
        province="Valencia",
        municipality="   ",
        postal_code="46430",
        asset_class="real_estate",
        asset_subclass="residential_property",
        is_vehicle=False,
        official_status="abierta",
        publication_date=None,
        opening_date=None,
        closing_date=None,
        appraisal_value=Decimal("150000.00"),
        starting_bid=None,
        current_bid=None,
        deposit=None,
        occupancy_status=None,
        encumbrances_summary=None,
        description=None,
        official_url=None,
    )

    normalized = normalize_auction(auction)

    assert normalized.municipality == "Sollana"


def test_normalize_auction_keeps_municipality_when_postal_code_has_no_supported_mapping() -> None:
    """Debe respetar el municipio original si el CP no tiene mapeo seguro."""
    auction = Auction(
        source="BOE",
        external_id="ID-POSTAL-3",
        title="Vivienda",
        province="Valencia",
        municipality="València",
        postal_code="46123",
        asset_class="real_estate",
        asset_subclass="residential_property",
        is_vehicle=False,
        official_status="abierta",
        publication_date=None,
        opening_date=None,
        closing_date=None,
        appraisal_value=Decimal("150000.00"),
        starting_bid=None,
        current_bid=None,
        deposit=None,
        occupancy_status=None,
        encumbrances_summary=None,
        description=None,
        official_url=None,
    )

    normalized = normalize_auction(auction)

    assert normalized.municipality == "Valencia"


def test_normalize_auction_uses_description_to_resolve_46730_playa_variant() -> None:
    """Debe usar description para resolver 46730 sin colapsarlo automáticamente a Gandia."""
    auction = Auction(
        source="BOE",
        external_id="ID-POSTAL-46730-PLAYA",
        title="Apartamento",
        province="Valencia",
        municipality="Gandía",
        postal_code="46730",
        asset_class="real_estate",
        asset_subclass="residential_property",
        is_vehicle=False,
        official_status="abierta",
        publication_date=None,
        opening_date=None,
        closing_date=None,
        appraisal_value=Decimal("150000.00"),
        starting_bid=None,
        current_bid=None,
        deposit=None,
        occupancy_status=None,
        encumbrances_summary=None,
        description="Apartamento con vistas en Playa de Gandía.",
        official_url=None,
    )

    normalized = normalize_auction(auction)

    assert normalized.municipality == "Playa de Gandia"


def test_normalize_auction_canonicalizes_known_status_variants() -> None:
    """Debe guardar official_status en forma canónica cuando se reconoce."""
    auction = Auction(
        source="BOE",
        external_id="ID-2",
        title="Local comercial",
        province="Valencia",
        municipality="Valencia",
        asset_class="real_estate",
        asset_subclass="commercial_property",
        is_vehicle=False,
        official_status="  Celebrándose con pujas ",
        publication_date=None,
        opening_date=None,
        closing_date=None,
        appraisal_value=Decimal("99000.00"),
        starting_bid=None,
        current_bid=None,
        deposit=None,
        occupancy_status=None,
        encumbrances_summary=None,
        description=None,
        official_url=None,
    )

    normalized = normalize_auction(auction)

    assert normalized.official_status == "abierta con pujas"


def test_normalize_auction_keeps_prudent_text_for_unknown_status() -> None:
    """Debe conservar una versión normalizada cuando el estado no es canónico."""
    auction = Auction(
        source="BOE",
        external_id="ID-3",
        title="Local comercial",
        province="Valencia",
        municipality="Valencia",
        asset_class="real_estate",
        asset_subclass="commercial_property",
        is_vehicle=False,
        official_status="  Pendiente de revisión ",
        publication_date=None,
        opening_date=None,
        closing_date=None,
        appraisal_value=Decimal("99000.00"),
        starting_bid=None,
        current_bid=None,
        deposit=None,
        occupancy_status=None,
        encumbrances_summary=None,
        description=None,
        official_url=None,
    )

    normalized = normalize_auction(auction)

    assert normalized.official_status == "pendiente de revision"


def test_normalize_auction_drops_risky_personal_text_markers() -> None:
    """Debe eliminar texto libre cuando tenga indicios claros de datos personales."""
    auction = Auction(
        source="BOE",
        external_id=None,
        title="Vivienda en Valencia",
        province="Valencia",
        municipality="Valencia",
        asset_class="real_estate",
        asset_subclass="residential_property",
        is_vehicle=False,
        official_status="abierta",
        publication_date=None,
        opening_date=None,
        closing_date=None,
        appraisal_value=Decimal("150000.00"),
        starting_bid=None,
        current_bid=None,
        deposit=None,
        occupancy_status="libre",
        encumbrances_summary=None,
        description="Nombre del deudor: Juan Perez",
        official_url=None,
    )

    normalized = normalize_auction(auction)

    assert normalized.external_id is None
    assert normalized.description is None


def test_dedupe_auctions_prefers_record_with_more_information() -> None:
    """Debe fusionar duplicados conservando el registro más útil."""
    sparse = Auction(
        source="BOE",
        external_id="SUB-1",
        title="Vivienda en Valencia",
        province="Valencia",
        municipality="Valencia",
        asset_class="real_estate",
        asset_subclass="residential_property",
        is_vehicle=False,
        official_status="abierta",
        publication_date=None,
        opening_date=None,
        closing_date=None,
        appraisal_value=Decimal("150000.00"),
        starting_bid=None,
        current_bid=None,
        deposit=None,
        occupancy_status=None,
        encumbrances_summary=None,
        description=None,
        official_url=None,
    )
    rich = Auction(
        source="BOE",
        external_id="SUB-1",
        title="Vivienda en Valencia",
        province="Valencia",
        municipality="Valencia",
        asset_class="real_estate",
        asset_subclass="residential_property",
        is_vehicle=False,
        official_status="abierta",
        publication_date=None,
        opening_date=None,
        closing_date=None,
        appraisal_value=Decimal("150000.00"),
        starting_bid=Decimal("120000.00"),
        current_bid=None,
        deposit=Decimal("7500.00"),
        occupancy_status="libre",
        encumbrances_summary="sin cargas",
        description="Subasta de vivienda",
        official_url="https://example.test/item",
    )

    deduped = dedupe_auctions([sparse, rich])

    assert len(deduped) == 1
    assert deduped[0].deposit == Decimal("7500.00")
    assert deduped[0].description == "Subasta de vivienda"


def test_merge_auctions_uses_fallback_key_when_external_id_is_missing() -> None:
    """Debe deduplicar también con la clave secundaria no personal."""
    left = Auction(
        source="BOE",
        external_id="",
        title="Local comercial",
        province="Valencia",
        municipality="Gandia",
        asset_class="real_estate",
        asset_subclass="commercial_property",
        is_vehicle=False,
        official_status="abierta",
        publication_date=None,
        opening_date=None,
        closing_date=None,
        appraisal_value=Decimal("99000.00"),
        starting_bid=None,
        current_bid=None,
        deposit=None,
        occupancy_status=None,
        encumbrances_summary=None,
        description=None,
        official_url=None,
    )
    right = Auction(
        source="BOE",
        external_id="",
        title="Local comercial",
        province="Valencia",
        municipality="Gandia",
        asset_class="real_estate",
        asset_subclass="commercial_property",
        is_vehicle=False,
        official_status="abierta",
        publication_date=None,
        opening_date=None,
        closing_date=None,
        appraisal_value=Decimal("99000.00"),
        starting_bid=Decimal("80000.00"),
        current_bid=None,
        deposit=Decimal("4000.00"),
        occupancy_status=None,
        encumbrances_summary=None,
        description="Activo comercial",
        official_url=None,
    )

    deduped = dedupe_auctions([left, right])
    merged = merge_auctions(left, right)

    assert len(deduped) == 1
    assert deduped[0].starting_bid == Decimal("80000.00")
    assert merged.deposit == Decimal("4000.00")


def test_dedupe_auctions_does_not_merge_fallback_matches_with_different_official_url() -> None:
    """No debe fusionar si la URL oficial entra en conflicto."""
    left = Auction(
        source="BOE",
        external_id=None,
        title="Local comercial",
        province="Valencia",
        municipality="Gandia",
        asset_class="real_estate",
        asset_subclass="commercial_property",
        is_vehicle=False,
        official_status="abierta",
        publication_date=None,
        opening_date=None,
        closing_date=None,
        appraisal_value=Decimal("99000.00"),
        starting_bid=Decimal("80000.00"),
        current_bid=None,
        deposit=None,
        score=None,
        occupancy_status=None,
        encumbrances_summary=None,
        description=None,
        official_url="https://example.test/item-1",
    )
    right = left.model_copy(update={"official_url": "https://example.test/item-2"})

    deduped = dedupe_auctions([left, right])

    assert len(deduped) == 2


def test_dedupe_auctions_does_not_merge_fallback_matches_with_different_dates() -> None:
    """No debe fusionar si las fechas disponibles contradicen la coincidencia."""
    left = Auction(
        source="BOE",
        external_id=None,
        title="Local comercial",
        province="Valencia",
        municipality="Gandia",
        asset_class="real_estate",
        asset_subclass="commercial_property",
        is_vehicle=False,
        official_status="abierta",
        publication_date=date(2026, 4, 1),
        opening_date=None,
        closing_date=None,
        appraisal_value=Decimal("99000.00"),
        starting_bid=Decimal("80000.00"),
        current_bid=None,
        deposit=None,
        score=None,
        occupancy_status=None,
        encumbrances_summary=None,
        description=None,
        official_url=None,
    )
    right = left.model_copy(update={"publication_date": date(2026, 4, 3)})

    deduped = dedupe_auctions([left, right])

    assert len(deduped) == 2


def test_dedupe_auctions_merges_sparse_and_rich_records_when_signals_do_not_conflict() -> None:
    """Debe fusionar si la base mínima coincide y no hay conflictos claros."""
    left = Auction(
        source="BOE",
        external_id=None,
        title="Local comercial",
        province="Valencia",
        municipality="Gandia",
        asset_class="real_estate",
        asset_subclass="commercial_property",
        is_vehicle=False,
        official_status="abierta",
        publication_date=None,
        opening_date=None,
        closing_date=None,
        appraisal_value=Decimal("99000.00"),
        starting_bid=None,
        current_bid=None,
        deposit=None,
        score=None,
        occupancy_status=None,
        encumbrances_summary=None,
        description=None,
        official_url=None,
    )
    right = left.model_copy(
        update={
            "starting_bid": Decimal("80000.00"),
            "official_url": "https://example.test/item-1",
            "closing_date": date(2026, 4, 27),
        }
    )

    deduped = dedupe_auctions([left, right])

    assert len(deduped) == 1
    assert deduped[0].starting_bid == Decimal("80000.00")
    assert deduped[0].official_url == "https://example.test/item-1"
    assert deduped[0].closing_date == date(2026, 4, 27)
