"""Tests for business derivations."""

from decimal import Decimal

from monitor.domain.enrich import build_record_derivations, infer_postal_code
from monitor.domain.models import AuctionRecord


def test_build_record_derivations_computes_ratios_and_flags() -> None:
    """Debe calcular ratios y derivadas explícitas sin decidir todavía el score."""
    record = AuctionRecord(
        auction_id="SUB-1",
        source_url=None,
        title="Vivienda en Valencia (2 lotes)",
        description="Vivienda residencial con referencia 46001 y subasta con varios lotes.",
        asset_type="real_estate",
        asset_subtype="residential_property",
        province="Valencia",
        municipality="Valencia",
        postal_code="46001",
        address_text=None,
        appraisal_value=Decimal("200000"),
        opening_bid=Decimal("80000"),
        deposit=Decimal("4000"),
        auction_date=None,
        has_lots=None,
        lot_count=None,
        charges_text="Cargas pendientes de revisar",
        occupancy_text=None,
        is_detail_complete=True,
        parser_warnings=[],
    )

    derivations = build_record_derivations(record)

    assert derivations.opening_bid_ratio == Decimal("0.4")
    assert derivations.deposit_ratio == Decimal("0.05")
    assert derivations.has_invalid_appraisal is False
    assert derivations.has_reference_price_data is True
    assert derivations.is_property is True
    assert derivations.is_residential_like is True
    assert derivations.is_in_target_area is True
    assert derivations.has_unknown_charges is True
    assert derivations.has_complex_lot_structure is True
    assert derivations.has_critical_missing_data is False
    assert derivations.description_is_poor is False


def test_infer_postal_code_reads_safe_public_text() -> None:
    """Debe extraer un código postal si aparece en texto público ya saneado."""
    record = AuctionRecord(
        auction_id="SUB-2",
        source_url=None,
        title="Local comercial",
        description="Activo sito en zona urbana 46730 con acceso directo",
        asset_type="real_estate",
        asset_subtype="commercial_property",
        province="Valencia",
        municipality="Gandia",
        postal_code=None,
        address_text=None,
        appraisal_value=None,
        opening_bid=None,
        deposit=None,
        auction_date=None,
        has_lots=None,
        lot_count=None,
        charges_text=None,
        occupancy_text=None,
        is_detail_complete=None,
        parser_warnings=[],
    )

    assert infer_postal_code(record) == "46730"


def test_infer_postal_code_prefers_explicit_valencia_range() -> None:
    """Debe priorizar un 46XXX cuando el foco es Valencia y hay varias señales."""
    record = AuctionRecord(
        auction_id="SUB-CP-VAL",
        source_url=None,
        title="Lote urbano",
        description=(
            "Finca de numero 37010/5, inscripcion 1a. "
            "Codigo Postal 46680. Localidad Algemesi."
        ),
        asset_type="real_estate",
        asset_subtype="land",
        province="Valencia",
        municipality="Algemesi",
        postal_code=None,
        address_text=None,
        appraisal_value=None,
        opening_bid=None,
        deposit=None,
        auction_date=None,
        has_lots=None,
        lot_count=None,
        charges_text=None,
        occupancy_text=None,
        is_detail_complete=None,
        parser_warnings=[],
    )

    assert infer_postal_code(record) == "46680"


def test_infer_postal_code_rejects_uncontextualized_registry_numbers() -> None:
    """No debe confundir numeros de finca con un codigo postal."""
    record = AuctionRecord(
        auction_id="SUB-CP-WRONG",
        source_url=None,
        title="Lote registral",
        description="Finca de numero 37010/5, inscripcion 1a, del Registro de la Propiedad.",
        asset_type="real_estate",
        asset_subtype="land",
        province="Valencia",
        municipality="Algemesi",
        postal_code=None,
        address_text=None,
        appraisal_value=None,
        opening_bid=None,
        deposit=None,
        auction_date=None,
        has_lots=None,
        lot_count=None,
        charges_text=None,
        occupancy_text=None,
        is_detail_complete=None,
        parser_warnings=[],
    )

    assert infer_postal_code(record) is None


def test_infer_postal_code_returns_none_without_clear_signal() -> None:
    """Debe devolver None si no hay un CP con contexto razonable."""
    record = AuctionRecord(
        auction_id="SUB-CP-NONE",
        source_url=None,
        title="Solar en Daimus",
        description="SOLAR. CL NOU ACCES A LA MAR S/N DAIMUS VALENCIA",
        asset_type="real_estate",
        asset_subtype="land",
        province="Valencia",
        municipality="Daimus",
        postal_code=None,
        address_text=None,
        appraisal_value=None,
        opening_bid=None,
        deposit=None,
        auction_date=None,
        has_lots=None,
        lot_count=None,
        charges_text=None,
        occupancy_text=None,
        is_detail_complete=None,
        parser_warnings=[],
    )

    assert infer_postal_code(record) is None


def test_build_record_derivations_marks_zero_appraisal_as_invalid_but_not_missing() -> None:
    """Debe distinguir una tasacion a cero de una ausencia real de datos."""
    record = AuctionRecord(
        auction_id="SUB-0",
        source_url=None,
        title="Activo con tasacion cero",
        description="Activo urbano con datos de localizacion suficientes.",
        asset_type="real_estate",
        asset_subtype="commercial_property",
        province="Valencia",
        municipality="Valencia",
        postal_code=None,
        address_text=None,
        appraisal_value=Decimal("0.00"),
        opening_bid=None,
        deposit=Decimal("1000.00"),
        auction_date=None,
        has_lots=False,
        lot_count=1,
        charges_text="Sin cargas conocidas",
        occupancy_text=None,
        is_detail_complete=False,
        parser_warnings=[],
    )

    derivations = build_record_derivations(record)

    assert derivations.has_invalid_appraisal is True
    assert derivations.has_reference_price_data is True
    assert derivations.has_critical_missing_data is False


def test_build_record_derivations_treats_lot_cases_as_special_not_missing() -> None:
    """Debe tratar la estructura por lotes como caso especial y no como ausencia pura."""
    record = AuctionRecord(
        auction_id="SUB-LOTES",
        source_url=None,
        title="Subasta con varios lotes",
        description="Ver valor de subasta en cada lote",
        asset_type="real_estate",
        asset_subtype="residential_property",
        province="Valencia",
        municipality="Gandia",
        postal_code=None,
        address_text=None,
        appraisal_value=None,
        opening_bid=None,
        deposit=None,
        auction_date=None,
        has_lots=None,
        lot_count=None,
        charges_text=None,
        occupancy_text=None,
        is_detail_complete=False,
        parser_warnings=[],
    )

    derivations = build_record_derivations(record)

    assert derivations.has_complex_lot_structure is True
    assert derivations.has_reference_price_data is False
    assert derivations.has_critical_missing_data is False
