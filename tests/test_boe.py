"""Tests for the BOE source adapter."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from monitor.sources.boe import (
    map_parsed_items_to_auctions,
    parse_detail_bids_page,
    parse_detail_bids_table_page,
    parse_detail_lot_general_page,
    parse_detail_lot_numbers_page,
    parse_detail_lots_page,
    parse_detail_page,
    parse_listing_page,
)

from tests.boe_case_loader import load_boe_case

REAL_STRUCTURE_VEHICLE_HTML = """
<html>
  <body>
    <div class="listadoResult">
      <ul>
        <li class="resultado-busqueda">
          <h3>SUBASTA SUB-VE-2026-000001</h3>
          <h4>U.R. SUBASTAS VALENCIA - VALENCIA (AEAT)</h4>
          <p>Estado: Celebrándose - [Conclusión prevista: 20/04/2026 a las 18:00:00]</p>
          <p>VEHÍCULO TURISMO MARCA EJEMPLO</p>
          <a href="./detalleSubasta.php?idSub=SUB-VE-2026-000001">Más...</a>
        </li>
      </ul>
    </div>
  </body>
</html>
"""

DETAIL_WITH_NUMERIC_VALUE_AND_NO_MINIMUM_BID_HTML = """
<html>
  <body>
    <h2>Subasta SUB-TEST-DETAIL-001</h2>
    <div id="idBloqueDatos1">
      <table>
        <tr><th>Identificador</th><td>SUB-TEST-DETAIL-001</td></tr>
        <tr><th>Fecha de inicio</th><td>07/04/2026</td></tr>
        <tr><th>Fecha de conclusión</th><td>27/04/2026</td></tr>
        <tr><th>Valor subasta</th><td>11.996,15 €</td></tr>
        <tr><th>Tasación</th><td>65.713,58 €</td></tr>
        <tr><th>Puja mínima</th><td>Sin puja mínima</td></tr>
        <tr><th>Importe del depósito</th><td>599,80 €</td></tr>
      </table>
    </div>
  </body>
</html>
"""

DETAIL_MULTI_LOT_TAB_HTML = """
<html>
  <body>
    <h2>Subasta SUB-JA-2026-256559</h2>
    <div id="idBloqueDatos3">
      <div class="bloque" id="idBloqueLote1">
        <div><div class="caja">NUMERO CINCO. Planta baja destinada a local comercial.</div></div>
        <div>
          <h3>Datos relacionados con la subasta del lote 1</h3>
          <table>
            <tr><th>Valor Subasta</th><td>128.211,40 €</td></tr>
            <tr><th>Valor de tasación</th><td>0,00 €</td></tr>
            <tr><th>Importe del depósito</th><td>6.410,57 €</td></tr>
            <tr><th>Puja mínima</th><td>Sin puja mínima</td></tr>
          </table>
        </div>
        <div>
          <h4>Bien 1 - Inmueble (Local comercial)</h4>
          <table>
            <tr><th>Descripción</th><td>NUMERO CINCO. Planta baja destinada a local comercial.</td></tr>
            <tr><th>Código Postal</th><td>46430</td></tr>
            <tr><th>Localidad</th><td>SOLLANA</td></tr>
            <tr><th>Provincia</th><td>Valencia/València</td></tr>
            <tr><th>Situación posesoria</th><td>No consta</td></tr>
            <tr><th>Información adicional</th><td>Cargas anteriores subsistentes.</td></tr>
          </table>
        </div>
      </div>
      <div class="bloque" id="idBloqueLote2">
        <div><div class="caja">URBANA: NUMERO TRES. Vivienda del tipo B.</div></div>
        <div>
          <h3>Datos relacionados con la subasta del lote 2</h3>
          <table>
            <tr><th>Valor Subasta</th><td>116.757,60 €</td></tr>
            <tr><th>Valor de tasación</th><td>0,00 €</td></tr>
            <tr><th>Importe del depósito</th><td>5.837,88 €</td></tr>
            <tr><th>Puja mínima</th><td>Sin puja mínima</td></tr>
          </table>
        </div>
        <div>
          <h4>Bien 1 - Inmueble (Vivienda)</h4>
          <table>
            <tr><th>Descripción</th><td>URBANA: NUMERO TRES. Vivienda del tipo B.</td></tr>
            <tr><th>Código Postal</th><td>46430</td></tr>
            <tr><th>Localidad</th><td>SOLLANA</td></tr>
            <tr><th>Provincia</th><td>Valencia/València</td></tr>
            <tr><th>Situación posesoria</th><td>No consta</td></tr>
          </table>
        </div>
      </div>
    </div>
  </body>
</html>
"""

DETAIL_MULTI_LOT_TABS_ONLY_HTML = """
<html>
  <body>
    <div id="tabsver">
      <a id="idTabLote1" href="?ver=3&idLote=1">Lote 1</a>
      <a id="idTabLote2" href="?ver=3&idLote=2">Lote 2</a>
      <a id="idTabLote10" href="?ver=3&idLote=10">Lote 10</a>
    </div>
    <div id="idBloqueDatos3">
      <div class="bloque" id="idBloqueLote1"></div>
    </div>
  </body>
</html>
"""

DETAIL_LOT_GENERAL_PAGE_HTML = """
<html>
  <body>
    <h2>Subasta SUB-LOT-GENERAL-001</h2>
    <div id="idBloqueDatos1">
      <table>
        <tr><th>Identificador</th><td>SUB-LOT-GENERAL-001</td></tr>
        <tr><th>Valor subasta</th><td>128.211,40 €</td></tr>
        <tr><th>Tasación</th><td>160.000,00 €</td></tr>
        <tr><th>Puja mínima</th><td>Sin puja mínima</td></tr>
        <tr><th>Importe del depósito</th><td>6.410,57 €</td></tr>
      </table>
    </div>
  </body>
</html>
"""

DETAIL_LOT_GENERAL_PAGE_WITH_TEXT_VALUES_HTML = """
<html>
  <body>
    <h2>Subasta SUB-LOT-GENERAL-002</h2>
    <div id="idBloqueDatos1">
      <table>
        <tr><th>Identificador</th><td>SUB-LOT-GENERAL-002</td></tr>
        <tr><th>Valor subasta</th><td>Ver valor de subasta en cada lote</td></tr>
        <tr><th>Tasación</th><td>Sin datos</td></tr>
        <tr><th>Importe del depósito</th><td>Ver importe del depósito en cada lote</td></tr>
      </table>
    </div>
  </body>
</html>
"""

DETAIL_BIDS_PAGE_HTML = """
<html>
  <body>
    <div id="idBloqueDatos5">
      <h4>Puja máxima de la subasta</h4>
      <div class="caja">
        105.000,00 €
      </div>
    </div>
  </body>
</html>
"""

DETAIL_BIDS_PAGE_WITHOUT_NUMERIC_AMOUNT_HTML = """
<html>
  <body>
    <div id="idBloqueDatos5">
      <h4>Puja máxima de la subasta</h4>
      <div class="caja">
        No consta puja pública
      </div>
    </div>
  </body>
</html>
"""

DETAIL_BIDS_TABLE_PAGE_HTML = """
<html>
  <body>
    <div id="idBloqueDatos5">
      <h4>Pujas máximas</h4>
      <table>
        <tr>
          <th>Lote</th>
          <th>Importe de la puja</th>
        </tr>
        <tr>
          <td>Lote 1</td>
          <td>71.159,88 €</td>
        </tr>
        <tr>
          <td>Lote 2</td>
          <td>Sin puja</td>
        </tr>
        <tr>
          <td>Lote 7</td>
          <td>Sin puja</td>
        </tr>
      </table>
    </div>
  </body>
</html>
"""


def _coerce_actual_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    return value


def _assert_expected_listing_items(items: list[Any], expected_items: list[dict[str, Any]]) -> None:
    for expected_item in expected_items:
        expected_external_id = expected_item.get("external_id")
        assert expected_external_id is not None

        matches = [item for item in items if item.external_id == expected_external_id]
        assert len(matches) == 1

        actual_item = matches[0]
        for field, expected_value in expected_item.items():
            if field == "official_url_contains":
                assert actual_item.official_url is not None
                assert expected_value in actual_item.official_url
                continue
            actual_value = _coerce_actual_value(getattr(actual_item, field))
            assert actual_value == expected_value


def _assert_expected_detail(actual_detail: Any, expected_detail: dict[str, Any]) -> None:
    for field, expected_value in expected_detail.items():
        actual_value = _coerce_actual_value(getattr(actual_detail, field))
        assert actual_value == expected_value


def test_parse_listing_page_real_valencia_fixture_finds_expected_items() -> None:
    """Debe encontrar las tarjetas reales observadas en el listado saneado."""
    case_data = load_boe_case("listing_valencia_real_01")

    items = parse_listing_page(case_data["html"])
    expected = case_data["expected"]

    assert len(items) == expected["item_count"]
    _assert_expected_listing_items(items, expected["items"])


def test_parse_listing_page_real_non_vehicle_fixture_keeps_non_vehicle_assets() -> None:
    """Debe clasificar y conservar activos no vehículo observados en fixtures reales."""
    case_data = load_boe_case("listing_non_vehicle_real_01")

    items = parse_listing_page(case_data["html"])
    auctions = map_parsed_items_to_auctions(items)
    expected = case_data["expected"]

    assert len(items) == expected["item_count"]
    _assert_expected_listing_items(items, expected["items"])
    assert len(auctions) == 2
    assert all(auction.is_vehicle is False for auction in auctions)


def test_parse_listing_page_keeps_partial_items_without_breaking_real_listing() -> None:
    """Debe seguir parseando el listado real aunque una tarjeta venga incompleta."""
    case_data = load_boe_case("listing_missing_fields_partial_01")

    items = parse_listing_page(case_data["html"])
    expected = case_data["expected"]

    assert len(items) == expected["item_count"]
    _assert_expected_listing_items(items, expected["items"])


def test_parse_listing_page_classifies_mixed_non_vehicle_asset_types() -> None:
    """Debe clasificar distintos tipos de activos no vehículo en un listado real heterogéneo."""
    case_data = load_boe_case("listing_non_vehicle_asset_types_01")

    items = parse_listing_page(case_data["html"])
    expected = case_data["expected"]

    _assert_expected_listing_items(items, expected["items"])


def test_parse_listing_page_keeps_status_metadata_across_variant_card_shapes() -> None:
    """Debe extraer estado y fecha final pese a variaciones reales en el bloque meta."""
    case_data = load_boe_case("listing_state_shape_variants_01")

    items = parse_listing_page(case_data["html"])
    expected = case_data["expected"]

    _assert_expected_listing_items(items, expected["items"])


def test_parse_listing_page_excludes_vehicle_variants_in_real_card_structure() -> None:
    """Debe excluir vehículos aunque aparezcan con variantes textuales en tarjetas reales."""
    items = parse_listing_page(REAL_STRUCTURE_VEHICLE_HTML)

    assert items == []


def test_parse_detail_page_extracts_only_validated_non_personal_fields() -> None:
    """Debe leer solo los campos fiables de la tabla general del detalle."""
    case_data = load_boe_case("detail_real_01")

    detail = parse_detail_page(case_data["html"])
    expected = case_data["expected"]

    assert detail is not None
    _assert_expected_detail(detail, expected["detail"])


def test_parse_detail_page_uses_valor_subasta_when_puja_minima_is_not_numeric() -> None:
    """Debe usar valor subasta como starting_bid si puja mínima no aporta importe."""
    detail = parse_detail_page(DETAIL_WITH_NUMERIC_VALUE_AND_NO_MINIMUM_BID_HTML)

    assert detail is not None
    assert detail.appraisal_value == Decimal("11996.15")
    assert detail.starting_bid == Decimal("11996.15")
    assert detail.deposit == Decimal("599.80")


def test_parse_detail_page_returns_none_for_lot_based_non_numeric_amounts() -> None:
    """Debe devolver None cuando los importes del detalle no son fiables."""
    case_data = load_boe_case("detail_lot_amounts_missing_01")

    detail = parse_detail_page(case_data["html"])
    expected = case_data["expected"]

    assert detail is not None
    _assert_expected_detail(detail, expected["detail"])


def test_parse_detail_page_sanitized_from_raw_fixture_keeps_expected_fields() -> None:
    """Debe seguir parseando un caso saneado desde raw sin perder campos validados."""
    case_data = load_boe_case("detail_sensitive_field_redacted_01")

    detail = parse_detail_page(case_data["html"])
    expected = case_data["expected"]

    assert detail is not None
    _assert_expected_detail(detail, expected["detail"])


def test_parse_detail_page_degrades_general_amounts_when_scoped_to_lots() -> None:
    """Debe degradar importes generales cuando el detalle los define solo por lote."""
    case_data = load_boe_case("detail_lot_scoped_amounts_01")

    detail = parse_detail_page(case_data["html"])
    expected = case_data["expected"]

    assert detail is not None
    _assert_expected_detail(detail, expected["detail"])


def test_parse_detail_lots_page_extracts_multiple_evaluable_lots() -> None:
    """Debe extraer lotes evaluables cuando la pestaña de lotes expone importes propios."""
    lots = parse_detail_lots_page(DETAIL_MULTI_LOT_TAB_HTML)

    assert len(lots) == 2
    assert lots[0].lot_number == 1
    assert lots[0].starting_bid == Decimal("128211.40")
    assert lots[0].deposit == Decimal("6410.57")
    assert lots[0].municipality == "SOLLANA"
    assert lots[1].lot_number == 2
    assert lots[1].starting_bid == Decimal("116757.60")
    assert lots[1].asset_class == "real_estate"


def test_parse_detail_lots_page_reads_postal_code_from_single_asset_table() -> None:
    """Debe capturar el CP aunque el bloque de lote solo exponga una tabla de metadatos."""
    html = """
    <div id="idBloqueDatos3">
      <div id="idBloqueLote1">
        <h4>Solar urbano</h4>
        <div class="caja">SOLAR. CL NOU ACCES A LA MAR S/N DAIMUS VALENCIA</div>
        <table>
          <tr><th>Código Postal</th><td>46710</td></tr>
          <tr><th>Localidad</th><td>DAIMUS</td></tr>
          <tr><th>Provincia</th><td>VALENCIA</td></tr>
        </table>
      </div>
    </div>
    """

    lots = parse_detail_lots_page(html)

    assert len(lots) == 1
    assert lots[0].postal_code == "46710"
    assert lots[0].municipality == "DAIMUS"
    assert lots[0].province == "VALENCIA"


def test_parse_detail_lots_page_reads_postal_code_from_real_style_boe_entities() -> None:
    """Debe capturar el CP cuando BOE usa entidades HTML en el bloque real del bien."""
    html = """
    <html>
      <body>
        <div id="idBloqueDatos3">
          <div>
            <div class="bloque" id="idBloqueLote1">
              <div>
                <div class="caja">VIVIENDA EN BENIMAMET</div>
              </div>
              <div>
                <h3>Datos del bien subastado</h3>
                <div>
                  <h4>Bien 1 - Inmueble (Vivienda)</h4>
                  <table>
                    <tr><th>Descripci&#xF3;n</th><td>VIVIENDA EN BENIMAMET</td></tr>
                    <tr><th>Direcci&#xF3;n</th><td>CALLE CAMPAMENTO 1</td></tr>
                    <tr><th>C&#xF3;digo Postal</th><td>46035</td></tr>
                    <tr><th>Localidad</th><td>BENIMAMET</td></tr>
                    <tr><th>Provincia</th><td>Valencia/Val&#xE8;ncia</td></tr>
                  </table>
                </div>
              </div>
            </div>
          </div>
        </div>
      </body>
    </html>
    """

    lots = parse_detail_lots_page(html)

    assert len(lots) == 1
    assert lots[0].postal_code == "46035"
    assert lots[0].municipality == "BENIMAMET"
    assert lots[0].province == "Valencia/València"


def test_parse_detail_lots_page_keeps_postal_code_empty_without_explicit_boe_row() -> None:
    """No debe inventar un CP cuando el HTML del bien no lo expone explícitamente."""
    html = """
    <html>
      <body>
        <div id="idBloqueDatos3">
          <div>
            <div class="bloque" id="idBloqueLote1">
              <div>
                <div class="caja">VIVIENDA EN VALENCIA</div>
              </div>
              <div>
                <h3>Datos del bien subastado</h3>
                <div>
                  <h4>Bien 0 - Inmueble (Vivienda)</h4>
                  <table>
                    <tr><th>Descripci&#xF3;n</th><td>AVENIDA REAL DE MADRID N&#xDA;MERO 59, PISO 2, PTA 20</td></tr>
                    <tr><th>Referencia catastral</th><td><a>4681912YJ2648B0028FZ</a></td></tr>
                    <tr><th>Direcci&#xF3;n</th><td>AVENIDA REAL DE MADRID N&#xDA;MERO 59, PISO 2, PTA 20</td></tr>
                    <tr><th>Localidad</th><td>Val&#xE8;ncia</td></tr>
                    <tr><th>Provincia</th><td>Valencia/Val&#xE8;ncia</td></tr>
                  </table>
                </div>
              </div>
            </div>
          </div>
        </div>
      </body>
    </html>
    """

    lots = parse_detail_lots_page(html)

    assert len(lots) == 1
    assert lots[0].postal_code is None
    assert lots[0].municipality == "València"


def test_parse_detail_lot_numbers_page_reads_available_tabs() -> None:
    """Debe leer los numeros de lote disponibles desde la navegacion de lotes."""
    lot_numbers = parse_detail_lot_numbers_page(DETAIL_MULTI_LOT_TAB_HTML)

    assert lot_numbers == [1, 2]


def test_parse_detail_lot_numbers_page_prefers_all_tab_ids_when_only_one_block_is_visible() -> None:
    """Debe detectar todos los lotes aunque el HTML visible solo muestre un bloque."""
    lot_numbers = parse_detail_lot_numbers_page(DETAIL_MULTI_LOT_TABS_ONLY_HTML)

    assert lot_numbers == [1, 2, 10]


def test_parse_detail_lot_general_page_extracts_numeric_amounts() -> None:
    """Debe extraer importes numéricos del tab general de un lote."""
    lot = parse_detail_lot_general_page(DETAIL_LOT_GENERAL_PAGE_HTML, lot_number=2)

    assert lot is not None
    assert lot.lot_number == 2
    assert lot.starting_bid == Decimal("128211.40")
    assert lot.appraisal_value == Decimal("160000.00")
    assert lot.deposit == Decimal("6410.57")


def test_parse_detail_lot_general_page_reads_structured_postal_metadata() -> None:
    """Debe leer codigo postal, localidad y provincia cuando el tab general los expone."""
    html = """
    <div id="idBloqueDatos1">
      <table>
        <tr><th>Valor subasta</th><td>128.211,40 €</td></tr>
        <tr><th>Tasación</th><td>160.000,00 €</td></tr>
        <tr><th>Importe del depósito</th><td>6.410,57 €</td></tr>
      </table>
      <table>
        <tr><th>Código Postal</th><td>46430</td></tr>
        <tr><th>Localidad</th><td>Sollana</td></tr>
        <tr><th>Provincia</th><td>Valencia</td></tr>
      </table>
    </div>
    """

    lot = parse_detail_lot_general_page(html, lot_number=2)

    assert lot is not None
    assert lot.postal_code == "46430"
    assert lot.municipality == "Sollana"
    assert lot.province == "Valencia"


def test_parse_detail_lot_general_page_keeps_textual_lot_scoped_amounts_as_none() -> None:
    """Debe dejar vacíos los importes no numéricos del tab general de lote."""
    lot = parse_detail_lot_general_page(DETAIL_LOT_GENERAL_PAGE_WITH_TEXT_VALUES_HTML, lot_number=3)

    assert lot is not None
    assert lot.starting_bid is None
    assert lot.appraisal_value is None
    assert lot.deposit is None


def test_parse_detail_bids_page_extracts_public_maximum_bid_amount() -> None:
    """Debe extraer la puja máxima pública desde la pestaña ver=5."""
    current_bid = parse_detail_bids_page(DETAIL_BIDS_PAGE_HTML)

    assert current_bid == Decimal("105000.00")


def test_parse_detail_bids_page_returns_none_without_numeric_public_amount() -> None:
    """Debe dejar current_bid vacío si la pestaña de pujas no muestra importe útil."""
    current_bid = parse_detail_bids_page(DETAIL_BIDS_PAGE_WITHOUT_NUMERIC_AMOUNT_HTML)

    assert current_bid is None


def test_parse_detail_bids_table_page_maps_lot_to_amounts() -> None:
    """Debe mapear importes por lote desde la tabla `Pujas máximas`."""
    bid_map = parse_detail_bids_table_page(DETAIL_BIDS_TABLE_PAGE_HTML)

    assert bid_map[1] == Decimal("71159.88")
    assert bid_map[2] is None
    assert bid_map[7] is None


def test_parse_detail_bids_page_reads_amount_from_pujas_maximas_table_for_one_lot() -> None:
    """Debe extraer la puja de un lote desde la tabla general cuando existe fila propia."""
    current_bid = parse_detail_bids_page(DETAIL_BIDS_TABLE_PAGE_HTML, lot_number=1)

    assert current_bid == Decimal("71159.88")


def test_parse_detail_bids_page_returns_none_when_lot_table_has_sin_puja_or_missing_row() -> None:
    """Debe dejar vacío current_bid si el lote no tiene puja o no aparece en la tabla."""
    assert parse_detail_bids_page(DETAIL_BIDS_TABLE_PAGE_HTML, lot_number=2) is None
    assert parse_detail_bids_page(DETAIL_BIDS_TABLE_PAGE_HTML, lot_number=99) is None
