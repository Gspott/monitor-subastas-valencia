"""Tests for conservative municipality normalization from postal code."""

from monitor.location import (
    normalize_municipality_with_postal_code,
    normalize_municipality_name,
    resolve_municipality_from_postal_code,
)


def test_resolve_municipality_from_postal_code_returns_supported_valencia_mapping() -> None:
    """Debe resolver municipios solo para CPs soportados explícitamente."""
    assert resolve_municipality_from_postal_code("46710") == "Daimus"
    assert resolve_municipality_from_postal_code("46001") == "Valencia"
    assert resolve_municipality_from_postal_code("46520") == "Puerto de Sagunto"
    assert resolve_municipality_from_postal_code("46730") is None


def test_resolve_municipality_from_postal_code_returns_none_for_unknown_code() -> None:
    """Debe abstenerse si el CP no está en el mapa conservador."""
    assert resolve_municipality_from_postal_code("99999") is None


def test_normalize_municipality_with_postal_code_fills_missing_value() -> None:
    """Debe rellenar municipality cuando falta y el CP es conocido."""
    normalized = normalize_municipality_with_postal_code(
        "   ",
        postal_code="46710",
    )

    assert normalized == "Daimus"


def test_normalize_municipality_with_postal_code_replaces_conflicting_value() -> None:
    """Debe corregir municipality si contradice un CP conocido y fiable."""
    normalized = normalize_municipality_with_postal_code(
        "Valencia",
        postal_code="46710",
    )

    assert normalized == "Daimus"


def test_normalize_municipality_name_removes_accents_from_algemesi() -> None:
    """Debe preferir la forma sin acento para Algemesi."""
    assert normalize_municipality_name("Algemesí") == "Algemesi"


def test_normalize_municipality_name_removes_accents_from_daimus() -> None:
    """Debe preferir la forma sin acento para Daimus."""
    assert normalize_municipality_name("Daimús") == "Daimus"


def test_normalize_municipality_name_keeps_strings_without_accents() -> None:
    """No debe alterar nombres ya canónicos sin acento."""
    assert normalize_municipality_name("Daimus") == "Daimus"


def test_normalize_municipality_with_46520_uses_puerto_de_sagunto_as_canonical_value() -> None:
    """Debe converger 46520 al canónico Puerto de Sagunto."""
    variants = [
        "Puerto De Sagunto",
        "Pto Sagunto",
        "Pto De Sagunto",
        "Pto Sagunt",
        "Pto. De Sagunt",
        "Puerto De Sagunt",
        "Puerto Sagunto",
        "Puerto Sangunto",
        "Sagunt-Port",
        "Sagunto-Port",
    ]

    for variant in variants:
        normalized = normalize_municipality_with_postal_code(
            variant,
            postal_code="46520",
        )
        assert normalized == "Puerto de Sagunto"


def test_normalize_municipality_name_keeps_puerto_de_sagunto_casing() -> None:
    """Debe conservar el casing canónico de Puerto de Sagunto."""
    assert normalize_municipality_name("Puerto de Sagunto") == "Puerto de Sagunto"


def test_normalize_municipality_with_postal_code_keeps_cleaned_value_without_mapping() -> None:
    """Debe conservar el municipio limpiado cuando no hay mapeo seguro."""
    normalized = normalize_municipality_with_postal_code(
        "  valència ",
        postal_code="46123",
    )

    assert normalized == "Valencia"


def test_normalize_municipality_with_46730_keeps_specific_playa_variant() -> None:
    """Debe respetar y canonizar Playa de Gandia cuando 46730 ya la identifica."""
    normalized = normalize_municipality_with_postal_code(
        "  Playa de Gandía ",
        postal_code="46730",
    )

    assert normalized == "Playa de Gandia"


def test_normalize_municipality_name_keeps_playa_de_gandia_casing() -> None:
    """Debe conservar el casing canónico de Playa de Gandia."""
    assert normalize_municipality_name("Playa de Gandia") == "Playa de Gandia"


def test_normalize_municipality_with_46730_uses_description_for_playa_when_generic() -> None:
    """Debe resolver Playa de Gandia si 46730 viene genérico y la descripción es explícita."""
    normalized = normalize_municipality_with_postal_code(
        "Gandía",
        postal_code="46730",
        description="Apartamento sito en Playa de Gandia con acceso al paseo.",
    )

    assert normalized == "Playa de Gandia"


def test_normalize_municipality_with_46730_uses_description_for_grao_when_generic() -> None:
    """Debe resolver Grao de Gandia si 46730 viene genérico y la descripción es explícita."""
    normalized = normalize_municipality_with_postal_code(
        "Gandia",
        postal_code="46730",
        description="Inmueble ubicado en Grao de Gandia, zona portuaria.",
    )

    assert normalized == "Grao de Gandia"


def test_normalize_municipality_name_keeps_grao_de_gandia_casing() -> None:
    """Debe conservar el casing canónico de Grao de Gandia."""
    assert normalize_municipality_name("Grao de Gandia") == "Grao de Gandia"


def test_normalize_municipality_with_46730_keeps_generic_value_without_clear_description() -> None:
    """Debe conservar Gandia si 46730 no trae una señal explícita suficiente."""
    normalized = normalize_municipality_with_postal_code(
        "Gandia",
        postal_code="46730",
        description="Vivienda sita en avenida principal de la zona marítima.",
    )

    assert normalized == "Gandia"


def test_normalize_municipality_with_46730_keeps_generic_value_with_ambiguous_description() -> None:
    """Debe abstenerse si la descripción menciona señales contradictorias."""
    normalized = normalize_municipality_with_postal_code(
        "Gandia",
        postal_code="46730",
        description="Activo entre Playa de Gandia y Grao de Gandia.",
    )

    assert normalized == "Gandia"
