"""Tests for the BOE case sanitizer."""

from scripts.boe_cases.sanitizer import sanitize_boe_html


def test_sanitize_boe_html_removes_known_ephemeral_query_params() -> None:
    """Debe eliminar parametros efimeros conocidos en atributos URL."""
    html = '<a href="./detalle.php?idSub=123&amp;token=secret&amp;sid=9&amp;view=1">Detalle</a>'

    sanitized = sanitize_boe_html(html)

    assert 'token=' not in sanitized
    assert 'sid=' not in sanitized
    assert 'idSub=123' in sanitized
    assert 'view=1' in sanitized


def test_sanitize_boe_html_keeps_non_ephemeral_query_params() -> None:
    """Debe conservar parametros que no estan en la lista cerrada."""
    html = '<a href="./detalle.php?idSub=123&amp;sort=desc">Detalle</a>'

    sanitized = sanitize_boe_html(html)

    assert sanitized == html


def test_sanitize_boe_html_redacts_clear_email() -> None:
    """Debe redactar un correo electronico inequivoco."""
    html = "<p>Contacto: persona@example.com</p>"

    sanitized = sanitize_boe_html(html)

    assert "persona@example.com" not in sanitized
    assert "[REDACTED]" in sanitized


def test_sanitize_boe_html_redacts_clear_dni_nif_nie() -> None:
    """Debe redactar identificadores personales con patron claro."""
    html = "<p>DNI general 12345678Z y NIE X1234567L</p>"

    sanitized = sanitize_boe_html(html)

    assert "12345678Z" not in sanitized
    assert "X1234567L" not in sanitized
    assert sanitized.count("[REDACTED]") == 2


def test_sanitize_boe_html_redacts_labeled_field_value_only() -> None:
    """Debe conservar la etiqueta y redactar solo el valor del campo."""
    html = "<p>Email: persona@example.com</p><p>Teléfono: 612345678</p>"

    sanitized = sanitize_boe_html(html)

    assert "Email: [REDACTED]" in sanitized
    assert "Teléfono: [REDACTED]" in sanitized
    assert "persona@example.com" not in sanitized
    assert "612345678" not in sanitized


def test_sanitize_boe_html_removes_known_technical_timestamp() -> None:
    """Debe eliminar marcas temporales tecnicas y volatiles."""
    html = "<p>Generado el 2026-04-08 10:13:22</p>"

    sanitized = sanitize_boe_html(html)

    assert "Generado el" not in sanitized
    assert "2026-04-08 10:13:22" not in sanitized


def test_sanitize_boe_html_preserves_functional_auction_dates() -> None:
    """Debe conservar fechas funcionales de la subasta."""
    html = "<td>Fecha de inicio 2026-04-07 18:00:00 CET</td>"

    sanitized = sanitize_boe_html(html)

    assert sanitized == html


def test_sanitize_boe_html_leaves_html_unchanged_when_no_rule_applies() -> None:
    """Debe dejar el HTML intacto cuando no aplica ninguna regla."""
    html = '<div class="case"><a href="./detalle.php?idSub=123">Detalle</a></div>'

    sanitized = sanitize_boe_html(html)

    assert sanitized == html
