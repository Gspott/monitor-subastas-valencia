from __future__ import annotations

import html
import re
from urllib.parse import urlsplit, urlunsplit


EPHEMERAL_QUERY_PARAMS = {"token", "session", "sid", "phpsessid"}
URL_ATTRIBUTE_RE = re.compile(
    r'(?P<attr>\b(?:href|src|action)\s*=\s*)(?P<quote>["\'])(?P<value>.*?)(?P=quote)',
    re.IGNORECASE | re.DOTALL,
)
EMAIL_RE = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
IDENTIFIER_RE = re.compile(r"\b(?:\d{8}[A-Z]|[XYZ]\d{7}[A-Z])\b", re.IGNORECASE)
PHONE_RE = re.compile(r"\b(?:\+34[\s-]?)?(?:[6789]\d{2}[\s-]?\d{3}[\s-]?\d{3})\b")
LABELED_FIELD_PATTERNS = (
    re.compile(r"(?P<label>\bDNI\s*:\s*)(?P<value>[^\s<][^<\n\r]*)", re.IGNORECASE),
    re.compile(r"(?P<label>\bNIF\s*:\s*)(?P<value>[^\s<][^<\n\r]*)", re.IGNORECASE),
    re.compile(r"(?P<label>\bNIE\s*:\s*)(?P<value>[^\s<][^<\n\r]*)", re.IGNORECASE),
    re.compile(r"(?P<label>\bEmail\s*:\s*)(?P<value>[^\s<][^<\n\r]*)", re.IGNORECASE),
    re.compile(
        r"(?P<label>\bCorreo electr[oó]nico\s*:\s*)(?P<value>[^\s<][^<\n\r]*)",
        re.IGNORECASE,
    ),
    re.compile(r"(?P<label>\bTel[eé]fono\s*:\s*)(?P<value>[^\s<][^<\n\r]*)", re.IGNORECASE),
)
TECHNICAL_TIMESTAMP_PATTERNS = (
    re.compile(r"Generado el \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", re.IGNORECASE),
    re.compile(r"Fecha de impresi[oó]n:\s*\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", re.IGNORECASE),
)
REDACTION_TEXT = "[REDACTED]"


def sanitize_url_attributes(html_text: str) -> str:
    def replace_attribute(match: re.Match[str]) -> str:
        attr_prefix = match.group("attr")
        quote = match.group("quote")
        raw_value = match.group("value")
        decoded_value = html.unescape(raw_value)
        split_value = urlsplit(decoded_value)

        if not split_value.query:
            return match.group(0)

        kept_query_parts = []
        for part in split_value.query.split("&"):
            key = part.split("=", 1)[0].lower()
            if key in EPHEMERAL_QUERY_PARAMS:
                continue
            kept_query_parts.append(part)

        sanitized_query = "&".join(kept_query_parts)
        sanitized_value = urlunsplit(
            (
                split_value.scheme,
                split_value.netloc,
                split_value.path,
                sanitized_query,
                split_value.fragment,
            )
        )

        if sanitized_value == decoded_value:
            return match.group(0)

        escaped_value = html.escape(sanitized_value, quote=False)
        return f"{attr_prefix}{quote}{escaped_value}{quote}"

    return URL_ATTRIBUTE_RE.sub(replace_attribute, html_text)


def redact_labeled_personal_fields(html_text: str) -> str:
    sanitized_text = html_text
    for pattern in LABELED_FIELD_PATTERNS:
        sanitized_text = pattern.sub(rf"\g<label>{REDACTION_TEXT}", sanitized_text)
    return sanitized_text


def redact_clear_personal_identifiers(html_text: str) -> str:
    sanitized_text = EMAIL_RE.sub(REDACTION_TEXT, html_text)
    sanitized_text = IDENTIFIER_RE.sub(REDACTION_TEXT, sanitized_text)
    sanitized_text = PHONE_RE.sub(REDACTION_TEXT, sanitized_text)
    return sanitized_text


def remove_volatile_technical_timestamps(html_text: str) -> str:
    sanitized_text = html_text
    for pattern in TECHNICAL_TIMESTAMP_PATTERNS:
        sanitized_text = pattern.sub("", sanitized_text)
    return sanitized_text


def sanitize_boe_html(html_text: str) -> str:
    sanitized_text = sanitize_url_attributes(html_text)
    sanitized_text = redact_labeled_personal_fields(sanitized_text)
    sanitized_text = redact_clear_personal_identifiers(sanitized_text)
    sanitized_text = remove_volatile_technical_timestamps(sanitized_text)
    return sanitized_text
