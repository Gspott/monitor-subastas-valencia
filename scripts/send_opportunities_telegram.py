"""Send only relevant opportunity updates to Telegram."""

from __future__ import annotations

import html
import json
import os
import sys
import tempfile
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import requests


# Permitir ejecutar el script desde la raiz del repositorio sin instalar el paquete.
REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from monitor.config import DATA_DIR
from monitor.opportunities.analysis import (
    DEFAULT_MAX_COMPLETED_HISTORY,
    DEFAULT_MIN_HISTORY_SAMPLE_SIZE,
    build_display_location,
    build_active_history_context,
    build_completed_history_rows,
    build_completed_history_signals,
    is_top_opportunity_evaluation,
    select_recent_completed_history_rows,
)
from monitor.pipeline.ranking import (
    export_opportunities_to_csv,
    rank_and_filter_opportunities,
)
from monitor.storage import fetch_all_auctions, fetch_all_completed_auctions


DEFAULT_CATEGORIES = {"high_interest", "review"}
MIN_SCORE = 60
TOP_N = 20
TELEGRAM_TOP_ITEMS = 10
MIN_HISTORY_SAMPLE_SIZE = DEFAULT_MIN_HISTORY_SAMPLE_SIZE
MAX_COMPLETED_HISTORY = DEFAULT_MAX_COMPLETED_HISTORY
RATIO_ALERT_THRESHOLD = Decimal("0.20")
SCORE_DELTA_ALERT_THRESHOLD = 10
STATE_FILE_PATH = DATA_DIR / "telegram_opportunity_state.json"
MOBILE_REPORT_PATH = REPO_ROOT / "output" / "mobile_report.html"
TELEGRAM_API_BASE_URL = "https://api.telegram.org"
REQUEST_TIMEOUT_SECONDS = 20


def read_bool_env(var_name: str, *, default: bool) -> bool:
    """Leer flags booleanas sin depender del shell interactivo."""
    raw_value = os.environ.get(var_name)
    if raw_value is None:
        return default

    normalized = raw_value.strip().casefold()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return default


ONLY_TOP_OPPORTUNITIES = read_bool_env(
    "TELEGRAM_ONLY_TOP_OPPORTUNITIES",
    default=True,
)
SEND_IF_NO_CHANGES = read_bool_env(
    "TELEGRAM_SEND_IF_NO_CHANGES",
    default=False,
)


def main() -> None:
    """Compute opportunities, compare with the last state, and notify Telegram."""
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        raise SystemExit(
            "Missing Telegram configuration. Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID."
        )

    auctions = fetch_all_auctions()
    evaluations = rank_and_filter_opportunities(
        auctions,
        categories=DEFAULT_CATEGORIES,
        min_score=MIN_SCORE,
        top_n=TOP_N,
    )
    historical_signals = build_telegram_historical_signals()
    evaluations = filter_telegram_candidate_evaluations(
        evaluations,
        only_top_opportunities=ONLY_TOP_OPPORTUNITIES,
        historical_signals=historical_signals,
    )

    previous_state = load_previous_state(STATE_FILE_PATH)
    relevant_updates = detect_relevant_updates(
        evaluations,
        previous_state=previous_state,
        ratio_alert_threshold=RATIO_ALERT_THRESHOLD,
        score_delta_alert_threshold=SCORE_DELTA_ALERT_THRESHOLD,
        historical_signals=historical_signals,
    )

    if not relevant_updates:
        if SEND_IF_NO_CHANGES:
            send_telegram_message(
                token=token,
                chat_id=chat_id,
                text=build_no_changes_message(
                    total_auctions=len(auctions), total_filtered=len(evaluations)
                ),
            )
            print("No relevant changes detected. Short Telegram message sent.")
        else:
            print("No relevant changes detected. Nothing sent.")
        save_current_state(STATE_FILE_PATH, evaluations)
        return

    relevant_evaluations = [item["evaluation"] for item in relevant_updates]
    mobile_report_path = generate_mobile_report(
        relevant_evaluations,
        output_path=MOBILE_REPORT_PATH,
    )
    message = format_relevant_updates_summary(
        total_auctions=len(auctions),
        updates=relevant_updates,
        mobile_report_path=mobile_report_path,
    )
    send_telegram_message(token=token, chat_id=chat_id, text=message)

    with tempfile.NamedTemporaryFile(
        mode="w+b",
        suffix=".csv",
        prefix="relevant_opportunities_",
        delete=False,
    ) as temp_file:
        temp_path = Path(temp_file.name)

    try:
        export_opportunities_to_csv(relevant_evaluations, temp_path)
        send_telegram_document(
            token=token,
            chat_id=chat_id,
            file_path=temp_path,
            caption=build_document_caption(len(relevant_evaluations)),
        )
    finally:
        temp_path.unlink(missing_ok=True)

    save_current_state(STATE_FILE_PATH, evaluations)

    print("Telegram update sent successfully.")
    print(f"Total auctions analyzed: {len(auctions)}")
    print(f"Candidate opportunities after Telegram mode filters: {len(evaluations)}")
    print(f"Relevant updates sent: {len(relevant_updates)}")
    print(f"Mobile report generated: {mobile_report_path}")


def filter_telegram_candidate_evaluations(
    evaluations,
    *,
    only_top_opportunities: bool,
    historical_signals: dict[str, dict[str, dict[str, object]]],
):
    """Filter Telegram candidate evaluations according to the configured delivery mode."""
    if not only_top_opportunities:
        return list(evaluations)

    return [
        evaluation
        for evaluation in evaluations
        if is_top_opportunity_evaluation(
            evaluation,
            historical_signals=historical_signals,
        )
    ]


def detect_relevant_updates(
    evaluations,
    *,
    previous_state: dict[str, dict[str, Any]],
    ratio_alert_threshold: Decimal,
    score_delta_alert_threshold: int,
    historical_signals: dict[str, dict[str, dict[str, object]]],
) -> list[dict[str, Any]]:
    """Select only the evaluations that represent a relevant change."""
    relevant_updates: list[dict[str, Any]] = []

    for evaluation in evaluations:
        auction_lot_id = build_auction_lot_id(
            evaluation.record.auction_id,
            evaluation.record.lot_number,
        )
        if not auction_lot_id:
            continue

        current_snapshot = build_evaluation_snapshot(evaluation)
        previous_snapshot = previous_state.get(auction_lot_id)
        change_reasons = collect_change_reasons(
            current_snapshot=current_snapshot,
            previous_snapshot=previous_snapshot,
            ratio_alert_threshold=ratio_alert_threshold,
            score_delta_alert_threshold=score_delta_alert_threshold,
        )
        if not change_reasons:
            continue

        has_price_data = (
            evaluation.record.opening_bid is not None
            and evaluation.record.opening_bid > 0
            and evaluation.record.appraisal_value is not None
            and evaluation.record.appraisal_value > 0
        )
        relevant_updates.append(
            {
                "auction_lot_id": auction_lot_id,
                "evaluation": evaluation,
                "change_reasons": change_reasons,
                "history_context": build_active_history_context(
                    municipality=evaluation.record.municipality,
                    postal_code=evaluation.record.postal_code,
                    opening_bid_ratio=evaluation.derivations.opening_bid_ratio,
                    has_price_data=has_price_data,
                    historical_signals=historical_signals,
                ),
            }
        )

    return relevant_updates


def collect_change_reasons(
    *,
    current_snapshot: dict[str, Any],
    previous_snapshot: dict[str, Any] | None,
    ratio_alert_threshold: Decimal,
    score_delta_alert_threshold: int,
) -> list[str]:
    """Explain why one evaluation should trigger a Telegram alert."""
    reasons: list[str] = []

    if previous_snapshot is None:
        reasons.append("new opportunity")

    previous_category = (
        previous_snapshot.get("category") if previous_snapshot is not None else None
    )
    if (
        current_snapshot["category"] == "high_interest"
        and previous_category != "high_interest"
    ):
        reasons.append("promoted to high_interest")

    current_ratio = _deserialize_decimal(current_snapshot.get("opening_bid_ratio"))
    previous_ratio = (
        _deserialize_decimal(previous_snapshot.get("opening_bid_ratio"))
        if previous_snapshot is not None
        else None
    )
    if current_ratio is not None and current_ratio < ratio_alert_threshold:
        if previous_ratio is None or previous_ratio >= ratio_alert_threshold:
            reasons.append(f"ratio below {format_decimal(ratio_alert_threshold)}")

    previous_score = (
        previous_snapshot.get("score") if previous_snapshot is not None else None
    )
    if (
        previous_score is not None
        and current_snapshot["score"] - previous_score >= score_delta_alert_threshold
    ):
        reasons.append(
            f"score increased by {current_snapshot['score'] - previous_score}"
        )

    return reasons


def format_relevant_updates_summary(
    *,
    total_auctions: int,
    updates: list[dict[str, Any]],
    mobile_report_path: Path,
) -> str:
    """Build a compact Telegram message optimized for small mobile screens."""
    lines = [
        "Monitor Subastas Valencia",
        f"Novedades relevantes: {len(updates)}",
        f"Analizadas: {total_auctions}",
        "",
    ]

    for index, item in enumerate(updates[:TELEGRAM_TOP_ITEMS], start=1):
        lines.extend(format_relevant_update_lines(index=index, update=item))
        lines.append("")

    lines.append(f"HTML local: {mobile_report_path}")

    return "\n".join(lines).rstrip()


def format_relevant_update_lines(*, index: int, update: dict[str, Any]) -> list[str]:
    """Build a short Telegram block tailored to quick mobile review."""
    evaluation = update["evaluation"]
    history_context = update.get("history_context", {})
    location = html.escape(
        build_display_location(
            municipality=evaluation.record.municipality,
            postal_code=evaluation.record.postal_code,
            province=evaluation.record.province,
        )
    )
    positive_reason = html.escape(first_reason(evaluation.positive_reasons) or "-")
    negative_reason = html.escape(first_reason(evaluation.negative_reasons) or "-")
    category_prefix = html.escape(category_prefix_for_telegram(evaluation.category))
    auction_lot_id = html.escape(update["auction_lot_id"])
    source_link = build_clickable_source_link(evaluation.record.source_url)

    return [
        f"{index}. {category_prefix} {auction_lot_id}",
        f"📍 {location}",
        (
            f"Puntuacion={evaluation.score} | Ratio={format_ratio(evaluation.derivations.opening_bid_ratio)}"
            f" | Apertura={format_decimal(evaluation.record.opening_bid)}"
            f" | Tasacion={format_decimal(evaluation.record.appraisal_value)}"
        ),
        f"✅ Punto fuerte: {positive_reason}",
        f"⚠️ Riesgo: {negative_reason}",
        (
            "🧊 Mercado historico: "
            f"{html.escape(format_history_heat_label(str(history_context.get('historical_heat_label', 'unknown'))))}"
        ),
        (
            "📊 Confianza: "
            f"{html.escape(format_history_confidence_label(str(history_context.get('historical_confidence', 'insufficient'))))}"
        ),
        f"🧾 Muestra: {format_history_sample_size(history_context.get('historical_sample_size'))}",
        source_link,
    ]


def build_no_changes_message(*, total_auctions: int, total_filtered: int) -> str:
    """Build a short message for the no-changes case."""
    return "\n".join(
        [
            "Monitor Subastas Valencia",
            "Sin novedades relevantes.",
            f"Analizadas: {total_auctions}",
            f"Oportunidades filtradas: {total_filtered}",
        ]
    )


def build_document_caption(opportunity_count: int) -> str:
    """Build a short caption for the attached CSV."""
    return f"Exportacion de oportunidades relevantes ({opportunity_count} filas)"


def build_clickable_source_link(source_url: str | None) -> str:
    """Build a short clickable Telegram link using HTML parse mode."""
    if not source_url:
        return "🔗 Sin ficha"

    escaped_url = html.escape(source_url, quote=True)
    return f'🔗 <a href="{escaped_url}">Abrir ficha</a>'


def format_location(
    *,
    municipality: str | None,
    postal_code: str | None,
    province: str | None,
) -> str:
    """Build a compact location string for mobile Telegram messages."""
    return build_display_location(
        municipality=municipality,
        postal_code=postal_code,
        province=province,
    )


def build_telegram_historical_signals() -> dict[str, dict[str, dict[str, object]]]:
    """Build the recent completed-history signals reused by Telegram top mode and text."""
    completed_auctions = fetch_all_completed_auctions()
    completed_history_rows = select_recent_completed_history_rows(
        build_completed_history_rows(completed_auctions),
        max_rows=MAX_COMPLETED_HISTORY,
    )
    return build_completed_history_signals(
        completed_history_rows,
        min_sample_size=MIN_HISTORY_SAMPLE_SIZE,
    )


def format_history_heat_label(label: str) -> str:
    """Translate history heat labels into short Spanish text."""
    mapping = {
        "cold_market": "mercado frio",
        "cold_market_low_confidence": "mercado frio (confianza baja)",
        "mixed_market": "mercado mixto",
        "mixed_market_low_confidence": "mercado mixto (confianza baja)",
        "hot_market": "mercado caliente",
        "hot_market_low_confidence": "mercado caliente (confianza baja)",
        "unknown": "sin historico fiable",
    }
    return mapping.get(label, label or "sin historico fiable")


def format_history_confidence_label(label: str) -> str:
    """Translate confidence labels into short Spanish text."""
    mapping = {
        "insufficient": "insuficiente",
        "low": "baja",
        "medium": "media",
        "high": "alta",
    }
    return mapping.get(label, label or "insuficiente")


def format_history_sample_size(value: object) -> str:
    """Format history sample size consistently for Telegram blocks."""
    if value in (None, "", 0):
        return "-"
    return str(value)


def generate_mobile_report(evaluations, *, output_path: Path) -> Path:
    """Generate a lightweight HTML report designed for quick mobile reading."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    cards_html = "\n".join(
        build_mobile_report_card(evaluation) for evaluation in evaluations
    )
    if not cards_html:
        cards_html = '<div class="empty">No relevant opportunities.</div>'

    document = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Monitor Subastas Valencia</title>
  <style>
    :root {{
      color-scheme: light;
      --bg: #f6f1e7;
      --card: #fffdf8;
      --ink: #1b1a17;
      --muted: #6c6254;
      --line: #dfd5c4;
      --good: #156f3b;
      --risk: #9d3d23;
      --accent: #0d5e8c;
    }}
    body {{
      margin: 0;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: var(--bg);
      color: var(--ink);
    }}
    main {{
      max-width: 720px;
      margin: 0 auto;
      padding: 16px 14px 32px;
    }}
    h1 {{
      margin: 0 0 8px;
      font-size: 28px;
      line-height: 1.1;
    }}
    p.lead {{
      margin: 0 0 18px;
      color: var(--muted);
      font-size: 16px;
    }}
    .card {{
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 14px;
      margin-bottom: 14px;
      box-shadow: 0 8px 20px rgba(0, 0, 0, 0.04);
    }}
    .eyebrow {{
      color: var(--muted);
      font-size: 13px;
      margin-bottom: 6px;
    }}
    .title {{
      font-size: 18px;
      font-weight: 700;
      line-height: 1.25;
      margin: 0 0 8px;
    }}
    .meta {{
      font-size: 15px;
      line-height: 1.5;
      margin: 0 0 10px;
    }}
    .reason {{
      margin: 6px 0;
      font-size: 15px;
      line-height: 1.45;
    }}
    .good {{ color: var(--good); }}
    .risk {{ color: var(--risk); }}
    .button {{
      display: inline-block;
      margin-top: 10px;
      padding: 12px 14px;
      border-radius: 12px;
      background: var(--accent);
      color: white;
      text-decoration: none;
      font-weight: 600;
      font-size: 15px;
    }}
    .empty {{
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 18px;
      color: var(--muted);
      font-size: 16px;
    }}
  </style>
</head>
<body>
  <main>
    <h1>Monitor Subastas Valencia</h1>
    <p class="lead">Resumen movil de oportunidades relevantes.</p>
    {cards_html}
  </main>
</body>
</html>
"""
    output_path.write_text(document, encoding="utf-8")
    return output_path


def build_mobile_report_card(evaluation) -> str:
    """Build one mobile-friendly HTML card from an opportunity evaluation."""
    location = html.escape(
        build_display_location(
            municipality=evaluation.record.municipality,
            postal_code=evaluation.record.postal_code,
            province=evaluation.record.province,
        )
    )
    auction_lot_id = html.escape(
        build_auction_lot_id(evaluation.record.auction_id, evaluation.record.lot_number)
    )
    title = html.escape(evaluation.record.title or "-")
    positive_reason = html.escape(first_reason(evaluation.positive_reasons) or "-")
    negative_reason = html.escape(first_reason(evaluation.negative_reasons) or "-")
    source_url = html.escape(evaluation.record.source_url or "#", quote=True)

    return f"""
    <section class="card">
      <div class="eyebrow">{auction_lot_id}</div>
      <h2 class="title">{title}</h2>
      <p class="meta">
        {location}<br>
        score={evaluation.score} | ratio={format_ratio(evaluation.derivations.opening_bid_ratio)}<br>
        apertura={format_decimal(evaluation.record.opening_bid)} | tasacion={format_decimal(evaluation.record.appraisal_value)}
      </p>
      <p class="reason good">✅ {positive_reason}</p>
      <p class="reason risk">⚠️ {negative_reason}</p>
      <a class="button" href="{source_url}">Abrir BOE</a>
    </section>
    """.strip()


def category_prefix_for_telegram(category: str) -> str:
    """Map categories to short visual Telegram prefixes."""
    if category == "high_interest":
        return "🔥 alta prioridad"
    if category == "review":
        return "👀 revisar"
    return "• descartar"


def build_evaluation_snapshot(evaluation) -> dict[str, Any]:
    """Serialize only the fields needed to detect meaningful changes."""
    return {
        "auction_lot_id": build_auction_lot_id(
            evaluation.record.auction_id,
            evaluation.record.lot_number,
        ),
        "score": evaluation.score,
        "category": evaluation.category,
        "opening_bid_ratio": _serialize_decimal(
            evaluation.derivations.opening_bid_ratio
        ),
        "sent_at": datetime.now(timezone.utc).isoformat(),
    }


def save_current_state(state_file_path: Path, evaluations) -> None:
    """Persist the latest evaluation snapshot for future diffing."""
    state_file_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "items": {
            snapshot["auction_lot_id"]: snapshot
            for snapshot in (
                build_evaluation_snapshot(evaluation) for evaluation in evaluations
            )
            if snapshot["auction_lot_id"]
        },
    }
    state_file_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8"
    )


def load_previous_state(state_file_path: Path) -> dict[str, dict[str, Any]]:
    """Load the previous alert state from disk if available."""
    if not state_file_path.exists():
        return {}

    try:
        payload = json.loads(state_file_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}

    items = payload.get("items", {})
    if not isinstance(items, dict):
        return {}

    previous_state: dict[str, dict[str, Any]] = {}
    for auction_lot_id, snapshot in items.items():
        if not isinstance(snapshot, dict):
            continue
        previous_state[auction_lot_id] = {
            "auction_lot_id": snapshot.get("auction_lot_id", auction_lot_id),
            "score": snapshot.get("score"),
            "category": snapshot.get("category"),
            "opening_bid_ratio": _deserialize_decimal(
                snapshot.get("opening_bid_ratio")
            ),
            "sent_at": snapshot.get("sent_at"),
        }
    return previous_state


def build_auction_lot_id(auction_id: str | None, lot_number: int | None) -> str:
    """Build a readable identifier that keeps lots explicit."""
    if auction_id is None:
        return ""
    if lot_number is None or auction_id.endswith(f"::lot:{lot_number}"):
        return auction_id
    return f"{auction_id}::lot:{lot_number}"


def first_reason(reasons: list[str]) -> str:
    """Return the first available reason or an empty string."""
    return reasons[0] if reasons else ""


def format_ratio(value) -> str:
    """Format the opening-bid ratio for Telegram output."""
    if value is None:
        return "-"
    return f"{float(value):.2f}"


def format_decimal(value: Decimal | None) -> str:
    """Format decimals consistently for short alert text."""
    if value is None:
        return "-"
    return format(value, "f")


def send_telegram_message(*, token: str, chat_id: str, text: str) -> None:
    """Send a plain Telegram message through the Bot API."""
    response = requests.post(
        build_telegram_url(token, "sendMessage"),
        data={
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        },
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    payload = response.json()
    if not payload.get("ok", False):
        raise RuntimeError(f"Telegram sendMessage failed: {payload}")


def send_telegram_document(
    *, token: str, chat_id: str, file_path: Path, caption: str
) -> None:
    """Send a CSV file through the Telegram Bot API."""
    with file_path.open("rb") as document_file:
        response = requests.post(
            build_telegram_url(token, "sendDocument"),
            data={
                "chat_id": chat_id,
                "caption": caption,
            },
            files={
                "document": (
                    "opportunities.csv",
                    document_file,
                    "text/csv",
                )
            },
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
    response.raise_for_status()
    payload = response.json()
    if not payload.get("ok", False):
        raise RuntimeError(f"Telegram sendDocument failed: {payload}")


def build_telegram_url(token: str, method_name: str) -> str:
    """Build the Telegram Bot API URL for one method."""
    return f"{TELEGRAM_API_BASE_URL}/bot{token}/{method_name}"


def _serialize_decimal(value: Decimal | None) -> str | None:
    """Serialize decimals safely for JSON state storage."""
    if value is None:
        return None
    return format(value, "f")


def _deserialize_decimal(value: Any) -> Decimal | None:
    """Deserialize decimals from stored JSON values."""
    if value in (None, ""):
        return None
    return Decimal(str(value))


if __name__ == "__main__":
    main()
