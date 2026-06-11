"""Slack alert dispatch — posts pacing alerts and daily digests via Block Kit.

Reads unsent alerts from BigQuery, formats them as rich Slack messages,
posts to the project's configured channel (or a fallback), and marks
them as sent.
"""

import json
import logging
from datetime import date, datetime, timezone

from google.cloud import bigquery
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from backend.config import settings
from backend.services import alert_charts
from backend.services import bigquery_client as bq

logger = logging.getLogger(__name__)

DEFAULT_CHANNEL = settings.slack_default_channel or "#cip-alerts"

# Brand status colours (ADA light-theme tokens — Slack renders on white).
SEVERITY_COLORS = {
    "critical": "#C4392E",
    "warning": "#B97D08",
    "info": "#3F6FB0",
}
ALL_CLEAR_COLOR = "#6F8C00"

SEVERITY_EMOJI = {
    "critical": ":rotating_light:",
    "warning": ":warning:",
    "info": ":information_source:",
}

# Verdict words — the same plain-language grammar the ADA UI speaks
# (Flightdeck verdicts + Triage Board zones). Keyed by (alert_type,
# severity) with alert_type-only and severity-only fallbacks.
VERDICT_WORDS: dict[tuple[str, str], str] = {
    ("pacing_over", "warning"): "RUNNING HOT",
    ("pacing_over", "critical"): "BURNING DOWN",
    ("pacing_under", "warning"): "LAGGING",
    ("pacing_under", "critical"): "STALLED",
    ("budget_exceeded", "critical"): "OVER BUDGET",
    ("flight_ending", "info"): "FINAL STRETCH",
    ("data_stale", "warning"): "GONE DARK",
    ("diagnostic_health_regression", "critical"): "HEALTH DOWN",
}
SEVERITY_VERDICTS = {
    "critical": "ACT NOW",
    "warning": "KEEP AN EYE ON",
    "info": "HEADS UP",
}


def _verdict_word(alert_type: str, severity: str) -> str:
    """Plain-language verdict for the alert headline, matching the UI."""
    if alert_type.startswith("diagnostic_signal_"):
        return "SIGNAL DOWN"
    return (
        VERDICT_WORDS.get((alert_type, severity))
        or VERDICT_WORDS.get((alert_type, "critical"))
        or SEVERITY_VERDICTS.get(severity, "ALERT")
    )


def _get_slack_client() -> WebClient | None:
    token = settings.slack_bot_token
    if not token:
        logger.warning("SLACK_BOT_TOKEN not set — Slack dispatch disabled")
        return None
    return WebClient(token=token)


def _project_channels() -> dict[str, str]:
    """Map project_code → slack_channel_id from dim_projects."""
    rows = bq.run_query(f"""
        SELECT project_code, slack_channel_id
        FROM {bq.table('dim_projects')}
        WHERE slack_channel_id IS NOT NULL AND slack_channel_id != ''
    """)
    return {r["project_code"]: r["slack_channel_id"] for r in rows}


# Human-readable event label per alert_type, used to build a descriptive
# headline (channel + line name are appended from the media plan at dispatch).
ALERT_TYPE_LABELS = {
    "budget_exceeded": "Budget exceeded",
    "pacing_over": "Overspending",
    "pacing_under": "Underspending",
    "flight_ending": "Flight ending soon",
    "data_stale": "Stale data",
    "diagnostic_health_regression": "Health regression",
}


def _clean_channel(site_network: str | None, channel_category: str | None) -> str:
    """Channel label from media-plan fields. site_network can carry newlines
    (e.g. 'Meta\\nFacebook & Instagram') so collapse any whitespace runs."""
    if site_network:
        return " ".join(site_network.split())
    return channel_category or ""


def _alert_headline(alert: dict, line: dict | None) -> str:
    """Descriptive title: '<event> - <channel> - <line name> (<#code>)'.

    Falls back to the stored title when there's no media-plan line context
    (e.g. data_stale alerts that aren't tied to a line)."""
    event = ALERT_TYPE_LABELS.get(alert.get("alert_type", "")) or alert.get("title") or "Alert"
    if not line:
        return alert.get("title") or event
    parts = [event]
    channel = _clean_channel(line.get("site_network"), line.get("channel_category"))
    if channel:
        parts.append(channel)
    line_name = line.get("audience_name")
    if line_name:
        parts.append(line_name)
    headline = " - ".join(parts)
    code = line.get("line_code")
    if code:
        headline = f"{headline} ({code})"
    return headline


def _project_label(pcode: str, proj: dict | None) -> str:
    """'26018 - CAPE - Pre-Bargaining Flight 1' from the dim_projects join."""
    proj = proj or {}
    bits = [pcode]
    if proj.get("client_name"):
        bits.append(proj["client_name"])
    if proj.get("project_name"):
        bits.append(proj["project_name"])
    return " - ".join(b for b in bits if b)


def _format_alert_blocks(
    alert: dict,
    proj_info: dict | None = None,
    line_info: dict | None = None,
) -> list[dict]:
    """Build Slack Block Kit blocks for a single alert.

    All content lives inside these blocks so the caller can render them inside a
    single coloured attachment (nothing spills outside the severity border). No
    leading severity emoji — the colour border and the verdict word carry that
    signal, the same way the ADA UI does.

    Layout:
        *RUNNING HOT* · 26018 - CUPE National - Hands Off Health Care
        *Overspending - Meta - Persuadable Parents 35-64 (M-01)*
        {message}  Pacing: 121.7%
        pacing_over · warning · Review & acknowledge in ADA →
    """
    severity = alert["severity"]
    pcode = alert.get("project_code", "")
    message = alert.get("message", "")
    alert_type = alert.get("alert_type", "")

    meta = {}
    if alert.get("metadata"):
        try:
            meta = json.loads(alert["metadata"]) if isinstance(alert["metadata"], str) else alert["metadata"]
        except (json.JSONDecodeError, TypeError):
            pass

    line = (line_info or {}).get(meta.get("line_id")) if meta.get("line_id") else None
    headline = _alert_headline(alert, line)
    verdict = _verdict_word(alert_type, severity)

    pacing_pct = meta.get("pacing_pct")
    body = message + (f"\nPacing: *{pacing_pct:.0f}%*" if pacing_pct else "")

    is_project_alert = bool(pcode) and pcode != "__system__"
    # Project links land on the Summary tab — verdict, projection, and the
    # acknowledge-and-note flow are all right there.
    view_url = (
        f"{settings.frontend_url}/project/{pcode}" if is_project_alert
        else f"{settings.frontend_url}/alerts"
    )

    project_line = _project_label(pcode, (proj_info or {}).get(pcode)) if is_project_alert else ""
    lead = f"*{verdict}*" + (f" · {project_line}" if project_line else "")

    context_elements = [{
        "type": "mrkdwn",
        "text": f"`{alert_type}` · `{severity}` · <{view_url}|Review & acknowledge in ADA →>",
    }]

    return [
        {"type": "section", "text": {"type": "mrkdwn", "text": lead}},
        {"type": "section", "text": {"type": "mrkdwn", "text": f"*{headline}*\n{body}"}},
        {"type": "context", "elements": context_elements},
    ]


def _alert_line_id(alert: dict) -> str | None:
    """Pull the media-plan line_id out of an alert's metadata, if present."""
    m = alert.get("metadata")
    if not m:
        return None
    try:
        meta = json.loads(m) if isinstance(m, str) else m
    except (json.JSONDecodeError, TypeError):
        return None
    return meta.get("line_id") if isinstance(meta, dict) else None


def _enrich_projects(codes: list[str]) -> dict[str, dict]:
    """project_code → {project_name, client_name} for the alert headers."""
    wanted = sorted({c for c in codes if c and c != "__system__"})
    if not wanted:
        return {}
    rows = bq.run_query(
        f"""
        SELECT p.project_code, p.project_name, c.client_name
        FROM {bq.table('dim_projects')} p
        LEFT JOIN {bq.table('dim_clients')} c USING (client_id)
        WHERE p.project_code IN UNNEST(@codes)
        """,
        [bq.array_param("codes", "STRING", wanted)],
    )
    return {r["project_code"]: r for r in rows}


def _enrich_lines(line_ids: list[str]) -> dict[str, dict]:
    """line_id → {line_code, channel_category, site_network, audience_name}.

    Uses the standard media_plan_lines dedup guard (latest sync_version per
    line_id, current plan only) so stale sync rows can't shadow the live name."""
    wanted = sorted({lid for lid in line_ids if lid})
    if not wanted:
        return {}
    rows = bq.run_query(
        f"""
        SELECT line_id, line_code, channel_category, site_network, audience_name
        FROM (
            SELECT l.line_id, l.line_code, l.channel_category, l.site_network,
                   l.audience_name,
                   ROW_NUMBER() OVER (
                       PARTITION BY l.line_id ORDER BY l.sync_version DESC
                   ) AS _rn
            FROM {bq.table('media_plan_lines')} l
            JOIN {bq.table('media_plans')} p
              ON l.plan_id = p.plan_id AND p.is_current = TRUE
            WHERE l.line_id IN UNNEST(@line_ids)
        )
        WHERE _rn = 1
        """,
        [bq.array_param("line_ids", "STRING", wanted)],
    )
    return {r["line_id"]: r for r in rows}


def dispatch_unsent_alerts() -> dict:
    """Find all unsent alerts and post them to Slack.

    Returns summary: {dispatched, failed, skipped_no_token}
    """
    client = _get_slack_client()
    if not client:
        return {"dispatched": 0, "failed": 0, "skipped_no_token": True}

    # Fetch unsent alerts
    rows = bq.run_query(f"""
        SELECT alert_id, project_code, alert_type, severity, title, message, metadata, created_at
        FROM {bq.table('alerts')}
        WHERE (slack_sent IS NULL OR slack_sent = FALSE)
        ORDER BY created_at ASC
        LIMIT 100
    """)

    if not rows:
        logger.info("No unsent alerts to dispatch")
        return {"dispatched": 0, "failed": 0, "skipped_no_token": False}

    channel_map = _project_channels()
    proj_info = _enrich_projects([a.get("project_code", "") for a in rows])
    line_info = _enrich_lines([lid for a in rows if (lid := _alert_line_id(a))])
    dispatched = 0
    failed = 0

    for alert in rows:
        pcode = alert.get("project_code", "")
        channel = channel_map.get(pcode, DEFAULT_CHANNEL)
        blocks = _format_alert_blocks(alert, proj_info, line_info)
        # Spend charts for over/under-spend alerts (best-effort; [] if disabled
        # or anything fails) — appended so they render inside the same border.
        blocks = blocks + alert_charts.build_alert_chart_blocks(alert)
        color = SEVERITY_COLORS.get(alert["severity"], "#8A8888")
        # `fallback` is the notification-preview text only. We deliberately do NOT
        # pass a top-level `text=` so nothing renders above (outside) the coloured
        # attachment border — the whole alert sits inside the severity colour.
        verdict = _verdict_word(alert.get("alert_type", ""), alert["severity"])
        fallback_text = f"{verdict} — {alert.get('title', 'Alert')}"
        attachments = [{"color": color, "fallback": fallback_text, "blocks": blocks}]

        try:
            resp = client.chat_postMessage(channel=channel, attachments=attachments)
            _mark_sent(alert["alert_id"], channel, resp.get("ts"))
            dispatched += 1
        except SlackApiError as e:
            logger.error("Slack send failed for alert %s: %s", alert["alert_id"], e.response["error"])
            # Try fallback channel if project channel failed
            if channel != DEFAULT_CHANNEL:
                try:
                    resp = client.chat_postMessage(channel=DEFAULT_CHANNEL, attachments=attachments)
                    _mark_sent(alert["alert_id"], DEFAULT_CHANNEL, resp.get("ts"))
                    dispatched += 1
                    continue
                except SlackApiError:
                    pass
            failed += 1

    result = {"dispatched": dispatched, "failed": failed, "skipped_no_token": False}
    logger.info("Slack dispatch: %s", result)
    return result


def _mark_sent(alert_id: str, channel: str, message_ts: str | None) -> None:
    """Update the alert row to record Slack delivery."""
    sql = f"""
        UPDATE {bq.table('alerts')}
        SET slack_sent = TRUE,
            slack_channel_id = @channel,
            slack_message_ts = @ts
        WHERE alert_id = @alert_id
    """
    params = [
        bq.string_param("alert_id", alert_id),
        bq.string_param("channel", channel),
        bq.string_param("ts", message_ts or ""),
    ]
    try:
        bq.run_query(sql, params)
    except Exception as e:
        logger.warning("Failed to mark alert %s as sent: %s", alert_id, e)


def post_daily_digest() -> dict:
    """Post a morning summary of all active alerts to the default channel."""
    client = _get_slack_client()
    if not client:
        return {"posted": False, "reason": "no_token"}

    # Get summary of active (unresolved) alerts by project
    rows = bq.run_query(f"""
        SELECT
            a.project_code,
            p.project_name,
            a.severity,
            COUNT(*) as alert_count
        FROM {bq.table('alerts')} a
        LEFT JOIN {bq.table('dim_projects')} p USING (project_code)
        WHERE a.resolved_at IS NULL
          AND a.created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
        GROUP BY a.project_code, p.project_name, a.severity
        ORDER BY a.project_code, a.severity
    """)

    today = date.today().strftime("%B %d, %Y")

    if not rows:
        # No active alerts — post an all-clear inside an on-pace green border
        try:
            client.chat_postMessage(
                channel=DEFAULT_CHANNEL,
                attachments=[{
                    "color": ALL_CLEAR_COLOR,
                    "fallback": "ADA Daily Digest — all quiet",
                    "blocks": [
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": f"*ADA Daily Digest — {today}*\n*ON PACE* — no active alerts. The money's landing.",
                            },
                        },
                        {
                            "type": "context",
                            "elements": [{
                                "type": "mrkdwn",
                                "text": f"<{settings.frontend_url}/|Open the Flightdeck →>",
                            }],
                        },
                    ],
                }],
            )
            return {"posted": True, "alerts": 0}
        except SlackApiError as e:
            logger.error("Daily digest failed: %s", e.response["error"])
            return {"posted": False, "reason": str(e)}

    # Build digest — one line per project, severity counts, deep links
    lines = []
    current_project = None
    worst = "info"
    for r in rows:
        pcode = r["project_code"] or "System"
        pname = r.get("project_name") or pcode
        sev = r["severity"]
        cnt = r["alert_count"]
        emoji = SEVERITY_EMOJI.get(sev, ":bell:")
        if sev == "critical" or (sev == "warning" and worst == "info"):
            worst = sev

        if pcode != current_project:
            current_project = pcode
            link = (
                f"<{settings.frontend_url}/project/{pcode}|{pcode}>"
                if pcode != "System"
                else pcode
            )
            lines.append(f"\n*{link} — {pname}*")
        lines.append(f"  {emoji} {cnt} {sev} alert{'s' if cnt > 1 else ''}")

    total = sum(r["alert_count"] for r in rows)
    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*ADA Daily Digest — {today}*\n" + "\n".join(lines),
            },
        },
        {
            "type": "context",
            "elements": [{
                "type": "mrkdwn",
                "text": (
                    f"{total} active alert{'s' if total != 1 else ''} in the last 24h · "
                    f"<{settings.frontend_url}/alerts|Review & acknowledge in ADA →>"
                ),
            }],
        },
    ]

    try:
        client.chat_postMessage(
            channel=DEFAULT_CHANNEL,
            attachments=[{
                "color": SEVERITY_COLORS.get(worst, "#8A8888"),
                "fallback": f"ADA Daily Digest — {total} active alerts",
                "blocks": blocks,
            }],
        )
        return {"posted": True, "alerts": total}
    except SlackApiError as e:
        logger.error("Daily digest failed: %s", e.response["error"])
        return {"posted": False, "reason": str(e)}
