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
from backend.services import bigquery_client as bq

logger = logging.getLogger(__name__)

DEFAULT_CHANNEL = settings.slack_default_channel or "#cip-alerts"

SEVERITY_COLORS = {
    "critical": "#dc2626",
    "warning": "#f59e0b",
    "info": "#3b82f6",
}

SEVERITY_EMOJI = {
    "critical": ":rotating_light:",
    "warning": ":warning:",
    "info": ":information_source:",
}


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


def _format_alert_blocks(alert: dict) -> list[dict]:
    """Build Slack Block Kit blocks for a single alert."""
    severity = alert["severity"]
    emoji = SEVERITY_EMOJI.get(severity, ":bell:")
    pcode = alert.get("project_code", "")
    title = alert.get("title", "Alert")
    message = alert.get("message", "")
    alert_type = alert.get("alert_type", "")

    # Try to extract pacing numbers from metadata
    meta = {}
    if alert.get("metadata"):
        try:
            meta = json.loads(alert["metadata"]) if isinstance(alert["metadata"], str) else alert["metadata"]
        except (json.JSONDecodeError, TypeError):
            pass

    pacing_pct = meta.get("pacing_pct")
    pacing_str = f"*{pacing_pct:.0f}%*" if pacing_pct else ""

    header_text = f"{emoji} *{title}*"
    if pcode:
        header_text = f"{emoji} `{pcode}` — *{title}*"

    blocks = [
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": header_text},
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": message + (f"\nPacing: {pacing_str}" if pacing_str else ""),
            },
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"Type: `{alert_type}` | Severity: `{severity}` | <{settings.frontend_url}/project/{pcode}|View in CIP>",
                }
            ],
        },
        {"type": "divider"},
    ]
    return blocks


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
    dispatched = 0
    failed = 0

    for alert in rows:
        pcode = alert.get("project_code", "")
        channel = channel_map.get(pcode, DEFAULT_CHANNEL)
        blocks = _format_alert_blocks(alert)
        color = SEVERITY_COLORS.get(alert["severity"], "#6b7280")
        fallback_text = f"[{alert['severity'].upper()}] {alert.get('title', 'Alert')}"

        try:
            resp = client.chat_postMessage(
                channel=channel,
                text=fallback_text,
                attachments=[{"color": color, "blocks": blocks}],
            )
            _mark_sent(alert["alert_id"], channel, resp.get("ts"))
            dispatched += 1
        except SlackApiError as e:
            logger.error("Slack send failed for alert %s: %s", alert["alert_id"], e.response["error"])
            # Try fallback channel if project channel failed
            if channel != DEFAULT_CHANNEL:
                try:
                    resp = client.chat_postMessage(
                        channel=DEFAULT_CHANNEL,
                        text=fallback_text,
                        attachments=[{"color": color, "blocks": blocks}],
                    )
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

    if not rows:
        # No active alerts — post an all-clear
        try:
            client.chat_postMessage(
                channel=DEFAULT_CHANNEL,
                text=":white_check_mark: *CIP Daily Digest* — No active alerts. All campaigns on track.",
            )
            return {"posted": True, "alerts": 0}
        except SlackApiError as e:
            logger.error("Daily digest failed: %s", e.response["error"])
            return {"posted": False, "reason": str(e)}

    # Build digest
    today = date.today().strftime("%B %d, %Y")
    lines = [f":bar_chart: *CIP Daily Digest — {today}*\n"]

    current_project = None
    for r in rows:
        pcode = r["project_code"] or "System"
        pname = r.get("project_name") or pcode
        sev = r["severity"]
        cnt = r["alert_count"]
        emoji = SEVERITY_EMOJI.get(sev, ":bell:")

        if pcode != current_project:
            current_project = pcode
            lines.append(f"\n*`{pcode}` — {pname}*")
        lines.append(f"  {emoji} {cnt} {sev} alert{'s' if cnt > 1 else ''}")

    total = sum(r["alert_count"] for r in rows)
    lines.append(f"\n_Total: {total} active alert{'s' if total != 1 else ''} | <{settings.frontend_url}/alerts|View all in CIP>_")

    try:
        client.chat_postMessage(
            channel=DEFAULT_CHANNEL,
            text="\n".join(lines),
        )
        return {"posted": True, "alerts": total}
    except SlackApiError as e:
        logger.error("Daily digest failed: %s", e.response["error"])
        return {"posted": False, "reason": str(e)}
