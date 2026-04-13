"""Pacing engine — the #1 business-critical feature.

Even-pacing model:
    planned_spend_to_date = (line_budget / total_active_days) × elapsed_active_days

Active days are derived from blocking_chart_weeks (only weeks where is_active=TRUE
count). Each active week contributes 7 days, capped to the flight start/end dates.

Alert thresholds (from CLAUDE.md):
    >130%  pacing_over    critical
    >115%  pacing_over    warning
    <70%   pacing_under   critical
    <85%   pacing_under   warning
    actual > budget        budget_exceeded  critical
    <7 days left + >15% unspent  flight_ending  info
"""

import json
import logging
import uuid
from datetime import date, timedelta
from decimal import Decimal

from google.cloud import bigquery
from google.cloud import exceptions as gcp_exceptions

from backend.config import settings
from backend.services import bigquery_client as bq

logger = logging.getLogger(__name__)

# ── Threshold constants ─────────────────────────────────────────────
PACING_OVER_CRITICAL = 130.0
PACING_OVER_WARNING = 115.0
PACING_UNDER_WARNING = 85.0
PACING_UNDER_CRITICAL = 70.0
FLIGHT_ENDING_DAYS = 7
FLIGHT_ENDING_UNSPENT_PCT = 15.0


def _float(v, default=0.0) -> float:
    if v is None:
        return default
    return float(v) if not isinstance(v, float) else v


def _count_active_days(
    blocking_weeks: list[dict],
    flight_start: date,
    flight_end: date,
) -> tuple[int, int]:
    """Return (total_active_days, elapsed_active_days up to today).

    Each blocking_chart_weeks row represents a 7-day window starting at
    week_start. We clamp to the flight start/end and to today.
    """
    today = date.today()
    total_active = 0
    elapsed_active = 0

    for week in blocking_weeks:
        if not week.get("is_active"):
            continue
        ws = week["week_start"]
        if isinstance(ws, str):
            ws = date.fromisoformat(ws)

        week_end = ws + timedelta(days=6)

        # Clamp to flight boundaries
        period_start = max(ws, flight_start)
        period_end = min(week_end, flight_end)
        if period_start > period_end:
            continue

        days_in_period = (period_end - period_start).days + 1
        total_active += days_in_period

        # Elapsed portion (up to today)
        if today >= period_start:
            elapsed_end = min(period_end, today)
            elapsed_active += (elapsed_end - period_start).days + 1

    return total_active, elapsed_active


def _generate_alerts(
    project_code: str,
    line_id: str,
    line_label: str,
    pacing_pct: float,
    actual: float,
    planned_budget: float,
    remaining_days: int,
    remaining_budget: float,
) -> list[dict]:
    """Return alert dicts for any breached thresholds."""
    alerts = []

    def _alert(alert_type: str, severity: str, title: str, msg: str, meta: dict):
        from datetime import datetime, timezone
        alerts.append({
            "alert_id": str(uuid.uuid4()),
            "project_code": project_code,
            "alert_type": alert_type,
            "severity": severity,
            "title": title,
            "message": msg,
            "metadata": json.dumps(meta),
            "created_at": datetime.now(timezone.utc).isoformat(),
            "slack_sent": False,
        })

    if actual > planned_budget and planned_budget > 0:
        _alert(
            "budget_exceeded", "critical",
            f"Budget exceeded — {line_label}",
            f"Actual spend ${actual:,.0f} exceeds planned budget ${planned_budget:,.0f}",
            {"line_id": line_id, "actual": actual, "budget": planned_budget},
        )
    elif pacing_pct > PACING_OVER_CRITICAL:
        _alert(
            "pacing_over", "critical",
            f"Critical overspend — {line_label}",
            f"Pacing at {pacing_pct:.0f}% (threshold {PACING_OVER_CRITICAL:.0f}%)",
            {"line_id": line_id, "pacing_pct": pacing_pct},
        )
    elif pacing_pct > PACING_OVER_WARNING:
        _alert(
            "pacing_over", "warning",
            f"Overspend warning — {line_label}",
            f"Pacing at {pacing_pct:.0f}% (threshold {PACING_OVER_WARNING:.0f}%)",
            {"line_id": line_id, "pacing_pct": pacing_pct},
        )
    elif pacing_pct < PACING_UNDER_CRITICAL and pacing_pct > 0:
        _alert(
            "pacing_under", "critical",
            f"Critical underspend — {line_label}",
            f"Pacing at {pacing_pct:.0f}% (threshold {PACING_UNDER_CRITICAL:.0f}%)",
            {"line_id": line_id, "pacing_pct": pacing_pct},
        )
    elif pacing_pct < PACING_UNDER_WARNING and pacing_pct > 0:
        _alert(
            "pacing_under", "warning",
            f"Underspend warning — {line_label}",
            f"Pacing at {pacing_pct:.0f}% (threshold {PACING_UNDER_WARNING:.0f}%)",
            {"line_id": line_id, "pacing_pct": pacing_pct},
        )

    if (
        0 < remaining_days <= FLIGHT_ENDING_DAYS
        and planned_budget > 0
        and (remaining_budget / planned_budget * 100) > FLIGHT_ENDING_UNSPENT_PCT
    ):
        _alert(
            "flight_ending", "info",
            f"Flight ending soon — {line_label}",
            f"{remaining_days} days left with {remaining_budget / planned_budget * 100:.0f}% budget unspent",
            {"line_id": line_id, "remaining_days": remaining_days,
             "unspent_pct": remaining_budget / planned_budget * 100},
        )

    return alerts


def run_pacing_for_project(project_code: str) -> dict:
    """Calculate pacing for every media plan line in a project.

    Returns a summary dict with line-level results and any alerts generated.
    """
    today = date.today()

    # ── 1. Fetch media plan lines for this project ──────────────────
    lines_sql = f"""
        SELECT
            l.line_id,
            l.line_code,
            l.platform_id,
            l.channel_category,
            l.site_network,
            l.budget,
            l.flight_start,
            l.flight_end
        FROM {bq.table('media_plan_lines')} l
        JOIN {bq.table('media_plans')} p ON l.plan_id = p.plan_id AND p.is_current = TRUE
        WHERE l.project_code = @project_code
            AND l.is_traditional = FALSE
    """
    lines = bq.run_query(lines_sql, [bq.string_param("project_code", project_code)])

    if not lines:
        logger.info("No media plan lines found for project %s", project_code)
        return {"project_code": project_code, "lines_processed": 0, "alerts": 0}

    line_ids = [r["line_id"] for r in lines]

    # ── 2. Fetch blocking chart weeks for all lines at once ─────────
    blocking_sql = f"""
        SELECT line_id, week_start, is_active
        FROM {bq.table('blocking_chart_weeks')}
        WHERE project_code = @project_code
        ORDER BY line_id, week_start
    """
    blocking_rows = bq.run_query(blocking_sql, [bq.string_param("project_code", project_code)])

    blocking_by_line: dict[str, list[dict]] = {}
    for r in blocking_rows:
        blocking_by_line.setdefault(r["line_id"], []).append(r)

    # ── 3. Fetch actual spend from fact_digital_daily ────────────────
    # Group lines by (platform_id, flight_start, flight_end) so we can
    # query spend once per unique flight window instead of once per line.
    spend_by_group: dict[tuple, float] = {}          # (platform_id, fs, fe) → total_spend
    spend_by_line_code: dict[str, float] = {}        # line_code → total_spend
    first_spend_date_by_line: dict[str, date] = {}   # line_code → first_spend_date (C1 fix)

    flight_groups: dict[tuple, list[dict]] = {}
    for line in lines:
        fs = line.get("flight_start")
        fe = line.get("flight_end")
        pid = line.get("platform_id")
        if isinstance(fs, str):
            fs = date.fromisoformat(fs)
        if isinstance(fe, str):
            fe = date.fromisoformat(fe)
        key = (pid, fs, fe)
        flight_groups.setdefault(key, []).append(line)

    for (pid, fs, fe), group_lines in flight_groups.items():
        if not pid:
            continue

        params = [
            bq.string_param("project_code", project_code),
            bq.string_param("platform_id", pid),
        ]

        if fs is not None and fe is not None:
            group_spend_sql = f"""
                SELECT SUM(spend) AS total_spend
                FROM {bq.table('fact_digital_daily')}
                WHERE project_code = @project_code
                    AND platform_id = @platform_id
                    AND date >= @flight_start
                    AND date <= @flight_end
            """
            params.append(bq.date_param("flight_start", fs))
            params.append(bq.date_param("flight_end", fe))
        else:
            # NULL flight dates — fall back to unfiltered (backward compat)
            group_spend_sql = f"""
                SELECT SUM(spend) AS total_spend
                FROM {bq.table('fact_digital_daily')}
                WHERE project_code = @project_code
                    AND platform_id = @platform_id
            """

        rows_result = bq.run_query(group_spend_sql, params)
        spend_by_group[(pid, fs, fe)] = _float(rows_result[0]["total_spend"]) if rows_result else 0.0

        # Also fetch spend by line_code within this flight window
        line_codes = [l["line_code"] for l in group_lines if l.get("line_code")]
        for lc in line_codes:
            lc_params = [
                bq.string_param("project_code", project_code),
                bq.string_param("line_code", lc),
            ]
            if fs is not None and fe is not None:
                lc_sql = f"""
                    SELECT SUM(spend) AS total_spend, MIN(date) AS first_spend_date
                    FROM {bq.table('fact_digital_daily')}
                    WHERE project_code = @project_code
                        AND line_code = @line_code
                        AND date >= @flight_start
                        AND date <= @flight_end
                        AND spend > 0
                """
                lc_params.append(bq.date_param("flight_start", fs))
                lc_params.append(bq.date_param("flight_end", fe))
            else:
                lc_sql = f"""
                    SELECT SUM(spend) AS total_spend, MIN(date) AS first_spend_date
                    FROM {bq.table('fact_digital_daily')}
                    WHERE project_code = @project_code
                        AND line_code = @line_code
                        AND spend > 0
                """
            lc_rows = bq.run_query(lc_sql, lc_params)
            spend_by_line_code[lc] = _float(lc_rows[0]["total_spend"]) if lc_rows else 0.0
            # C1: Track first_spend_date for grace period calculation
            if lc_rows and lc_rows[0].get("first_spend_date"):
                fsd = lc_rows[0]["first_spend_date"]
                if isinstance(fsd, str):
                    fsd = date.fromisoformat(fsd)
                first_spend_date_by_line[lc] = fsd

    # ── 4. Compute pacing per line ──────────────────────────────────
    tracking_rows = []
    all_alerts = []

    for line in lines:
        line_id = line["line_id"]
        line_code = line.get("line_code")
        platform_id = line.get("platform_id")
        budget = _float(line.get("budget"))
        flight_start = line.get("flight_start")
        flight_end = line.get("flight_end")

        if not flight_start or not flight_end or budget <= 0:
            continue

        if isinstance(flight_start, str):
            flight_start = date.fromisoformat(flight_start)
        if isinstance(flight_end, str):
            flight_end = date.fromisoformat(flight_end)

        # Get blocking chart for this line
        weeks = blocking_by_line.get(line_id, [])

        if weeks:
            total_active_days, elapsed_active_days = _count_active_days(
                weeks, flight_start, flight_end
            )
        else:
            # No blocking chart — fall back to full flight as active
            total_active_days = (flight_end - flight_start).days + 1
            elapsed_days_raw = (min(today, flight_end) - flight_start).days + 1
            elapsed_active_days = max(0, min(elapsed_days_raw, total_active_days))

        # Determine line status based on flight timing
        # Data lag: ad platforms report with ~1-day delay through Funnel,
        # and the server runs in UTC which can be a day ahead of Eastern.
        # C1: Grace period is now spend-history-aware: only apply grace if
        # the line has zero historical spend AND flight_start is within 2 days.
        if today < flight_start:
            line_status = "not_started"
        elif today > flight_end:
            line_status = "completed"
        else:
            # Check if line has any historical spend
            # Conservative approach: only apply grace period if we can definitively
            # identify THIS line's spend history via line_code. If no line_code,
            # don't assume grace period — require historical spend to NOT apply it.
            has_historical_spend = (
                (line_code and line_code in first_spend_date_by_line) or
                (platform_id and any(
                    lc in first_spend_date_by_line
                    for lc in spend_by_line_code.keys()
                ))
            )
            # Grace period: no spend yet AND flight started within 2 days
            in_grace_period = (
                not has_historical_spend and
                (today - flight_start).days <= 2
            )
            if in_grace_period:
                line_status = "pending"  # just started — no data expected yet
            else:
                line_status = "active"

        # Even pacing calculation
        if line_status in ("active", "completed") and total_active_days > 0 and elapsed_active_days > 0:
            planned_spend_to_date = (budget / total_active_days) * elapsed_active_days
        else:
            planned_spend_to_date = 0.0

        # Match actual spend: prefer line_code match, then split by flight group
        group_key = (platform_id, flight_start, flight_end)
        actual_spend = 0.0
        if line_code and line_code in spend_by_line_code:
            actual_spend = spend_by_line_code[line_code]
        elif platform_id and group_key in spend_by_group:
            # Split proportionally by budget among lines in the same flight group
            group_total_budget = sum(
                _float(l.get("budget"))
                for l in flight_groups.get(group_key, [])
            )
            if group_total_budget > 0:
                actual_spend = spend_by_group[group_key] * (budget / group_total_budget)

        remaining_budget = budget - actual_spend
        remaining_days = max(0, (flight_end - today).days)
        pacing_pct = (actual_spend / planned_spend_to_date * 100) if planned_spend_to_date > 0 else 0.0
        daily_required = remaining_budget / remaining_days if remaining_days > 0 else None

        is_over = pacing_pct > PACING_OVER_WARNING
        is_under = 0 < pacing_pct < PACING_UNDER_WARNING

        # Only generate alerts for active or completed flights — not for
        # flights that haven't started or are still in their data-lag grace period.
        line_label = line_code or platform_id or line_id
        line_alerts = []
        if line_status in ("active", "completed"):
            line_alerts = _generate_alerts(
                project_code, line_id, line_label,
                pacing_pct, actual_spend, budget,
                remaining_days, remaining_budget,
            )
        all_alerts.extend(line_alerts)

        tracking_rows.append({
            "date": today.isoformat(),
            "project_code": project_code,
            "line_id": line_id,
            "line_code": line_code,
            "platform_id": platform_id,
            "channel_category": line.get("channel_category"),
            "line_status": line_status,
            "planned_budget": budget,
            "planned_spend_to_date": round(planned_spend_to_date, 2),
            "actual_spend_to_date": round(actual_spend, 2),
            "remaining_budget": round(remaining_budget, 2),
            "remaining_days": remaining_days,
            "pacing_percentage": round(pacing_pct, 1),
            "daily_budget_required": round(daily_required, 2) if daily_required is not None else None,
            "is_over_pacing": is_over,
            "is_under_pacing": is_under,
        })

    # ── 5. Write to budget_tracking ─────────────────────────────────
    if tracking_rows:
        _write_budget_tracking(project_code, today, tracking_rows)

    # ── 6. Write alerts ─────────────────────────────────────────────
    if all_alerts:
        _write_alerts(all_alerts)

    logger.info(
        "Pacing for %s: %d lines processed, %d alerts generated",
        project_code, len(tracking_rows), len(all_alerts),
    )

    return {
        "project_code": project_code,
        "lines_processed": len(tracking_rows),
        "alerts": len(all_alerts),
    }


def _write_budget_tracking(project_code: str, as_of: date, rows: list[dict]) -> None:
    """Delete today's rows for this project and insert fresh ones."""
    mtl = bigquery.Client(project=settings.gcp_project_id, location=settings.gcp_region)
    try:
        target = f"{settings.gcp_project_id}.{settings.bigquery_dataset}.budget_tracking"

        mtl.query(
            f"DELETE FROM `{target}` WHERE project_code = @pc AND date = @d",
            job_config=bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("pc", "STRING", project_code),
                bigquery.ScalarQueryParameter("d", "DATE", as_of.isoformat()),
            ]),
        ).result()

        load_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        mtl.load_table_from_json(rows, target, job_config=load_config).result()
        logger.info("  Wrote %d rows to budget_tracking for %s", len(rows), project_code)
    finally:
        mtl.close()


def _deduplicate_alerts(alerts: list[dict]) -> list[dict]:
    """Filter out alerts that already exist (same project, type, severity) in the last 24h."""
    if not alerts:
        return []

    project_codes = list({a["project_code"] for a in alerts if a.get("project_code")})
    if not project_codes:
        return alerts

    params = []
    conditions = []
    for i, pc in enumerate(project_codes):
        pname = f"pc_{i}"
        conditions.append(f"@{pname}")
        params.append(bq.string_param(pname, pc))

    sql = f"""
        SELECT project_code, alert_type, severity
        FROM {bq.table('alerts')}
        WHERE project_code IN ({", ".join(conditions)})
          AND created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
          AND resolved_at IS NULL
    """
    existing = bq.run_query(sql, params)
    existing_keys = {
        (r["project_code"], r["alert_type"], r["severity"])
        for r in existing
    }

    deduped = []
    for alert in alerts:
        key = (alert["project_code"], alert["alert_type"], alert["severity"])
        if key in existing_keys:
            logger.debug("Skipping duplicate alert: %s", key)
            continue
        deduped.append(alert)

    skipped = len(alerts) - len(deduped)
    if skipped:
        logger.info("  Deduplication: skipped %d duplicate alerts out of %d", skipped, len(alerts))
    return deduped


def _write_alerts(alerts: list[dict]) -> None:
    """Insert alert rows into the alerts table after deduplication."""
    alerts = _deduplicate_alerts(alerts)
    if not alerts:
        return

    mtl = bigquery.Client(project=settings.gcp_project_id, location=settings.gcp_region)
    try:
        target = f"{settings.gcp_project_id}.{settings.bigquery_dataset}.alerts"
        load_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        mtl.load_table_from_json(alerts, target, job_config=load_config).result()
        logger.info("  Wrote %d alerts", len(alerts))
    finally:
        mtl.close()


def run_all_active() -> dict:
    """Run pacing for every active project that has a current media plan."""
    projects_sql = f"""
        SELECT DISTINCT p.project_code
        FROM {bq.table('dim_projects')} p
        JOIN {bq.table('media_plans')} mp ON p.project_code = mp.project_code AND mp.is_current = TRUE
        WHERE p.status IN ('active', 'in_flight')
    """
    projects = bq.run_query(projects_sql)

    results = []
    for row in projects:
        code = row["project_code"]
        try:
            r = run_pacing_for_project(code)
            results.append(r)
        except (gcp_exceptions.GoogleCloudError, ValueError, KeyError) as e:
            logger.error("Pacing failed for project %s (continuing to process remaining projects): %s", code, e, exc_info=True)
            results.append({"project_code": code, "status": "failed"})

    total_lines = sum(r.get("lines_processed", 0) for r in results)
    total_alerts = sum(r.get("alerts", 0) for r in results)

    return {
        "status": "success",
        "projects_processed": len(results),
        "total_lines": total_lines,
        "total_alerts": total_alerts,
        "details": results,
    }
