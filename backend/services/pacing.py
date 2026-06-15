"""Pacing engine — the #1 business-critical feature.

Even-pacing model:
    planned_spend_to_date = (line_budget / total_active_days) × elapsed_active_days

Day counts (total + elapsed) come from the line's authoritative flight_start /
flight_end (media-plan detail tab, via PR #67), NOT the blocking chart. The
blocking chart (blocking_chart_weeks) is weekly-granularity and describes
within-week spend distribution only; it must never determine how many days a
line has been active.

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
import re
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

# A line only fires a critical budget_exceeded alert when actual spend is more
# than this many dollars above the line budget. Lifetime-budget delivery and the
# budget-proportional spend split both routinely land a line at exactly 100% of
# budget (or a sub-cent floating-point hair over), which previously tripped a
# spurious "Budget exceeded — actual $239 exceeds budget $239" critical. The
# buffer suppresses that noise while still catching any genuine dollar overage.
BUDGET_EXCEEDED_TOLERANCE = 1.0

# Post-flight reconciliation window: how many days after a project's booked
# end_date the daily sweep keeps re-pacing it. Ad-platform spend for the final
# 1-3 days of a flight typically lands in fact_digital_daily 1-2 days after the
# campaign ends (Funnel reporting lag). Without this window, _auto_complete_projects
# flips the project to 'completed' the morning after end_date and it drops out of
# run_all_active, freezing budget_tracking at a partial-spend snapshot.
POST_FLIGHT_RECONCILE_DAYS = 7


def _float(v, default=0.0) -> float:
    if v is None:
        return default
    return float(v) if not isinstance(v, float) else v


# ── Ad-set → line audience attribution ──────────────────────────────
# Meta (and other ad-platform) rows in fact_digital_daily carry an ad_set_name
# but no usable line_code: the scalar column is never populated, and
# vw_fact_digital_daily derives line_codes from the ad set's leading number,
# which collides across campaigns (a Conversion and a Reach campaign each number
# their ad sets 01/02). When line_code attribution yields nothing for a line, we
# match the ad set's audience text to the plan line's audience_name so per-line
# spend is a real measurement instead of a budget-weighted estimate. Ad sets that
# match no line stay unattributed and flow into the existing budget-weight group
# split, so no spend is ever dropped.

# Pure structural/stopword tokens that must never carry a match on their own.
_AUDIENCE_STOPWORDS = frozenset({
    "the", "and", "of", "a", "an", "in", "on", "for", "to", "with",
    "at", "by", "or", "ad", "ads", "set", "audience", "targeting",
})


def _audience_tokens(s: str | None) -> set[str]:
    """Normalise a label to a set of significant tokens: lowercase, split on
    non-alphanumerics, drop pure numbers + stopwords, and singularise (strip a
    trailing 's' on tokens longer than 3) so 'Lookalikes' == 'Lookalike' and
    'Attendees' == 'Attendee'."""
    if not s:
        return set()
    out: set[str] = set()
    for tok in re.split(r"[^a-z0-9]+", s.lower()):
        if not tok or tok.isdigit() or tok in _AUDIENCE_STOPWORDS:
            continue
        if len(tok) > 3 and tok.endswith("s"):
            tok = tok[:-1]
        out.add(tok)
    return out


def _match_adset_to_line_id(
    ad_set_name: str | None, candidates: list[dict]
) -> str | None:
    """Return the line_id whose audience_name best matches ``ad_set_name``, or
    None when there is no confident, unambiguous match.

    Score = |audience_tokens ∩ adset_tokens| / |audience_tokens| (how much of the
    plan line's audience label appears in the ad set name). A match requires:
      • score ≥ 0.5, and
      • at least 2 overlapping tokens OR a full (100%) audience match — so a
        single generic shared token (e.g. 'list') can't carry a match, and
      • a strict single winner — ties are treated as ambiguous → no match.
    ``candidates`` should exclude bundle children (their spend is handled by the
    bundle aggregate)."""
    adset = _audience_tokens(ad_set_name)
    if not adset:
        return None
    scored: list[tuple[float, int, str]] = []
    for line in candidates:
        aud = _audience_tokens(line.get("audience_name"))
        if not aud:
            continue
        overlap = len(aud & adset)
        if overlap == 0:
            continue
        scored.append((overlap / len(aud), overlap, line["line_id"]))
    if not scored:
        return None
    scored.sort(reverse=True)
    top_score, top_overlap, top_line_id = scored[0]
    if top_score < 0.5:
        return None
    if top_overlap < 2 and top_score < 0.999:
        return None
    if len(scored) > 1 and scored[1][0] == top_score:
        return None  # ambiguous — two lines match the ad set equally well
    return top_line_id


def _count_active_days(
    blocking_weeks: list[dict],
    flight_start: date,
    flight_end: date,
    as_of_date: date,
) -> tuple[int, int]:
    """Return (total_active_days, elapsed_active_days up to ``as_of_date``).

    NOTE: as of the flight-date day-count switch, ``run_pacing_for_project`` no
    longer calls this for the headline pacing baseline — day counts come from
    flight_start/flight_end. Retained for an optional within-week weighting pass
    over the blocking chart.

    Each blocking_chart_weeks row represents a 7-day window starting at
    week_start. We clamp to the flight start/end and to ``as_of_date`` (in
    live mode this is today; in retrospective replay it's the replay date
    so elapsed never peeks past the snapshot point).
    """
    today = as_of_date
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

    if planned_budget > 0 and actual > planned_budget + BUDGET_EXCEEDED_TOLERANCE:
        overage = actual - planned_budget
        _alert(
            "budget_exceeded", "critical",
            f"Budget exceeded — {line_label}",
            f"Actual spend ${actual:,.2f} is ${overage:,.2f} "
            f"({overage / planned_budget * 100:.1f}%) over the ${planned_budget:,.2f} budget",
            {"line_id": line_id, "actual": actual, "budget": planned_budget,
             "overage": overage},
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


def _sync_in_progress(project_code: str) -> bool:
    """True when a media-plan sync is currently rewriting this project's lines.

    Set by media_plan_sync._acquire_sync_lock at the start of a sync, cleared
    at the end. Pacing skips a locked project so it never reads a half-written
    media_plan_lines state (the 26023 zero-spend incident: a pace ran mid-sync,
    saw the direct buys momentarily is_direct=NULL + no Meta baseline, and wrote
    a snapshot that read as zero). The 10-minute bound auto-expires a stale lock
    if a sync crashed without releasing. Fail-open: if the lock table is
    unavailable, pacing proceeds (best-effort guard, never a hard block).
    """
    try:
        rows = bq.run_query(
            f"""
            SELECT 1 FROM {bq.table('pacing_sync_locks')}
            WHERE project_code = @pc
              AND locked_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE)
            LIMIT 1
            """,
            [bq.string_param("pc", project_code)],
        )
        return bool(rows)
    except Exception as e:
        logger.warning("Sync-lock check failed for %s (proceeding): %s", project_code, e)
        return False


def run_pacing_for_project(
    project_code: str,
    as_of_date: date,
    skip_writes: bool = False,
) -> dict:
    """Calculate pacing for every media plan line in a project as of ``as_of_date``.

    ``as_of_date`` is REQUIRED (as of ADAC-51 commit 3). Live callers pass
    ``date.today()``; retrospective callers pass the replay date so spend
    queries, elapsed-active-days, and the `today < flight_start` branching
    stay consistent with the point-in-time view.

    ``skip_writes=True`` short-circuits both the ``budget_tracking`` write
    and the alert write. Retrospective callers set this so a replay doesn't
    pollute live tracking history or fire Slack alerts about yesterday's
    state. See ``backend.services.snapshots`` (ADAC-51 commit 4) for the
    find-or-compute wrapper that uses this.

    Returns a summary dict with line-level results and any alerts generated.
    """
    today = as_of_date

    # Race guard: skip if a media-plan sync is mid-flight for this project, so
    # we never pace against a half-written media_plan_lines state. Only on the
    # live write path — retrospective replay (skip_writes) is read-only and
    # never persists a snapshot, so it can compute regardless.
    if not skip_writes and _sync_in_progress(project_code):
        logger.warning(
            "Pacing skipped for %s: media-plan sync in progress; "
            "preserving the prior budget_tracking snapshot", project_code,
        )
        return {
            "project_code": project_code,
            "lines_processed": 0,
            "alerts": 0,
            "lines": [],
            "skipped": "sync_in_progress",
        }

    # ── 1. Fetch media plan lines for this project ──────────────────
    # Deduplicate: if old sync versions weren't cleaned up, multiple rows
    # per line_id can exist.  Keep only the latest sync_version per line_id
    # to prevent doubled lines and halved spend in proportional splits.
    # Multi-plan support (2026-04-25): the JOIN through project_media_plans
    # restricts pacing to lines whose sheet is registered + active for the
    # project. Aggregates correctly across every active phase of a multi-flight
    # campaign. Retired phases (is_active=FALSE) drop out without losing data.
    lines_sql = f"""
        SELECT line_id, line_code, platform_id, channel_category,
               site_network, budget, flight_start, flight_end,
               bundle_id, bundle_role, audience_name
        FROM (
            SELECT
                l.line_id,
                l.line_code,
                l.audience_name,
                l.platform_id,
                l.channel_category,
                l.site_network,
                l.budget,
                l.flight_start,
                l.flight_end,
                l.bundle_id,
                l.bundle_role,
                ROW_NUMBER() OVER (
                    PARTITION BY l.line_id
                    ORDER BY l.sync_version DESC
                ) AS _rn
            FROM {bq.table('media_plan_lines')} l
            JOIN {bq.table('media_plans')} p
              ON l.plan_id = p.plan_id AND p.is_current = TRUE
            JOIN {bq.table('project_media_plans')} pmp
              ON p.project_code = pmp.project_code
             AND p.sheet_id   = pmp.sheet_id
             AND pmp.is_active = TRUE
            WHERE l.project_code = @project_code
                -- Pacing inclusion is governed by TRACKABILITY, not media type.
                -- A line paces iff it has a self-serve spend feed, i.e. is NOT a
                -- direct buy (is_direct). The old `is_traditional = FALSE` clause
                -- was removed: is_traditional is a keyword-based MEDIA-TYPE label
                -- (kept as an informational column) and was wrongly excluding a
                -- recognised-platform line whose label merely reads "traditional"
                -- — e.g. 26023's DOOH, now StackAdapt-backed (is_direct=FALSE),
                -- which must pace once StackAdapt DOOH spend flows. Verified safe:
                -- across all media_plan_lines exactly ONE recognised traditional
                -- line exists (26023 DOOH), so dropping the filter changes only
                -- that intended line.
                --
                -- is_direct buys (no self-serve feed) are excluded — they can
                -- never produce budget_tracking rows or pacing alarms. NULL is
                -- ALSO excluded (not paced): a NULL means the line is either
                -- unclassified or mid-sync, and pacing a transiently-NULL line
                -- is exactly what wrote the 26023 zero-spend snapshot (a pace
                -- ran while a sync had the direct buys momentarily is_direct=NULL
                -- and paced them). Every active project has been re-synced, so no
                -- legitimate NULL remains. The effective value is
                -- COALESCE(is_direct_override, is_direct): a user's manual
                -- override wins over auto-classification, so only an effective
                -- FALSE paces (and NULL/NULL stays excluded).
                AND COALESCE(l.is_direct_override, l.is_direct) = FALSE
        )
        WHERE _rn = 1
    """
    lines = bq.run_query(lines_sql, [bq.string_param("project_code", project_code)])

    if not lines:
        logger.info("No media plan lines found for project %s", project_code)
        return {
            "project_code": project_code,
            "lines_processed": 0,
            "alerts": 0,
            "lines": [],
        }

    line_ids = [r["line_id"] for r in lines]

    # ── 2. Fetch blocking chart weeks for all lines at once ─────────
    # Finding 6: dedup by latest sync_version per (line_id, week_start), mirroring
    # the media_plan_lines read above. blocking_chart_weeks carries a sync_version
    # (added by migration; written by _write_records_with_version). If
    # _delete_old_versions ever fails to clear a prior sync's rows, duplicate
    # weeks would otherwise inflate _count_active_days while the (deduped) budget
    # stays correct, desyncing the two inputs to the even-pacing arithmetic.
    blocking_sql = f"""
        SELECT line_id, week_start, is_active FROM (
            SELECT line_id, week_start, is_active,
                ROW_NUMBER() OVER (
                    PARTITION BY line_id, week_start ORDER BY sync_version DESC
                ) AS _rn
            FROM {bq.table('blocking_chart_weeks')}
            WHERE project_code = @project_code
        )
        WHERE _rn = 1
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
    spend_by_line_audience: dict[str, float] = {}    # line_id → ad-set-name-matched spend
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

        # All spend queries carry an ``AND date <= @as_of_date`` clamp so
        # retrospective replay never peeks past the snapshot point.
        # In live mode this is a no-op (fact_digital_daily has no future rows).
        params.append(bq.date_param("as_of_date", today))
        if fs is not None and fe is not None:
            group_spend_sql = f"""
                SELECT SUM(spend) AS total_spend
                FROM {bq.table('fact_digital_daily')}
                WHERE project_code = @project_code
                    AND platform_id = @platform_id
                    AND date >= @flight_start
                    AND date <= @flight_end
                    AND date <= @as_of_date
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
                    AND date <= @as_of_date
            """

        rows_result = bq.run_query(group_spend_sql, params)
        spend_by_group[(pid, fs, fe)] = _float(rows_result[0]["total_spend"]) if rows_result else 0.0

        # Also fetch spend by line_code within this flight window. We query
        # vw_fact_digital_daily (not fact_digital_daily directly) — the view
        # exposes `line_codes ARRAY<STRING>` derived from ad_set_name, which
        # is how line_code attribution actually works. (The scalar `line_code`
        # column on fact_digital_daily itself is never populated.)
        line_codes = [l["line_code"] for l in group_lines if l.get("line_code")]
        for lc in line_codes:
            lc_params = [
                bq.string_param("project_code", project_code),
                bq.string_param("line_code", lc),
                bq.date_param("as_of_date", today),
            ]
            if fs is not None and fe is not None:
                lc_sql = f"""
                    SELECT SUM(spend) AS total_spend, MIN(date) AS first_spend_date
                    FROM {bq.table('vw_fact_digital_daily')}
                    WHERE project_code = @project_code
                        AND @line_code IN UNNEST(line_codes)
                        AND date >= @flight_start
                        AND date <= @flight_end
                        AND date <= @as_of_date
                        AND spend > 0
                """
                lc_params.append(bq.date_param("flight_start", fs))
                lc_params.append(bq.date_param("flight_end", fe))
            else:
                lc_sql = f"""
                    SELECT SUM(spend) AS total_spend, MIN(date) AS first_spend_date
                    FROM {bq.table('vw_fact_digital_daily')}
                    WHERE project_code = @project_code
                        AND @line_code IN UNNEST(line_codes)
                        AND date <= @as_of_date
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

        # ── 3a. Audience-name attribution (ad_set_name → line) ──────
        # Pull this group's per-ad-set spend and match each ad set to a plan
        # line by audience text. Matched spend is attributed directly (a
        # measurement); unmatched ad sets stay in spend_by_group and flow
        # through the budget-weight residual split below, so nothing is lost.
        # Bundle children are excluded — their spend is handled by the bundle
        # aggregate, not by an audience match.
        audience_candidates = [
            l for l in group_lines
            if l.get("bundle_role") not in ("suggested_child", "confirmed_child")
            and l.get("audience_name")
        ]
        if audience_candidates:
            if fs is not None and fe is not None:
                adset_sql = f"""
                    SELECT ad_set_name, SUM(spend) AS spend
                    FROM {bq.table('fact_digital_daily')}
                    WHERE project_code = @project_code
                        AND platform_id = @platform_id
                        AND date >= @flight_start
                        AND date <= @flight_end
                        AND date <= @as_of_date
                        AND spend > 0
                    GROUP BY ad_set_name
                """
            else:
                adset_sql = f"""
                    SELECT ad_set_name, SUM(spend) AS spend
                    FROM {bq.table('fact_digital_daily')}
                    WHERE project_code = @project_code
                        AND platform_id = @platform_id
                        AND date <= @as_of_date
                        AND spend > 0
                    GROUP BY ad_set_name
                """
            for ar in bq.run_query(adset_sql, params):
                matched_id = _match_adset_to_line_id(
                    ar.get("ad_set_name"), audience_candidates
                )
                if matched_id:
                    spend_by_line_audience[matched_id] = (
                        spend_by_line_audience.get(matched_id, 0.0)
                        + _float(ar.get("spend"))
                    )

    # ── 3b. Bundle spend aggregation (PR 4) ─────────────────────────
    # For each bundle, query TOTAL spend where ANY of the bundle's member
    # line codes appears in the ad set's line_codes array. This is the only
    # safe way to attribute spend for CBO-style bundles — summing per-code
    # spend would double-count ad sets labelled with multiple codes (e.g.
    # "#11 viewers BC, #12 list" contributing once to #11 and once to #12).
    bundle_spend: dict[str, float] = {}
    bundle_member_codes: dict[str, list[str]] = {}
    bundle_parent_line: dict[str, dict] = {}
    # Finding 7: a bundle's spend window must span ALL members, not just the
    # parent's flight. A child can run earlier/longer than the parent; clamping
    # to the parent's window would drop that out-of-window delivery from the
    # bundle total and under-report the bundle's spend.
    bundle_member_flights: dict[str, list[tuple]] = {}
    for line in lines:
        bid = line.get("bundle_id")
        if not bid:
            continue
        lc = line.get("line_code")
        if lc:
            bundle_member_codes.setdefault(bid, []).append(lc)
        if line.get("bundle_role") in ("suggested_parent", "confirmed_parent"):
            bundle_parent_line[bid] = line
        m_fs = line.get("flight_start")
        m_fe = line.get("flight_end")
        if isinstance(m_fs, str):
            m_fs = date.fromisoformat(m_fs)
        if isinstance(m_fe, str):
            m_fe = date.fromisoformat(m_fe)
        bundle_member_flights.setdefault(bid, []).append((m_fs, m_fe))

    for bid, member_codes in bundle_member_codes.items():
        if not member_codes:
            continue
        parent = bundle_parent_line.get(bid)
        if not parent:
            logger.warning(
                "Bundle %s has member line_codes but no parent; skipping "
                "bundle spend aggregation", bid,
            )
            continue
        # Span the bundle window across all members (Finding 7), falling back to
        # the parent's flight when a member carries no dates.
        member_flights = bundle_member_flights.get(bid, [])
        starts = [f[0] for f in member_flights if f[0] is not None]
        ends = [f[1] for f in member_flights if f[1] is not None]
        pfs = min(starts) if starts else parent.get("flight_start")
        pfe = max(ends) if ends else parent.get("flight_end")
        if isinstance(pfs, str):
            pfs = date.fromisoformat(pfs)
        if isinstance(pfe, str):
            pfe = date.fromisoformat(pfe)

        bundle_params = [
            bq.string_param("project_code", project_code),
            bq.array_param("member_codes", "STRING", member_codes),
            bq.date_param("as_of_date", today),
        ]
        if pfs is not None and pfe is not None:
            bundle_sql = f"""
                SELECT SUM(spend) AS total_spend
                FROM {bq.table('vw_fact_digital_daily')}
                WHERE project_code = @project_code
                  AND EXISTS (
                    SELECT 1 FROM UNNEST(line_codes) AS lc
                    WHERE lc IN UNNEST(@member_codes)
                  )
                  AND date >= @flight_start
                  AND date <= @flight_end
                  AND date <= @as_of_date
                  AND spend > 0
            """
            bundle_params.append(bq.date_param("flight_start", pfs))
            bundle_params.append(bq.date_param("flight_end", pfe))
        else:
            bundle_sql = f"""
                SELECT SUM(spend) AS total_spend
                FROM {bq.table('vw_fact_digital_daily')}
                WHERE project_code = @project_code
                  AND EXISTS (
                    SELECT 1 FROM UNNEST(line_codes) AS lc
                    WHERE lc IN UNNEST(@member_codes)
                  )
                  AND date <= @as_of_date
                  AND spend > 0
            """
        bundle_rows = bq.run_query(bundle_sql, bundle_params)
        bundle_spend[bid] = (
            _float(bundle_rows[0]["total_spend"]) if bundle_rows else 0.0
        )

    # Finding 5: residual group-split pool. The platform group-split fallback
    # apportions spend_by_group across lines that have no bundle/line_code spend.
    # But spend_by_group is the FULL platform total, so it already includes spend
    # claimed by line_code-matched siblings and by bundle parents. Splitting the
    # full total double-counts that claimed spend. Precompute, per flight group,
    # the spend already claimed (split only the residual) and the budget of the
    # still-unattributed lines (split only across them).
    group_residual_spend: dict[tuple, float] = {}
    group_residual_pool_budget: dict[tuple, float] = {}
    for gkey, glines in flight_groups.items():
        claimed = 0.0
        pool_budget = 0.0
        seen_bundles: set = set()
        for gl in glines:
            role = gl.get("bundle_role")
            if role in ("suggested_child", "confirmed_child"):
                continue
            gbid = gl.get("bundle_id")
            glc = gl.get("line_code")
            glc_spend = spend_by_line_code.get(glc, 0.0) if glc else 0.0
            ga_spend = spend_by_line_audience.get(gl["line_id"], 0.0)
            gb_spend = (
                bundle_spend.get(gbid, 0.0)
                if gbid and role in ("suggested_parent", "confirmed_parent")
                else 0.0
            )
            if gb_spend > 0:
                if gbid not in seen_bundles:
                    claimed += gb_spend
                    seen_bundles.add(gbid)
            elif glc_spend > 0:
                claimed += glc_spend
            elif ga_spend > 0:
                claimed += ga_spend
            else:
                pool_budget += _float(gl.get("budget"))
        group_residual_spend[gkey] = max(0.0, spend_by_group.get(gkey, 0.0) - claimed)
        group_residual_pool_budget[gkey] = pool_budget

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
        bundle_id = line.get("bundle_id")
        bundle_role = line.get("bundle_role")

        # PR 4: Bundle children inherit pacing from their parent. They have
        # budget=NULL by design; the parent's row carries the shared pool.
        # Skip them explicitly so downstream accounting (alerts, tracking
        # rows) stays clean instead of relying on the budget<=0 guard below.
        if bundle_role in ("suggested_child", "confirmed_child"):
            continue

        if not flight_start or not flight_end or budget <= 0:
            continue

        if isinstance(flight_start, str):
            flight_start = date.fromisoformat(flight_start)
        if isinstance(flight_end, str):
            flight_end = date.fromisoformat(flight_end)

        # Day counts come straight from the authoritative flight dates (detail
        # tab, via PR #67) — NEVER the blocking chart. The chart is weekly-
        # granularity and only models within-week spend distribution; deriving
        # the day count from its active-week grid let a Monday-aligned grid that
        # started a week after flight_start understate elapsed days and produce
        # false overpacing (26023 Meta read 250% on day 5). total_active_days is
        # the full flight span; elapsed is flight_start→today, clamped to the
        # flight. (blocking_by_line stays fetched above, available for an optional
        # within-week weighting pass; it no longer drives the day count.)
        total_active_days = (flight_end - flight_start).days + 1
        elapsed_days_raw = (min(today, flight_end) - flight_start).days + 1
        elapsed_active_days = max(0, min(elapsed_days_raw, total_active_days))

        # Match actual spend FIRST: bundle > line_code > audience-name match >
        # flight-group split. Computed before line_status because the grace-
        # period check below now keys off whether the line has ANY attributed
        # spend, not just line_code-attributed spend.
        group_key = (platform_id, flight_start, flight_end)
        actual_spend = 0.0
        if bundle_role in ("suggested_parent", "confirmed_parent") and bundle_id:
            # Bundle parent — use the set-containment aggregate so multi-code
            # ad sets don't double-count. Falls back to line_code match, then
            # group-split, in case bundle attribution is unresolvable.
            actual_spend = bundle_spend.get(bundle_id, 0.0)
            if actual_spend == 0.0 and line_code and line_code in spend_by_line_code:
                actual_spend = spend_by_line_code[line_code]
        elif line_code and line_code in spend_by_line_code:
            actual_spend = spend_by_line_code[line_code]
        # Audience-name match (ad_set_name → this line's audience_name): used when
        # bundle + line_code attribution found nothing for the line, but BEFORE
        # the budget-weight group split, so a line with a matching ad set gets a
        # real measurement instead of a proportional estimate.
        if actual_spend == 0.0 and spend_by_line_audience.get(line_id, 0.0) > 0:
            actual_spend = spend_by_line_audience[line_id]
        if actual_spend == 0.0 and platform_id and group_key in spend_by_group:
            # Split only the RESIDUAL (unclaimed) group spend across only the
            # still-unattributed lines, so spend already claimed by line_code or
            # bundle siblings is not double-counted (Finding 5; see precompute).
            pool_budget = group_residual_pool_budget.get(group_key, 0.0)
            if pool_budget > 0:
                actual_spend = group_residual_spend.get(group_key, 0.0) * (
                    budget / pool_budget
                )

        # Determine line status based on flight timing.
        # Data lag: ad platforms report with ~1-day delay through Funnel,
        # and the server runs in UTC which can be a day ahead of Eastern.
        # Grace period: a line is held "pending" ONLY while it has no
        # attributed spend at all AND the flight started within 2 days. As soon
        # as ANY spend is attributed — line_code, bundle, OR the platform
        # group-split fallback — the line is "active" so its planned spend is
        # computed and it counts toward the project pacing aggregate.
        #
        # Prior bug (26023): the grace check recognised only line_code-
        # attributed spend (first_spend_date_by_line). A line delivering under
        # the group-split fallback — the common case while line_code attribution
        # is still landing — stayed "pending" with planned_spend_to_date=0 for
        # its first 2 days, which zeroed overall_pacing_percentage and rendered
        # the whole project "DARK / no data" on the Summary tab despite real
        # spend (header showed Spent $988 sourced straight from the fact table).
        if today < flight_start:
            line_status = "not_started"
        elif today > flight_end:
            line_status = "completed"
        elif actual_spend <= 0 and (today - flight_start).days <= 2:
            line_status = "pending"  # just started — no data has landed yet
        else:
            line_status = "active"

        # Even pacing across the authoritative flight window. Because the day
        # counts above are flight-date based (not grid based), this single
        # expression is the whole baseline: the old blocking-grid "floor"
        # fallback is gone, since the grid can no longer understate the day count
        # and drop a live line out of the pacing denominator.
        if line_status in ("active", "completed") and total_active_days > 0 and elapsed_active_days > 0:
            planned_spend_to_date = (budget / total_active_days) * elapsed_active_days
        else:
            planned_spend_to_date = 0.0

        remaining_budget = budget - actual_spend
        remaining_days = max(0, (flight_end - today).days)
        pacing_pct = (actual_spend / planned_spend_to_date * 100) if planned_spend_to_date > 0 else 0.0
        daily_required = remaining_budget / remaining_days if remaining_days > 0 else None

        is_over = pacing_pct > PACING_OVER_WARNING
        is_under = 0 < pacing_pct < PACING_UNDER_WARNING

        # Alert generation preserves the original data-lag suppression: during
        # the first 2 days of a flight, before any line_code-attributed spend
        # has landed, hold off on pacing alerts (early pacing numbers are
        # unreliable under reporting lag). This is now decoupled from
        # line_status so the line still displays real pacing while staying
        # quiet — previously the suppression rode on line_status="pending",
        # which also hid the line from the dashboard (the 26023 bug above).
        has_linecode_spend = (
            (line_code and line_code in first_spend_date_by_line) or
            (platform_id and any(
                lc in first_spend_date_by_line
                for lc in spend_by_line_code.keys()
            ))
        )
        suppress_early_alerts = (
            (today - flight_start).days <= 2 and not has_linecode_spend
        )
        line_label = line_code or platform_id or line_id
        line_alerts = []
        if line_status in ("active", "completed") and not suppress_early_alerts:
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
            # PR 4: surface bundle context for the UI's expandable bundle card.
            "bundle_id": bundle_id,
            "bundle_role": bundle_role,
        })

    # ── 5. Write to budget_tracking ─────────────────────────────────
    # Retrospective replay uses skip_writes=True: we don't want a snapshot
    # reconstruction to backfill budget_tracking with reconstructed rows
    # (the live pipeline's row for that date is the source of truth), and
    # we don't want to page anyone about last week's pacing via Slack.
    if tracking_rows and not skip_writes:
        _write_budget_tracking(project_code, today, tracking_rows)

    # ── 6. Write alerts ─────────────────────────────────────────────
    if all_alerts and not skip_writes:
        _write_alerts(all_alerts)

    logger.info(
        "Pacing for %s: %d lines processed, %d alerts generated",
        project_code, len(tracking_rows), len(all_alerts),
    )

    return {
        "project_code": project_code,
        "lines_processed": len(tracking_rows),
        "alerts": len(all_alerts),
        # AI-070/072: expose the computed per-line rows so the retrospective
        # read path (routers/pacing.py compute-on-miss) can serve a replay
        # when budget_tracking has no stored snapshot for the requested date.
        # Shape matches the budget_tracking row schema written in step 5.
        # Existing callers (daily_job, /run endpoints, retro router) only
        # read lines_processed/alerts — additive and backward-compatible.
        "lines": tracking_rows,
    }


_BUDGET_TRACKING_MIGRATED = False


def _ensure_budget_tracking_schema(mtl: bigquery.Client) -> None:
    """Idempotent ALTER TABLE to keep budget_tracking in sync with tracking_rows.

    infrastructure/bigquery/schema.sql is the desired state for new installs
    but the prod table has drifted (line_status was added without updating
    the file). Rather than fix the drift separately, we enforce required
    columns here on every startup. Cheap and self-healing.
    """
    global _BUDGET_TRACKING_MIGRATED
    if _BUDGET_TRACKING_MIGRATED:
        return
    prefix = f"`{settings.gcp_project_id}.{settings.bigquery_dataset}"
    stmts = [
        f"ALTER TABLE {prefix}.budget_tracking` ADD COLUMN IF NOT EXISTS line_status STRING",
        # PR 5: bundle context carried through from media_plan_lines so the
        # UI can render bundle cards without a separate query.
        f"ALTER TABLE {prefix}.budget_tracking` ADD COLUMN IF NOT EXISTS bundle_id STRING",
        f"ALTER TABLE {prefix}.budget_tracking` ADD COLUMN IF NOT EXISTS bundle_role STRING",
    ]
    for sql in stmts:
        try:
            mtl.query(sql).result()
        except Exception as e:
            if "Already Exists" in str(e) or "Duplicate" in str(e):
                pass
            else:
                logger.warning("  budget_tracking schema migration warning: %s", e)
    _BUDGET_TRACKING_MIGRATED = True


def _write_budget_tracking(project_code: str, as_of: date, rows: list[dict]) -> None:
    """Delete today's rows for this project, purge orphans, and insert fresh ones."""
    mtl = bigquery.Client(project=settings.gcp_project_id, location=settings.gcp_region)
    try:
        _ensure_budget_tracking_schema(mtl)
        target = f"{settings.gcp_project_id}.{settings.bigquery_dataset}.budget_tracking"
        mpl_table = f"{settings.gcp_project_id}.{settings.bigquery_dataset}.media_plan_lines"

        # Delete today's rows (existing behavior)
        mtl.query(
            f"DELETE FROM `{target}` WHERE project_code = @pc AND date = @d",
            job_config=bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("pc", "STRING", project_code),
                bigquery.ScalarQueryParameter("d", "DATE", as_of.isoformat()),
            ]),
        ).result()

        # Purge orphaned rows whose line_id no longer exists in media_plan_lines.
        # This prevents stale data from accumulating after plan resyncs where
        # line_ids change (new plan_id = new line_id prefix).
        orphan_result = mtl.query(
            f"""DELETE FROM `{target}`
            WHERE project_code = @pc
              AND line_id NOT IN (
                SELECT line_id FROM `{mpl_table}`
                WHERE project_code = @pc
              )""",
            job_config=bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("pc", "STRING", project_code),
            ]),
        ).result()
        orphans_deleted = orphan_result.num_dml_affected_rows or 0
        if orphans_deleted > 0:
            logger.info(
                "  Purged %d orphaned budget_tracking rows for %s",
                orphans_deleted, project_code,
            )

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


def run_all_active(as_of_date: date, skip_writes: bool = False) -> dict:
    """Run pacing for every active project that has a current media plan.

    ``as_of_date`` is REQUIRED (as of ADAC-51 commit 3). Live callers pass
    ``date.today()``; retrospective batch callers pass the replay date.
    ``skip_writes`` is forwarded to each per-project call.
    """
    # Multi-plan support (2026-04-25): a project counts as having a media plan
    # if at least one of its registered, active sheets has a current row in
    # media_plans. Same JOIN shape as the per-project dedup guard.
    #
    # Post-flight reconciliation (2026-06-08): we also pick up projects that
    # completed within the last POST_FLIGHT_RECONCILE_DAYS days. run_pacing_for_project
    # already handles 'completed' line_status correctly, so re-running it after the
    # flight ends simply overwrites the snapshot with the now-complete actual spend
    # once trailing Funnel data has landed. See the 26018 CAPE spend-mismatch fix.
    projects_sql = f"""
        SELECT DISTINCT p.project_code
        FROM {bq.table('dim_projects')} p
        JOIN {bq.table('media_plans')} mp
          ON p.project_code = mp.project_code AND mp.is_current = TRUE
        JOIN {bq.table('project_media_plans')} pmp
          ON mp.project_code = pmp.project_code
         AND mp.sheet_id   = pmp.sheet_id
         AND pmp.is_active = TRUE
        WHERE p.status IN ('active', 'in_flight')
           OR (
                p.status = 'completed'
                AND p.end_date IS NOT NULL
                AND p.end_date >= DATE_SUB(@as_of_date, INTERVAL {POST_FLIGHT_RECONCILE_DAYS} DAY)
              )
    """
    projects = bq.run_query(projects_sql, [bq.date_param("as_of_date", as_of_date)])

    results = []
    for row in projects:
        code = row["project_code"]
        try:
            r = run_pacing_for_project(code, as_of_date, skip_writes=skip_writes)
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
