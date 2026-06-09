"""Spend charts for pacing alerts.

For an over/under-spend alert we render two PNGs and upload them to GCS so the
dispatcher can embed them as Slack image blocks *inside* the alert's coloured
border:

  1. last-7-days daily spend (bar)
  2. cumulative spend vs budget, with a trailing-7-day-rate projection to flight
     end (the gap to the budget line is exactly what the alert is flagging)

Everything here is best-effort. If the bucket isn't configured, matplotlib or
the GCS client is unavailable, or any query/render/upload fails,
``build_alert_chart_blocks`` returns ``[]`` and the alert still sends without
charts. Chart generation must never block or fail an alert.

Per-line daily spend mirrors the pacing engine's attribution: the line_code
series from ``vw_fact_digital_daily`` when the ad sets are tagged with the
``#code``, otherwise the platform-group daily spend scaled by the line's budget
share — so the chart agrees with the number in the alert body.
"""

import io
import json
import logging
import uuid
from datetime import date, timedelta

from backend.config import settings
from backend.services import bigquery_client as bq

logger = logging.getLogger(__name__)

# Alert types that get spend charts attached.
CHART_ALERT_TYPES = {"budget_exceeded", "pacing_over", "pacing_under"}
TRAILING_DAYS = 7

# Chart palette (kept in sync with the alert design; Claude Design owns final UI).
_C_ACTUAL = "#185FA5"   # blue
_C_PROJECT = "#BA7517"  # amber
_C_BUDGET = "#888780"   # grey
_C_BAR = "#1D9E75"      # teal


def _line_for_chart(line_id: str) -> dict | None:
    """Chart-relevant media-plan line fields (dedup guard: latest sync, current plan)."""
    rows = bq.run_query(
        f"""
        SELECT project_code, line_id, line_code, platform_id, budget,
               flight_start, flight_end
        FROM (
            SELECT l.project_code, l.line_id, l.line_code, l.platform_id, l.budget,
                   l.flight_start, l.flight_end,
                   ROW_NUMBER() OVER (
                       PARTITION BY l.line_id ORDER BY l.sync_version DESC
                   ) AS _rn
            FROM {bq.table('media_plan_lines')} l
            JOIN {bq.table('media_plans')} p
              ON l.plan_id = p.plan_id AND p.is_current = TRUE
            WHERE l.line_id = @line_id
        )
        WHERE _rn = 1
        """,
        [bq.string_param("line_id", line_id)],
    )
    return rows[0] if rows else None


def _group_budget(line: dict) -> float:
    """Total budget of the lines sharing this line's platform + flight window."""
    rows = bq.run_query(
        f"""
        SELECT SUM(budget) AS b FROM (
            SELECT l.budget,
                   ROW_NUMBER() OVER (
                       PARTITION BY l.line_id ORDER BY l.sync_version DESC
                   ) AS _rn
            FROM {bq.table('media_plan_lines')} l
            JOIN {bq.table('media_plans')} p
              ON l.plan_id = p.plan_id AND p.is_current = TRUE
            WHERE l.project_code = @pc AND l.platform_id = @pid
              AND l.flight_start = @fs AND l.flight_end = @fe
        )
        WHERE _rn = 1
        """,
        [bq.string_param("pc", line["project_code"]),
         bq.string_param("pid", line["platform_id"]),
         bq.date_param("fs", line["flight_start"]),
         bq.date_param("fe", line["flight_end"])],
    )
    return float(rows[0]["b"]) if rows and rows[0]["b"] else 0.0


def _daily_spend(line: dict, as_of: date) -> list[tuple[date, float]]:
    """Per-line daily spend, one entry per day from flight_start..min(as_of, flight_end)."""
    pc, pid, code = line["project_code"], line["platform_id"], line.get("line_code")
    fs, fe = line["flight_start"], line["flight_end"]
    end = min(as_of, fe)

    rows = []
    if code:
        rows = bq.run_query(
            f"""
            SELECT date, SUM(spend) AS spend
            FROM {bq.table('vw_fact_digital_daily')}
            WHERE project_code = @pc AND @code IN UNNEST(line_codes)
              AND date BETWEEN @fs AND @end
            GROUP BY date
            """,
            [bq.string_param("pc", pc), bq.string_param("code", code),
             bq.date_param("fs", fs), bq.date_param("end", end)],
        )

    share = 1.0
    if not rows:  # attribution miss → platform-group daily scaled by budget share
        rows = bq.run_query(
            f"""
            SELECT date, SUM(spend) AS spend
            FROM {bq.table('fact_digital_daily')}
            WHERE project_code = @pc AND platform_id = @pid
              AND date BETWEEN @fs AND @end
            GROUP BY date
            """,
            [bq.string_param("pc", pc), bq.string_param("pid", pid),
             bq.date_param("fs", fs), bq.date_param("end", end)],
        )
        gb = _group_budget(line)
        share = (float(line["budget"]) / gb) if gb > 0 else 1.0

    by_date = {r["date"]: float(r["spend"] or 0) * share for r in rows}
    out, d = [], fs
    while d <= end:
        out.append((d, by_date.get(d, 0.0)))
        d += timedelta(days=1)
    return out


def _project(daily: list[tuple[date, float]], flight_end: date):
    """Return (cumulative, projection, projected_end, daily_rate).

    Projection extends from the last data point to flight_end at the trailing
    TRAILING_DAYS-day average daily spend.
    """
    cum, run = [], 0.0
    for d, v in daily:
        run += v
        cum.append((d, run))
    window = daily[-TRAILING_DAYS:]
    rate = (sum(v for _, v in window) / len(window)) if window else 0.0

    last_d = cum[-1][0] if cum else flight_end
    proj, p, d = [(last_d, run)], run, last_d + timedelta(days=1)
    while d <= flight_end:
        p += rate
        proj.append((d, p))
        d += timedelta(days=1)
    return cum, proj, (proj[-1][1] if proj else run), rate


def _money(x, _pos=None) -> str:
    return f"${x:,.0f}"


def _render_7day_png(daily: list[tuple[date, float]]) -> bytes:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.ticker import FuncFormatter

    last7 = daily[-TRAILING_DAYS:]
    labels = [d.strftime("%b %d") for d, _ in last7]
    vals = [v for _, v in last7]

    fig, ax = plt.subplots(figsize=(7, 2.9), dpi=140)
    ax.bar(labels, vals, color=_C_BAR, width=0.62)
    ax.set_title("Last 7 days of spend", loc="left", fontsize=14, color="#222", pad=8)
    ax.yaxis.set_major_formatter(FuncFormatter(_money))
    ax.grid(True, color="#eeeeee", axis="y"); ax.set_axisbelow(True)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    ax.margins(y=0.18)
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    plt.close(fig)
    return buf.getvalue()


def _render_cumulative_png(cum, proj, projected_end: float, budget: float) -> bytes:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    from matplotlib.ticker import FuncFormatter

    fig, ax = plt.subplots(figsize=(7, 3.3), dpi=140)
    ax.plot([x for x, _ in cum], [y for _, y in cum], color=_C_ACTUAL, lw=2.4,
            label="Actual cumulative")
    ax.plot([x for x, _ in proj], [y for _, y in proj], color=_C_PROJECT, lw=2.2,
            ls=(0, (5, 3)), label="Projection (7-day rate)")
    if budget > 0:
        ax.axhline(budget, color=_C_BUDGET, lw=1.6, ls=(0, (2, 2)),
                   label=f"Budget ${budget:,.0f}")
    if proj:
        ax.scatter([proj[0][0]], [proj[0][1]], color=_C_ACTUAL, zorder=5, s=28)
        ax.annotate(f"Projected ${projected_end:,.0f}", xy=(proj[-1][0], projected_end),
                    xytext=(-2, -15), textcoords="offset points", fontsize=10,
                    color=_C_PROJECT, ha="right", fontweight="bold")
    ax.set_title("Cumulative spend toward budget", loc="left", fontsize=14, color="#222", pad=10)
    ax.yaxis.set_major_formatter(FuncFormatter(_money))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=5))
    ax.grid(True, color="#eeeeee"); ax.set_axisbelow(True)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    ax.set_ylim(0, max(budget, projected_end) * 1.28)
    ax.legend(loc="upper left", frameon=False, fontsize=10, handlelength=1.8, borderaxespad=0.6)
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    plt.close(fig)
    return buf.getvalue()


def _upload_png(png: bytes, object_name: str) -> str | None:
    """Upload to the configured bucket and return a short-lived signed GET URL.

    The bucket enforces Public Access Prevention, so we keep objects private and
    hand Slack a V4 signed URL instead. Cloud Run's runtime service account has
    no local key, so signing goes through the IAM signBlob API — the SA needs
    roles/iam.serviceAccountTokenCreator on itself. Slack fetches and caches the
    image when the message is posted, so a 7-day expiry is plenty.
    """
    bucket_name = settings.alert_charts_bucket
    if not bucket_name:
        return None

    import google.auth
    from google.auth.transport import requests as ga_requests
    from google.cloud import storage

    client = storage.Client(project=settings.gcp_project_id)
    blob = client.bucket(bucket_name).blob(object_name)
    blob.cache_control = "private, max-age=604800"
    blob.upload_from_string(png, content_type="image/png")

    creds = getattr(client, "_credentials", None)
    if creds is None:
        creds, _ = google.auth.default()
    try:
        creds.refresh(ga_requests.Request())
    except Exception:
        pass

    return blob.generate_signed_url(
        version="v4",
        expiration=timedelta(days=7),
        method="GET",
        service_account_email=getattr(creds, "service_account_email", None),
        access_token=getattr(creds, "token", None),
    )


def build_alert_chart_blocks(alert: dict, as_of: date | None = None) -> list[dict]:
    """Slack image blocks (cumulative, then 7-day) for a chartable alert, or []."""
    if alert.get("alert_type") not in CHART_ALERT_TYPES or not settings.alert_charts_bucket:
        return []
    try:
        meta = alert.get("metadata")
        meta = json.loads(meta) if isinstance(meta, str) else (meta or {})
        line_id = meta.get("line_id") if isinstance(meta, dict) else None
        if not line_id:
            return []
        line = _line_for_chart(line_id)
        if not line or not line.get("flight_start") or not line.get("flight_end"):
            return []

        as_of = as_of or date.today()
        daily = _daily_spend(line, as_of)
        if not any(v for _, v in daily):
            return []  # nothing to plot yet

        budget = float(line.get("budget") or 0)
        cum, proj, projected_end, _rate = _project(daily, line["flight_end"])
        prefix = f"alert-charts/{line['project_code']}/{alert.get('alert_id') or uuid.uuid4()}"

        cum_url = _upload_png(_render_cumulative_png(cum, proj, projected_end, budget),
                              f"{prefix}-cumulative.png")
        bar_url = _upload_png(_render_7day_png(daily), f"{prefix}-7day.png")

        blocks = []
        if cum_url:
            blocks.append({
                "type": "image", "image_url": cum_url,
                "alt_text": f"Cumulative spend, projected ${projected_end:,.0f} of ${budget:,.0f} budget",
            })
        if bar_url:
            blocks.append({
                "type": "image", "image_url": bar_url,
                "alt_text": "Daily spend over the last 7 days",
            })
        return blocks
    except Exception:
        logger.warning(
            "Alert chart generation failed for %s; sending alert without charts",
            alert.get("alert_id"), exc_info=True,
        )
        return []
