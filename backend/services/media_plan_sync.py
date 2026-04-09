"""Media Plan Sync — reads Google Sheets media plans and populates
media_plans, media_plan_lines, and blocking_chart_weeks in BigQuery.

Uses label-based discovery to find metadata and column positions,
making the parser resilient to minor layout variations between plans.
"""

import logging
import re
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any

import gspread
from google.cloud import bigquery
from google.oauth2.service_account import Credentials as SACredentials

from backend.config import settings

logger = logging.getLogger(__name__)

# ── Platform name normalisation ─────────────────────────────────────
PLATFORM_MAP = {
    "open internet": "stackadapt",
    "stackadapt": "stackadapt",
    "meta": "meta",
    "meta (facebook, instagram, threads)": "meta",
    "meta (facebook, instagram)": "meta",
    "facebook": "meta",
    "instagram": "meta",
    "linkedin": "linkedin",
    "google": "google_ads",
    "google ads": "google_ads",
    "youtube": "google_ads",
    "tiktok": "tiktok",
    "snapchat": "snapchat",
    "snap": "snapchat",
    "perion": "perion",
    "hivestack": "perion",
    "dooh": "perion",
    "pinterest": "pinterest",
    "reddit": "reddit",
}

# Patterns that look like section/flight headers, NOT real platforms
_FLIGHT_RE = re.compile(
    r"^(flight\s+\w+|phase\s+\d+|wave\s+\d+|burst\s+\d+)$", re.IGNORECASE
)

# Project code regex — matches 5-digit codes starting with 2x (e.g. 25042, 26009)
_PROJECT_CODE_RE = re.compile(r'(?:^|\b)(2[0-9]\d{3})(?:\b|\s|-|_|$)')


def _sum_tab_budgets(all_data: list[list[str]]) -> float:
    """Sum all numeric values in the 'Budget' column of a media plan tab."""
    # Find the header row and budget column
    budget_col = None
    header_row_idx = None
    for r in range(min(15, len(all_data))):
        for c in range(len(all_data[r])):
            if all_data[r][c].strip().lower() == "budget":
                budget_col = c
                header_row_idx = r
                break
        if budget_col is not None:
            break
    if budget_col is None or header_row_idx is None:
        return 0.0
    total = 0.0
    for r in range(header_row_idx + 1, len(all_data)):
        if r < len(all_data) and budget_col < len(all_data[r]):
            val = _parse_money(all_data[r][budget_col])
            if val and val > 0:
                total += val
    return total


def _tab_belongs_to_project(
    title: str,
    all_data: list[list[str]],
    bc_metadata: dict,
    project_code: str,
) -> tuple[bool, str]:
    """Check whether a media plan tab belongs to the current project.

    Returns (keep, reason) — reason explains why it was kept or skipped.
    """
    # ── Check 1: project code in tab title (no API call needed) ─────
    codes_in_title = _PROJECT_CODE_RE.findall(title)
    if codes_in_title:
        if project_code in codes_in_title:
            return True, f"tab title contains project code {project_code}"
        return False, f"tab title contains code(s) {codes_in_title}, not {project_code}"

    # ── Check 2: read tab metadata and compare to blocking chart ────
    if len(all_data) < 5:
        logger.warning("  Tab '%s': too few rows for metadata, including by default", title)
        return True, "too few rows for metadata, including by default"

    client_pos = _find_label(all_data, "Client")
    project_pos = _find_label(all_data, "Project")

    tab_client = _cell(all_data, client_pos[0], client_pos[1] + 1) if client_pos else ""
    tab_project = _cell(all_data, project_pos[0], project_pos[1] + 1) if project_pos else ""

    bc_client = bc_metadata.get("client_name", "")
    bc_project = bc_metadata.get("project_name", "")

    # If the tab has no metadata at all, include by default
    if not tab_client and not tab_project:
        logger.warning("  Tab '%s': no Client/Project metadata found, including by default", title)
        return True, "no Client/Project metadata found, including by default"

    # Compare: reject if EITHER client or project mismatches (when both sides have values)
    client_mismatch = tab_client and bc_client and tab_client.lower() != bc_client.lower()
    project_mismatch = tab_project and bc_project and tab_project.lower() != bc_project.lower()

    if client_mismatch or project_mismatch:
        return False, (
            f"metadata mismatch — tab has client='{tab_client}', project='{tab_project}'; "
            f"blocking chart has client='{bc_client}', project='{bc_project}'"
        )

    # ── Check 3: compare tab budget total to blocking chart budget ──
    bc_budget = bc_metadata.get("net_budget")
    if bc_budget and bc_budget > 0:
        tab_budget = _sum_tab_budgets(all_data)
        if tab_budget > 0:
            ratio = tab_budget / bc_budget
            if ratio < 0.3 or ratio > 3.0:
                return False, (
                    f"budget mismatch — tab total ${tab_budget:,.0f} vs "
                    f"blocking chart ${bc_budget:,.0f} (ratio {ratio:.1f})"
                )

    return True, f"metadata compatible (client='{tab_client}', project='{tab_project}')"


def _line_belongs_to_project(mp_line: dict, project_code: str) -> bool:
    """Check if a media plan line likely belongs to the target project.

    Scans text fields for project codes. If any are found and NONE match
    the target, the line is from a different project.
    """
    text_fields = [
        mp_line.get("audience_name", ""),
        mp_line.get("audience_targeting", ""),
        mp_line.get("technical_targeting", ""),
        mp_line.get("goal", ""),
        mp_line.get("platform", ""),
        mp_line.get("landing_page", ""),
    ]
    combined = " ".join(text_fields)
    codes = _PROJECT_CODE_RE.findall(combined)
    if not codes:
        return True  # no project code found — can't tell, include
    return project_code in codes


def _is_section_header(raw: str) -> bool:
    """Return True if the value looks like a flight/section header, not a platform."""
    return bool(_FLIGHT_RE.match(raw.strip()))


def _normalise_platform(raw: str | None) -> str | None:
    if not raw:
        return None
    key = raw.strip().lower()
    for pattern, platform_id in PLATFORM_MAP.items():
        if pattern in key:
            return platform_id
    return raw.strip().lower().replace(" ", "_")


def _parse_date(val: str | None, ref_year: int | None = None) -> date | None:
    """Best-effort date parse. Handles 'March 5', 'Mar 22', '5 Mar', etc."""
    if not val or not val.strip():
        return None
    val = val.strip()

    for fmt in ("%B %d", "%b %d", "%d %b", "%d %B"):
        try:
            parsed = datetime.strptime(val, fmt).date()
            year = ref_year or date.today().year
            return parsed.replace(year=year)
        except ValueError:
            continue

    for fmt in ("%B %d, %Y", "%b %d, %Y", "%Y-%m-%d", "%m/%d/%Y",
                "%d/%m/%Y", "%b %d %Y", "%d-%b-%Y", "%d-%b-%y"):
        try:
            return datetime.strptime(val, fmt).date()
        except ValueError:
            continue

    try:
        from dateutil.parser import parse as du_parse
        return du_parse(val).date()
    except Exception:
        return None


def _parse_money(val: str | None) -> float | None:
    if not val:
        return None
    cleaned = re.sub(r"[^\d.\-]", "", str(val))
    try:
        return float(cleaned)
    except ValueError:
        return None


def _parse_pct(val: str | None) -> float | None:
    if not val:
        return None
    cleaned = re.sub(r"[^\d.\-]", "", str(val))
    try:
        return float(cleaned)
    except ValueError:
        return None


def _get_gspread_client() -> gspread.Client:
    """Create gspread client using service account or ADC."""
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    ]

    if settings.sheets_service_account_file:
        creds = SACredentials.from_service_account_file(
            settings.sheets_service_account_file, scopes=scopes,
        )
    else:
        from google.auth import default
        creds, _ = default(scopes=scopes)

    return gspread.authorize(creds)


def _mtl_client() -> bigquery.Client:
    return bigquery.Client(
        project=settings.gcp_project_id,
        location=settings.gcp_region,
    )


# ── Generic label finder ────────────────────────────────────────────

def _find_label(data: list[list[str]], label: str, max_row: int = 15) -> tuple[int, int] | None:
    """Find (row_idx, col_idx) of a cell containing the label text."""
    label_lower = label.lower().strip()
    for r in range(min(max_row, len(data))):
        for c in range(min(10, len(data[r]))):
            if data[r][c].strip().lower().startswith(label_lower):
                return (r, c)
    return None


def _cell(data: list[list[str]], row: int, col: int) -> str:
    if row < len(data) and col < len(data[row]):
        return data[row][col].strip()
    return ""


# ── Blocking Chart Parser ───────────────────────────────────────────

def _parse_blocking_chart(ws: gspread.Worksheet) -> dict:
    """Parse the Blocking Chart tab using label-based discovery."""
    all_data = ws.get_all_values()
    if len(all_data) < 8:
        raise ValueError(f"Blocking Chart has only {len(all_data)} rows — expected >= 8")

    # ── Discover metadata positions ─────────────────────────────────
    client_pos = _find_label(all_data, "Client")
    project_pos = _find_label(all_data, "Project")
    dates_pos = _find_label(all_data, "Start & End") or _find_label(all_data, "Run Dates")
    budget_pos = _find_label(all_data, "Net Budget")

    client_name = _cell(all_data, client_pos[0], client_pos[1] + 1) if client_pos else ""
    project_name = _cell(all_data, project_pos[0], project_pos[1] + 1) if project_pos else ""

    ref_year = date.today().year

    start_date = None
    end_date = None
    if dates_pos:
        r, c = dates_pos
        # Start/end may be in adjacent cells (c+1, c+2) or (c+1, c+3)
        for offset in range(1, 5):
            d = _parse_date(_cell(all_data, r, c + offset), ref_year)
            if d:
                if start_date is None:
                    start_date = d
                else:
                    end_date = d
                    break
    if start_date:
        ref_year = start_date.year

    net_budget = None
    if budget_pos:
        r, c = budget_pos
        for offset in range(1, 5):
            val = _parse_money(_cell(all_data, r, c + offset))
            if val and val > 0:
                net_budget = val
                break

    metadata = {
        "client_name": client_name,
        "project_name": project_name,
        "start_date": start_date,
        "end_date": end_date,
        "net_budget": net_budget,
    }

    # ── Find header row (contains "Platform") ───────────────────────
    header_row_idx = None
    for r in range(4, min(15, len(all_data))):
        row_text = " ".join(c.strip().lower() for c in all_data[r])
        if "platform" in row_text:
            header_row_idx = r
            break
    if header_row_idx is None:
        raise ValueError("Could not find header row with 'Platform' in Blocking Chart")

    headers = all_data[header_row_idx]

    # Identify column roles by header text
    platform_col = None
    objective_col = None
    budget_col = None
    obj_pct_col = None
    week_cols: list[tuple[int, date]] = []  # (col_idx, week_start_date)

    for i, h in enumerate(headers):
        h_stripped = h.strip()
        h_lower = h_stripped.lower()
        if h_lower.startswith("platform"):
            platform_col = i
        elif "objective" in h_lower and "format" in h_lower:
            objective_col = i
        elif h_lower == "budget":
            budget_col = i
        elif "objective" in h_lower and "%" in h_lower:
            obj_pct_col = i
        elif h_stripped and platform_col is not None and budget_col is None:
            wd = _parse_date(h_stripped, ref_year)
            if wd:
                week_cols.append((i, wd))

    if platform_col is None:
        platform_col = 1
    if objective_col is None:
        objective_col = platform_col + 1

    logger.info("  Blocking Chart layout: platform=%d, objective=%d, weeks=%d cols, budget=%s",
                platform_col, objective_col, len(week_cols), budget_col)

    # ── Parse line items ────────────────────────────────────────────
    lines = []
    weeks = []
    current_platform = None
    data_start = header_row_idx + 1

    for row_idx in range(data_start, len(all_data)):
        row = all_data[row_idx]
        if not row or all(c.strip() == "" for c in row):
            continue

        plat_raw = _cell(all_data, row_idx, platform_col)
        obj_fmt = _cell(all_data, row_idx, objective_col)

        if plat_raw:
            # Skip flight/section headers — don't let them become current_platform
            if _is_section_header(plat_raw):
                logger.debug("  Skipping section header: %s", plat_raw)
                continue
            current_platform = plat_raw

        if not current_platform or current_platform.lower() in ("total", "grand total", ""):
            continue

        line_budget = _parse_money(_cell(all_data, row_idx, budget_col)) if budget_col else None
        if line_budget is None or line_budget <= 0:
            continue

        # Skip totals rows: no platform in THIS row and no objective text
        if not plat_raw and not obj_fmt:
            continue

        obj_pct = _parse_pct(_cell(all_data, row_idx, obj_pct_col)) if obj_pct_col else None

        # Determine active weeks: find first and last non-empty week cells
        # for this line, then mark all weeks in that range as active.
        first_active_week = None
        last_active_week = None
        line_start = None
        line_end = None

        for col_idx, week_date in week_cols:
            cell_val = _cell(all_data, row_idx, col_idx)
            if cell_val:
                parsed_cell = _parse_date(cell_val, ref_year)
                if first_active_week is None:
                    first_active_week = week_date
                    line_start = parsed_cell or week_date
                last_active_week = week_date
                line_end = parsed_cell or (week_date + timedelta(days=6))

        line_idx = len(lines)
        lines.append({
            "platform": current_platform,
            "platform_id": _normalise_platform(current_platform),
            "objective_format": obj_fmt,
            "budget": line_budget,
            "objective_pct": obj_pct,
            "flight_start": line_start or start_date,
            "flight_end": line_end or end_date,
        })

        # Generate week entries — all weeks between first and last active
        for col_idx, week_date in week_cols:
            if first_active_week and last_active_week:
                is_active = first_active_week <= week_date <= last_active_week
            else:
                is_active = False
            weeks.append({
                "line_index": line_idx,
                "week_start": week_date,
                "is_active": is_active,
            })

    return {"metadata": metadata, "lines": lines, "weeks": weeks}


# ── Media Plan Tab Parser ───────────────────────────────────────────

def _parse_media_plan_tab(ws: gspread.Worksheet, prefetched_data: list[list[str]] | None = None) -> list[dict]:
    """Parse the Media Plan tab for detailed line items with targeting info."""
    all_data = prefetched_data or ws.get_all_values()
    if len(all_data) < 14:
        return []

    ref_year = date.today().year

    # Find header row by looking for "Site/Network" or "Goal"
    header_row_idx = None
    for r in range(4, min(15, len(all_data))):
        row_text = " ".join(c.strip().lower() for c in all_data[r])
        if "site/network" in row_text or ("goal" in row_text and "start" in row_text):
            header_row_idx = r
            break
    if header_row_idx is None:
        logger.warning("Could not find header row in Media Plan tab")
        return []

    headers = all_data[header_row_idx]

    # Build column map from header names
    col_map: dict[str, int] = {}
    for i, h in enumerate(headers):
        h_lower = h.strip().lower()
        if "site" in h_lower or "network" in h_lower:
            col_map["platform"] = i
        elif h_lower in ("goal", "goal "):
            col_map["goal"] = i
        elif "goal" in h_lower and "freq" in h_lower:
            col_map["frequency"] = i
        elif h_lower.strip() in ("start", "start "):
            col_map["start"] = i
        elif h_lower.strip() in ("end", "end "):
            col_map["end"] = i
        elif h_lower.strip() == "days":
            col_map["days"] = i
        elif h_lower.strip() == "id":
            col_map["id"] = i
        elif "audience name" in h_lower or "ad set name" in h_lower or "adset name" in h_lower:
            col_map["audience_name"] = i
        elif "geo" in h_lower:
            col_map["geo"] = i
        elif "audience" in h_lower and "targeting" in h_lower:
            col_map["audience_targeting"] = i
        elif "technical" in h_lower:
            col_map["technical"] = i
        elif "landing" in h_lower:
            col_map["landing_page"] = i
        elif "creative" in h_lower:
            col_map["creative"] = i
        elif "pricing" in h_lower:
            col_map["pricing"] = i
        elif "est" in h_lower and "audience" in h_lower:
            col_map["est_audience"] = i
        elif "bid" in h_lower:
            col_map["bid"] = i
        elif "est" in h_lower and "impression" in h_lower:
            col_map["est_impressions"] = i
        elif "freq" in h_lower and "goal" not in h_lower:
            col_map["frequency"] = i
        elif h_lower.strip() == "budget":
            col_map["budget"] = i

    # Fallback: if no audience_name column was found, try less-specific headers
    if "audience_name" not in col_map:
        for i, h in enumerate(headers):
            h_lower = h.strip().lower()
            if h_lower == "audience" or h_lower == "name":
                col_map["audience_name"] = i
                break

    logger.info("  Media Plan tab column map: %s", col_map)

    def gc(row: list[str], key: str) -> str:
        idx = col_map.get(key)
        if idx is not None and idx < len(row):
            return row[idx].strip()
        return ""

    lines = []
    current_platform = None
    data_start = header_row_idx + 1

    for row_idx in range(data_start, len(all_data)):
        row = all_data[row_idx]
        if not row or all(c.strip() == "" for c in row):
            continue

        plat_raw = gc(row, "platform")
        if plat_raw:
            # Skip flight/section headers in media plan tabs too
            if _is_section_header(plat_raw):
                continue
            current_platform = plat_raw

        line_code = gc(row, "id")
        goal = gc(row, "goal")
        budget = _parse_money(gc(row, "budget"))

        # Skip total/footer rows
        if current_platform and current_platform.lower() in ("total", "grand total", "terms"):
            continue
        if not goal and not line_code:
            continue
        if "total" in goal.lower():
            continue
        # Rows without budget still have targeting info, so include if they have a line_code
        if budget is not None and budget <= 0:
            budget = None

        lines.append({
            "platform": current_platform,
            "platform_id": _normalise_platform(current_platform),
            "goal": goal,
            "flight_start": _parse_date(gc(row, "start"), ref_year),
            "flight_end": _parse_date(gc(row, "end"), ref_year),
            "days": gc(row, "days"),
            "line_code": line_code,
            "audience_name": gc(row, "audience_name"),
            "geo_targeting": gc(row, "geo"),
            "audience_targeting": gc(row, "audience_targeting"),
            "technical_targeting": gc(row, "technical"),
            "landing_page": gc(row, "landing_page"),
            "creative": gc(row, "creative"),
            "pricing_model": gc(row, "pricing"),
            "estimated_impressions": _parse_money(gc(row, "est_impressions")),
            "frequency_cap": gc(row, "frequency"),
            "budget": budget,
        })

    return lines


def _synthesise_lines_from_mp(
    mp_lines: list[dict], metadata: dict
) -> list[dict]:
    """Create blocking-chart-style line dicts from media plan tab lines.

    Used as a fallback when the blocking chart only had section/flight headers
    and no actual platform rows.
    """
    lines = []
    seen: set[tuple] = set()
    for mp in mp_lines:
        pid = mp.get("platform_id")
        if not pid:
            continue
        # Only include lines with recognised platforms — skip flight headers
        # that slipped through or unknown platform names
        if pid not in PLATFORM_MAP.values():
            logger.debug("  Skipping unrecognised platform_id in fallback: %s", pid)
            continue
        budget = mp.get("budget")
        if not budget or budget <= 0:
            continue
        # Deduplicate across multiple flight tabs with identical content
        fs = mp.get("flight_start") or metadata.get("start_date")
        fe = mp.get("flight_end") or metadata.get("end_date")
        dedup_key = (pid, budget, mp.get("goal", ""), str(fs), str(fe), mp.get("audience_name", ""))
        if dedup_key in seen:
            continue
        seen.add(dedup_key)
        lines.append({
            "platform": mp.get("platform", ""),
            "platform_id": pid,
            "objective_format": mp.get("goal", ""),
            "budget": budget,
            "objective_pct": None,
            "flight_start": fs,
            "flight_end": fe,
            "audience_name": mp.get("audience_name", ""),
        })
    return lines


def _mp_lines_have_audience_data(mp_lines: list[dict]) -> bool:
    """Check if media plan tab lines have audience-level detail worth using."""
    return any(
        mp.get("audience_name") and mp.get("budget") and mp.get("budget") > 0
        for mp in mp_lines
    )


# ── Sync Orchestrator ───────────────────────────────────────────────

def sync_media_plan(sheet_id: str, project_code: str, tab_name: str | None = None) -> dict:
    """Sync a Google Sheets media plan into BigQuery.

    Args:
        sheet_id: Google Sheets document ID.
        project_code: YYNNN project code.
        tab_name: Optional specific tab name to sync. If provided, only this
            tab is processed (case-insensitive match). If omitted, all matching
            tabs are used (existing behaviour).

    Returns:
        Summary dict with counts of records written.
    """
    logger.info("Syncing media plan for %s from sheet %s", project_code, sheet_id)

    gc = _get_gspread_client()
    ss = gc.open_by_key(sheet_id)

    # Find tabs by name — skip example/template tabs, collect ALL media plan tabs
    blocking_ws = None
    media_plan_tabs: list[gspread.Worksheet] = []
    _skip_words = {"example", "template", "sample"}

    for ws in ss.worksheets():
        title_lower = ws.title.lower()
        # Skip example/template tabs
        if any(w in title_lower for w in _skip_words):
            logger.info("  Skipping tab: %s", ws.title)
            continue
        if "blocking" in title_lower and "chart" in title_lower:
            blocking_ws = ws
        elif "media plan" in title_lower or title_lower == "media plan":
            media_plan_tabs.append(ws)

    if not blocking_ws:
        sheets = ss.worksheets()
        if len(sheets) >= 2:
            blocking_ws = sheets[1]
            if not media_plan_tabs:
                media_plan_tabs = [sheets[0]]
        else:
            raise ValueError("Could not find Blocking Chart tab")

    # ── Parse blocking chart + filter media plan tabs to this project ─
    bc = _parse_blocking_chart(blocking_ws)

    filtered_tabs: list[tuple[gspread.Worksheet, list[list[str]]]] = []
    for mp_ws in media_plan_tabs:
        # If a specific tab was requested, skip non-matching tabs
        if tab_name:
            if mp_ws.title.strip().lower() != tab_name.strip().lower():
                logger.debug("  Skipping tab '%s' — doesn't match requested tab_name='%s'",
                             mp_ws.title, tab_name)
                continue
            else:
                logger.info("  Tab '%s' matches requested tab_name — processing", mp_ws.title)
        tab_data = mp_ws.get_all_values()
        keep, reason = _tab_belongs_to_project(mp_ws.title, tab_data, bc["metadata"], project_code)
        if keep:
            logger.info("  Including media plan tab: '%s' — %s", mp_ws.title, reason)
            filtered_tabs.append((mp_ws, tab_data))
        else:
            logger.warning("  Skipping media plan tab: '%s' — %s", mp_ws.title, reason)

    mp_lines: list[dict] = []
    for mp_ws, tab_data in filtered_tabs:
        mp_lines.extend(_parse_media_plan_tab(mp_ws, prefetched_data=tab_data))

    # ── Row-level project filter: remove lines that contain a different
    #    project code in their text fields (catches mixed-project tabs)
    before_count = len(mp_lines)
    mp_lines = [l for l in mp_lines if _line_belongs_to_project(l, project_code)]
    if len(mp_lines) < before_count:
        logger.info("  Row-level project filter: %d → %d lines (removed %d from other projects)",
                     before_count, len(mp_lines), before_count - len(mp_lines))

    # ── Prefer media plan tab lines when they have audience-level detail.
    #    The blocking chart often has more granular budget rows that don't
    #    match the intended line structure. Media plan tabs with audience
    #    names are the authoritative source.
    if mp_lines and _mp_lines_have_audience_data(mp_lines):
        if bc["lines"]:
            # Enrich blocking chart lines with audience data from media plan tabs
            # rather than replacing them — preserves weekly activation patterns
            logger.info("  Media plan tabs have audience data — enriching %d blocking chart lines", len(bc["lines"]))
            # The _match_mp_line() call below handles enrichment
        else:
            # Blocking chart had no line items — use media plan tabs as primary source
            logger.warning("  Blocking chart produced 0 lines but mp tabs have audience data — synthesising")
            bc["lines"] = _synthesise_lines_from_mp(mp_lines, bc["metadata"])
    elif not bc["lines"] and mp_lines:
        logger.warning("  Blocking chart produced 0 lines — falling back to media plan tab lines")
        bc["lines"] = _synthesise_lines_from_mp(mp_lines, bc["metadata"])

    meta = bc["metadata"]
    plan_id = f"plan-{project_code}-{uuid.uuid4().hex[:8]}"

    logger.info("  Parsed: %d blocking chart lines, %d media plan lines, %d week entries",
                len(bc["lines"]), len(mp_lines), len(bc["weeks"]))

    # ── Build records ───────────────────────────────────────────────
    now = datetime.now(timezone.utc).isoformat()

    media_plan_record = {
        "plan_id": plan_id,
        "project_code": project_code,
        "sheet_id": sheet_id,
        "sheet_name": ss.title,
        "client_name": meta.get("client_name"),
        "project_name": meta.get("project_name"),
        "start_date": meta["start_date"].isoformat() if meta.get("start_date") else None,
        "end_date": meta["end_date"].isoformat() if meta.get("end_date") else None,
        "net_budget": meta.get("net_budget"),
        "version": 1,
        "is_current": True,
        "synced_at": now,
    }

    line_records = []
    week_records = []
    used_indices: set[int] = set()

    for i, bc_line in enumerate(bc["lines"]):
        line_id = f"{plan_id}-line-{i:03d}"

        # Match with media plan tab detail by platform + line_code or budget
        mp_detail = _match_mp_line(bc_line, mp_lines, used_indices)

        flight_start = bc_line.get("flight_start") or meta.get("start_date")
        flight_end = bc_line.get("flight_end") or meta.get("end_date")

        # Use media plan tab's line_code if matched, otherwise generate one
        line_code = mp_detail.get("line_code") if mp_detail else None

        line_records.append({
            "line_id": line_id,
            "plan_id": plan_id,
            "project_code": project_code,
            "line_code": line_code,
            "platform_id": bc_line["platform_id"],
            "site_network": bc_line["platform"],
            "channel_category": _channel_category(bc_line.get("objective_format", "")),
            "flight_start": flight_start.isoformat() if flight_start else None,
            "flight_end": flight_end.isoformat() if flight_end else None,
            "objective": bc_line.get("objective_format"),
            "audience_name": bc_line.get("audience_name") or (mp_detail.get("audience_name") if mp_detail else None),
            "audience_targeting": mp_detail.get("audience_targeting") if mp_detail else None,
            "landing_page": mp_detail.get("landing_page") if mp_detail else None,
            "pricing_model": mp_detail.get("pricing_model") if mp_detail else None,
            "budget": bc_line["budget"],
            "estimated_impressions": (
                int(mp_detail["estimated_impressions"])
                if mp_detail and mp_detail.get("estimated_impressions")
                else None
            ),
            "frequency_cap": mp_detail.get("frequency_cap") if mp_detail else None,
            "geo_targeting": mp_detail.get("geo_targeting") if mp_detail else None,
            "is_traditional": _is_traditional_media(bc_line.get("platform"), bc_line.get("platform_id")),
        })

        line_has_weeks = False
        for w in bc["weeks"]:
            if w["line_index"] == i:
                line_has_weeks = True
                week_records.append({
                    "id": f"{line_id}-w-{w['week_start'].isoformat()}",
                    "line_id": line_id,
                    "project_code": project_code,
                    "week_start": w["week_start"].isoformat(),
                    "is_active": w["is_active"],
                })

        # If no blocking chart weeks exist for this line (e.g. fallback
        # from media plan tabs), generate weekly entries from flight dates
        if not line_has_weeks and flight_start and flight_end:
            week_cursor = flight_start - timedelta(days=flight_start.weekday())  # Monday
            while week_cursor <= flight_end:
                is_active = flight_start <= week_cursor + timedelta(days=6) and week_cursor <= flight_end
                week_records.append({
                    "id": f"{line_id}-w-{week_cursor.isoformat()}",
                    "line_id": line_id,
                    "project_code": project_code,
                    "week_start": week_cursor.isoformat(),
                    "is_active": is_active,
                })
                week_cursor += timedelta(days=7)

    # ── Write to BigQuery ───────────────────────────────────────────
    mtl = _mtl_client()
    try:
        _clear_existing_plan(mtl, project_code)
        _write_records(mtl, "media_plans", [media_plan_record])
        _write_records(mtl, "media_plan_lines", line_records)
        _write_records(mtl, "blocking_chart_weeks", week_records)
    finally:
        mtl.close()

    result = {
        "status": "success",
        "project_code": project_code,
        "plan_id": plan_id,
        "sheet_title": ss.title,
        "client_name": meta.get("client_name"),
        "project_name": meta.get("project_name"),
        "start_date": meta["start_date"].isoformat() if meta.get("start_date") else None,
        "end_date": meta["end_date"].isoformat() if meta.get("end_date") else None,
        "net_budget": meta.get("net_budget"),
        "lines_created": len(line_records),
        "weeks_created": len(week_records),
    }
    logger.info("  Sync complete: %s", result)
    return result


def _match_mp_line(bc_line: dict, mp_lines: list[dict], used_indices: set[int]) -> dict | None:
    """Best-effort match a blocking chart line to a media plan tab line.

    Uses used_indices to track which mp_lines have already been matched,
    avoiding order-dependent side effects from mutating the list.
    """
    if not mp_lines:
        return None
    bc_plat = bc_line.get("platform_id")
    bc_budget = bc_line.get("budget", 0)

    candidates = [(i, l) for i, l in enumerate(mp_lines)
                  if l.get("platform_id") == bc_plat and i not in used_indices]
    if not candidates:
        return None

    # Prefer match with budget within 10%, and that has a line_code
    with_budget = [(i, c) for i, c in candidates if c.get("budget")]
    if with_budget:
        best_i, best = min(with_budget, key=lambda t: abs((t[1].get("budget") or 0) - bc_budget))
        if abs(best["budget"] - bc_budget) / max(bc_budget, 1) < 0.1:
            used_indices.add(best_i)
            return best

    # Fall back to first candidate with a line_code
    with_code = [(i, c) for i, c in candidates if c.get("line_code")]
    if with_code:
        pick_i, pick = with_code[0]
        used_indices.add(pick_i)
        return pick

    if len(candidates) == 1:
        used_indices.add(candidates[0][0])
        return candidates[0][1]
    return None


def _channel_category(objective_format: str) -> str:
    lower = objective_format.lower()
    if "display" in lower:
        return "Display"
    if "social" in lower:
        return "Social"
    if "search" in lower:
        return "Search"
    if "video" in lower:
        return "Video"
    if "audio" in lower:
        return "Audio"
    return "Digital"


# Traditional (non-digital) media keywords
_TRADITIONAL_KEYWORDS = {
    "direct mail", "personalized mail", "direct personalized mail",
    "print", "newspaper", "magazine", "radio", "billboard",
    "out of home", "ooh", "transit", "flyer", "brochure",
    "tv spot", "television",
}


def _is_traditional_media(platform: str | None, platform_id: str | None) -> bool:
    """Return True if this line is traditional (non-digital) media."""
    if not platform:
        return False
    plower = platform.strip().lower()
    return any(kw in plower for kw in _TRADITIONAL_KEYWORDS)


def _clear_existing_plan(mtl: bigquery.Client, project_code: str) -> None:
    """Remove old plan data before writing a fresh sync."""
    prefix = f"`{settings.gcp_project_id}.{settings.bigquery_dataset}"
    param = [bigquery.ScalarQueryParameter("pc", "STRING", project_code)]
    cfg = lambda: bigquery.QueryJobConfig(query_parameters=param)

    mtl.query(
        f"UPDATE {prefix}.media_plans` SET is_current = FALSE WHERE project_code = @pc AND is_current = TRUE",
        job_config=cfg(),
    ).result()
    mtl.query(
        f"DELETE FROM {prefix}.media_plan_lines` WHERE project_code = @pc",
        job_config=cfg(),
    ).result()
    mtl.query(
        f"DELETE FROM {prefix}.blocking_chart_weeks` WHERE project_code = @pc",
        job_config=cfg(),
    ).result()


def _write_records(mtl: bigquery.Client, table_name: str, records: list[dict]) -> None:
    if not records:
        return
    target = f"{settings.gcp_project_id}.{settings.bigquery_dataset}.{table_name}"
    load_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    mtl.load_table_from_json(records, target, job_config=load_config).result()
    logger.info("    Wrote %d rows to %s", len(records), table_name)
