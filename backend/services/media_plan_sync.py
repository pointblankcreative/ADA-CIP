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
import google.cloud.exceptions
from google.oauth2.service_account import Credentials as SACredentials

from backend.config import settings

logger = logging.getLogger(__name__)

# ── Platform name normalisation ─────────────────────────────────────
PLATFORM_MAP = {
    "open internet": "stackadapt",
    "stackadapt": "stackadapt",
    # PB's media plans label StackAdapt buys "Programmatic (Native)" /
    # "(Display)" / "(OLV)" etc. Since Hivestack's removal (2026-05-14)
    # programmatic == StackAdapt at PB. Without this alias the sync silently
    # dropped such rows (26018: two StackAdapt lines, $3,750 — AI-002/AI-022
    # root cause, fixed 2026-06-04).
    "programmatic": "stackadapt",
    # Open Web (Video/Display/Native) buys also run on StackAdapt at PB; without
    # this alias the row was silently dropped (26023 "Open Web Video", $2,000).
    # Substring-matched, so "Open Web Video" resolves here too.
    "open web": "stackadapt",
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
    # DOOH is now bought through StackAdapt (Perion's DOOH supply was retired at
    # PB). Both the short "DOOH" tag and the spelled-out "Digital Out Of Home"
    # label (26023, $3,500) therefore route to StackAdapt — a recognised
    # self-serve feed — so DOOH lines stay is_direct=FALSE and (post the pacing
    # trackability swap) pace once StackAdapt DOOH spend flows. The literal
    # "perion"/"hivestack" aliases above are left intact for any legacy label.
    "dooh": "stackadapt",
    "digital out of home": "stackadapt",
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


_NON_CANONICAL_TAB_PATTERNS = {"[client]", "only", "draft", "old", "archive", "backup", "copy"}


def _filter_canonical_tabs(tab_titles: list[str]) -> list[str]:
    """Return only canonical media plan tab titles, filtering out copies/subsets.

    When a sheet has multiple tabs matching "media plan" (e.g. "Media Plan V2",
    "[CLIENT] Media Plan V2", "Media Plan V2 F1 Only"), this filters out
    non-canonical variants to avoid merging lines from duplicates or subsets.

    If ALL tabs match a non-canonical pattern, returns all of them unchanged
    (let downstream filtering decide).
    """
    if len(tab_titles) <= 1:
        return tab_titles
    canonical = [
        t for t in tab_titles
        if not any(p in t.lower() for p in _NON_CANONICAL_TAB_PATTERNS)
    ]
    return canonical if canonical else tab_titles


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
    # Tightened: require at least one metadata field (client or project) to match
    # Budget heuristic is only a fallback if metadata is present
    bc_budget = bc_metadata.get("net_budget")
    if bc_budget and bc_budget > 0:
        tab_budget = _sum_tab_budgets(all_data)
        if tab_budget > 0:
            ratio = tab_budget / bc_budget
            # Tighter band: ±20% instead of 0.3–3.0×
            if ratio < 0.8 or ratio > 1.2:
                # Reject only if we also don't have metadata match
                # If metadata is present (client or project), trust metadata
                if not (tab_client or tab_project):
                    return False, (
                        f"budget mismatch — tab total ${tab_budget:,.0f} vs "
                        f"blocking chart ${bc_budget:,.0f} (ratio {ratio:.1f}x) — "
                        f"and no client/project metadata to confirm"
                    )
                # If metadata exists but is blank on one side, it's a soft warn
                if not tab_client and not tab_project and not bc_client and not bc_project:
                    return False, (
                        f"budget mismatch (0.8–1.2x required) — tab ${tab_budget:,.0f} vs "
                        f"blocking chart ${bc_budget:,.0f} — and no metadata to confirm"
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
    # Most-specific-match-wins: try longer patterns before their substrings, so a
    # label containing several aliases binds to the most specific one (e.g.
    # "google ads" before bare "google"). The match stays substring-based, which
    # is load-bearing for verbose labels like "Facebook, Instagram & Threads\nMeta".
    # (The keep-vs-drop asymmetry the audit flagged is already resolved by the
    # is_direct model: budgeted unrecognised lines are captured, not dropped, on
    # both the blocking-chart and synthesis paths.)
    for pattern in sorted(PLATFORM_MAP, key=len, reverse=True):
        if pattern in key:
            return PLATFORM_MAP[pattern]
    return raw.strip().lower().replace(" ", "_")


def _parse_date(val: str | None, ref_year: int | None = None) -> date | None:
    """Best-effort date parse. Handles 'March 5', 'Mar 22', '5 Mar', etc.

    ``ref_year`` is the year applied to month-day-only cells (e.g. "June 1",
    which carries no year). It MUST be the campaign's actual year, NOT
    ``date.today().year`` — for a campaign that already landed, "today" is a
    year (or more) ahead of when the flight actually ran, which shifts every
    parsed flight a year forward and makes the pacing engine treat a finished
    campaign as "not yet started" (the 24058 / OPSEU "Worth Fighting For"
    failure: a 2025-06→2025-12 campaign stored as 2026-06→2026-12). Callers
    are expected to resolve ``ref_year`` from the project's own window via
    ``_resolve_ref_year`` before reaching this parser. We keep
    ``date.today().year`` only as a last-resort fallback for the genuinely
    year-less, anchor-less case (a brand-new plan with no project window yet).
    """
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


_EXPLICIT_YEAR_RE = re.compile(r"(?:^|\D)(19|20)\d{2}(?:\D|$)")


def _cell_has_explicit_year(val: str | None) -> bool:
    """True if a date cell carries a 4-digit year (e.g. "June 1, 2025",
    "2025-06-01", "6/1/2025"). Month-day-only cells ("June 1", "Jun 1")
    return False, signalling that the year must be inferred from an anchor.
    """
    if not val or not val.strip():
        return False
    return bool(_EXPLICIT_YEAR_RE.search(val.strip()))


def _resolve_ref_year(
    bc_start: date | None,
    bc_dates_are_year_less: bool,
    project_anchor_year: int | None,
) -> int | None:
    """Decide the reference year for month-day-only flight cells.

    This is the year-inference fix for the 24058 date-year bug. Inputs:

      * ``bc_start`` — the blocking chart's parsed start_date (may itself have
        been forward-defaulted to today's year if its cell was month-day-only).
      * ``bc_dates_are_year_less`` — True when the blocking chart's own
        Start/End cells carried NO explicit year (so ``bc_start``'s year is
        only as trustworthy as whatever default produced it).
      * ``project_anchor_year`` — the year of ``dim_projects.start_date`` for
        this project, i.e. the authoritative campaign window. None when the
        project has no recorded window yet (brand-new plan).

    Resolution order:

      1. If the blocking chart gave an EXPLICIT year (cell like "June 1, 2025"
         or "2025-06-01"), trust it — return ``bc_start.year``. The planner
         wrote a real year; never override it.
      2. Else, if we have a project anchor year, use it. This is the fix: a
         landed campaign whose sheet cells are bare "June 1" resolves into the
         campaign's real year instead of today's.
      3. Else fall back to ``bc_start.year`` if present, else None (which lets
         ``_parse_date`` apply ``date.today().year`` — unchanged legacy
         behaviour for the anchor-less, year-less case).

    Crucially this never *shifts* a correctly-yeared plan: when the sheet has
    explicit years (every other project synced today), branch 1 short-circuits
    and the anchor is ignored, so there is no regression.
    """
    if bc_start is not None and not bc_dates_are_year_less:
        return bc_start.year
    if project_anchor_year is not None:
        return project_anchor_year
    if bc_start is not None:
        return bc_start.year
    return None


def _lookup_project_anchor_year(mtl: "bigquery.Client", project_code: str) -> int | None:
    """Return the year of ``dim_projects.start_date`` for ``project_code``.

    This is the authoritative campaign window (set when the project is created,
    independent of the media-plan sheet's date formatting), so it's a safe
    anchor for resolving year-less flight cells. Read-only single-row lookup.
    Returns None when the project has no row or no start_date (e.g. a brand-new
    project synced before its window is filled in) — callers then fall back to
    the blocking chart / today, preserving legacy behaviour.
    """
    try:
        prefix = f"`{settings.gcp_project_id}.{settings.bigquery_dataset}"
        rows = list(
            mtl.query(
                f"SELECT start_date FROM {prefix}.dim_projects` "
                f"WHERE project_code = @pc LIMIT 1",
                job_config=bigquery.QueryJobConfig(query_parameters=[
                    bigquery.ScalarQueryParameter("pc", "STRING", project_code),
                ]),
            ).result()
        )
    except Exception as exc:  # table missing in dev, transient BQ error, etc.
        logger.warning(
            "  Could not look up dim_projects anchor year for %s: %s",
            project_code, exc,
        )
        return None
    if rows and rows[0].get("start_date"):
        return rows[0]["start_date"].year
    return None


def _parse_money(val: str | None) -> float | None:
    if not val:
        return None
    cleaned = re.sub(r"[^\d.\-]", "", str(val))
    try:
        return float(cleaned)
    except ValueError:
        return None


# ── Line-code extraction ─────────────────────────────────────────────
# Bundled-optimization support: media plans tag individual lines with short
# codes. Two formats in the wild:
#   - Squamish (25034) Col G "Group Name": "#09 North Van Engagers" —
#     '#' prefix + digits + optional description.
#   - OSSTF (25042) Col G "ID": "1A", "1B", "2A", "2B", or bare "1"/"2" —
#     no '#' prefix; whole cell is the code.
# A permissive fallback also handles "1A Description" (OSSTF code + tail).

_LINE_CODE_FULL_RE = re.compile(r"^#?\d+[A-Za-z]?$")
_LINE_CODE_PREFIX_RE = re.compile(r"^(#?\d+[A-Za-z]?)\s+(.+)$")


def _extract_line_code(raw: str | None) -> tuple[str, str]:
    """Split a cell value into (line_code, remainder).

    Preserves the '#' prefix as-given:
      - "#09 Engagers"  → ("#09", "Engagers")
      - "#91"           → ("#91", "")
      - "1A"            → ("1A", "")
      - "2B Teachers"   → ("2B", "Teachers")
      - "Retargeting"   → ("", "Retargeting")
      - "" / None       → ("", "")
    """
    if not raw:
        return ("", "")
    s = raw.strip()
    if not s:
        return ("", "")
    if _LINE_CODE_FULL_RE.match(s):
        return (s, "")
    m = _LINE_CODE_PREFIX_RE.match(s)
    if m:
        return (m.group(1), m.group(2).strip())
    return ("", s)


# ── Ad-set-name line-code extraction ─────────────────────────────────
# Used by PR 4 (pacing) to attribute fact_digital_daily spend back to
# media_plan_lines via their `line_code`. Kept in sync with the BigQuery
# view (`vw_fact_digital_daily`): both use `r'#\d+[A-Za-z]?'`.
#
# Requires the '#' prefix on purpose — bare numbers inside ad set names
# (impressions, years, etc.) are ambiguous and would corrupt attribution.
# OSSTF-style bare codes ('1A', '2B') aren't extracted here; they'd
# need a separate, plan-specific heuristic.

_ADSET_LINE_CODE_RE = re.compile(r"#\d+[A-Za-z]?")

# The identical BigQuery RE2 pattern (kept here as a string so the view
# SQL and Python stay provably in sync).
BQ_LINE_CODE_REGEX = r"#\d+[A-Za-z]?"


def extract_line_codes_from_adset_name(name: str | None) -> list[str]:
    """Extract all `#XX` line codes from an ad set name.

    Returns codes in order of appearance, preserving duplicates (caller
    decides whether to dedupe). Examples:
      - "#11 viewers BC"              → ["#11"]
      - "#11 viewers BC, #12 list"    → ["#11", "#12"]
      - "Conversions CA"              → []
      - "24 hours"                    → []  (no '#')
      - "#14A Retargeting"            → ["#14A"]
    """
    if not name:
        return []
    return _ADSET_LINE_CODE_RE.findall(name)


# ── Merge metadata ───────────────────────────────────────────────────
# Media planners use merged cells in the Budget column to signal shared-budget
# (CBO-style) bundles — e.g. Squamish (25034) Flight 2 Meta has three 2-row
# merges creating three distinct sub-bundles. gspread's get_all_values() strips
# merges (value only in the top-left cell), so we fetch the merge ranges from
# the spreadsheet metadata and stamp child rows with `merged_with_previous`.
# PR 3 consumes that flag to populate the bundle sidecar table.

def _fetch_worksheet_merges(ws: "gspread.Worksheet | None") -> list[dict]:
    """Return merge ranges for a worksheet as a list of GSheets API dicts
    (keys: startRowIndex, endRowIndex, startColumnIndex, endColumnIndex).

    Best-effort: if the metadata fetch fails, we log and return []. Parsing
    falls back to treating every line as a standalone (pre-PR-1b behaviour).
    """
    if ws is None:
        return []
    try:
        meta = ws.spreadsheet.fetch_sheet_metadata(
            params={"fields": "sheets(properties(sheetId,title),merges)"}
        )
    except Exception as exc:  # network error, auth error, schema drift, etc.
        logger.warning(
            "Could not fetch merge metadata for worksheet %r: %s",
            getattr(ws, "title", "?"), exc,
        )
        return []
    for sheet in meta.get("sheets", []):
        props = sheet.get("properties", {})
        if props.get("sheetId") == getattr(ws, "id", None) or (
            props.get("title") == getattr(ws, "title", None)
        ):
            return sheet.get("merges", []) or []
    return []


def _assign_bundle_groups(mp_lines: list[dict]) -> None:
    """In-place: annotate mp_lines with `bundle_group` (int or None).

    Walks lines in order. A line with `merged_with_previous=True` inherits the
    previous line's group. Otherwise the line starts a fresh candidate group.
    Singleton groups (standalone rows with no merged neighbours) collapse to
    `bundle_group=None`.

    Edge case: the first line never has a real parent, so a stray
    `merged_with_previous=True` on lines[0] is treated as False (standalone).
    """
    if not mp_lines:
        return

    # Pass 1 — assign raw candidate group indices.
    next_group = 0
    for i, line in enumerate(mp_lines):
        inherits = i > 0 and bool(line.get("merged_with_previous"))
        if inherits:
            line["bundle_group"] = mp_lines[i - 1]["bundle_group"]
        else:
            line["bundle_group"] = next_group
            next_group += 1

    # Pass 2 — strip singletons (group size < 2).
    counts: dict[int, int] = {}
    for line in mp_lines:
        g = line["bundle_group"]
        counts[g] = counts.get(g, 0) + 1
    for line in mp_lines:
        if counts[line["bundle_group"]] < 2:
            line["bundle_group"] = None


def _compute_bundle_id(project_code: str, members: list[dict]) -> str:
    """Build a stable, human-readable bundle_id from the group parent.

    Format: ``{project_code}-{platform_id}-{first_line_code_sans_hash}``
      e.g. "25034-meta-09" for a Squamish Meta bundle starting at #09.
      e.g. "25042-meta-1A" for an OSSTF-style bare-code bundle.

    Falls back to an 8-char MD5 digest of the members' audience_names when
    no usable line_code is available — deterministic, but opaque.
    """
    first = members[0] if members else {}
    platform = first.get("platform_id") or "unknown"
    first_code = (first.get("line_code") or "").lstrip("#").strip()
    if first_code:
        return f"{project_code}-{platform}-{first_code}"
    import hashlib

    tag = "|".join((m.get("audience_name") or "") for m in members)
    digest = hashlib.md5(tag.encode("utf-8")).hexdigest()[:8]
    return f"{project_code}-{platform}-{digest}"


def _merged_child_rows_for_column(
    merges: list[dict], col_index: int
) -> set[int]:
    """Return the set of 0-indexed sheet rows that are merged children
    (not the top-left) for the given column index.

    A merge with startRowIndex=6, endRowIndex=8, startColumnIndex=13,
    endColumnIndex=14 covers rows 6-7 inclusive. Row 6 is the parent
    (carries the value); row 7 is the merged child.
    """
    children: set[int] = set()
    for m in merges:
        try:
            c_start = int(m["startColumnIndex"])
            c_end = int(m["endColumnIndex"])
            r_start = int(m["startRowIndex"])
            r_end = int(m["endRowIndex"])
        except (KeyError, TypeError, ValueError):
            continue
        if not (c_start <= col_index < c_end):
            continue
        if r_end - r_start <= 1:
            continue
        for r in range(r_start + 1, r_end):
            children.add(r)
    return children


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


# ── Schema migrations (idempotent) ────────────────────────────────

_MIGRATIONS_RUN = False


def _ensure_schema_migrations(mtl: bigquery.Client) -> None:
    """Run one-time schema migrations. Guarded so it executes at most once per process."""
    global _MIGRATIONS_RUN
    if _MIGRATIONS_RUN:
        return
    prefix = f"`{settings.gcp_project_id}.{settings.bigquery_dataset}"

    stmts = [
        # Bug 5 (ADAC-26): per-line flight dates
        f"ALTER TABLE {prefix}.media_plan_lines` ADD COLUMN IF NOT EXISTS flight_start DATE",
        f"ALTER TABLE {prefix}.media_plan_lines` ADD COLUMN IF NOT EXISTS flight_end DATE",
        # bcdirect: is_direct marks a line with a real budget but NO self-serve
        # spend feed (platform not in PLATFORM_MAP) — a "direct buy" (CTV, DOOH
        # direct, building projection, LED truck, transit, …). Pacing excludes
        # these exactly like is_traditional so they never produce budget_tracking
        # rows / alarms. Orthogonal to is_traditional (media type, keyword-based);
        # a line can be both. Mirrors the is_traditional / flight_start ADD COLUMN
        # IF NOT EXISTS pattern so it self-heals on the prod table.
        f"ALTER TABLE {prefix}.media_plan_lines` ADD COLUMN IF NOT EXISTS is_direct BOOLEAN",
        # is_direct_override: the user's manual override of the auto is_direct
        # classification. NULL = no override (use auto). Re-applied after each
        # sync from media_plan_line_overrides, so it survives re-syncs while
        # auto-classification still runs underneath. Effective is_direct =
        # COALESCE(is_direct_override, is_direct).
        f"ALTER TABLE {prefix}.media_plan_lines` ADD COLUMN IF NOT EXISTS is_direct_override BOOLEAN",
        # Versioned-write pattern: sync_version tracking for all tables
        f"ALTER TABLE {prefix}.media_plans` ADD COLUMN IF NOT EXISTS sync_version STRING",
        f"ALTER TABLE {prefix}.media_plan_lines` ADD COLUMN IF NOT EXISTS sync_version STRING",
        f"ALTER TABLE {prefix}.blocking_chart_weeks` ADD COLUMN IF NOT EXISTS sync_version STRING",
        # Bundled-optimization (PR 3): bundle_id links sibling lines, bundle_role
        # tracks planner intent state (suggested_* on first detection; confirmed_*
        # / rejected once a user acts in the UI — PR 5).
        f"ALTER TABLE {prefix}.media_plan_lines` ADD COLUMN IF NOT EXISTS bundle_id STRING",
        f"ALTER TABLE {prefix}.media_plan_lines` ADD COLUMN IF NOT EXISTS bundle_role STRING",
        # Bundle children carry bundle_id but budget IS NULL — the parent row
        # holds the pool total. Make budget nullable so BQ's NOT NULL constraint
        # doesn't reject child inserts. (The original schema.sql had
        # `budget NUMERIC NOT NULL`.)
        f"ALTER TABLE {prefix}.media_plan_lines` ALTER COLUMN budget DROP NOT NULL",
        # Bug 3 (ADAC-18): audience_name overrides table
        f"""CREATE TABLE IF NOT EXISTS {prefix}.media_plan_line_overrides` (
            project_code STRING,
            platform_id STRING,
            budget FLOAT64,
            audience_name STRING,
            is_direct_override BOOLEAN,
            updated_at TIMESTAMP
        )""",
        # is_direct_override added via ALTER too, so existing prod override
        # tables gain the column (CREATE IF NOT EXISTS is a no-op once it exists).
        f"ALTER TABLE {prefix}.media_plan_line_overrides` ADD COLUMN IF NOT EXISTS is_direct_override BOOLEAN",
        # Multi-plan support (2026-04-25): join table mapping projects to one or
        # more media plan sheets. Backfill from dim_projects.media_plan_sheet_id
        # is handled by the live migration script
        # (infrastructure/bigquery/migrations/2026-04-25_project_media_plans.sql);
        # this DDL exists so dev/test environments still get the table.
        f"""CREATE TABLE IF NOT EXISTS {prefix}.project_media_plans` (
            project_code STRING NOT NULL,
            sheet_id STRING NOT NULL,
            phase_label STRING,
            display_order INT64,
            is_active BOOL,
            created_at TIMESTAMP
        )""",
        # Pacing race guard: a media-plan sync writes a lock row here while it
        # rewrites media_plan_lines; a concurrent pacing run skips a locked
        # project so it never reads a half-written state (26023 zero-spend).
        f"""CREATE TABLE IF NOT EXISTS {prefix}.pacing_sync_locks` (
            project_code STRING NOT NULL,
            locked_at TIMESTAMP
        )""",
    ]
    for sql in stmts:
        try:
            mtl.query(sql).result()
        except Exception as e:
            # Column/table already exists — safe to ignore
            if "Already Exists" in str(e) or "Duplicate" in str(e):
                pass
            else:
                logger.warning("  Schema migration warning: %s", e)

    _MIGRATIONS_RUN = True
    logger.info("  Schema migrations verified")


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

def _parse_blocking_chart(
    ws: gspread.Worksheet, anchor_year: int | None = None
) -> dict:
    """Parse the Blocking Chart tab using label-based discovery.

    ``anchor_year`` is the project's authoritative campaign year (from
    ``dim_projects.start_date``). It is used ONLY to resolve the year for
    Start/End cells that carry no explicit year — the 24058 date-year fix.
    When the chart's date cells include a real year, that year always wins and
    the anchor is ignored (no regression for correctly-dated plans).
    """
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

    # First pass: locate the Start/End cells and note whether they carry an
    # explicit year. We seed the parse with the anchor year so a year-less
    # "June 1" lands in the campaign's real year on the FIRST parse (not
    # today's), then re-derive ref_year from the resolved start_date.
    start_cell_has_year = False
    start_date = None
    end_date = None
    seed_year = anchor_year or date.today().year
    if dates_pos:
        r, c = dates_pos
        # Start/end may be in adjacent cells (c+1, c+2) or (c+1, c+3)
        for offset in range(1, 5):
            raw_cell = _cell(all_data, r, c + offset)
            d = _parse_date(raw_cell, seed_year)
            if d:
                if start_date is None:
                    start_date = d
                    start_cell_has_year = _cell_has_explicit_year(raw_cell)
                else:
                    end_date = d
                    break

    # Resolve the canonical ref_year for the rest of the chart (week-header
    # and per-cell date parsing below). Explicit year in the sheet wins;
    # otherwise the project anchor; otherwise today (legacy fallback).
    ref_year = _resolve_ref_year(
        bc_start=start_date,
        bc_dates_are_year_less=not start_cell_has_year,
        project_anchor_year=anchor_year,
    ) or date.today().year

    # If the seed year differed from the resolved year (e.g. we seeded with
    # today because no anchor was passed, but the sheet actually had an
    # explicit year), realign start/end onto the resolved year for year-less
    # cells so the metadata window matches the per-line flights. Preserve
    # year-rollover: a window like "Dec 15 → Jan 9" puts the end in the NEXT
    # year, so only shift the end by the same delta the start moved.
    if start_date is not None and not start_cell_has_year and start_date.year != ref_year:
        year_delta = ref_year - start_date.year
        start_date = start_date.replace(year=ref_year)
        if end_date is not None:
            end_date = end_date.replace(year=end_date.year + year_delta)

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

def _parse_media_plan_tab(
    ws: "gspread.Worksheet | None",
    prefetched_data: list[list[str]] | None = None,
    ref_year: int | None = None,
    prefetched_merges: list[dict] | None = None,
    stats: dict | None = None,
) -> list[dict]:
    """Parse the Media Plan tab for detailed line items with targeting info.

    Args:
        ws: gspread Worksheet to parse. May be None if `prefetched_data` and
            `prefetched_merges` are both provided (test path).
        prefetched_data: Pre-fetched sheet data to avoid redundant API calls.
        ref_year: Reference year for month-day dates (from blocking chart start_date).
                 If not provided, defaults to today's year (fallback).
        prefetched_merges: Pre-fetched merge metadata (list of GSheets merge
            range dicts). If None, fetches from `ws`. Pass `[]` to explicitly
            skip merge detection (e.g. tests that don't care about bundles).
        stats: Optional mutable dict the caller passes to collect parse
            diagnostics. When provided, the key
            ``"rows_skipped_unparseable_budget"`` is INCREMENTED (created at 0
            if absent) for every data row whose Budget cell held content that
            ``_parse_money`` could not turn into a number (ADAC-69). Omitting
            ``stats`` leaves behaviour and the plain-list return type unchanged,
            so existing callers/tests are unaffected.
    """
    all_data = prefetched_data or ws.get_all_values()
    if len(all_data) < 14:
        return []

    # Use ref_year from blocking chart if available, otherwise fallback to today
    if ref_year is None:
        ref_year = date.today().year

    # Find the header row by word-level presence. Search EVERY row
    # (from row 0 through the end of the tab):
    #   - Most plans have Client / Project / Run-Dates metadata at rows
    #     0–3 and the header starting at row 4+, but some don't.
    #     Squamish's "Combined Plan for Frazer" puts the header at row 0
    #     with no preamble — starting the search at row 4 skipped past it.
    #   - Some plans have long preamble (summary blocks, Blocking Chart
    #     refs) that pushes the header past row 14 — can't cap the
    #     search at row 14 either.
    # We check for word-level presence rather than a fixed substring
    # because headers often have whitespace or newlines around
    # separators (e.g. "Site / Network", "Campaign Type/\nObjective"),
    # which break a naive substring match like `"site/network" in row_text`.
    header_row_idx = None
    for r in range(len(all_data)):
        row_text = " ".join(c.strip().lower() for c in all_data[r])
        has_site_network = "site" in row_text and "network" in row_text
        has_goal_start = "goal" in row_text and "start" in row_text
        has_start_end = "start date" in row_text and "end date" in row_text
        if has_site_network or has_goal_start or has_start_end:
            header_row_idx = r
            logger.info(
                "  Media Plan tab header row located at row %d: %r",
                r,
                [c[:40] for c in all_data[r] if c.strip()],
            )
            break
    if header_row_idx is None:
        # Diagnostic dump: print the first 30 rows with non-empty content so
        # we can see what the parser is actually looking at when it can't
        # find a header. Each row truncated to 200 chars to keep logs sane.
        sample = []
        for i, row in enumerate(all_data[:30]):
            non_empty = [c for c in row if c.strip()]
            if non_empty:
                joined = " | ".join(non_empty)
                sample.append(f"row {i}: {joined[:200]!r}")
        logger.warning(
            "Could not find header row in Media Plan tab. "
            "len(all_data)=%d; first non-empty rows:\n%s",
            len(all_data),
            "\n".join(sample) if sample else "(all rows empty)",
        )
        return []

    headers = all_data[header_row_idx]

    # Build column map from header names.
    #
    # Priority note: matchers are ordered specific → general to avoid
    # collisions (e.g. "Goal Frequency" must hit `frequency`, not `goal`;
    # "Group Name" must hit `group_name`, not `audience_name` via a generic
    # "name" fallback).
    #
    # Known header variants this matcher handles:
    #   OSSTF (25042): "Site/Network", "Goal", "Start", "End", "Days", "ID",
    #                  "Audience Name", "Geo", "Audience Targeting", ...
    #   Squamish (25034): "Site/Network", "Campaign Type/Objective",
    #                     "Start Date", "End Date", "# Days", "Audience Group",
    #                     "Group Name", "Notes/Targeting", "Geo Target",
    #                     "Creative", "Pricing", "Est'd Rate",
    #                     "Est'd Impressions", "Budget $"
    col_map: dict[str, int] = {}
    for i, h in enumerate(headers):
        h_lower = h.strip().lower()
        if not h_lower:
            continue

        # --- Specific matchers first ---
        # "Goal Frequency" / "Frequency" → frequency (not goal)
        if ("goal" in h_lower and "freq" in h_lower) or (
            "freq" in h_lower and "goal" not in h_lower
        ):
            col_map["frequency"] = i
        elif "site" in h_lower or "network" in h_lower:
            col_map["platform"] = i
        # "Group Name" (Squamish Col G) — must precede audience_name matcher
        # because "name" could otherwise collide via the legacy fallback.
        elif "group name" in h_lower:
            col_map["group_name"] = i
        # "Audience Group" (Squamish Col F) — broader audience category.
        elif "audience group" in h_lower:
            col_map["audience_group"] = i
        elif "est" in h_lower and "impression" in h_lower:
            col_map["est_impressions"] = i
        elif "est" in h_lower and "audience" in h_lower:
            col_map["est_audience"] = i
        elif (
            "audience name" in h_lower
            or "ad set name" in h_lower
            or "adset name" in h_lower
        ):
            col_map["audience_name"] = i
        elif "audience" in h_lower and "targeting" in h_lower:
            col_map["audience_targeting"] = i
        elif h_lower in ("id", "line id", "line code") or "internal adset" in h_lower:
            col_map["id"] = i
        # Goal / Objective — widened to accept "Campaign Type/Objective"
        # (Squamish) and bare "Objective".
        elif (
            h_lower in ("goal", "objective")
            or "campaign type" in h_lower
        ):
            col_map["goal"] = i
        # Start / End — widened to accept "Start Date" / "End Date".
        elif h_lower == "start" or "start date" in h_lower:
            col_map["start"] = i
        elif h_lower == "end" or "end date" in h_lower:
            col_map["end"] = i
        # Days — widened to accept "# Days".
        elif h_lower in ("days", "# days") or "# days" in h_lower:
            col_map["days"] = i
        # Budget — widened to accept "Budget $".
        elif h_lower in ("budget", "budget $"):
            col_map["budget"] = i
        elif "geo" in h_lower:
            col_map["geo"] = i
        elif "technical" in h_lower:
            col_map["technical"] = i
        elif "landing" in h_lower:
            col_map["landing_page"] = i
        elif "creative" in h_lower:
            col_map["creative"] = i
        elif "pricing" in h_lower:
            col_map["pricing"] = i
        elif "bid" in h_lower:
            col_map["bid"] = i

    # Fallback: if no audience_name column was found, try less-specific headers.
    # Must NOT hijack "Group Name" — the group_name slot has its own path below.
    if "audience_name" not in col_map:
        for i, h in enumerate(headers):
            h_lower = h.strip().lower()
            if h_lower in ("audience", "name") and i != col_map.get("group_name"):
                col_map["audience_name"] = i
                break

    # Silent-fail warnings: log expected-but-missing columns. Parsing continues.
    _expected_critical = {"start", "end", "budget"}
    _missing = _expected_critical - col_map.keys()
    if _missing:
        logger.warning(
            "Media Plan tab missing expected columns %s; "
            "parsed headers were: %r",
            sorted(_missing),
            headers,
        )
    if not (
        "id" in col_map
        or "audience_name" in col_map
        or "group_name" in col_map
    ):
        logger.warning(
            "Media Plan tab has no line-identifier column "
            "(id / audience_name / group_name); rows will lack identifiers"
        )

    logger.info("  Media Plan tab column map: %s", col_map)

    # Merged-cell detection on the Budget column — primary bundle signal.
    # See _fetch_worksheet_merges docstring for background.
    if prefetched_merges is not None:
        merges = prefetched_merges
    else:
        merges = _fetch_worksheet_merges(ws)

    budget_col = col_map.get("budget")
    merged_budget_rows: set[int] = (
        _merged_child_rows_for_column(merges, budget_col)
        if budget_col is not None
        else set()
    )
    if merged_budget_rows:
        logger.info(
            "  Media Plan tab found %d merged-budget child rows (bundle signal)",
            len(merged_budget_rows),
        )

    def gc(row: list[str], key: str) -> str:
        idx = col_map.get(key)
        if idx is not None and idx < len(row):
            return row[idx].strip()
        return ""

    lines = []
    current_platform = None
    current_goal = None
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
            if plat_raw != current_platform:
                # New platform block — never leak a goal across platforms.
                current_goal = None
            current_platform = plat_raw

        line_code = gc(row, "id")
        goal = gc(row, "goal")
        # Campaign Type / Goal cells are vertically merged across language
        # pairs in PB plans (EN row carries the value, FR row reads empty),
        # which used to make the `not goal and not line_code` guard silently
        # drop every FR row (26018 lost $2,245 across 3 lines — AI-022).
        # Mirror the current_platform carry-forward, but ONLY onto rows that
        # look like data rows (dates or audience present); footer/total rows
        # must keep failing the guard rather than inherit a goal.
        if goal:
            current_goal = goal
        elif current_goal and (
            gc(row, "start") or gc(row, "audience_group") or gc(row, "group_name")
        ):
            goal = current_goal
        budget_raw = gc(row, "budget")
        budget = _parse_money(budget_raw)
        audience_name = gc(row, "audience_name")

        # Squamish Col G "Group Name" carries both the line_code and the
        # audience description in a single cell (e.g. "#09 North Van Engagers").
        # Derive each field where it isn't already set by a dedicated column.
        group_name_raw = gc(row, "group_name")
        if group_name_raw:
            gn_code, gn_rest = _extract_line_code(group_name_raw)
            if not line_code and gn_code:
                line_code = gn_code
            if not audience_name:
                audience_name = gn_rest or group_name_raw

        # Skip total/footer rows
        if current_platform and current_platform.lower() in ("total", "grand total", "terms"):
            continue
        if not goal and not line_code:
            continue
        if "total" in goal.lower():
            continue

        # ADAC-69: surface budget cells we couldn't parse instead of dropping
        # them silently. The row survives (its targeting is still useful and it
        # may carry a line_code), but budget lands as None and would otherwise
        # vanish from any budget rollup with no trace. Only count rows where the
        # cell actually HAD content — a blank Budget cell is a deliberate
        # planner choice (e.g. bundle children whose pool lives on the parent),
        # not a parse failure. Counted here, after the identity gates, so
        # footer/total rows can't inflate the figure. The structured log carries
        # the offending value + row index for fast triage.
        if budget is None and budget_raw.strip():
            if stats is not None:
                stats["rows_skipped_unparseable_budget"] = (
                    stats.get("rows_skipped_unparseable_budget", 0) + 1
                )
            logger.warning(
                "  Media Plan row %d: unparseable budget %r (platform=%r, "
                "line_code=%r) — keeping row but budget is NULL",
                row_idx, budget_raw, current_platform, line_code,
            )

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
            "audience_name": audience_name,
            "audience_group": gc(row, "audience_group"),
            "geo_targeting": gc(row, "geo"),
            "audience_targeting": gc(row, "audience_targeting"),
            "technical_targeting": gc(row, "technical"),
            "landing_page": gc(row, "landing_page"),
            "creative": gc(row, "creative"),
            "pricing_model": gc(row, "pricing"),
            "estimated_impressions": _parse_money(gc(row, "est_impressions")),
            "frequency_cap": gc(row, "frequency"),
            "budget": budget,
            "merged_with_previous": row_idx in merged_budget_rows,
        })

    # PR 3: annotate bundle_group on each line so sync_media_plan can emit
    # bundle siblings (suggested_child rows) alongside the bundle parent.
    _assign_bundle_groups(lines)

    return lines


def _synthesise_lines_from_mp(
    mp_lines: list[dict],
    metadata: dict,
    dropped: list[dict] | None = None,
) -> list[dict]:
    """Create blocking-chart-style line dicts from media plan tab lines.

    Used as the authoritative detail-tab line source (and as a fallback when
    the blocking chart only had section/flight headers and no actual platform
    rows).

    Two axes decide a row's fate:

      * BUDGET. A row with no budget or budget<=0 is DROPPED. It carries no
        spend pool, so it can't pace or reconcile. When ``dropped`` is passed,
        a structured ``{label, platform_id, budget, reason: "no_budget"}``
        record is appended for surfacing upstream (item 1 of the bcdirect
        build) instead of vanishing into a log line.

      * TRACKABILITY (``is_direct``). A row with a real budget (>0) but a
        platform that isn't in ``PLATFORM_MAP.values()`` has NO self-serve
        spend feed (StackAdapt / Meta / etc.) — it's a "direct buy" (CTV,
        DOOH-as-a-direct-line, building projection, LED truck, transit, …).
        Such a row is NO LONGER dropped: it's retained and tagged
        ``is_direct=True`` so the line-record builder writes it through to
        BigQuery and pacing can exclude it (like ``is_traditional``). A row
        whose platform IS recognised stays ``is_direct=False`` — unchanged
        behaviour for every existing project, all of whose lines are on
        recognised platforms.

    ``is_direct`` is the trackability axis (PLATFORM_MAP membership). It is
    ORTHOGONAL to ``is_traditional`` (media type, keyword-based, set later in
    ``_build_line_records_for_bc_line``); a line can be both, and this function
    never touches ``is_traditional``.
    """
    lines = []
    seen: set[tuple] = set()
    for mp in mp_lines:
        pid = mp.get("platform_id")
        if not pid:
            continue

        is_direct = pid not in PLATFORM_MAP.values()
        budget = mp.get("budget")

        # Bundle children carry budget=NULL BY DESIGN — the merged-cell pool
        # lives on the bundle PARENT, and the child IS written to BigQuery (with
        # budget=NULL) under that parent via _build_line_records_for_bc_line. So
        # a no-budget bundle child is NOT a lost line and must never surface as a
        # "dropped" cry-wolf (24058 reported dropped=23 — exactly its 23 pooled
        # bundle children). A row is a bundle child when the parser flagged its
        # Budget cell as a merge continuation (merged_with_previous) and/or it
        # was grouped into a multi-row bundle (bundle_group is not None).
        is_bundle_child = bool(
            mp.get("merged_with_previous") or mp.get("bundle_group") is not None
        )

        # No spend pool → drop, but surface it (don't vanish silently) UNLESS it
        # is a pooled bundle child, which is written under its parent (above).
        if not budget or budget <= 0:
            if dropped is not None and not is_bundle_child:
                dropped.append({
                    "label": mp.get("platform", "") or mp.get("audience_name", ""),
                    "platform_id": pid,
                    "budget": budget,
                    # An unrecognised platform that ALSO has no budget is
                    # reported as no_budget — without a budget we can't capture
                    # it as a direct line anyway, and no_budget is the more
                    # actionable signal (a real direct buy would carry a budget).
                    "reason": "no_budget",
                })
            continue

        if is_direct:
            # Budgeted but no self-serve feed: capture instead of drop, tagged
            # is_direct so it's retained, written, and excluded from pacing.
            logger.warning(
                "Media plan sync: capturing direct-buy row (no self-serve feed) "
                "platform=%r (platform_id=%r, budget=%s) as is_direct — excluded "
                "from pacing, counted in reconciliation",
                mp.get("platform"), pid, budget,
            )

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
            # Trackability axis: TRUE = no self-serve feed (direct buy).
            # Threaded into _build_line_records_for_bc_line via the bc_line.
            "is_direct": is_direct,
        })
    return lines


def _mp_lines_have_audience_data(mp_lines: list[dict]) -> bool:
    """Check if media plan tab lines have audience-level detail worth using."""
    return any(
        mp.get("audience_name") and mp.get("budget") and mp.get("budget") > 0
        for mp in mp_lines
    )


def _detail_tab_lines_usable(detail_lines: list[dict]) -> bool:
    """True if detail-tab-derived lines are good enough to be authoritative.

    ``detail_lines`` here is the output of ``_synthesise_lines_from_mp`` — it
    has already been through the recognised-platform filter and the >0-budget
    gate, so each surviving line carries a real per-line budget and a known
    platform. We only require that at least one such line exists. When the
    detail tab is missing, malformed, or yields nothing recognisable, this
    returns False and the caller keeps the blocking chart as the source
    (legacy fallback — protects existing projects).
    """
    return bool(detail_lines)


def _reattach_bc_weeks_to_lines(
    old_bc_lines: list[dict],
    old_bc_weeks: list[dict],
    new_lines: list[dict],
) -> list[dict]:
    """Re-key the blocking chart's weekly flighting onto a new line set.

    The blocking chart is the legitimate source of weekly *activation*
    patterns (which weeks a platform is live / paused — the burst-pause
    grid). But when the media-plan DETAIL tab becomes the authoritative
    source for line identity/budgets/platforms, the original
    ``blocking_chart_weeks`` are keyed by ``line_index`` into the OLD
    blocking-chart line list, which no longer corresponds 1:1 to the new
    lines. This helper transfers each old line's week pattern onto the new
    line(s) on the SAME ``platform_id`` so the activation grid is preserved
    rather than dropped.

    Matching is by ``platform_id`` because that's the only field both the
    blocking chart and the detail tab reliably share (the blocking chart's
    merged-budget rows can't be matched by budget, and line_codes don't
    exist on the chart). When several new lines share a platform that had a
    single blocking-chart pattern, each inherits a copy of that pattern —
    the activation calendar is identical for co-platform lines, which is the
    correct default (the blocking chart only ever drew one row per platform).

    Returns a fresh ``weeks`` list shaped exactly like the parser's output
    (``{"line_index", "week_start", "is_active"}``) but indexed into
    ``new_lines``. Lines whose platform had no blocking-chart weeks are
    simply absent here; the caller's existing flight-date fallback then
    synthesises their weeks from flight_start/flight_end (so weeks are never
    zeroed — only re-sourced).
    """
    if not old_bc_weeks or not new_lines:
        return []

    # Group the old week patterns by the platform_id of the old line they
    # belonged to. Preserve order so a platform that legitimately had two
    # blocking-chart rows (rare) keeps both patterns available round-robin.
    weeks_by_old_idx: dict[int, list[dict]] = {}
    for w in old_bc_weeks:
        weeks_by_old_idx.setdefault(w["line_index"], []).append(w)

    patterns_by_platform: dict[str | None, list[list[dict]]] = {}
    for old_idx, wk_list in weeks_by_old_idx.items():
        if old_idx >= len(old_bc_lines):
            continue
        pid = old_bc_lines[old_idx].get("platform_id")
        patterns_by_platform.setdefault(pid, []).append(wk_list)

    # Round-robin cursor per platform so multiple old rows on one platform
    # are spread across multiple new lines on that platform if present.
    cursor: dict[str | None, int] = {}
    new_weeks: list[dict] = []
    for new_idx, line in enumerate(new_lines):
        pid = line.get("platform_id")
        patterns = patterns_by_platform.get(pid)
        if not patterns:
            continue
        i = cursor.get(pid, 0)
        chosen = patterns[i % len(patterns)]
        cursor[pid] = i + 1
        for w in chosen:
            new_weeks.append({
                "line_index": new_idx,
                "week_start": w["week_start"],
                "is_active": w["is_active"],
            })
    return new_weeks


# ── Line-record builder (extracted from sync_media_plan for testability) ──


def _build_line_records_for_bc_line(
    bc_line: dict,
    mp_detail: dict | None,
    all_mp_lines: list[dict],
    plan_id: str,
    line_id: str,
    project_code: str,
    meta: dict,
) -> list[dict]:
    """Turn one blocking-chart line (+ its matched mp line) into 1..N records.

    Standalone lines produce exactly one record. Bundled lines (where
    mp_detail carries a non-None ``bundle_group``) produce one
    ``suggested_parent`` record — the bc_line's own row — plus one
    ``suggested_child`` record for every other mp_line in the same bundle
    group. Children carry ``budget=NULL`` so ``SUM(budget) GROUP BY bundle_id``
    gives the pool total without double-counting.

    Pure function: no BQ, no Sheets, no filesystem. Easy to unit-test.
    """
    flight_start = bc_line.get("flight_start") or meta.get("start_date")
    flight_end = bc_line.get("flight_end") or meta.get("end_date")
    line_code = mp_detail.get("line_code") if mp_detail else None

    # Bundle detection: does this bc_line match an mp_line that belongs to a
    # multi-row merged-budget group?
    bundle_group = mp_detail.get("bundle_group") if mp_detail else None
    bundle_siblings: list[dict] = []
    bundle_id: str | None = None
    bundle_role: str | None = None
    if bundle_group is not None:
        bundle_siblings = [
            mp for mp in all_mp_lines
            if mp is not mp_detail and mp.get("bundle_group") == bundle_group
        ]
        if bundle_siblings:
            bundle_id = _compute_bundle_id(
                project_code, [mp_detail] + bundle_siblings
            )
            bundle_role = "suggested_parent"

    channel_category = _channel_category(bc_line.get("objective_format", ""))
    is_traditional = _is_traditional_media(
        bc_line.get("platform"), bc_line.get("platform_id")
    )
    # Trackability axis (bcdirect): TRUE when the line has a budget but no
    # self-serve feed. Carried on the bc_line by _synthesise_lines_from_mp.
    # Defaults to False, so blocking-chart-sourced lines and synthetic bundle
    # parents (legacy paths) stay is_direct=False — recognised-platform lines
    # for all existing projects are unaffected. Independent of is_traditional.
    is_direct = bool(bc_line.get("is_direct", False))

    records: list[dict] = [{
        "line_id": line_id,
        "plan_id": plan_id,
        "project_code": project_code,
        "line_code": line_code,
        "platform_id": bc_line["platform_id"],
        "site_network": bc_line["platform"],
        "channel_category": channel_category,
        "flight_start": flight_start.isoformat() if flight_start else None,
        "flight_end": flight_end.isoformat() if flight_end else None,
        "objective": bc_line.get("objective_format"),
        "audience_name": bc_line.get("audience_name") or (
            mp_detail.get("audience_name") if mp_detail else None
        ),
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
        "is_traditional": is_traditional,
        "is_direct": is_direct,
        "bundle_id": bundle_id,
        "bundle_role": bundle_role,
    }]

    # Emit suggested_child rows for every sibling. Children carry bundle_id
    # but budget IS NULL — the parent row holds the pool.
    for j, sib in enumerate(bundle_siblings, start=1):
        sib_flight_start = sib.get("flight_start") or flight_start
        sib_flight_end = sib.get("flight_end") or flight_end
        records.append({
            "line_id": f"{line_id}-bundled-{j:02d}",
            "plan_id": plan_id,
            "project_code": project_code,
            "line_code": sib.get("line_code"),
            "platform_id": bc_line["platform_id"],
            "site_network": bc_line["platform"],
            "channel_category": channel_category,
            "flight_start": (
                sib_flight_start.isoformat() if sib_flight_start else None
            ),
            "flight_end": (
                sib_flight_end.isoformat() if sib_flight_end else None
            ),
            "objective": bc_line.get("objective_format"),
            "audience_name": sib.get("audience_name"),
            "audience_targeting": sib.get("audience_targeting"),
            "landing_page": sib.get("landing_page"),
            "pricing_model": sib.get("pricing_model"),
            # CRITICAL: children must have NULL budget. Pool lives on parent.
            "budget": None,
            "estimated_impressions": (
                int(sib["estimated_impressions"])
                if sib.get("estimated_impressions")
                else None
            ),
            "frequency_cap": sib.get("frequency_cap"),
            "geo_targeting": sib.get("geo_targeting"),
            "is_traditional": is_traditional,
            # A bundle's children share the parent's trackability: a direct-buy
            # parent's children are direct too (and vice-versa). In practice
            # direct lines aren't merged-budget bundles, so this is almost
            # always False; carried for completeness/consistency.
            "is_direct": is_direct,
            "bundle_id": bundle_id,
            "bundle_role": "suggested_child",
        })

    return records


def _build_reconciliation(
    line_records: list[dict],
    dropped_lines: list[dict],
    net_budget: float | None,
) -> dict:
    """Compute the bcdirect capture-vs-net reconciliation (item 3).

    Pure function over the FINAL ``line_records`` (what actually gets written
    to BigQuery) plus the surfaced ``dropped_lines`` and the plan's
    ``net_budget`` (from bc metadata). Buckets:

      * ``captured_self_serve`` — sum of budget on lines that WILL be paced:
        NOT is_direct AND NOT is_traditional. Bundle children carry NULL budget
        (the parent holds the pool), so ``SUM(budget)`` doesn't double-count.
      * ``captured_direct`` — sum of budget on is_direct lines (direct buys with
        no self-serve feed). These are written but excluded from pacing.
      * ``dropped_budget`` — sum of budget on dropped rows that HAD a budget.
        Should be ~0 now that budgeted unrecognised lines are captured as
        is_direct rather than dropped (only no_budget rows drop, budget None).
      * ``delta`` — ``net_budget - (captured_self_serve + captured_direct)``.

    This is a SOFT signal: ``delta`` can be legitimately non-zero (rounding,
    fee lines, a plan total that intentionally exceeds the sum of buyable
    lines). Callers must never raise/fail on it — it's surfaced for human eyes.
    """
    def _b(v) -> float:
        try:
            return float(v) if v is not None else 0.0
        except (TypeError, ValueError):
            return 0.0

    captured_self_serve = sum(
        _b(r.get("budget"))
        for r in line_records
        if not r.get("is_direct") and not r.get("is_traditional")
    )
    captured_direct = sum(
        _b(r.get("budget")) for r in line_records if r.get("is_direct")
    )
    # Also surface traditional capture so the breakdown sums cleanly to the
    # written total (traditional lines are written but, like is_direct, excluded
    # from pacing). Kept separate so captured_self_serve stays "what paces".
    captured_traditional = sum(
        _b(r.get("budget"))
        for r in line_records
        if r.get("is_traditional") and not r.get("is_direct")
    )
    dropped_budget = sum(
        _b(d.get("budget")) for d in dropped_lines if d.get("budget")
    )

    captured_total = captured_self_serve + captured_direct
    net = _b(net_budget) if net_budget is not None else None
    delta = (net - captured_total) if net is not None else None

    return {
        "net_budget": net,
        "captured_self_serve": round(captured_self_serve, 2),
        "captured_direct": round(captured_direct, 2),
        "captured_traditional": round(captured_traditional, 2),
        "captured_total": round(captured_total, 2),
        "dropped_budget": round(dropped_budget, 2),
        "dropped_count": len(dropped_lines),
        "delta": round(delta, 2) if delta is not None else None,
    }


# ── Sync Orchestrator ───────────────────────────────────────────────


def _list_active_plans(project_code: str) -> list[dict]:
    """Return active rows from project_media_plans for the given project.

    Each entry is ``{"sheet_id": ..., "phase_label": ..., "display_order": ...}``
    sorted by display_order (NULLS LAST), then created_at. Used by
    ``sync_all_for_project`` and the admin API for the Plans section.

    Falls back to an empty list when project_media_plans doesn't exist yet
    (first run before _ensure_schema_migrations) so callers can degrade
    gracefully to legacy single-sheet behaviour.
    """
    mtl = _mtl_client()
    try:
        _ensure_schema_migrations(mtl)
        prefix = f"`{settings.gcp_project_id}.{settings.bigquery_dataset}"
        sql = f"""
            SELECT pmp.sheet_id, pmp.phase_label, pmp.display_order,
                   IF(pmp.sheet_id = dp.media_plan_sheet_id,
                      dp.media_plan_tab_name, NULL) AS tab_name
            FROM {prefix}.project_media_plans` pmp
            LEFT JOIN {prefix}.dim_projects` dp
              ON dp.project_code = pmp.project_code
            WHERE pmp.project_code = @pc
              AND pmp.is_active = TRUE
            ORDER BY pmp.display_order NULLS LAST, pmp.created_at ASC
        """
        try:
            rows = list(
                mtl.query(
                    sql,
                    job_config=bigquery.QueryJobConfig(query_parameters=[
                        bigquery.ScalarQueryParameter("pc", "STRING", project_code),
                    ]),
                ).result()
            )
        except google.cloud.exceptions.NotFound:
            return []
    finally:
        mtl.close()

    return [
        {
            "sheet_id": r["sheet_id"],
            "phase_label": r.get("phase_label"),
            "display_order": r.get("display_order"),
            # Threaded from dim_projects so sync_media_plan can resolve a
            # configured tab whose title doesn't contain "media plan" — e.g.
            # the typo'd "Boosted Impact Media Pan" on 26023. NULL unless this
            # sheet is the project's canonical media_plan_sheet_id.
            "tab_name": r.get("tab_name"),
        }
        for r in rows
    ]


def _acquire_sync_lock(project_code: str) -> None:
    """Mark a media-plan sync in progress so a concurrent pacing run skips this
    project instead of reading half-written media_plan_lines (the 26023
    zero-spend incident). Best-effort: failures are logged, never raised, so a
    lock hiccup can't block a sync. Self-heals the table if it's missing."""
    mtl = _mtl_client()
    try:
        prefix = f"`{settings.gcp_project_id}.{settings.bigquery_dataset}"
        mtl.query(
            f"""CREATE TABLE IF NOT EXISTS {prefix}.pacing_sync_locks` (
                project_code STRING NOT NULL, locked_at TIMESTAMP
            )"""
        ).result()
        mtl.query(
            f"""MERGE {prefix}.pacing_sync_locks` t
            USING (SELECT @pc AS project_code) s ON t.project_code = s.project_code
            WHEN MATCHED THEN UPDATE SET locked_at = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT (project_code, locked_at)
                VALUES (@pc, CURRENT_TIMESTAMP())""",
            job_config=bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("pc", "STRING", project_code),
            ]),
        ).result()
    except Exception as e:
        logger.warning("Could not acquire pacing sync lock for %s: %s", project_code, e)
    finally:
        mtl.close()


def _release_sync_lock(project_code: str) -> None:
    """Clear the sync lock so pacing can resume for this project."""
    mtl = _mtl_client()
    try:
        prefix = f"`{settings.gcp_project_id}.{settings.bigquery_dataset}"
        mtl.query(
            f"DELETE FROM {prefix}.pacing_sync_locks` WHERE project_code = @pc",
            job_config=bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("pc", "STRING", project_code),
            ]),
        ).result()
    except Exception as e:
        logger.warning("Could not release pacing sync lock for %s: %s", project_code, e)
    finally:
        mtl.close()


def sync_all_for_project(project_code: str) -> dict:
    """Sync every active media plan for a project, holding a sync lock so a
    concurrent pacing run skips the project until the rewrite is committed
    (see _acquire_sync_lock / pacing._sync_in_progress)."""
    _acquire_sync_lock(project_code)
    try:
        return _sync_all_for_project_impl(project_code)
    finally:
        _release_sync_lock(project_code)


def _sync_all_for_project_impl(project_code: str) -> dict:
    """Sync every active media plan registered against ``project_code``.

    Iterates ``project_media_plans`` rows in display order and calls
    ``sync_media_plan`` for each. Each sheet is treated as a fully
    independent sync — its own ``plan_id``, its own ``sync_version``, and
    its own scoped delete (see ``_delete_old_versions``). One sheet failing
    does not abort the others; per-sheet errors are returned in the summary
    so the admin UI can surface them next to the offending row.

    Returns a summary dict::

        {
            "project_code": "25013",
            "sheets_attempted": 3,
            "sheets_succeeded": 3,
            "sheets_failed": 0,
            "results": [
                {"sheet_id": "...", "phase_label": "Phase 1",
                 "status": "success", "lines_created": 12, ...},
                ...
            ],
        }
    """
    plans = _list_active_plans(project_code)

    if not plans:
        # No registered plans — fall back to the legacy single-sheet column on
        # dim_projects so existing one-sheet projects keep working even before
        # the backfill migration runs.
        mtl = _mtl_client()
        try:
            prefix = f"`{settings.gcp_project_id}.{settings.bigquery_dataset}"
            rows = list(
                mtl.query(
                    f"SELECT media_plan_sheet_id, media_plan_tab_name "
                    f"FROM {prefix}.dim_projects` WHERE project_code = @pc",
                    job_config=bigquery.QueryJobConfig(query_parameters=[
                        bigquery.ScalarQueryParameter("pc", "STRING", project_code),
                    ]),
                ).result()
            )
        finally:
            mtl.close()
        if rows and rows[0].get("media_plan_sheet_id"):
            plans = [{
                "sheet_id": rows[0]["media_plan_sheet_id"],
                "phase_label": None,
                "display_order": 1,
                "tab_name": rows[0].get("media_plan_tab_name") or None,
            }]

    if not plans:
        logger.info("sync_all_for_project: no active plans for %s", project_code)
        return {
            "project_code": project_code,
            "sheets_attempted": 0,
            "sheets_succeeded": 0,
            "sheets_failed": 0,
            "results": [],
        }

    results: list[dict] = []
    succeeded = 0
    failed = 0
    for plan in plans:
        sheet_id = plan["sheet_id"]
        phase_label = plan.get("phase_label")
        tab_name = plan.get("tab_name")  # only set on the legacy fallback path
        try:
            res = sync_media_plan(
                sheet_id=sheet_id,
                project_code=project_code,
                tab_name=tab_name,
            )
            res.setdefault("status", "success")
            res["sheet_id"] = sheet_id
            res["phase_label"] = phase_label
            results.append(res)
            succeeded += 1
        except Exception as e:
            logger.warning(
                "sync_all_for_project: sheet %s for %s failed: %s",
                sheet_id, project_code, e,
            )
            results.append({
                "sheet_id": sheet_id,
                "phase_label": phase_label,
                "status": "error",
                "message": str(e),
            })
            failed += 1

    return {
        "project_code": project_code,
        "sheets_attempted": len(plans),
        "sheets_succeeded": succeeded,
        "sheets_failed": failed,
        "results": results,
    }


def _emit_line_weeks(
    line_weeks: list[dict],
    line_id: str,
    project_code: str,
    flight_start: "date | None",
    flight_end: "date | None",
) -> list[dict]:
    """Build budget-grid week records for one line, reconciling the
    blocking-chart grid against the (authoritative, possibly detail-tab-enriched)
    flight window.

    The grid (which weeks are on/off) comes from the blocking chart while a
    line's flight dates can come from the media-plan detail tab. If the grid's
    ACTIVE weeks fall entirely outside the flight window, emitting the stale grid
    would starve pacing's active-day count and collapse the planned baseline to 0
    (a silent detail-tab vs blocking-chart conflict). When flight dates are
    available to fall back on, regenerate an even flight-window grid (and warn)
    instead. Otherwise the grid is emitted as-is (the common path), and a line
    with no grid at all gets the even flight-window grid exactly as before.
    """
    have_flight = bool(flight_start and flight_end)
    grid_overlaps_flight = have_flight and any(
        w["is_active"]
        and w["week_start"] <= flight_end
        and w["week_start"] + timedelta(days=6) >= flight_start
        for w in line_weeks
    )
    # Regenerate only when we CAN (flight dates present) and the existing grid is
    # unusable (no weeks at all, or no active week overlaps the flight window).
    regenerate = have_flight and not grid_overlaps_flight

    records: list[dict] = []
    if line_weeks and not regenerate:
        for w in line_weeks:
            records.append({
                "id": f"{line_id}-w-{w['week_start'].isoformat()}",
                "line_id": line_id,
                "project_code": project_code,
                "week_start": w["week_start"].isoformat(),
                "is_active": w["is_active"],
            })
        return records

    if regenerate:
        if line_weeks:
            logger.warning(
                "  Line %s: blocking-chart active weeks do not overlap the "
                "flight window %s..%s (detail-tab vs grid date conflict); "
                "regenerating an even flight-window grid",
                line_id, flight_start, flight_end,
            )
        week_cursor = flight_start - timedelta(days=flight_start.weekday())  # Monday
        while week_cursor <= flight_end:
            is_active = (
                flight_start <= week_cursor + timedelta(days=6)
                and week_cursor <= flight_end
            )
            records.append({
                "id": f"{line_id}-w-{week_cursor.isoformat()}",
                "line_id": line_id,
                "project_code": project_code,
                "week_start": week_cursor.isoformat(),
                "is_active": is_active,
            })
            week_cursor += timedelta(days=7)
    return records


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

    # Ensure schema is up to date (idempotent, runs once per process)
    init_mtl = _mtl_client()
    try:
        _ensure_schema_migrations(init_mtl)
    finally:
        init_mtl.close()

    gc = _get_gspread_client()
    try:
        try:
            ss = gc.open_by_key(sheet_id)
        except gspread.exceptions.SpreadsheetNotFound:
            logger.error("Spreadsheet not found: %s", sheet_id)
            raise ValueError(f"Spreadsheet {sheet_id} not found or not accessible")
        except gspread.exceptions.APIError as e:
            logger.error("Sheets API error opening spreadsheet: %s", e)
            raise ValueError(f"Failed to open spreadsheet: {str(e)}")

        # Find tabs by name — skip example/template tabs, collect ALL media plan tabs
        blocking_ws = None
        media_plan_tabs: list[gspread.Worksheet] = []
        _skip_words = {"example", "template", "sample"}

        try:
            # Normalise the tab_name override for title-match comparison.
            _requested_tab_lower = (
                tab_name.strip().lower() if tab_name else None
            )
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
                # Fix: when the caller (or dim_projects.media_plan_tab_name)
                # specifies a canonical tab that doesn't contain "media plan"
                # in its title (e.g. Squamish's "Combined Plan for Frazer"),
                # still include it. Without this, the processing loop's
                # tab_name filter silently drops every discovered tab and
                # zero mp_lines get parsed.
                elif _requested_tab_lower and title_lower.strip() == _requested_tab_lower:
                    logger.info(
                        "  Including tab by explicit tab_name override: %r",
                        ws.title,
                    )
                    media_plan_tabs.append(ws)
        except gspread.exceptions.APIError as e:
            logger.error("Sheets API error listing tabs: %s", e)
            raise ValueError(f"Failed to list spreadsheet tabs: {str(e)}")

        # ── Disambiguate media plan tabs: prefer canonical over copies/subsets ──
        if len(media_plan_tabs) > 1:
            all_titles = [ws.title for ws in media_plan_tabs]
            canonical_titles = set(_filter_canonical_tabs(all_titles))
            if len(canonical_titles) < len(all_titles):
                removed = [ws.title for ws in media_plan_tabs if ws.title not in canonical_titles]
                logger.info("  Filtered non-canonical media plan tabs: %s", removed)
                media_plan_tabs = [ws for ws in media_plan_tabs if ws.title in canonical_titles]

        if not blocking_ws:
            sheets = ss.worksheets()
            if len(sheets) >= 2:
                blocking_ws = sheets[1]
                if not media_plan_tabs:
                    media_plan_tabs = [sheets[0]]
            else:
                raise ValueError("Could not find Blocking Chart tab")
    finally:
        # Ensure gspread client resources are cleaned up
        try:
            gc.auth.token = None  # Clear auth token to free resources
        except Exception:
            pass  # Safe to ignore

    # ── Resolve the project's authoritative campaign year (24058 fix) ─
    # dim_projects.start_date is set at project creation and is independent of
    # the media-plan sheet's date formatting, so it's the safe anchor for
    # year-less flight cells. Without it, a landed campaign whose sheet uses
    # bare "June 1" cells gets stamped with today's year and shifts a year
    # forward (24058 / OPSEU: 2025-06→12 stored as 2026-06→12, breaking pacing).
    anchor_mtl = _mtl_client()
    try:
        project_anchor_year = _lookup_project_anchor_year(anchor_mtl, project_code)
    finally:
        anchor_mtl.close()
    if project_anchor_year:
        logger.info(
            "  Project anchor year for %s = %d (from dim_projects.start_date)",
            project_code, project_anchor_year,
        )

    # ── Parse blocking chart + filter media plan tabs to this project ─
    bc = _parse_blocking_chart(blocking_ws, anchor_year=project_anchor_year)

    # Extract ref_year from blocking chart start_date for consistent date
    # parsing. The blocking-chart parser has already resolved the correct year
    # using the anchor, so its start_date.year is now trustworthy for year-less
    # sheets too.
    ref_year = None
    if bc["metadata"].get("start_date"):
        ref_year = bc["metadata"]["start_date"].year
        logger.info("  Using ref_year=%d from blocking chart start_date", ref_year)
    elif project_anchor_year:
        # Blocking chart yielded no start_date at all — fall back to the anchor
        # so per-line month-day flights still resolve to the campaign year.
        ref_year = project_anchor_year
        logger.info("  Using ref_year=%d from project anchor (no bc start_date)", ref_year)

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
    # ADAC-69: accumulate per-tab parse diagnostics across all media plan tabs.
    # Currently tracks rows dropped to a NULL budget because their Budget cell
    # was non-empty but unparseable; surfaced in the response payload + a
    # structured log so a bad data cell can't vanish silently.
    parse_stats: dict = {"rows_skipped_unparseable_budget": 0}
    for mp_ws, tab_data in filtered_tabs:
        mp_lines.extend(_parse_media_plan_tab(
            mp_ws, prefetched_data=tab_data, ref_year=ref_year, stats=parse_stats,
        ))

    rows_skipped_unparseable_budget = parse_stats.get(
        "rows_skipped_unparseable_budget", 0
    )
    if rows_skipped_unparseable_budget:
        logger.warning(
            "  Media plan sync for %s (sheet %s): %d row(s) had an unparseable "
            "budget cell — kept with NULL budget. See per-row warnings above.",
            project_code, sheet_id, rows_skipped_unparseable_budget,
        )

    # ── Row-level project filter: remove lines that contain a different
    #    project code in their text fields (catches mixed-project tabs)
    before_count = len(mp_lines)
    mp_lines = [l for l in mp_lines if _line_belongs_to_project(l, project_code)]
    if len(mp_lines) < before_count:
        logger.info("  Row-level project filter: %d → %d lines (removed %d from other projects)",
                     before_count, len(mp_lines), before_count - len(mp_lines))

    # ── Source of truth: the media-plan DETAIL tab ──────────────────
    #   The DETAIL tab carries one row per line with a REAL per-line Budget
    #   column, the platform, the line_code, the audience, and the bundle
    #   structure. The Blocking Chart's Budget column is frequently a SINGLE
    #   MERGED cell holding the plan TOTAL across every platform row (verified
    #   on 26023: W12:W17 = $99,362.06 across all 6 rows), so the chart can
    #   only ever emit ONE line carrying the whole budget — the other platform
    #   rows read empty and are dropped. Treating the chart as authoritative
    #   therefore mis-assigns the plan total to one line and silently loses
    #   the rest (26023: Meta #03 showed $99,362 instead of its real $4,090;
    #   DOOH / Open Web / CTV never appeared).
    #
    #   So: when the detail tab yields usable lines (recognised platform +
    #   real >0 budget, post-filter), those LINES — their budgets, platforms,
    #   line_codes, audiences and bundle grouping — become authoritative. The
    #   blocking chart then contributes ONLY (a) weekly flighting (activation /
    #   burst-pause grid, re-keyed onto the detail lines by platform) and
    #   (b) metadata (flight window / net_budget / ref_year), both of which
    #   it parses reliably.
    #
    #   FALLBACK (unchanged legacy behaviour, protects existing projects):
    #   when there is no detail tab, or it yields no usable lines, the
    #   blocking chart provides the lines exactly as before. Its weeks stay
    #   keyed to those lines untouched.
    mp_matches: dict[int, dict] = {}

    # bcdirect (item 1): collect structured drop/redirect records from the
    # detail→line path so they surface in logs AND the sync response instead of
    # vanishing into a warning. Each record is
    # {label, platform_id, budget, reason} where reason is "no_budget" or
    # "unrecognised_platform". Populated by _synthesise_lines_from_mp below.
    dropped_lines: list[dict] = []

    detail_lines = (
        _synthesise_lines_from_mp(mp_lines, bc["metadata"], dropped=dropped_lines)
        if mp_lines else []
    )

    if _detail_tab_lines_usable(detail_lines):
        # DETAIL TAB AUTHORITATIVE. Swap the chart's merged-budget lines for
        # the detail tab's real per-line set, but re-key the chart's weekly
        # flighting onto the new lines by platform so the burst-pause grid is
        # preserved (never zeroed). Lines whose platform had no chart weeks
        # fall through to the flight-date week synthesis below.
        new_weeks = _reattach_bc_weeks_to_lines(
            bc["lines"], bc["weeks"], detail_lines
        )
        logger.info(
            "  Detail tab authoritative: %d detail line(s) replace %d "
            "blocking-chart line(s); re-keyed %d/%d chart week rows by platform",
            len(detail_lines), len(bc["lines"]), len(new_weeks), len(bc["weeks"]),
        )
        bc["lines"] = detail_lines
        bc["weeks"] = new_weeks
        # Match the (now authoritative) detail lines back to the raw mp_lines
        # so bundle structure, targeting, and line_code enrichment still flow
        # through _build_line_records_for_bc_line exactly as before.
        mp_matches = _match_all_mp_lines(bc["lines"], mp_lines)
    elif not bc["lines"] and mp_lines:
        # No usable detail lines AND the chart is empty — last-resort
        # synthesis (e.g. chart had only flight/section headers). Unchanged.
        logger.warning("  Blocking chart produced 0 lines — falling back to media plan tab lines")
        # Reuse the already-synthesised detail_lines (drops were captured on the
        # first call above; pass dropped=None here so they aren't double-counted).
        bc["lines"] = detail_lines or _synthesise_lines_from_mp(
            mp_lines, bc["metadata"], dropped=None
        )
        mp_matches = _match_all_mp_lines(bc["lines"], mp_lines)
    elif bc["lines"] and mp_lines and _mp_lines_have_audience_data(mp_lines):
        # FALLBACK ENRICHMENT: detail tab had no usable (recognised-platform,
        # budgeted) lines, but the raw mp_lines still carry audience/date
        # detail. Keep the blocking chart authoritative (and its weeks intact)
        # and enrich in place — the pre-existing behaviour for plans whose
        # detail tab is present but doesn't survive the recognised-platform
        # filter on its own.
        mp_matches = _match_all_mp_lines(bc["lines"], mp_lines)
        logger.info("  Media plan tabs have audience data — enriching %d blocking chart lines "
                    "(%d matched)", len(bc["lines"]), len(mp_matches))
        for bc_idx, mp_detail in mp_matches.items():
            bc_line = bc["lines"][bc_idx]
            if not bc_line.get("audience_name") and mp_detail.get("audience_name"):
                bc_line["audience_name"] = mp_detail["audience_name"]
            # Use media plan tab's explicit per-line dates when available
            if mp_detail.get("flight_start"):
                bc_line["flight_start"] = mp_detail["flight_start"]
            if mp_detail.get("flight_end"):
                bc_line["flight_end"] = mp_detail["flight_end"]

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

    # ── Bundle-rollup detection (pre-loop) ──────────────────────────
    # When a single Blocking Chart row represents a budget pool that the
    # media plan splits into 2+ sub-bundles (e.g. Squamish Flight 2 Meta:
    # one $7,729.90 bc row vs three mp sub-bundles of $2,238 / $3,104 /
    # $2,388), the bc row should be *absorbed* by the sub-bundles — we
    # emit the sub-bundles directly instead of a single bc-rooted line.
    # Otherwise we'd either (a) double-count budget when both emit, or
    # (b) pair the bc row to an unrelated mp_line via weak line_code
    # heuristic (the bug this replaces).
    mp_by_bundle_group: dict[int, list[dict]] = {}
    for _mp in mp_lines:
        _bg = _mp.get("bundle_group")
        if _bg is not None:
            mp_by_bundle_group.setdefault(_bg, []).append(_mp)

    absorbed_bc_indices: set[int] = set()
    bundle_groups_absorbed_by: dict[int, list[int]] = {}  # bc_idx -> [bundle_groups]
    already_absorbed_groups: set[int] = set()  # each group absorbs once (Finding 9)

    for _bc_idx, _bc_line in enumerate(bc["lines"]):
        _bc_plat = _bc_line.get("platform_id")
        _bc_budget = _bc_line.get("budget") or 0
        _bc_fs = _bc_line.get("flight_start") or meta.get("start_date")
        _bc_fe = _bc_line.get("flight_end") or meta.get("end_date")
        if not _bc_budget or not _bc_plat:
            continue
        # Find mp_bundles on same platform whose parent budget > 0
        # and whose flight window fits within this bc window (if both
        # sides have dates).
        _candidates: list[tuple[int, float]] = []  # (bundle_group, parent_budget)
        for _bg, _members in mp_by_bundle_group.items():
            _parent = _members[0]
            if _parent.get("platform_id") != _bc_plat:
                continue
            _parent_budget = _parent.get("budget") or 0
            if _parent_budget <= 0:
                continue
            _mp_fs = _parent.get("flight_start")
            _mp_fe = _parent.get("flight_end")
            if _bc_fs and _mp_fs and _bc_fe and _mp_fe:
                if not (_mp_fs >= _bc_fs and _mp_fe <= _bc_fe):
                    continue
            _candidates.append((_bg, _parent_budget))
        # Finding 9: a bundle group can be absorbed by at most ONE bc_line.
        # Without this, two same-platform bc_lines with near-equal budgets can
        # both absorb the same sub-bundle set (when per-line dates are missing so
        # the containment guard above is skipped), dropping both bc rollups while
        # the sub-bundles represent only one and vanishing the other's budget.
        _candidates = [
            (_bg, _b) for _bg, _b in _candidates
            if _bg not in already_absorbed_groups
        ]
        if len(_candidates) < 2:
            continue  # need ≥ 2 sub-bundles to justify absorption
        _sum_bundles = sum(b for _, b in _candidates)
        _diff = abs(_sum_bundles - _bc_budget) / _bc_budget
        if _diff < 0.02:
            absorbed_bc_indices.add(_bc_idx)
            bundle_groups_absorbed_by[_bc_idx] = [bg for bg, _ in _candidates]
            already_absorbed_groups.update(bg for bg, _ in _candidates)
            logger.info(
                "  bc_line %d ($%.2f, %s) absorbed by %d mp sub-bundles "
                "summing to $%.2f (diff %.2f%%)",
                _bc_idx, _bc_budget, _bc_plat, len(_candidates),
                _sum_bundles, _diff * 100,
            )

    absorbed_bundle_groups: set[int] = set()
    for _groups in bundle_groups_absorbed_by.values():
        absorbed_bundle_groups.update(_groups)

    # ── bc_line loop ────────────────────────────────────────────────
    for i, bc_line in enumerate(bc["lines"]):
        line_id = f"{plan_id}-line-{i:03d}"
        if i in absorbed_bc_indices:
            # Skip: mp sub-bundles will emit in the synthesis pass below.
            continue
        mp_detail = mp_matches.get(i)  # pre-computed global match
        records_for_bc = _build_line_records_for_bc_line(
            bc_line=bc_line,
            mp_detail=mp_detail,
            all_mp_lines=mp_lines,
            plan_id=plan_id,
            line_id=line_id,
            project_code=project_code,
            meta=meta,
        )
        line_records.extend(records_for_bc)

        flight_start = bc_line.get("flight_start") or meta.get("start_date")
        flight_end = bc_line.get("flight_end") or meta.get("end_date")

        # Reconcile the blocking-chart grid against the (enriched) flight window
        # before emitting, and synthesise an even grid for lines with no usable
        # grid. See _emit_line_weeks.
        line_weeks = [w for w in bc["weeks"] if w["line_index"] == i]
        week_records.extend(
            _emit_line_weeks(
                line_weeks, line_id, project_code, flight_start, flight_end
            )
        )

    # ── Synthesize bundles for mp-bundles not consumed by any bc_line ─
    # Two reasons a mp_bundle might not have been consumed:
    #   1. It was absorbed by a rollup bc_line (bundle_groups_absorbed_by
    #      above) — we EXPLICITLY want to emit these, since the bc_line
    #      was skipped.
    #   2. The matcher rejected all its mp_lines (e.g. no bc_line had a
    #      close-enough budget to match the bundle parent). Still emit
    #      so the data isn't silently dropped.
    # Either way: if the bundle's parent mp_line wasn't used as a match
    # on any bc_line, emit the bundle standalone.
    matched_mp_identities: set[int] = set()
    for _mp_detail in mp_matches.values():
        if _mp_detail:
            matched_mp_identities.add(id(_mp_detail))

    synth_idx = 0
    for bg, members in mp_by_bundle_group.items():
        if len(members) < 2:
            continue  # singleton — not a real bundle
        # Check if any member was consumed by a match
        if any(id(m) in matched_mp_identities for m in members):
            continue
        parent_mp = members[0]
        synth_idx += 1
        synth_line_id = f"{plan_id}-synth-bundle-{synth_idx:03d}"

        # Build a synthetic bc_line from the parent mp_line so we can
        # reuse _build_line_records_for_bc_line.
        synth_bc = {
            "platform": parent_mp.get("platform"),
            "platform_id": parent_mp.get("platform_id"),
            "budget": parent_mp.get("budget"),
            "objective_format": parent_mp.get("goal"),
            "flight_start": parent_mp.get("flight_start"),
            "flight_end": parent_mp.get("flight_end"),
            "audience_name": None,  # force enrichment from mp_detail
        }
        synth_records = _build_line_records_for_bc_line(
            bc_line=synth_bc,
            mp_detail=parent_mp,
            all_mp_lines=mp_lines,
            plan_id=plan_id,
            line_id=synth_line_id,
            project_code=project_code,
            meta=meta,
        )
        line_records.extend(synth_records)
        logger.info(
            "  Synthesized bundle from unmatched mp_bundle_group %s "
            "(platform=%s, parent line_code=%r, %d members)",
            bg, parent_mp.get("platform_id"),
            parent_mp.get("line_code"), len(members),
        )

        # Weeks for synthesized bundle parent — generate from flight dates.
        fs = parent_mp.get("flight_start") or meta.get("start_date")
        fe = parent_mp.get("flight_end") or meta.get("end_date")
        if fs and fe:
            week_cursor = fs - timedelta(days=fs.weekday())
            while week_cursor <= fe:
                is_active = fs <= week_cursor + timedelta(days=6) and week_cursor <= fe
                week_records.append({
                    "id": f"{synth_line_id}-w-{week_cursor.isoformat()}",
                    "line_id": synth_line_id,
                    "project_code": project_code,
                    "week_start": week_cursor.isoformat(),
                    "is_active": is_active,
                })
                week_cursor += timedelta(days=7)

    # ── Surfaced drops (item 1) + reconciliation (item 3) ───────────
    # Both are computed against the FINAL line_records (what's about to be
    # written) and emitted as structured logs AND returned in the response so
    # they're visible without grepping logs. Neither raises — reconciliation in
    # particular is a SOFT signal (delta can be legitimately non-zero).
    if dropped_lines:
        logger.warning(
            "  Media plan sync for %s (sheet %s): %d line(s) dropped from the "
            "detail→line path: %s",
            project_code, sheet_id, len(dropped_lines), dropped_lines,
        )

    reconciliation = _build_reconciliation(
        line_records, dropped_lines, meta.get("net_budget")
    )
    logger.info(
        "  Reconciliation for %s: net=$%s captured_self_serve=$%s "
        "captured_direct=$%s captured_traditional=$%s captured_total=$%s "
        "dropped_budget=$%s delta=$%s (soft signal; non-zero is allowed)",
        project_code,
        reconciliation["net_budget"], reconciliation["captured_self_serve"],
        reconciliation["captured_direct"], reconciliation["captured_traditional"],
        reconciliation["captured_total"], reconciliation["dropped_budget"],
        reconciliation["delta"],
    )

    # ── Write to BigQuery ───────────────────────────────────────────
    mtl = _mtl_client()
    try:
        # Use versioned-write pattern for atomic sync:
        # 1. Write new data with sync_version timestamp first (always succeeds)
        # 2. Then delete old versions (no window of missing data)
        sync_version = datetime.now(timezone.utc).isoformat()

        # Snapshot FFS wizard state before the sync wipes it. Re-applied below
        # so user-entered form-friction data survives media plan re-syncs.
        ffs_snapshot = _snapshot_ffs_state(mtl, project_code)

        _write_records_with_version(mtl, "media_plans", [media_plan_record], sync_version)
        _write_records_with_version(mtl, "media_plan_lines", line_records, sync_version)
        _write_records_with_version(mtl, "blocking_chart_weeks", week_records, sync_version)

        # Now delete old versions in a single scripting block for atomicity.
        # Multi-plan support (2026-04-25): delete is scoped to (project_code,
        # sheet_id) so syncing one sheet does not wipe another sheet's lines.
        _delete_old_versions(mtl, project_code, sheet_id, sync_version)

        # Apply any saved audience_name overrides so manual edits survive re-sync
        _apply_audience_overrides(mtl, project_code)

        # Apply user-confirmed bundle states so Confirm survives re-sync.
        # ADAC-54 follow-up: parser's bundle suggestions get overwritten with
        # the user's locked-in choice on each sync.
        _apply_bundle_overrides(mtl, project_code)

        # Re-apply FFS wizard state for lines that survived the sync
        _restore_ffs_state(mtl, project_code, ffs_snapshot)
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
        # ADAC-69: N rows whose Budget cell had content we couldn't parse. Kept
        # with NULL budget rather than dropped; surfaced so the admin layer can
        # flag a malformed plan instead of it failing silently.
        "rows_skipped_unparseable_budget": rows_skipped_unparseable_budget,
        # bcdirect item 1: structured records for every line dropped/redirected
        # in the detail→line path. Each is {label, platform_id, budget, reason}
        # with reason ∈ {"no_budget", "unrecognised_platform"}. Empty list when
        # nothing was dropped. Drops are visible here, not just in logs.
        "dropped_lines": dropped_lines,
        "dropped_lines_count": len(dropped_lines),
        # bcdirect item 3: capture-vs-net reconciliation. SOFT signal — delta
        # can be legitimately non-zero; never an error. Shape documented in
        # _build_reconciliation.
        "reconciliation": reconciliation,
    }
    logger.info("  Sync complete: %s", result)
    return result


def _match_all_mp_lines(
    bc_lines: list[dict], mp_lines: list[dict]
) -> dict[int, dict]:
    """Optimally match blocking chart lines to media plan tab lines.

    Builds a score matrix for all (bc_line, mp_line) pairs and greedily
    assigns best matches by descending score.  This avoids the order-dependent
    side effects of one-at-a-time matching — two similar Meta lines with close
    budgets now both get correct matches.

    Returns a dict mapping bc_line_index → matched mp_line dict.
    """
    if not mp_lines or not bc_lines:
        return {}

    # Build scored pairs: (score, bc_idx, mp_idx)
    scored_pairs: list[tuple[float, int, int]] = []

    for bc_idx, bc_line in enumerate(bc_lines):
        bc_plat = bc_line.get("platform_id")
        bc_budget = bc_line.get("budget", 0)

        for mp_idx, mp_line in enumerate(mp_lines):
            if mp_line.get("platform_id") != bc_plat:
                continue

            score = 0.0
            has_budget_match = False
            mp_budget = mp_line.get("budget") or 0

            # Budget proximity — tight match worth most
            if mp_budget and bc_budget:
                budget_diff = abs(mp_budget - bc_budget) / max(bc_budget, 1)
                if budget_diff < 0.01:
                    score += 100
                    has_budget_match = True
                elif budget_diff < 0.1:
                    score += 80 - (budget_diff * 200)
                    has_budget_match = True
                elif budget_diff < 0.5:
                    score += 20
                    has_budget_match = True

            # Bonus for having a line_code (can match even without budget)
            if mp_line.get("line_code"):
                score += 10

            # Bonus for having audience_name (only if budget or line_code matched)
            if mp_line.get("audience_name") and (has_budget_match or mp_line.get("line_code")):
                score += 5

            # Require at least a budget match or a line_code to consider pairing.
            # EXCEPT: if the mp_line is part of a bundle (bundle_group is not
            # None), require a true budget match. Pairing a bundled mp_line by
            # line_code alone lets a bc_line absorb a mp_bundle that doesn't
            # actually share its budget — e.g. Squamish Flight 2 Meta's
            # $7,729.90 bc row vs Flight 1 Meta's #02 ($NULL child of a
            # different bundle pool). That case is better handled later by
            # rollup absorption + mp-bundle synthesis; the matcher should
            # stay out of the way.
            is_bundled_mp = mp_line.get("bundle_group") is not None
            if score > 0 and (
                has_budget_match
                or (mp_line.get("line_code") and not is_bundled_mp)
            ):
                scored_pairs.append((score, bc_idx, mp_idx))

    # Greedy assignment by descending score
    scored_pairs.sort(key=lambda t: t[0], reverse=True)
    used_bc: set[int] = set()
    used_mp: set[int] = set()
    result: dict[int, dict] = {}

    for score, bc_idx, mp_idx in scored_pairs:
        if bc_idx in used_bc or mp_idx in used_mp:
            continue
        result[bc_idx] = mp_lines[mp_idx]
        used_bc.add(bc_idx)
        used_mp.add(mp_idx)

    return result


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


def _apply_audience_overrides(mtl: bigquery.Client, project_code: str) -> None:
    """Re-apply saved audience_name overrides after a fresh sync.

    Matches overrides on (project_code, platform_id, budget ±1%, audience_name, flight_dates)
    so they survive line_id changes across re-syncs and don't stale across media plan changes.
    Also cleans up overrides whose (project_code, platform_id) no longer exist in media_plan_lines.
    """
    prefix = f"`{settings.gcp_project_id}.{settings.bigquery_dataset}"

    # Apply overrides that still have matching lines (widened key)
    sql_apply = f"""
        UPDATE {prefix}.media_plan_lines` l
        SET l.audience_name = o.audience_name
        FROM {prefix}.media_plan_line_overrides` o
        WHERE l.project_code = o.project_code
          AND l.platform_id = o.platform_id
          AND ABS(l.budget - o.budget) / GREATEST(o.budget, 1) < 0.01
          AND COALESCE(l.audience_name, '') != COALESCE(o.audience_name, '')
          AND l.project_code = @pc
    """

    # Clean up overrides whose lines no longer exist (prevent stale rows)
    sql_cleanup = f"""
        DELETE FROM {prefix}.media_plan_line_overrides` o
        WHERE o.project_code = @pc
          AND NOT EXISTS (
              SELECT 1 FROM {prefix}.media_plan_lines` l
              WHERE l.project_code = o.project_code
                AND l.platform_id = o.platform_id
                AND ABS(l.budget - o.budget) / GREATEST(o.budget, 1) < 0.01
          )
    """

    param_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("pc", "STRING", project_code),
    ])

    try:
        result = mtl.query(sql_apply, job_config=param_config).result()
        affected = result.num_dml_affected_rows or 0
        if affected:
            logger.info("    Applied %d audience_name overrides for %s", affected, project_code)
    except google.cloud.exceptions.NotFound:
        logger.debug("  Overrides table not found yet (first run)")
    except Exception as e:
        logger.warning("  Could not apply audience overrides: %s", e)

    # Re-apply is_direct overrides (the manual direct/tracked classification).
    # Auto-classification already set l.is_direct this sync; this layers the
    # user's choice on top, preserved across re-syncs.
    sql_apply_isdirect = f"""
        UPDATE {prefix}.media_plan_lines` l
        SET l.is_direct_override = o.is_direct_override
        FROM {prefix}.media_plan_line_overrides` o
        WHERE l.project_code = o.project_code
          AND l.platform_id = o.platform_id
          AND ABS(l.budget - o.budget) / GREATEST(o.budget, 1) < 0.01
          AND o.is_direct_override IS NOT NULL
          AND l.project_code = @pc
    """
    try:
        result = mtl.query(sql_apply_isdirect, job_config=param_config).result()
        affected = result.num_dml_affected_rows or 0
        if affected:
            logger.info("    Applied %d is_direct overrides for %s", affected, project_code)
    except google.cloud.exceptions.NotFound:
        logger.debug("  Overrides table not found yet (first run)")
    except Exception as e:
        logger.warning("  Could not apply is_direct overrides: %s", e)

    try:
        result = mtl.query(sql_cleanup, job_config=param_config).result()
        cleaned = result.num_dml_affected_rows or 0
        if cleaned:
            logger.info("    Cleaned up %d stale audience overrides for %s", cleaned, project_code)
    except google.cloud.exceptions.NotFound:
        pass  # Table doesn't exist yet
    except Exception as e:
        logger.warning("  Could not cleanup stale overrides: %s", e)


def _apply_bundle_overrides(mtl: bigquery.Client, project_code: str) -> None:
    """Re-apply saved bundle confirmations and rejections after a fresh sync.

    The parser detects bundles from merged Budget cells and emits them as
    ``suggested_parent`` / ``suggested_child``. Once a user clicks Confirm or
    Reject in the UI, an override row is written to
    ``media_plan_bundle_overrides`` keyed on (project_code, bundle_id). On
    every subsequent sync we overwrite the parser's suggestions with the
    user's decision so the bundle state stays locked in.

    Override types stored in ``media_plan_bundle_overrides.bundle_role``:

    - ``'confirmed_parent'`` — user confirmed the suggestion is real. Parent
      and child roles get promoted from ``'suggested_*'`` to
      ``'confirmed_*'``.
    - ``'rejected'``         — user rejected the suggestion. Every member's
      role becomes ``'rejected'`` regardless of budget. The pacing service
      treats rejected lines as not-parents and not-children: the former
      parent shows up as a standalone with the pool budget, while children
      whose budgets were zeroed by the parser fall through pacing's
      ``budget<=0`` skip and disappear from the dashboard. Documented in
      the Reject button tooltip on the frontend.

    bundle_id stability is good enough for this: it's
    ``{project_code}-{platform_id}-{first_line_code_sans_hash}`` and only
    changes if the first member's line_code changes. If the source plan
    drifts that far, the override becomes orphaned and the cleanup step
    below removes it on the next sync — at which point the user re-confirms
    or re-rejects the new shape.

    Mirrors ``_apply_audience_overrides`` in structure: apply, then clean
    up rows whose bundle_id no longer exists in media_plan_lines.
    """
    prefix = f"`{settings.gcp_project_id}.{settings.bigquery_dataset}"

    # Apply: any line whose bundle_id matches an override gets its
    # bundle_role rewritten based on the override TYPE. A 'rejected' override
    # collapses every member to 'rejected' (no parent/child distinction).
    # A 'confirmed_parent' override promotes parents and children separately
    # using the budget-IS-NULL split (parents hold the pool total, children
    # have NULL budgets by design — see schema.sql comment on bundle_id).
    sql_apply = f"""
        UPDATE {prefix}.media_plan_lines` l
        SET l.bundle_role = CASE
              WHEN o.bundle_role = 'rejected' THEN 'rejected'
              WHEN l.budget IS NULL THEN 'confirmed_child'
              ELSE 'confirmed_parent'
            END
        FROM {prefix}.media_plan_bundle_overrides` o
        WHERE l.project_code = o.project_code
          AND l.bundle_id   = o.bundle_id
          AND l.project_code = @pc
          AND o.bundle_role IN ('confirmed_parent', 'rejected')
    """

    # Clean up overrides whose bundle no longer exists. Same pattern as
    # _apply_audience_overrides — keeps the table from accumulating dead
    # rows when plans drift.
    sql_cleanup = f"""
        DELETE FROM {prefix}.media_plan_bundle_overrides` o
        WHERE o.project_code = @pc
          AND NOT EXISTS (
              SELECT 1 FROM {prefix}.media_plan_lines` l
              WHERE l.project_code = o.project_code
                AND l.bundle_id    = o.bundle_id
          )
    """

    param_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("pc", "STRING", project_code),
    ])

    try:
        result = mtl.query(sql_apply, job_config=param_config).result()
        affected = result.num_dml_affected_rows or 0
        if affected:
            logger.info("    Applied %d bundle overrides for %s", affected, project_code)
    except google.cloud.exceptions.NotFound:
        logger.debug("  Bundle overrides table not found yet (first run)")
    except Exception as e:
        logger.warning("  Could not apply bundle overrides: %s", e)

    try:
        result = mtl.query(sql_cleanup, job_config=param_config).result()
        cleaned = result.num_dml_affected_rows or 0
        if cleaned:
            logger.info("    Cleaned up %d stale bundle overrides for %s", cleaned, project_code)
    except google.cloud.exceptions.NotFound:
        pass
    except Exception as e:
        logger.warning("  Could not cleanup stale bundle overrides: %s", e)


def _snapshot_ffs_state(mtl: bigquery.Client, project_code: str) -> list[dict]:
    """Snapshot FFS wizard state for a project before a sync wipes it.

    Returns a list of {line_id, ffs_entry_id, ffs_override, ffs_score, ffs_inputs}
    for every line in the project that has any FFS data set. Used by
    ``_restore_ffs_state`` below to re-apply after the versioned write+delete.
    """
    prefix = f"`{settings.gcp_project_id}.{settings.bigquery_dataset}"
    sql = f"""
        SELECT
          line_id,
          ffs_entry_id,
          ffs_override,
          ffs_score,
          TO_JSON_STRING(ffs_inputs) AS ffs_inputs_json
        FROM {prefix}.media_plan_lines`
        WHERE project_code = @pc
          AND (ffs_entry_id IS NOT NULL
               OR ffs_override = TRUE
               OR ffs_score IS NOT NULL)
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("pc", "STRING", project_code),
    ])
    try:
        rows = list(mtl.query(sql, job_config=job_config).result())
    except google.cloud.exceptions.NotFound:
        return []
    except Exception as e:
        logger.warning("  Could not snapshot FFS state for %s: %s", project_code, e)
        return []

    snapshot: list[dict] = []
    for r in rows:
        snapshot.append({
            "line_id": r["line_id"],
            "ffs_entry_id": r.get("ffs_entry_id"),
            "ffs_override": r.get("ffs_override"),
            "ffs_score": r.get("ffs_score"),
            "ffs_inputs_json": r.get("ffs_inputs_json"),
        })
    return snapshot


def _restore_ffs_state(
    mtl: bigquery.Client, project_code: str, snapshot: list[dict]
) -> None:
    """Re-apply snapshot ffs_* columns to surviving lines (matched on line_id).

    Lines whose line_id didn't survive the sync are skipped and logged — in
    practice this happens when the sheet row was materially edited (budget,
    platform, etc.) since that changes the derived line_id. For stable rows
    the FFS state is preserved exactly.
    """
    if not snapshot:
        return

    prefix = f"`{settings.gcp_project_id}.{settings.bigquery_dataset}"
    applied = 0
    skipped = 0

    for entry in snapshot:
        line_id = entry["line_id"]
        params = [
            bigquery.ScalarQueryParameter("line_id", "STRING", line_id),
            bigquery.ScalarQueryParameter("pc", "STRING", project_code),
            bigquery.ScalarQueryParameter("ffs_entry_id", "STRING", entry.get("ffs_entry_id")),
            bigquery.ScalarQueryParameter("ffs_override", "BOOL", bool(entry.get("ffs_override") or False)),
            bigquery.ScalarQueryParameter("ffs_score", "FLOAT64", entry.get("ffs_score")),
        ]
        inputs_json = entry.get("ffs_inputs_json")
        if inputs_json:
            params.append(bigquery.ScalarQueryParameter("ffs_inputs_json", "STRING", inputs_json))
            inputs_expr = "PARSE_JSON(@ffs_inputs_json)"
        else:
            inputs_expr = "NULL"

        sql = f"""
            UPDATE {prefix}.media_plan_lines`
            SET
              ffs_entry_id = @ffs_entry_id,
              ffs_override = @ffs_override,
              ffs_score    = @ffs_score,
              ffs_inputs   = {inputs_expr}
            WHERE line_id = @line_id AND project_code = @pc
        """
        try:
            result = mtl.query(sql, job_config=bigquery.QueryJobConfig(
                query_parameters=params
            )).result()
            if (result.num_dml_affected_rows or 0) > 0:
                applied += 1
            else:
                skipped += 1
        except Exception as e:
            logger.warning("  FFS restore failed for line %s: %s", line_id, e)
            skipped += 1

    if applied:
        logger.info("    Restored FFS state on %d lines for %s", applied, project_code)
    if skipped:
        logger.info(
            "    Skipped %d FFS lines for %s (line_id changed across sync)",
            skipped, project_code,
        )


def _write_records_with_version(
    mtl: bigquery.Client, table_name: str, records: list[dict], sync_version: str
) -> None:
    """Write records with a sync_version timestamp for versioned-write pattern."""
    if not records:
        return
    # Add sync_version to each record
    for record in records:
        record["sync_version"] = sync_version
    _write_records(mtl, table_name, records)


def _delete_old_versions(
    mtl: bigquery.Client,
    project_code: str,
    sheet_id: str,
    current_sync_version: str,
) -> None:
    """Delete old sync versions in a single BigQuery scripting block for atomicity.

    Multi-plan scoping (2026-04-25): the delete is now scoped to
    (project_code, sheet_id) — never just project_code — so syncing one sheet
    does not wipe another sheet's lines for the same project. Identification
    runs through the media_plans table, which is the only table that carries
    sheet_id natively; lines and weeks inherit their scope via sync_version.

    Also performs an orphan cleanup: media_plan_lines and blocking_chart_weeks
    rows whose sync_version doesn't appear in ANY media_plans row for this
    project are unreachable through the read paths anyway, but reclaiming the
    space preserves the old code's housekeeping behaviour.

    Retries up to 3 times on failure. If all retries fail, raises the exception
    so the caller knows cleanup didn't happen — silently swallowing this error
    causes duplicate media_plan_lines, which halves spend in pacing calculations.
    """
    import time

    prefix = f"`{settings.gcp_project_id}.{settings.bigquery_dataset}"

    # Use a scripting block to wrap all deletes atomically
    script = f"""
    BEGIN
        -- Mark old media plans for THIS sheet as non-current. Other sheets'
        -- current rows are untouched.
        UPDATE {prefix}.media_plans`
        SET is_current = FALSE
        WHERE project_code = @pc
          AND sheet_id = @sheet_id
          AND sync_version != @sv
          AND is_current = TRUE;

        -- Delete old media plan lines for THIS sheet. The sync_version IN
        -- subquery scopes the delete to plans whose sheet_id matches; rows
        -- belonging to other sheets keep their sync_version and survive.
        DELETE FROM {prefix}.media_plan_lines`
        WHERE project_code = @pc
          AND sync_version != @sv
          AND sync_version IN (
            SELECT sync_version
            FROM {prefix}.media_plans`
            WHERE project_code = @pc AND sheet_id = @sheet_id
          );

        -- Delete old blocking chart weeks for THIS sheet (same scoping as above).
        DELETE FROM {prefix}.blocking_chart_weeks`
        WHERE project_code = @pc
          AND sync_version != @sv
          AND sync_version IN (
            SELECT sync_version
            FROM {prefix}.media_plans`
            WHERE project_code = @pc AND sheet_id = @sheet_id
          );

        -- Orphan cleanup: rows whose sync_version doesn't tie back to ANY
        -- media_plans row for this project. Unreachable through the dedup
        -- guard, but holds onto storage. Cross-sheet by design — these
        -- rows belong to no sheet at all.
        DELETE FROM {prefix}.media_plan_lines`
        WHERE project_code = @pc
          AND sync_version IS NOT NULL
          AND sync_version NOT IN (
            SELECT sync_version FROM {prefix}.media_plans`
            WHERE project_code = @pc AND sync_version IS NOT NULL
          );

        DELETE FROM {prefix}.blocking_chart_weeks`
        WHERE project_code = @pc
          AND sync_version IS NOT NULL
          AND sync_version NOT IN (
            SELECT sync_version FROM {prefix}.media_plans`
            WHERE project_code = @pc AND sync_version IS NOT NULL
          );
    END;
    """

    max_retries = 3
    for attempt in range(1, max_retries + 1):
        try:
            mtl.query(
                script,
                job_config=bigquery.QueryJobConfig(query_parameters=[
                    bigquery.ScalarQueryParameter("pc", "STRING", project_code),
                    bigquery.ScalarQueryParameter("sheet_id", "STRING", sheet_id),
                    bigquery.ScalarQueryParameter("sv", "STRING", current_sync_version),
                ]),
            ).result()
            logger.info(
                "  Deleted old sync versions for %s sheet %s",
                project_code, sheet_id,
            )
            return
        except Exception as e:
            if attempt < max_retries:
                logger.warning(
                    "  Failed to delete old versions for %s sheet %s (attempt %d/%d): %s",
                    project_code, sheet_id, attempt, max_retries, e,
                )
                time.sleep(1 * attempt)  # brief backoff
            else:
                logger.error(
                    "  CRITICAL: Failed to delete old sync versions for %s sheet %s after %d attempts. "
                    "Duplicate media_plan_lines will exist until next successful sync. Error: %s",
                    project_code, sheet_id, max_retries, e,
                )
                raise


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
