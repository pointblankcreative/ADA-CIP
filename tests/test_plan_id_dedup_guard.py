"""Regression guard: every BQ query that reads from media_plan_lines must
filter through both ``media_plans`` (current) AND ``project_media_plans``
(active) so that stale plan_ids and retired phases don't leak into engine
output.

Background — pre-2026-04-12 syncs wrote brand-new line_ids without purging
old plan_ids' rows. The standard ROW_NUMBER OVER (PARTITION BY line_id)
dedup CTE never collides across syncs in that case, so stale rows from
old plan_ids survived forever (project 26009 carried 17 inflated lines
instead of 3, $415k of stale budget).

The original fix added ``AND plan_id IN (SELECT plan_id FROM media_plans
WHERE ... AND is_current = TRUE)`` to every callsite.

Multi-plan support (2026-04-25) extended that guard to also JOIN through
``project_media_plans`` and filter on ``pmp.is_active = TRUE`` so retired
phases don't bleed back in.

This test asserts the multi-plan-aware guard is present in every known
callsite by inspecting the file source. It's intentionally crude — we'd
rather catch a refactor that drops the guard than ship silently double-
counted budgets again.

If you intentionally rewrite one of these queries (e.g. switching to a
shared CTE helper), update the GUARD_PATTERNS regexes below to match the
new structure.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parent.parent

# Files that read from media_plan_lines and must include the multi-plan guard.
# Format: (relative_path, expected_count_of_guards)
GUARDED_FILES = [
    ("backend/services/diagnostics/engine.py",  1),
    ("backend/routers/pacing.py",               2),  # pacing endpoint + bundle members
    ("backend/routers/performance.py",          1),
    ("backend/routers/benchmarks.py",           1),
    ("backend/routers/creative.py",             2),  # objectives + audience roles
    ("backend/services/ffs_entries.py",         2),  # list + get
]

# The guard must include BOTH media_plans (with is_current=TRUE) AND
# project_media_plans (with is_active=TRUE). Two SQL shapes are valid:
#
#   (A) IN-subquery shape — used by engine.py, ffs_entries.py, the bundle
#       members query in pacing.py, performance.py, benchmarks.py:
#
#         AND plan_id IN (
#           SELECT mp.plan_id
#           FROM   {bq.table('media_plans')} mp
#           JOIN   {bq.table('project_media_plans')} pmp
#             ON   ...
#           WHERE  mp.project_code = @project_code
#             AND  mp.is_current = TRUE
#             AND  pmp.is_active = TRUE
#         )
#
#   (B) Direct JOIN shape — used by the main pacing-endpoint query at the
#       top of pacing.py, where media_plan_lines is joined directly to
#       media_plans and project_media_plans without a sub-query:
#
#         FROM media_plan_lines l
#         JOIN media_plans mp ON l.plan_id = mp.plan_id AND mp.is_current = TRUE
#         JOIN project_media_plans pmp ON mp.project_code = pmp.project_code
#                                     AND mp.sheet_id = pmp.sheet_id
#                                     {pmp_active_filter}
#
# The active-filter on shape (B) is sometimes a templated string (`{pmp_active_filter}`)
# rather than a literal so that retrospective replays can opt out. We accept
# either the literal `pmp.is_active` reference or the template placeholder.

GUARD_IN_SUBQUERY = re.compile(
    r"AND\s+plan_id\s+IN\s*\("                                       # AND plan_id IN (
    r"[^)]*?SELECT\s+\w*\.?plan_id"                                  # SELECT [mp.]plan_id
    r"[^)]*?\{bq\.table\(['\"]media_plans['\"]\)\}"                  # FROM {bq.table('media_plans')}
    r"[^)]*?\{bq\.table\(['\"]project_media_plans['\"]\)\}"          # JOIN {bq.table('project_media_plans')}
    r"[^)]*?is_current\s*=\s*TRUE"                                   # is_current = TRUE
    r"[^)]*?(?:pmp\.is_active|is_active)\s*=\s*TRUE"                 # is_active = TRUE
    r"[^)]*?\)",                                                     # closing paren
    re.IGNORECASE | re.DOTALL,
)

GUARD_DIRECT_JOIN = re.compile(
    r"FROM\s+\{bq\.table\(['\"]media_plan_lines['\"]\)\}\s+\w+"      # FROM media_plan_lines l
    r".{0,400}?"
    r"JOIN\s+\{bq\.table\(['\"]media_plans['\"]\)\}"                 # JOIN media_plans
    r".{0,400}?is_current\s*=\s*TRUE"                                # is_current = TRUE
    r".{0,400}?"
    r"JOIN\s+\{bq\.table\(['\"]project_media_plans['\"]\)\}"         # JOIN project_media_plans
    r".{0,400}?(?:pmp\.is_active|\{pmp_active_filter\})",            # active filter or template
    re.IGNORECASE | re.DOTALL,
)


def count_guards(src: str) -> int:
    """Count the total guards present, summing both shapes."""
    return len(GUARD_IN_SUBQUERY.findall(src)) + len(GUARD_DIRECT_JOIN.findall(src))


@pytest.mark.parametrize("rel_path,expected_count", GUARDED_FILES)
def test_plan_id_guard_present(rel_path: str, expected_count: int):
    """Every known media_plan_lines reader contains the multi-plan-aware
    guard the expected number of times. Refactors that drop or restructure
    the guard will fail this test."""
    src = (REPO_ROOT / rel_path).read_text()
    matches = count_guards(src)
    assert matches == expected_count, (
        f"{rel_path}: expected {expected_count} multi-plan guard(s), found "
        f"{matches}. Each guard must reference BOTH media_plans (is_current=TRUE) "
        f"AND project_media_plans (is_active=TRUE / pmp_active_filter). "
        f"If you intentionally restructured this query, update GUARD_IN_SUBQUERY / "
        f"GUARD_DIRECT_JOIN or GUARDED_FILES in this test."
    )


def test_no_unguarded_partition_by_line_id():
    """Every PARTITION BY line_id ORDER BY sync_version DESC dedup CTE in
    the backend code lives in one of the GUARDED_FILES. If a new file
    starts using the dedup pattern, it needs a guard added (and listed in
    GUARDED_FILES above) — this test surfaces the regression early."""
    backend_root = REPO_ROOT / "backend"
    pattern = re.compile(r"PARTITION BY line_id\s+ORDER BY sync_version DESC", re.IGNORECASE)

    found_files: set[str] = set()
    for py_file in backend_root.rglob("*.py"):
        text = py_file.read_text()
        if pattern.search(text):
            rel = str(py_file.relative_to(REPO_ROOT))
            found_files.add(rel)

    expected_files = {rel for rel, _ in GUARDED_FILES}
    new_files = found_files - expected_files
    assert not new_files, (
        f"New files use the PARTITION BY line_id dedup pattern but aren't "
        f"in GUARDED_FILES: {sorted(new_files)}. Add them to the guard list."
    )

    missing_files = expected_files - found_files
    assert not missing_files, (
        f"Files listed in GUARDED_FILES no longer use the dedup pattern: "
        f"{sorted(missing_files)}. Either restore the dedup or remove "
        f"from GUARDED_FILES (and from this test)."
    )
