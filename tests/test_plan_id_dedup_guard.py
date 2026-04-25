"""Regression guard: every BQ query that reads from media_plan_lines must
filter to the current plan_id.

Background: pre-2026-04-12 syncs wrote brand-new line_ids without purging
old plan_ids' rows. The standard ROW_NUMBER OVER (PARTITION BY line_id)
dedup CTE never collides across syncs in that case, so stale rows from
old plan_ids survived forever (project 26009 carried 17 inflated lines
instead of 3, $415k of stale budget).

The fix: every WHERE clause on media_plan_lines now also includes
``AND plan_id IN (SELECT plan_id FROM media_plans WHERE project_code = ?
AND is_current = TRUE)``. This test asserts that pattern is present in
every known callsite by inspecting the file source. It's intentionally
crude — we'd rather catch a refactor that drops the guard than ship
silently-double-counted budgets again.

If you intentionally rewrite one of these queries (e.g. switching to a
shared CTE helper), update the EXPECTED_PATTERN regex below to match the
new pattern.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parent.parent

# Files that read from media_plan_lines and must include the plan_id guard.
# Format: (relative_path, expected_count_of_guards)
GUARDED_FILES = [
    ("backend/services/diagnostics/engine.py",  1),
    ("backend/routers/pacing.py",               2),  # pacing endpoint + bundle members
    ("backend/routers/performance.py",          1),
    ("backend/routers/benchmarks.py",           1),
    ("backend/services/ffs_entries.py",         2),  # list + get
]

# The guard pattern. Captures both common parameter naming conventions
# (@project_code and @pc).
GUARD_PATTERN = re.compile(
    r"AND\s+plan_id\s+IN\s*\(\s*"
    r"SELECT\s+plan_id\s+FROM\s+\{bq\.table\(['\"]media_plans['\"]\)\}\s+"
    r"WHERE\s+project_code\s*=\s*@(?:project_code|pc)\s+AND\s+is_current\s*=\s*TRUE\s*"
    r"\)",
    re.IGNORECASE | re.MULTILINE,
)


@pytest.mark.parametrize("rel_path,expected_count", GUARDED_FILES)
def test_plan_id_guard_present(rel_path: str, expected_count: int):
    """Every known media_plan_lines reader contains the plan_id guard the
    expected number of times. Refactors that drop or restructure the guard
    will fail this test."""
    src = (REPO_ROOT / rel_path).read_text()
    matches = GUARD_PATTERN.findall(src)
    assert len(matches) == expected_count, (
        f"{rel_path}: expected {expected_count} plan_id guard(s), found "
        f"{len(matches)}. If you intentionally restructured this query, "
        f"update GUARD_PATTERN or GUARDED_FILES in this test."
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

    # Also surface if a guarded file dropped the pattern entirely (could
    # mean it was refactored away — confirm the guard test still passes
    # for it, then remove from GUARDED_FILES).
    missing_files = expected_files - found_files
    assert not missing_files, (
        f"Files listed in GUARDED_FILES no longer use the dedup pattern: "
        f"{sorted(missing_files)}. Either restore the dedup or remove "
        f"from GUARDED_FILES (and from this test)."
    )
