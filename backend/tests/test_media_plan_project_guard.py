"""Registration guardrail against attaching a foreign-project sheet (F2 / ADA 26023).

Incident: project 26034's "26034 - UNITE HERE Local 40" media-plan sheet
(id 10EOl46IuuHXcf8ZNs87NE6zNQXipyOjAd_CvV-yLf8k) was registered as an ACTIVE
plan under project 26023 via the admin add-plan endpoint, displacing 26023's real
plan. No registration path validated the sheet's embedded YYNNN code against the
target project.

These tests lock in the guard added to backend/routers/admin.py:
  * _assert_sheet_matches_project — pure matcher (positive-mismatch-only,
    title-primary, conservative tab handling).
  * admin_add_project_plan — the confirmed incident vector returns HTTP 400 and
    writes NOTHING on a mismatch.
  * _ensure_plan_registered — the create / update / manual-sync registration
    paths raise (not a silently-passed reversed arg order) and write nothing.

Deterministic: gspread is mocked (no live Sheets I/O); admin.bq.run_query is a
Mock so we can assert zero BigQuery writes.
"""

import asyncio
from unittest.mock import Mock

import pytest
from fastapi import HTTPException

from backend.routers import admin as admin_router
from backend.routers.admin import _assert_sheet_matches_project

# The two real sheet ids from the incident (used as opaque identifiers here).
FOREIGN_SHEET_ID = "10EOl46IuuHXcf8ZNs87NE6zNQXipyOjAd_CvV-yLf8k"
FOREIGN_TITLE = "26034 - UNITE HERE Local 40 - Affordability Campaign"


# ── fake gspread plumbing ────────────────────────────────────────────


class _FakeWorksheet:
    def __init__(self, title):
        self.title = title


class _FakeSpreadsheet:
    def __init__(self, title, tab_titles):
        self.title = title
        self._tabs = [_FakeWorksheet(t) for t in tab_titles]

    def worksheets(self):
        return list(self._tabs)


class _FakeGspreadClient:
    def __init__(self, title, tab_titles):
        self._ss = _FakeSpreadsheet(title, tab_titles)

    def open_by_key(self, sheet_id):
        return self._ss


def _patch_sheet(monkeypatch, title, tab_titles=("Media Plan",)):
    """Point admin._get_gspread_client at a fake client serving (title, tabs)."""
    client = _FakeGspreadClient(title, list(tab_titles))
    monkeypatch.setattr(admin_router, "_get_gspread_client", lambda: client)
    return client


# ── pure helper: _assert_sheet_matches_project ───────────────────────


def test_helper_raises_on_foreign_26034_title():
    """A spreadsheet titled for 26034 must be refused under project 26023."""
    with pytest.raises(ValueError) as exc:
        _assert_sheet_matches_project(FOREIGN_TITLE, ["Media Plan"], "26023")
    assert "26034" in str(exc.value)


def test_helper_allows_no_code_sierra_club_title():
    """The legit 26023 Sierra Club sheet embeds NO code — never reject on absence."""
    _assert_sheet_matches_project(
        "Sierra Club BC | FIFA Old Growth Campaign | Media Plan | EXTERNAL",
        ["Media Plan"],
        "26023",
    )  # does not raise


def test_helper_allows_matching_26023_title():
    """A title carrying the target code passes."""
    _assert_sheet_matches_project("26023 - Sierra Club - Media Plan", ["Media Plan"], "26023")


def test_helper_does_not_overblock_codeless_tab_in_multitab_workbook():
    """A shared multi-tab workbook where the TARGET code appears somewhere must
    NOT be blocked just because an unrelated tab carries another code.

    Also confirms the conservative tab handling: a foreign code on a tab only
    contributes to the reject decision when the target code is absent from BOTH
    the title AND every tab.
    """
    # Target code present on a tab alongside an unrelated code-bearing tab → allow.
    _assert_sheet_matches_project(
        "Media Plan",
        ["26023 - Sierra Club", "26034 UNITE HERE reference"],
        "26023",
    )  # does not raise

    # Target code present in the title; a code-less working tab must not block it.
    _assert_sheet_matches_project(
        "26023 - Sierra Club - Media Plan",
        ["Working Draft", "Notes"],
        "26023",
    )  # does not raise

    # Conservative-tab proof: title code-less, target absent everywhere, a tab
    # carries a foreign code → THAT is a positive mismatch and must raise.
    with pytest.raises(ValueError) as exc:
        _assert_sheet_matches_project("Media Plan", ["26034 UNITE HERE"], "26023")
    assert "26034" in str(exc.value)


# ── router: admin_add_project_plan (confirmed incident vector) ───────


def test_add_plan_endpoint_returns_400_and_writes_nothing_on_mismatch(monkeypatch):
    """POSTing the foreign 10EO sheet to project 26023's add-plan endpoint returns
    HTTP 400 naming 26034 and issues ZERO BigQuery writes (guard runs before the
    display_order SELECT and the project_media_plans MERGE)."""
    _patch_sheet(monkeypatch, FOREIGN_TITLE, tab_titles=["Media Plan"])
    run_query = Mock()
    monkeypatch.setattr(admin_router.bq, "run_query", run_query)
    # Cache invalidation is never reached on the reject path; stub it anyway.
    monkeypatch.setattr(admin_router.projects_router, "invalidate_project", lambda pc: None)

    body = admin_router.ProjectPlanCreate(sheet_url_or_id=FOREIGN_SHEET_ID, auto_sync=False)

    with pytest.raises(HTTPException) as exc:
        asyncio.run(admin_router.admin_add_project_plan("26023", body))

    assert exc.value.status_code == 400
    assert "26034" in str(exc.value.detail)
    run_query.assert_not_called()


# ── _ensure_plan_registered (create / update / manual-sync paths) ────


def test_ensure_plan_registered_guard_raises_and_writes_nothing_on_mismatch(monkeypatch):
    """The guard must fire INSIDE _ensure_plan_registered with the correct arg
    order (sheet_id, project_code) — a reversed order would silently pass the
    foreign sheet. So a mismatched sheet raises and writes zero rows."""
    _patch_sheet(monkeypatch, FOREIGN_TITLE, tab_titles=["Media Plan"])
    run_query = Mock()
    monkeypatch.setattr(admin_router.bq, "run_query", run_query)

    with pytest.raises(ValueError) as exc:
        admin_router._ensure_plan_registered("26023", FOREIGN_SHEET_ID)

    assert "26034" in str(exc.value)
    run_query.assert_not_called()


def test_ensure_plan_registered_allows_matching_sheet(monkeypatch):
    """A sheet carrying the target code (or none) sails through to the MERGE —
    the guard is strictly additive and must not regress legitimate registration."""
    _patch_sheet(monkeypatch, "26023 - Sierra Club - Media Plan", tab_titles=["Media Plan"])
    run_query = Mock()
    monkeypatch.setattr(admin_router.bq, "run_query", run_query)

    admin_router._ensure_plan_registered("26023", "1cWa18ShUbTRlc380GzDMdGWBHhOY-2p9dzc3WvBQubU")

    run_query.assert_called_once()


def test_peek_swallows_sheet_open_errors(monkeypatch):
    """A transient Sheets open/API failure is non-fatal — the peek logs and
    returns so a previously-working add is never regressed by a network hiccup."""

    class _BoomClient:
        def open_by_key(self, sheet_id):
            raise RuntimeError("APIError: transient 503")

    monkeypatch.setattr(admin_router, "_get_gspread_client", lambda: _BoomClient())

    # No raise — swallowed.
    admin_router._peek_and_assert_sheet_project(FOREIGN_SHEET_ID, "26023")
