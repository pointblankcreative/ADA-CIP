"""P-FRESH-PACE: projects.py surfaces self_serve_budget (the non-direct,
pacing-inclusion line budget) alongside direct_budget, from a SINGLE deduped
read of media_plan_lines."""

import asyncio
from unittest.mock import MagicMock

import pytest

from backend.routers import projects


@pytest.fixture(autouse=True)
def _no_cache(monkeypatch):
    projects._LIST_CACHE.clear()
    projects._DETAIL_CACHE.clear()
    monkeypatch.setattr(projects.settings, "projects_cache_ttl_seconds", 0)
    yield
    projects._LIST_CACHE.clear()
    projects._DETAIL_CACHE.clear()


def test_list_projects_surfaces_self_serve_budget(monkeypatch):
    row = {
        "project_code": "26023",
        "project_name": "Sierra Club BC",
        "direct_budget": 5000.0,
        "self_serve_budget": 42000.0,
    }
    monkeypatch.setattr(projects.bq, "run_query", MagicMock(return_value=[row]))
    res = asyncio.run(projects.list_projects())
    assert res[0].self_serve_budget == 42000.0
    assert res[0].direct_budget == 5000.0


def test_get_project_surfaces_self_serve_budget(monkeypatch):
    row = {
        "project_code": "26023",
        "project_name": "Sierra Club BC",
        "direct_budget": 5000.0,
        "self_serve_budget": 42000.0,
    }
    monkeypatch.setattr(projects.bq, "run_query", MagicMock(return_value=[row]))
    res = asyncio.run(projects.get_project("26023"))
    assert res.self_serve_budget == 42000.0


def test_self_serve_query_is_single_media_plan_lines_read(monkeypatch):
    """direct_budget + self_serve_budget come from ONE media_plan_lines
    subquery — the file does not add a second read (no dedup-guard entry
    needed). Assert only one media_plan_lines reference in the list SQL."""
    captured = {}

    def qr(sql, params=None):
        captured["sql"] = sql
        return [{"project_code": "26023", "project_name": "X"}]

    monkeypatch.setattr(projects.bq, "run_query", qr)
    asyncio.run(projects.list_projects())
    # Exactly one media_plan_lines dedup subquery (both budgets share it).
    assert captured["sql"].count("PARTITION BY mpl.line_id") == 1
    assert "self_serve_budget" in captured["sql"]
