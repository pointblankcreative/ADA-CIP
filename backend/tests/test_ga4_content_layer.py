"""Tests for the utm_content data layer.

Covers:
  - GA4 transform SQL builder: wires in the content column when funnel_data has it,
    degrades to a constant '(not set)' (and no extra GROUP BY term) when it does not.
  - GA4 analytics resolution: look at the utm_content data layer first (auto-map by
    project-code prefix), and fall back to the manual project_ga4_urls pattern otherwise.
"""

import asyncio
from unittest.mock import patch

from ingestion.transformation.ga4_transform import _build_select_sql
from backend.routers.ga4 import get_ga4_analytics


CONTENT_COL = "Session_manual_ad_content___GA4__Google_Analytics"


# ── Transform SQL builder ──────────────────────────────────────────


class TestBuildSelectSql:
    def test_content_column_present_wires_it_in(self):
        sql = _build_select_sql("AND Date >= '2026-06-01'", CONTENT_COL)
        # column is read (not the constant) and grouped at content grain
        assert f"IFNULL(`{CONTENT_COL}`, '(not set)') AS session_content" in sql
        assert "session_campaign, session_content" in sql
        # date filter still threaded through
        assert "AND Date >= '2026-06-01'" in sql

    def test_content_column_absent_uses_constant_and_no_extra_groupby(self):
        sql = _build_select_sql("", None)
        assert "'(not set)' AS session_content" in sql
        # no content grain in the GROUP BY when the column is missing
        assert "session_campaign, session_content" not in sql
        # the group-by still terminates cleanly at session_campaign
        assert "session_medium, session_campaign\n" in sql


# ── Analytics resolution ───────────────────────────────────────────


class _Router:
    """Dispatches mocked bq.run_query by SQL content and records the calls."""

    def __init__(self, url_rows, content_rows, fallback_rows):
        self.url_rows = url_rows
        self.content_rows = content_rows
        self.fallback_rows = fallback_rows
        self.calls = []

    def __call__(self, sql, params=None):
        self.calls.append((sql, params))
        if "project_ga4_urls" in sql:
            return self.url_rows
        if "session_content LIKE" in sql:
            return self.content_rows
        return self.fallback_rows


def _patch_bq(mock_bq, router):
    mock_bq.run_query.side_effect = router
    mock_bq.table.side_effect = lambda name: f"`proj.ds.{name}`"
    mock_bq.string_param.side_effect = lambda n, v: (n, v)
    mock_bq.date_param.side_effect = lambda n, v: (n, v)


class TestAnalyticsContentFirst:
    @patch("backend.routers.ga4.bq")
    @patch("backend.routers.ga4._ensure_table")
    def test_uses_content_layer_when_present(self, _ensure, mock_bq):
        router = _Router(
            url_rows=[],  # no manual mapping configured at all
            content_rows=[
                {"date": "2026-06-10", "sessions": 80, "page_views": 60, "conversions": 8},
                {"date": "2026-06-11", "sessions": 20, "page_views": 15, "conversions": 2},
            ],
            fallback_rows=[{"date": "2026-06-10", "sessions": 999, "conversions": 999}],
        )
        _patch_bq(mock_bq, router)

        res = asyncio.run(get_ga4_analytics("26023", start_date=None, end_date=None))

        assert res.has_ga4 is True
        assert res.total_sessions == 100
        assert res.total_conversions == 10
        # looked up by the project-code prefix
        assert any(p and ("content_prefix", "26023-%") in p for _, p in router.calls)
        # and never consulted the manual-pattern fallback
        assert not any("LOWER(session_campaign)" in s for s, _ in router.calls)


class TestAnalyticsFallback:
    @patch("backend.routers.ga4.bq")
    @patch("backend.routers.ga4._ensure_table")
    def test_falls_back_to_manual_pattern_when_no_content(self, _ensure, mock_bq):
        router = _Router(
            url_rows=[{"id": "u1", "ga4_property_id": "491551036", "url_pattern": "26023", "label": "x"}],
            content_rows=[],  # nothing tagged to the standard yet
            fallback_rows=[{"date": "2026-06-10", "sessions": 5, "conversions": 1}],
        )
        _patch_bq(mock_bq, router)

        res = asyncio.run(get_ga4_analytics("26023", start_date=None, end_date=None))

        assert res.has_ga4 is True
        assert res.total_sessions == 5
        # the manual campaign/source/medium pattern filter was used
        assert any("LOWER(session_campaign)" in s for s, _ in router.calls)

    @patch("backend.routers.ga4.bq")
    @patch("backend.routers.ga4._ensure_table")
    def test_no_content_and_no_mapping_reads_not_connected(self, _ensure, mock_bq):
        router = _Router(url_rows=[], content_rows=[], fallback_rows=[])
        _patch_bq(mock_bq, router)

        res = asyncio.run(get_ga4_analytics("26023", start_date=None, end_date=None))

        assert res.has_ga4 is False
