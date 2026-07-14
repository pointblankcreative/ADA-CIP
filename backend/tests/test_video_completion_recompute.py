"""Standardized video metrics — ADA 1215989989043460.

The Video Completion Rate was ``video_completions (Meta ThruPlay ≈ 15s) ÷
video_views (raw plays, autoplay-inflated ≈4x)`` — a mixed-denominator ratio
that read alarmingly low (~1.4%) and matched no platform's own completion
figure. It is now a TRUE quartile-based completion: the deepest reported
quartile (``video_q100`` — 100% on most platforms, 95% on StackAdapt) ÷ the
canonical "video start" — the 3-second intentional view (``video_views_3s``),
falling back to the 25% quartile where a platform has quartiles but no 3-second
signal. That is the SAME start the diagnostics A1 engine scores completion on,
so the engine and the read-path now agree on one definition.

This pins:
  (a) the shared ``_completion_rate`` helper (creative router);
  (b) the ``_vcr_sql`` builder (performance router) — quartile-based, capped
      at 1.0, and NOT the old ThruPlay/plays ratio;
  (c) the performance read-path SQL blocks emit the new expression;
  (d) the creative × platform matrix cell recomputes completion and exposes
      the canonical ``video_start`` the frontend funnel anchors on;
  (e) a completion-only platform with no quartile funnel (e.g. Google TrueView)
      gets an honest None, not a fabricated rate.

Hermetic: BigQuery is patched; where a builder returns SQL text we assert on
the string it constructs.
"""

from __future__ import annotations

import asyncio
import datetime
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import creative as creative_router
from backend.routers import performance as perf_router


# ── (a) the shared completion-rate helper ────────────────────────────


class TestCompletionRateHelper:
    def test_uses_3s_start_as_denominator(self):
        # 100 completed ÷ 400 three-second starts = 25%. Note the q25 (350)
        # is NOT the denominator when a 3-second start is present.
        assert creative_router._completion_rate(100, 400, 350, True) == 0.25

    def test_falls_back_to_q25_when_no_3s(self):
        # No 3-second signal → the 25% quartile is the start proxy.
        assert creative_router._completion_rate(100, 0, 400, True) == 0.25

    def test_capped_at_one(self):
        # A quartile-vs-start reporting quirk can never surface > 100%.
        assert creative_router._completion_rate(500, 400, 0, True) == 1.0

    def test_none_under_volume_guard(self):
        assert creative_router._completion_rate(100, 400, 0, False) is None

    def test_none_without_a_start(self):
        # Completion-only platform (no quartiles, no 3s) → honest None.
        assert creative_router._completion_rate(0, 0, 0, True) is None

    def test_zero_completions_is_a_real_zero(self):
        # Started but nobody finished is a real 0%, not a missing read.
        assert creative_router._completion_rate(0, 400, 0, True) == 0.0


# ── (b) the performance VCR SQL builder ──────────────────────────────


class TestVcrSqlBuilder:
    def test_inline_is_quartile_over_start_capped(self):
        sql = perf_router._vcr_sql("f.")
        assert "SUM(f.video_q100)" in sql
        assert "COALESCE(NULLIF(f.video_views_3s, 0), f.video_q25)" in sql
        assert sql.startswith("LEAST(")
        assert ", 1.0)" in sql

    def test_presummed_does_not_wrap_in_sum(self):
        sql = perf_router._vcr_sql("a.", presummed=True)
        assert "SAFE_DIVIDE(a.video_q100," in sql
        assert "SUM(a.video_q100)" not in sql

    def test_is_not_the_old_thruplay_over_plays_ratio(self):
        sql = perf_router._vcr_sql("f.")
        # The old numerator/denominator pair must be gone from the expression.
        assert "video_completions" not in sql
        assert "video_views," not in sql  # raw plays as a bare denominator


# ── (c) the read-path SQL blocks emit the new expression ─────────────


def _string_param(name, value):
    return ("string", name, value)


def _date_param(name, value):
    return ("date", name, value)


def _array_param(name, type_, values):
    return ("array", name, type_, list(values))


class _Recorder:
    def __init__(self):
        self.calls = []
        self.responses = []

    def __call__(self, sql, params=None):
        self.calls.append(sql)
        if self.responses:
            return self.responses.pop(0)
        return []


def _perf_app() -> FastAPI:
    app = FastAPI()
    app.include_router(perf_router.router)
    return app


def _get_perf(rec, path):
    patches = [
        patch.object(perf_router.bq, "run_query", side_effect=rec),
        patch.object(perf_router.bq, "string_param", _string_param),
        patch.object(perf_router.bq, "date_param", _date_param),
        patch.object(perf_router.bq, "array_param", _array_param),
        patch.object(perf_router.bq, "table", lambda n: f"`d.{n}`"),
    ]
    for p in patches:
        p.start()
    try:
        return TestClient(_perf_app()).get(path)
    finally:
        for p in patches:
            p.stop()


_NEW_START = "COALESCE(NULLIF(f.video_views_3s, 0), f.video_q25)"
_OLD_RATIO = "SAFE_DIVIDE(SUM(f.video_completions), NULLIF(SUM(f.video_views), 0)) AS vcr"


class TestReadPathSqlUsesNewVcr:
    def test_ads_block(self):
        rec = _Recorder()
        rec.responses = [[]]
        resp = _get_perf(rec, "/api/performance/26018/ads")
        assert resp.status_code == 200, resp.text
        sql = rec.calls[0]
        assert f"LEAST(SAFE_DIVIDE(SUM(f.video_q100), NULLIF(SUM({_NEW_START})" in sql
        assert _OLD_RATIO not in sql

    def test_adsets_block_presummed_cte(self):
        rec = _Recorder()
        rec.responses = [[]]
        resp = _get_perf(rec, "/api/performance/26018/adsets")
        assert resp.status_code == 200, resp.text
        sql = rec.calls[0]
        # The CTE carries the quartile + start sums …
        assert "SUM(f.video_q100) AS video_q100" in sql
        assert "SUM(f.video_views_3s) AS video_views_3s" in sql
        # … and the outer vcr divides the pre-summed columns (presummed form).
        assert "SAFE_DIVIDE(a.video_q100," in sql
        assert "SAFE_DIVIDE(a.video_completions" not in sql


# ── (d)/(e) creative matrix cell recompute + honest None ─────────────


def _patch_creative_bq(mock_bq):
    def run_query(sql, params=None):
        return []
    mock_bq.run_query.side_effect = run_query
    mock_bq.table.side_effect = lambda name: f"`proj.ds.{name}`"
    mock_bq.string_param.side_effect = lambda n, v: (n, v)
    mock_bq.date_param.side_effect = lambda n, v: (n, v)
    mock_bq.array_param.side_effect = lambda n, t, v: (n, t, list(v))


def _cell(**over):
    base = {
        "creative_variant": "Creative", "platform_id": "meta",
        "spend": 500.0, "impressions": 5000, "clicks": 20,
        "clicks_reported": 4, "conversions": 0.0, "engagements": 0,
        "video_views": 0, "video_completions": 0, "video_views_3s": 0,
        "video_q25": 0, "video_q50": 0, "video_q75": 0, "video_q100": 0,
    }
    base.update(over)
    return base


class TestCreativeMatrixCompletion:
    @patch("backend.routers.creative._query_creative_platform_cells")
    @patch("backend.routers.creative.bq")
    def test_cell_completion_and_start(self, mock_bq, mock_cells):
        _patch_creative_bq(mock_bq)
        mock_cells.return_value = [
            # Meta video: 400 starts (3s), 100 finished → 25% completion.
            _cell(creative_variant="Hero", platform_id="meta",
                  video_views=1600, video_completions=250, video_views_3s=400,
                  video_q25=350, video_q50=220, video_q75=150, video_q100=100),
            # Google TrueView: reports plays + completions but no quartiles and
            # no 3-second start → completion is an honest None, not a rate.
            _cell(creative_variant="Search", platform_id="google_ads",
                  video_views=9000, video_completions=9000, video_views_3s=0,
                  video_q25=0, video_q50=0, video_q75=0, video_q100=0),
        ]
        resp = asyncio.run(creative_router.get_creative_matrix("26023"))

        hero = resp.cells["Hero"]["meta"]
        assert hero.completion_rate == 0.25          # 100 / 400
        assert hero.video_start == 400               # canonical 3-second start
        assert hero.video_q100 == 100                # raw quartile still passed

        search = resp.cells["Search"]["google_ads"]
        assert search.completion_rate is None        # no quartile funnel
        assert search.video_start == 0
