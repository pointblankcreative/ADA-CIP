"""Tests for pacing-alert spend charts."""

import json
from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from backend.services import alert_charts as ac


def _line(**kw):
    base = {
        "project_code": "26018", "line_id": "L", "line_code": "#3",
        "platform_id": "meta", "budget": 4297.41,
        "flight_start": date(2026, 5, 7), "flight_end": date(2026, 6, 5),
    }
    base.update(kw)
    return base


class TestProjection:
    def test_trailing_rate_extends_to_flight_end(self):
        daily = [(date(2026, 5, d), 100.0) for d in range(1, 8)]  # 7 days @ $100
        cum, proj, projected_end, rate = ac._project(daily, date(2026, 5, 10))
        assert rate == 100.0
        assert cum[-1] == (date(2026, 5, 7), 700.0)
        assert proj[0] == (date(2026, 5, 7), 700.0)        # anchored at last actual
        assert proj[-1] == (date(2026, 5, 10), 1000.0)     # +3 days @ $100
        assert projected_end == 1000.0

    def test_completed_flight_has_no_projection_tail(self):
        daily = [(date(2026, 5, d), 50.0) for d in range(1, 6)]
        cum, proj, projected_end, rate = ac._project(daily, date(2026, 5, 5))
        assert proj == [(date(2026, 5, 5), 250.0)]
        assert projected_end == 250.0


class TestDailySpend:
    def test_line_code_series_filled_with_zero_days(self):
        rows = [{"date": date(2026, 5, 2), "spend": 50},
                {"date": date(2026, 5, 4), "spend": 70}]
        with patch.object(ac.bq, "run_query", return_value=rows), \
             patch.object(ac.bq, "string_param", return_value=MagicMock()), \
             patch.object(ac.bq, "date_param", return_value=MagicMock()):
            out = ac._daily_spend(
                _line(flight_start=date(2026, 5, 1), flight_end=date(2026, 5, 5)),
                date(2026, 5, 5),
            )
        assert out == [
            (date(2026, 5, 1), 0.0), (date(2026, 5, 2), 50.0),
            (date(2026, 5, 3), 0.0), (date(2026, 5, 4), 70.0),
            (date(2026, 5, 5), 0.0),
        ]


def _alert(**kw):
    base = {"alert_id": "a1", "project_code": "26018", "alert_type": "pacing_under",
            "severity": "warning", "metadata": json.dumps({"line_id": "L"})}
    base.update(kw)
    return base


class TestBuildBlocks:
    def test_skips_non_chart_alert_types(self):
        assert ac.build_alert_chart_blocks(_alert(alert_type="flight_ending")) == []

    def test_skips_when_bucket_unconfigured(self, monkeypatch):
        monkeypatch.setattr(ac.settings, "alert_charts_bucket", "")
        assert ac.build_alert_chart_blocks(_alert()) == []

    def test_happy_path_returns_two_image_blocks(self, monkeypatch):
        monkeypatch.setattr(ac.settings, "alert_charts_bucket", "bkt")
        daily = [(date(2026, 5, 7), 100.0), (date(2026, 5, 8), 100.0)]
        with patch.object(ac, "_line_for_chart", return_value=_line()), \
             patch.object(ac, "_daily_spend", return_value=daily), \
             patch.object(ac, "_render_cumulative_png", return_value=b"CUM"), \
             patch.object(ac, "_render_7day_png", return_value=b"BAR"), \
             patch.object(ac, "_upload_png",
                          side_effect=lambda png, name: f"https://x/{name}"):
            blocks = ac.build_alert_chart_blocks(_alert(), as_of=date(2026, 5, 20))
        assert [b["type"] for b in blocks] == ["image", "image"]
        assert blocks[0]["image_url"].endswith("-cumulative.png")
        assert blocks[1]["image_url"].endswith("-7day.png")
        assert all(b["alt_text"] for b in blocks)

    def test_render_failure_degrades_to_no_charts(self, monkeypatch):
        monkeypatch.setattr(ac.settings, "alert_charts_bucket", "bkt")
        with patch.object(ac, "_line_for_chart", return_value=_line()), \
             patch.object(ac, "_daily_spend",
                          return_value=[(date(2026, 5, 7), 100.0)]), \
             patch.object(ac, "_render_cumulative_png", side_effect=RuntimeError("boom")):
            assert ac.build_alert_chart_blocks(_alert(), as_of=date(2026, 5, 20)) == []

    def test_no_spend_yet_returns_empty(self, monkeypatch):
        monkeypatch.setattr(ac.settings, "alert_charts_bucket", "bkt")
        with patch.object(ac, "_line_for_chart", return_value=_line()), \
             patch.object(ac, "_daily_spend",
                          return_value=[(date(2026, 5, 7), 0.0), (date(2026, 5, 8), 0.0)]):
            assert ac.build_alert_chart_blocks(_alert(), as_of=date(2026, 5, 20)) == []


class TestRenderSmoke:
    def test_pngs_are_emitted(self):
        daily = [(date(2026, 5, d), float(d * 10)) for d in range(1, 9)]
        cum, proj, projected_end, _ = ac._project(daily, date(2026, 5, 12))
        assert ac._render_7day_png(daily)[:4] == b"\x89PNG"
        assert ac._render_cumulative_png(cum, proj, projected_end, 1000.0)[:4] == b"\x89PNG"
