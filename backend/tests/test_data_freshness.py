"""Tests for the P-FRESH-PACE per-platform freshness primitive.

compute_platform_freshness consolidates the admin-panel and daily-sweep
staleness reads into one as_of-aware function. These mock bq.run_query and
assert the staleness rule (36h absolute floor OR relative-lag) plus the
flight-end guard (a platform whose lines have all ended is never stale).
"""

from datetime import date, datetime, timedelta, timezone
from unittest.mock import patch

from backend.services import data_freshness
from backend.services.data_freshness import compute_platform_freshness, DATA_STALE_HOURS


AS_OF = date(2026, 7, 17)


def _loaded(hours_ago: float) -> datetime:
    """A UTC load timestamp ``hours_ago`` hours before now (age math uses now)."""
    return datetime.now(timezone.utc) - timedelta(hours=hours_ago)


def _freshness_router(fact_rows, flight_rows=None):
    """Dispatch bq.run_query: fact_digital_daily → fact_rows; media_plan_lines
    (the flight-end guard) → flight_rows."""
    def _router(sql, params=None):
        if "media_plan_lines" in sql:
            return flight_rows or []
        if "fact_digital_daily" in sql:
            return fact_rows
        return []
    return _router


# ── (a) 40h stale with a live flight → is_stale ─────────────────────


def test_absolute_stale_with_live_flight_is_stale():
    fact_rows = [
        {"platform_id": "stackadapt", "latest_data_date": AS_OF,
         "latest_loaded_at": _loaded(40), "total_days": 30, "total_rows": 100},
    ]
    # A still-live line on stackadapt (flight_end in the future).
    flight_rows = [{"platform_id": "stackadapt", "max_flight_end": AS_OF + timedelta(days=5)}]

    with patch.object(data_freshness, "bq") as mock_bq:
        mock_bq.table.side_effect = lambda n: f"`ds.{n}`"
        mock_bq.string_param.side_effect = lambda n, v: (n, v)
        mock_bq.run_query.side_effect = _freshness_router(fact_rows, flight_rows)
        out = compute_platform_freshness(AS_OF, project_code="26023")

    sa = {p["platform_id"]: p for p in out}["stackadapt"]
    assert sa["is_stale"] is True
    assert sa["age_hours"] > DATA_STALE_HOURS
    assert sa["stale_reason"]


# ── (b) 40h stale but flight ended → NOT stale ──────────────────────


def test_absolute_stale_but_flight_ended_is_not_stale():
    fact_rows = [
        {"platform_id": "stackadapt", "latest_data_date": AS_OF - timedelta(days=3),
         "latest_loaded_at": _loaded(40), "total_days": 30, "total_rows": 100},
    ]
    # The platform's only line ended before as_of → expected to stop reporting.
    flight_rows = [{"platform_id": "stackadapt", "max_flight_end": AS_OF - timedelta(days=2)}]

    with patch.object(data_freshness, "bq") as mock_bq:
        mock_bq.table.side_effect = lambda n: f"`ds.{n}`"
        mock_bq.string_param.side_effect = lambda n, v: (n, v)
        mock_bq.run_query.side_effect = _freshness_router(fact_rows, flight_rows)
        out = compute_platform_freshness(AS_OF, project_code="26023")

    sa = {p["platform_id"]: p for p in out}["stackadapt"]
    assert sa["is_stale"] is False
    assert sa["stale_reason"] is None


# ── (c) 10h old → NOT stale ─────────────────────────────────────────


def test_recent_load_is_not_stale():
    fact_rows = [
        {"platform_id": "meta", "latest_data_date": AS_OF,
         "latest_loaded_at": _loaded(10), "total_days": 30, "total_rows": 500},
    ]
    with patch.object(data_freshness, "bq") as mock_bq:
        mock_bq.table.side_effect = lambda n: f"`ds.{n}`"
        mock_bq.string_param.side_effect = lambda n, v: (n, v)
        mock_bq.run_query.side_effect = _freshness_router(fact_rows)
        out = compute_platform_freshness(AS_OF)  # global call, no guard

    meta = {p["platform_id"]: p for p in out}["meta"]
    assert meta["is_stale"] is False
    assert meta["age_hours"] < DATA_STALE_HOURS


# ── (d) relative-lag path: recent load, but data date trails freshest ─


def test_relative_lag_flags_platform_that_stopped_producing_days():
    fact_rows = [
        # Fresh platform: newest data date is today.
        {"platform_id": "meta", "latest_data_date": AS_OF,
         "latest_loaded_at": _loaded(6), "total_days": 30, "total_rows": 500},
        # Loaded recently (10h — under the 36h floor) but its newest DATA date
        # is 3 days behind meta's → it has stopped producing new days.
        {"platform_id": "google_ads", "latest_data_date": AS_OF - timedelta(days=3),
         "latest_loaded_at": _loaded(10), "total_days": 30, "total_rows": 400},
    ]
    with patch.object(data_freshness, "bq") as mock_bq:
        mock_bq.table.side_effect = lambda n: f"`ds.{n}`"
        mock_bq.string_param.side_effect = lambda n, v: (n, v)
        mock_bq.run_query.side_effect = _freshness_router(fact_rows)
        out = compute_platform_freshness(AS_OF)  # global, no flight guard

    by_id = {p["platform_id"]: p for p in out}
    assert by_id["meta"]["is_stale"] is False
    # google_ads: absolute age is fine (10h) but relative lag (3d > 1d) → stale
    assert by_id["google_ads"]["is_stale"] is True
    assert by_id["google_ads"]["age_hours"] < DATA_STALE_HOURS
    assert "lags freshest" in by_id["google_ads"]["stale_reason"]


def test_loaded_at_missing_falls_back_to_data_date_age():
    # No loaded_at → age = (as_of - latest_data_date).days * 24. 3 days = 72h > 36h.
    fact_rows = [
        {"platform_id": "tiktok", "latest_data_date": AS_OF - timedelta(days=3),
         "latest_loaded_at": None, "total_days": 10, "total_rows": 50},
    ]
    with patch.object(data_freshness, "bq") as mock_bq:
        mock_bq.table.side_effect = lambda n: f"`ds.{n}`"
        mock_bq.string_param.side_effect = lambda n, v: (n, v)
        mock_bq.run_query.side_effect = _freshness_router(fact_rows)
        out = compute_platform_freshness(AS_OF)

    tt = {p["platform_id"]: p for p in out}["tiktok"]
    assert tt["age_hours"] == 72.0
    assert tt["is_stale"] is True


def test_project_scoped_measurement_flags_target_even_when_globally_fresh():
    """The headline 26023 case: platform 'meta' is GLOBALLY fresh (another live
    project loaded it today) but 26023's OWN meta line stopped 3 days ago. The
    measurement must be project-scoped — with project_code the SQL adds
    `WHERE project_code = @project_code` and reads the STALE per-project data;
    without it (agency sweep) it reflects the globally-fresh row. This exercises
    the real project_code branch, not a mocked-past result."""
    global_rows = [
        {"platform_id": "meta", "latest_data_date": AS_OF,
         "latest_loaded_at": _loaded(4), "total_days": 60, "total_rows": 9000},
    ]
    scoped_rows = [
        {"platform_id": "meta", "latest_data_date": AS_OF - timedelta(days=3),
         "latest_loaded_at": _loaded(72), "total_days": 20, "total_rows": 300},
    ]
    # 26023's meta line is still in flight (plan to 07-19) so the guard keeps it.
    flight_rows = [{"platform_id": "meta", "max_flight_end": AS_OF + timedelta(days=2)}]

    def router(sql, params=None):
        if "media_plan_lines" in sql:
            return flight_rows
        if "fact_digital_daily" in sql:
            # the scoping branch adds this WHERE clause + the param.
            if "project_code = @project_code" in sql:
                assert params and any(p == ("project_code", "26023") for p in params)
                return scoped_rows
            assert not params
            return global_rows
        return []

    with patch.object(data_freshness, "bq") as mock_bq:
        mock_bq.table.side_effect = lambda n: f"`ds.{n}`"
        mock_bq.string_param.side_effect = lambda n, v: (n, v)
        mock_bq.run_query.side_effect = router
        scoped = compute_platform_freshness(AS_OF, project_code="26023")
        glob = compute_platform_freshness(AS_OF)

    # project-scoped: 26023's meta is stale (its own data stopped)
    assert {p["platform_id"]: p for p in scoped}["meta"]["is_stale"] is True
    # agency-wide sweep: meta reads globally fresh
    assert {p["platform_id"]: p for p in glob}["meta"]["is_stale"] is False


def test_global_call_skips_flight_guard_and_does_not_query_media_plan_lines():
    fact_rows = [
        {"platform_id": "stackadapt", "latest_data_date": AS_OF - timedelta(days=2),
         "latest_loaded_at": _loaded(50), "total_days": 5, "total_rows": 20},
    ]
    seen = {"mpl": False}

    def _router(sql, params=None):
        if "media_plan_lines" in sql:
            seen["mpl"] = True
            return []
        if "fact_digital_daily" in sql:
            return fact_rows
        return []

    with patch.object(data_freshness, "bq") as mock_bq:
        mock_bq.table.side_effect = lambda n: f"`ds.{n}`"
        mock_bq.string_param.side_effect = lambda n, v: (n, v)
        mock_bq.run_query.side_effect = _router
        out = compute_platform_freshness(AS_OF)  # no project_code

    assert seen["mpl"] is False
    assert {p["platform_id"]: p for p in out}["stackadapt"]["is_stale"] is True
