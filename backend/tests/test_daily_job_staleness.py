"""P-FRESH-PACE: daily-sweep staleness detection + alert reshaping.

The prior _write_stale_alerts wrote columns that don't exist in cip.alerts
(metric_value / threshold_value / is_resolved) so the load was silently
rejected — zero stale alerts ever landed. These lock in the corrected
metadata-JSON shape, the __system__ + per-project scoping, and the delegation
of detection to compute_platform_freshness.
"""

import json
from datetime import date
from unittest.mock import patch

from backend.services import data_freshness  # noqa: F401 (patched by path)
from backend.services.daily_job import (
    _check_data_staleness,
    _projects_on_platform,
    _write_stale_alerts,
    DATA_STALE_HOURS,
)


# ── _check_data_staleness delegates to compute_platform_freshness ───


@patch("backend.services.data_freshness.compute_platform_freshness")
def test_check_staleness_filters_and_reshapes(mock_fresh):
    mock_fresh.return_value = [
        {"platform_id": "meta", "is_stale": False,
         "latest_data_date": date(2026, 7, 17), "age_hours": 5.0},
        {"platform_id": "stackadapt", "is_stale": True,
         "latest_data_date": date(2026, 7, 14), "age_hours": 50.0},
    ]
    out = _check_data_staleness()
    assert len(out) == 1
    sp = out[0]
    assert sp["platform_id"] == "stackadapt"
    assert sp["hours_since_load"] == 50.0
    assert sp["latest_date"] == "2026-07-14"


@patch("backend.services.data_freshness.compute_platform_freshness",
       side_effect=RuntimeError("boom"))
def test_check_staleness_fails_open(mock_fresh):
    # A freshness read error must never take the pipeline down.
    assert _check_data_staleness() == []


# ── _write_stale_alerts shape + scoping ─────────────────────────────


@patch("backend.services.pacing._write_alerts")
@patch("backend.services.daily_job._projects_on_platform")
def test_write_stale_alerts_shape_and_scoping(mock_projects, mock_write):
    mock_projects.return_value = ["26023"]
    stale = [{"platform_id": "stackadapt",
              "latest_date": "2026-07-14", "hours_since_load": 50.0}]

    n = _write_stale_alerts(stale)

    # one __system__ outage alert + one per affected project
    assert n == 2
    alerts = mock_write.call_args[0][0]
    assert len(alerts) == 2

    project_codes = {a["project_code"] for a in alerts}
    assert project_codes == {"__system__", "26023"}

    for a in alerts:
        # the corrected shape: metadata-JSON, no invalid top-level columns
        assert a["alert_type"] == "data_stale"
        assert a["severity"] == "warning"
        assert a["slack_sent"] is False
        assert "created_at" in a
        assert "metric_value" not in a
        assert "threshold_value" not in a
        assert "is_resolved" not in a
        meta = json.loads(a["metadata"])
        assert meta["platform_id"] == "stackadapt"
        assert meta["hours_since_load"] == 50.0
        assert meta["threshold"] == DATA_STALE_HOURS
        assert meta["latest_date"] == "2026-07-14"


@patch("backend.services.pacing._write_alerts")
@patch("backend.services.daily_job._projects_on_platform")
def test_write_stale_alerts_system_only_when_no_projects(mock_projects, mock_write):
    mock_projects.return_value = []  # platform outage with no affected campaign
    stale = [{"platform_id": "tiktok",
              "latest_date": "2026-07-13", "hours_since_load": 72.0}]

    n = _write_stale_alerts(stale)
    assert n == 1
    alerts = mock_write.call_args[0][0]
    assert alerts[0]["project_code"] == "__system__"


@patch("backend.services.pacing._write_alerts")
def test_write_stale_alerts_empty_is_noop(mock_write):
    assert _write_stale_alerts([]) == 0
    assert not mock_write.called


# ── _projects_on_platform filters to still-in-flight lines ──────────


def test_projects_on_platform_filters_to_in_flight():
    """Review #4b: a project whose only line on the stale platform has already
    COMPLETED must not get a 'not reporting' alert — the scoping query carries
    the same flight_start<=today<=flight_end window pacing.py uses."""
    captured = {}

    def qr(sql, params=None):
        captured["sql"] = sql
        return []

    with patch("backend.services.daily_job.bq") as mock_bq:
        mock_bq.table.side_effect = lambda n: f"`ds.{n}`"
        mock_bq.string_param.side_effect = lambda n, v: (n, v)
        mock_bq.run_query.side_effect = qr
        _projects_on_platform("meta")

    sql = captured["sql"]
    assert "flight_start <= CURRENT_DATE()" in sql
    assert "flight_end" in sql and "CURRENT_DATE()" in sql
