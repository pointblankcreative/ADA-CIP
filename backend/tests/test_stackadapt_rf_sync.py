"""Tests for the StackAdapt reach/frequency direct feed
(backend/services/stackadapt_rf_sync.py — Asana 1215990005858637).

Coverage:
  (a) no-op + status dict when STACKADAPT_API_KEY is unset;
  (b) field mapping (uniqueImpressions→reach_individual,
      periodResidentialUniqueImp→reach_household, periodStart→period_start, …);
  (c) an HTTP-200 `errors[]` body is treated as a source failure (not parsed as
      data) and the run survives;
  (d) a throttle error's retryAfterInSeconds is honoured;
  (e) the MERGE upsert key is (campaign_id, period_days, period_start);
  (f) run_sync never raises even when the HTTP client throws.

All network + BigQuery access is mocked — nothing leaves the process.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import patch

import pytest

from backend.services import stackadapt_rf_sync as srf


# ── fakes ──────────────────────────────────────────────────────────────


class FakeResponse:
    def __init__(self, payload: dict, status: int = 200):
        self._payload = payload
        self.status = status

    def raise_for_status(self):
        if self.status >= 400:
            raise RuntimeError(f"HTTP {self.status}")

    def json(self):
        return self._payload


class FakeHttpClient:
    """Serves a queued list of payloads (or Exception instances) from post()."""

    def __init__(self, responses):
        self._responses = list(responses)
        self.calls: list[dict] = []

    def post(self, url, json=None, headers=None):
        self.calls.append(json)
        item = self._responses.pop(0) if self._responses else {"data": {"reachFrequency": {}}}
        if isinstance(item, Exception):
            raise item
        return FakeResponse(item)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakeJob:
    def result(self):
        return None


class FakeBQClient:
    def __init__(self):
        self.loaded_rows = None
        self.load_table = None
        self.query_sql = None

    def load_table_from_json(self, rows, table, job_config=None):
        self.loaded_rows = rows
        self.load_table = table
        return FakeJob()

    def query(self, sql):
        self.query_sql = sql
        return FakeJob()


def _rf_payload(nodes, has_next=False, cursor=None):
    return {
        "data": {
            "reachFrequency": {
                "totalCount": len(nodes),
                "pageInfo": {"hasNextPage": has_next, "endCursor": cursor},
                "nodes": nodes,
            }
        }
    }


_NODE = {
    "campaign": {"id": "3272754", "name": "26023 - Decision Makers - Video Reach"},
    "channel": "display",
    "periodStart": "2026-07-01",
    "periodEnd": "2026-07-31",
    "impressions": 120000,
    "uniqueImpressions": 30229,
    "frequency": 3.97,
    "periodResidentialImp": 90000,
    "periodResidentialUniqueImp": 12000,
    "periodResidentialFrequency": 7.5,
}


# ── (a) no-op when key unset ───────────────────────────────────────────


def test_run_sync_no_ops_without_key(monkeypatch):
    monkeypatch.setattr(srf.settings, "stackadapt_api_key", "")
    result = srf.run_sync()
    assert result["status"] == "skipped"
    assert result["reason"] == "no_key"
    assert result["rows_upserted"] == 0


# ── (b) field mapping ──────────────────────────────────────────────────


def test_node_to_row_maps_every_field():
    row = srf._node_to_row(_NODE, period_days=30, fetched_at="2026-07-09T00:00:00+00:00")
    assert row == {
        "campaign_id": "3272754",
        "campaign_name": "26023 - Decision Makers - Video Reach",
        "channel": "display",
        "period_days": 30,
        "period_start": "2026-07-01",
        "period_end": "2026-07-31",
        "reach_individual": 30229,
        "frequency_individual": 3.97,
        "reach_household": 12000,
        "frequency_household": 7.5,
        "impressions": 120000,
        "impressions_household": 90000,
        "fetched_at": "2026-07-09T00:00:00+00:00",
    }


def test_node_to_row_drops_rows_without_grain_keys():
    # No campaign id → can't MERGE.
    assert srf._node_to_row({"periodStart": "2026-07-01"}, 1, "t") is None
    # No period_start → can't MERGE.
    assert srf._node_to_row({"campaign": {"id": "1"}}, 1, "t") is None


def test_node_to_row_normalises_datetime_to_date():
    node = {"campaign": {"id": "1"}, "periodStart": "2026-07-01T00:00:00Z",
            "periodEnd": "2026-07-31T23:59:59Z"}
    row = srf._node_to_row(node, 7, "t")
    assert row["period_start"] == "2026-07-01"
    assert row["period_end"] == "2026-07-31"


# ── (c) HTTP-200 errors[] body is a failure, not data; run survives ────


def test_errors_body_is_treated_as_failure():
    breaker = srf._CircuitBreaker()
    http = FakeHttpClient([{"data": None, "errors": [{"message": "The access token expired"}]}])
    conn = srf._post_rf(http, {"f": {}, "cursor": None}, breaker)
    assert conn is None
    assert breaker.consecutive == 1


def test_run_survives_all_errors(monkeypatch):
    monkeypatch.setattr(srf.settings, "stackadapt_api_key", "key")
    monkeypatch.setattr(srf.bq, "run_query", lambda *a, **k: [{"campaign_id": "3272754"}])
    monkeypatch.setattr(srf.bq, "string_param", lambda n, v: (n, v))
    monkeypatch.setattr(srf.bq, "table", lambda n: f"`t.{n}`")

    err = {"data": None, "errors": [{"message": "boom"}]}
    # One error per grain (1 batch × 1 page × 3 periods).
    fake = FakeHttpClient([err, err, err])
    captured = {}

    def _capture_upsert(rows):
        captured["rows"] = rows
        return len(rows)

    with patch.object(srf, "_http", lambda: fake), \
         patch.object(srf, "_upsert_rows", _capture_upsert):
        result = srf.run_sync()

    assert result["status"] == "success"
    assert result["rows_upserted"] == 0
    assert captured["rows"] == []
    # Three consecutive failures, still under the breaker threshold.
    assert result["breaker_tripped"] is False


# ── (d) throttle retryAfterInSeconds honoured ──────────────────────────


def test_throttle_retry_after_is_honoured():
    breaker = srf._CircuitBreaker()
    throttle_err = {
        "data": None,
        "errors": [{
            "message": "throttled",
            "extensions": {"cost": {"throttle": {"retryAfterInSeconds": 1.5}}},
        }],
    }
    good = _rf_payload([_NODE])
    http = FakeHttpClient([throttle_err, good])

    slept: list[float] = []
    with patch.object(srf.time, "sleep", lambda s: slept.append(s)):
        conn = srf._post_rf(http, {"f": {}, "cursor": None}, breaker)

    assert slept == [1.5]
    assert conn is not None
    assert conn["nodes"][0]["uniqueImpressions"] == 30229
    # A success after the retry resets the breaker.
    assert breaker.consecutive == 0


def test_throttle_retry_after_extraction():
    assert srf._throttle_retry_after(
        [{"extensions": {"cost": {"throttle": {"retryAfterInSeconds": 4}}}}]
    ) == 4.0
    # A non-throttle error yields None → the caller does NOT retry.
    assert srf._throttle_retry_after([{"message": "schema error"}]) is None


# ── (e) MERGE upsert key ───────────────────────────────────────────────


def test_upsert_merges_on_campaign_period_days_period_start():
    client = FakeBQClient()
    row = srf._node_to_row(_NODE, 30, "2026-07-09T00:00:00+00:00")
    with patch.object(srf.bq, "get_client", lambda: client):
        n = srf._upsert_rows([row])

    assert n == 1
    assert client.loaded_rows == [row]
    sql = client.query_sql
    assert "t.campaign_id = s.campaign_id" in sql
    assert "t.period_days = s.period_days" in sql
    assert "t.period_start = s.period_start" in sql
    assert "MERGE" in sql
    # Staging table sits in the StackAdapt dataset, MERGE targets the real one.
    assert client.load_table == srf.settings.stackadapt_rf_table + "_staging"


def test_upsert_noop_on_empty_rows():
    # Must not even reach for a BQ client when there's nothing to write.
    with patch.object(srf.bq, "get_client", side_effect=AssertionError("should not be called")):
        assert srf._upsert_rows([]) == 0


# ── (f) run_sync never raises when the HTTP client throws ───────────────


def test_run_sync_survives_http_crash(monkeypatch):
    monkeypatch.setattr(srf.settings, "stackadapt_api_key", "key")
    monkeypatch.setattr(srf.bq, "run_query", lambda *a, **k: [{"campaign_id": "3272754"}])
    monkeypatch.setattr(srf.bq, "string_param", lambda n, v: (n, v))
    monkeypatch.setattr(srf.bq, "table", lambda n: f"`t.{n}`")

    def _boom():
        raise RuntimeError("connection reset")

    captured = {}
    with patch.object(srf, "_http", _boom), \
         patch.object(srf, "_upsert_rows", lambda rows: len(rows)):
        result = srf.run_sync()  # must not raise

    assert result["status"] == "success"
    assert result["rows_upserted"] == 0


def test_run_sync_survives_post_exception(monkeypatch):
    monkeypatch.setattr(srf.settings, "stackadapt_api_key", "key")
    monkeypatch.setattr(srf.bq, "run_query", lambda *a, **k: [{"campaign_id": "3272754"}])
    monkeypatch.setattr(srf.bq, "string_param", lambda n, v: (n, v))
    monkeypatch.setattr(srf.bq, "table", lambda n: f"`t.{n}`")

    fake = FakeHttpClient([RuntimeError("reset"), RuntimeError("reset"), RuntimeError("reset")])
    with patch.object(srf, "_http", lambda: fake), \
         patch.object(srf, "_upsert_rows", lambda rows: len(rows)):
        result = srf.run_sync()

    assert result["status"] == "success"


# ── grain windows ──────────────────────────────────────────────────────


def test_monthly_window_starts_first_of_previous_month():
    start, end = srf._grain_window(30, date(2026, 7, 9))
    assert start == "2026-06-01T00:00:00Z"
    assert end == "2026-07-09T00:00:00Z"


def test_daily_window_looks_back_35_days():
    start, _ = srf._grain_window(1, date(2026, 7, 9))
    assert start == "2026-06-04T00:00:00Z"


# ── tracked campaigns ──────────────────────────────────────────────────


def test_tracked_campaign_ids_filters_stackadapt(monkeypatch):
    seen = {}

    def _run_query(sql, params=None):
        seen["sql"] = sql
        seen["params"] = params
        return [{"campaign_id": "3272754"}, {"campaign_id": None}, {"campaign_id": "999"}]

    monkeypatch.setattr(srf.bq, "run_query", _run_query)
    monkeypatch.setattr(srf.bq, "string_param", lambda n, v: (n, v))
    monkeypatch.setattr(srf.bq, "table", lambda n: f"`t.{n}`")

    ids = srf._tracked_campaign_ids()
    assert ids == ["3272754", "999"]
    assert "platform_id" in seen["sql"]
    assert ("platform_id", srf.STACKADAPT_PLATFORM_ID) in seen["params"]


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
