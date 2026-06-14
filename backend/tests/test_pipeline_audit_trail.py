"""Pipeline audit-trail hardening tests (A4 hardening swarm — ADAC-63 + ADAC-69).

Two backend resilience fixes, so a failed/large run or a bad data cell can't
vanish silently:

  ADAC-63 — a FULL transform run must leave an ``ingestion_log`` row even when
            it fails. The FULL path reads all history and has been OOM-killed
            mid-run; previously nothing was written up front, so the run left no
            trace at all. The fix writes a 'running' row BEFORE the heavy work
            and UPDATEs it to success/failed at the end (with an INSERT fallback
            if the up-front write itself failed).

  ADAC-69 — media-plan rows whose Budget cell is non-empty but unparseable were
            silently dropped from budget rollups. The fix counts them, emits a
            structured warning, and surfaces an "N rows skipped" count in the
            sync response payload.

Mocking mirrors tests/test_transformation.py: the BigQuery clients are
MagicMocks and we assert on the SQL strings handed to ``mtl.query``. The
media-plan half drives the pure parser with prefetched data (no gspread), like
backend/tests/test_media_plan_sync.py.
"""

from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

import pytest


# ─────────────────────────────────────────────────────────────────────
# ADAC-63 — FULL transform always writes an ingestion_log row
# ─────────────────────────────────────────────────────────────────────


def _make_fake_row() -> dict:
    """One JSON-safe row shaped like a serialized fact_digital_daily row."""
    return {
        "date": date(2026, 3, 1).isoformat(),
        "platform_id": "meta",
        "campaign_id": "c1",
        "ad_set_id": "",
        "ad_id": "",
        "campaign_name": "test",
        "ad_set_name": "test",
        "ad_name": "test",
        "account_id": "a1",
        "account_name": "acct",
        "project_code": "25042",
        "spend": 100.0,
        "impressions": 1000,
        "clicks": 50,
        "reach": 500,
        "frequency": 2.0,
        "video_views": 0,
        "video_completions": 0,
        "conversions": 5.0,
        "engagements": 10,
        "cpm": 10.0,
        "cpc": 2.0,
        "ctr": 0.05,
        "ingestion_source": "funnel",
        "loaded_at": datetime.now(timezone.utc).isoformat(),
    }


class _FakeRow(dict):
    """dict subclass that behaves like a BigQuery Row under dict(r)."""


def _run_full_transform(load_raises: Exception | None = None,
                        running_insert_raises: bool = False):
    """Run run_transformation('full') with mocked BQ clients.

    Returns (result, mtl_queries) where mtl_queries is the ordered list of SQL
    strings handed to the Montreal client. If ``load_raises`` is set, the
    load_table_from_json().result() raises it, simulating an OOM / mid-run
    failure on the FULL path. If ``running_insert_raises`` is True, the FIRST
    mtl.query (the up-front 'running' INSERT) raises, exercising the
    INSERT-fallback branch of _log_run_finish.
    """
    fake_rows = [_FakeRow(_make_fake_row()) for _ in range(5)]

    us_client = MagicMock()
    us_job = MagicMock()
    us_job.result.return_value = fake_rows
    us_client.query.return_value = us_job

    mtl_client = MagicMock()

    if running_insert_raises:
        # First query (running INSERT) blows up; every later query is fine.
        good_job = MagicMock()
        good_job.result.return_value = None

        def _query_side_effect(*args, **kwargs):
            if not getattr(_query_side_effect, "fired", False):
                _query_side_effect.fired = True
                raise RuntimeError("simulated running-row insert failure")
            return good_job

        mtl_client.query.side_effect = _query_side_effect
    else:
        mtl_query_job = MagicMock()
        mtl_query_job.result.return_value = None
        mtl_client.query.return_value = mtl_query_job

    load_job = MagicMock()
    if load_raises is not None:
        load_job.result.side_effect = load_raises
    else:
        load_job.result.return_value = None
    load_job.output_rows = 5
    mtl_client.load_table_from_json.return_value = load_job

    fake_sql = "WITH enriched_data AS (SELECT 1) MERGE INTO dummy"

    with patch("backend.services.transformation._us_client", return_value=us_client), \
         patch("backend.services.transformation._mtl_client", return_value=mtl_client), \
         patch("backend.services.transformation.DAILY_SQL") as mock_daily, \
         patch("backend.services.transformation.FULL_SQL") as mock_full:

        mock_full.exists.return_value = True
        mock_full.read_text.return_value = fake_sql
        mock_daily.exists.return_value = True
        mock_daily.read_text.return_value = fake_sql

        from backend.services.transformation import run_transformation
        result = run_transformation(mode="full")

    mtl_queries = [
        c.args[0] if c.args else c.kwargs.get("query", "")
        for c in mtl_client.query.call_args_list
    ]
    return result, mtl_queries


class TestFullTransformAuditRow:
    """A FULL transform must always leave an ingestion_log row."""

    def test_running_row_written_before_heavy_work_on_success(self):
        """The very first ingestion_log write is a 'running' INSERT — it lands
        before the SELECT/TRUNCATE/load so an OOM-kill still leaves a trace."""
        result, queries = self._first_two(self._success_queries())
        first = queries[0]
        assert "INSERT INTO" in first and "ingestion_log" in first
        assert "'running'" in first, f"first log write should be 'running', got: {first}"

    def test_success_updates_the_running_row(self):
        """On success the run is finalized via UPDATE of the same row (one row
        per run), not a second INSERT."""
        result, queries = _run_full_transform()
        log_writes = [q for q in queries if "ingestion_log" in q]
        assert any(q.strip().startswith("INSERT") for q in log_writes)
        update_writes = [q for q in log_writes if "UPDATE" in q]
        assert len(update_writes) >= 1, f"expected an UPDATE finalize, got: {log_writes}"
        # The finalizing UPDATE carries the terminal status.
        assert any("status = @status" in q for q in update_writes)
        assert result["status"] == "success"

    def test_failure_mid_run_still_finalizes_the_audit_row(self):
        """THE ADAC-63 GUARANTEE: if the load OOMs/raises mid-run, the audit
        row is still written — a 'running' INSERT up front AND a 'failed'
        UPDATE in the except path."""
        result, queries = _run_full_transform(
            load_raises=MemoryError("simulated OOM during load")
        )
        log_writes = [q for q in queries if "ingestion_log" in q]
        # running INSERT happened
        assert any(q.strip().startswith("INSERT") and "'running'" in q
                   for q in log_writes), f"no up-front running row: {log_writes}"
        # failed UPDATE happened
        assert any("UPDATE" in q and "status = @status" in q
                   for q in log_writes), f"no terminal status row: {log_writes}"
        assert result["status"] == "failed"

    def test_failure_falls_back_to_insert_when_running_row_missing(self):
        """If the up-front 'running' INSERT itself failed, the terminal write
        falls back to an INSERT (not an UPDATE of a row that never existed), so
        a row still lands."""
        result, queries = _run_full_transform(
            load_raises=MemoryError("OOM"),
            running_insert_raises=True,
        )
        log_writes = [q for q in queries if "ingestion_log" in q]
        # No UPDATE should have been attempted (there's no running row to update);
        # the terminal write is a fresh INSERT carrying the failed status.
        assert not any("UPDATE" in q for q in log_writes), (
            f"should not UPDATE a non-existent running row: {log_writes}"
        )
        terminal_inserts = [q for q in log_writes if q.strip().startswith("INSERT")]
        # One terminal INSERT carrying status param (the fallback). The failed
        # up-front insert also counts as an INSERT attempt, so assert at least
        # one INSERT references the status parameter.
        assert any("@status" in q for q in terminal_inserts), (
            f"expected a status-bearing INSERT fallback, got: {terminal_inserts}"
        )
        assert result["status"] == "failed"

    # — helpers —

    def _success_queries(self):
        result, queries = _run_full_transform()
        return result, queries

    @staticmethod
    def _first_two(run_output):
        result, queries = run_output
        assert len(queries) >= 1
        return result, queries


# ─────────────────────────────────────────────────────────────────────
# ADAC-69 — unparseable budget rows are counted, not silently dropped
# ─────────────────────────────────────────────────────────────────────

from backend.services.media_plan_sync import _parse_media_plan_tab  # noqa: E402


def _mp_grid(budget_cell: str) -> list[list[str]]:
    """Minimal media-plan tab grid with a header row + one data row.

    The data row carries a goal + a budget cell whose content the caller
    controls, so we can exercise parseable vs. unparseable budgets.
    """
    header = [
        "Site/Network", "Goal", "Start Date", "End Date",
        "# Days", "ID", "Audience Name", "Budget",
    ]
    # pad preamble so len(all_data) >= 14 (parser guard)
    preamble = [[""] * len(header) for _ in range(13)]
    data_row = [
        "Meta", "Awareness", "Jun 1", "Jun 30",
        "30", "#01", "Lookalikes", budget_cell,
    ]
    return preamble + [header, data_row]


class TestUnparseableBudgetSurfaced:
    """The skipped-row count must reach the caller via the stats dict."""

    def test_unparseable_budget_increments_stats(self):
        """A non-empty-but-unparseable Budget cell ("TBD") increments
        rows_skipped_unparseable_budget and the row is KEPT with NULL budget."""
        stats: dict = {}
        lines = _parse_media_plan_tab(
            None, prefetched_data=_mp_grid("TBD"),
            ref_year=2026, prefetched_merges=[], stats=stats,
        )
        assert stats.get("rows_skipped_unparseable_budget") == 1
        # row survives (targeting still useful) but budget is NULL
        assert len(lines) == 1
        assert lines[0]["budget"] is None

    def test_parseable_budget_does_not_increment(self):
        """A clean "$5,000" budget parses and is NOT counted as skipped."""
        stats: dict = {}
        lines = _parse_media_plan_tab(
            None, prefetched_data=_mp_grid("$5,000"),
            ref_year=2026, prefetched_merges=[], stats=stats,
        )
        assert stats.get("rows_skipped_unparseable_budget", 0) == 0
        assert len(lines) == 1
        assert lines[0]["budget"] == 5000.0

    def test_blank_budget_is_not_a_skip(self):
        """An EMPTY Budget cell is a deliberate planner choice (e.g. bundle
        child), not a parse failure — it must not inflate the count."""
        stats: dict = {}
        lines = _parse_media_plan_tab(
            None, prefetched_data=_mp_grid(""),
            ref_year=2026, prefetched_merges=[], stats=stats,
        )
        assert stats.get("rows_skipped_unparseable_budget", 0) == 0
        assert len(lines) == 1
        assert lines[0]["budget"] is None

    def test_stats_optional_backward_compatible(self):
        """Omitting stats keeps the plain-list return + behaviour unchanged
        (guards the existing callers/tests that pass no stats)."""
        lines = _parse_media_plan_tab(
            None, prefetched_data=_mp_grid("garbage!!!"),
            ref_year=2026, prefetched_merges=[],
        )
        assert isinstance(lines, list)
        assert len(lines) == 1
        assert lines[0]["budget"] is None

    def test_counter_accumulates_across_calls(self):
        """The same stats dict passed to multiple tab parses accumulates —
        this is how sync_media_plan aggregates across all media plan tabs."""
        stats: dict = {}
        _parse_media_plan_tab(
            None, prefetched_data=_mp_grid("TBD"),
            ref_year=2026, prefetched_merges=[], stats=stats,
        )
        _parse_media_plan_tab(
            None, prefetched_data=_mp_grid("n/a"),
            ref_year=2026, prefetched_merges=[], stats=stats,
        )
        assert stats.get("rows_skipped_unparseable_budget") == 2
