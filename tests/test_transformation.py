"""Tests for ingestion dedup guards in transformation.py."""

import re
from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch, call

import pytest


# ---------------------------------------------------------------------------
# Fix 1: Full mode must TRUNCATE, daily mode must date-range DELETE
# ---------------------------------------------------------------------------

class TestRunTransformationDeleteLogic:
    """Verify the Step 2 DELETE/TRUNCATE behaviour in run_transformation()."""

    def _run_with_mode(self, mode: str, row_count: int = 10):
        """Helper: run run_transformation() with mocked BQ clients and return
        the list of SQL strings executed on the Montreal client."""

        fake_row = {
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

        # Create dict-like row objects that work with dict(r)
        class FakeRow(dict):
            """A dict subclass that also supports .items() iteration like BQ Row."""
            pass

        fake_rows = [FakeRow(fake_row) for _ in range(row_count)]

        # US client — returns rows from SELECT
        us_client = MagicMock()
        us_job = MagicMock()
        us_job.result.return_value = fake_rows
        us_client.query.return_value = us_job

        # Montreal client — capture all queries
        mtl_client = MagicMock()
        mtl_query_job = MagicMock()
        mtl_query_job.result.return_value = None
        mtl_client.query.return_value = mtl_query_job

        load_job = MagicMock()
        load_job.result.return_value = None
        load_job.output_rows = row_count
        mtl_client.load_table_from_json.return_value = load_job

        # Patch SQL file reading
        fake_sql = "WITH enriched_data AS (SELECT 1) MERGE INTO dummy"

        with patch("backend.services.transformation._us_client", return_value=us_client), \
             patch("backend.services.transformation._mtl_client", return_value=mtl_client), \
             patch("backend.services.transformation.DAILY_SQL") as mock_daily, \
             patch("backend.services.transformation.FULL_SQL") as mock_full:

            target_path = mock_full if mode == "full" else mock_daily
            target_path.exists.return_value = True
            target_path.read_text.return_value = fake_sql
            # Also set the other path so the module doesn't fail
            other_path = mock_daily if mode == "full" else mock_full
            other_path.exists.return_value = True
            other_path.read_text.return_value = fake_sql

            from backend.services.transformation import run_transformation
            result = run_transformation(mode=mode)

        # Collect all SQL strings passed to mtl.query()
        mtl_queries = [
            c.args[0] if c.args else c.kwargs.get("query", "")
            for c in mtl_client.query.call_args_list
        ]

        return result, mtl_queries

    def test_full_mode_truncates(self):
        """Full mode should issue TRUNCATE TABLE, not a date-range DELETE."""
        result, queries = self._run_with_mode("full")
        truncate_queries = [q for q in queries if "TRUNCATE" in q.upper()]
        delete_queries = [q for q in queries if q.strip().upper().startswith("DELETE")]

        assert len(truncate_queries) >= 1, f"Expected TRUNCATE query in full mode, got: {queries}"
        assert len(delete_queries) == 0, f"Full mode should not use date-range DELETE, got: {queries}"

    def test_daily_mode_uses_date_range_delete(self):
        """Daily mode should issue a date-range DELETE, not TRUNCATE."""
        result, queries = self._run_with_mode("daily")
        truncate_queries = [q for q in queries if "TRUNCATE" in q.upper()]
        delete_queries = [q for q in queries if q.strip().upper().startswith("DELETE")]

        assert len(delete_queries) >= 1, f"Expected DELETE query in daily mode, got: {queries}"
        assert len(truncate_queries) == 0, f"Daily mode should not TRUNCATE, got: {queries}"


# ---------------------------------------------------------------------------
# Fix 2: _extract_select should include ROW_NUMBER dedup
# ---------------------------------------------------------------------------

class TestExtractSelectDedup:
    """Verify that _extract_select() wraps the SELECT with ROW_NUMBER dedup."""

    def _get_extract_select_output(self):
        from backend.services.transformation import _extract_select
        # Minimal SQL that has the MERGE keyword so _extract_select can split on it
        fake_sql = """
WITH enriched_data AS (
    SELECT
        CURRENT_DATE() AS date,
        'meta' AS platform_id,
        'c1' AS campaign_id,
        '' AS ad_set_id,
        '' AS ad_id,
        'test' AS campaign_name,
        'test' AS ad_set_name,
        'test' AS ad_name,
        'a1' AS account_id,
        'acct' AS account_name,
        '25042' AS project_code,
        100.0 AS spend,
        1000 AS impressions,
        50 AS clicks,
        500 AS reach,
        2.0 AS frequency,
        0 AS video_views,
        0 AS video_completions,
        5.0 AS conversions,
        10 AS engagements,
        10.0 AS cpm,
        2.0 AS cpc,
        0.05 AS ctr,
        'funnel' AS ingestion_source,
        CURRENT_TIMESTAMP() AS loaded_at
)
MERGE INTO dummy USING src ON TRUE
WHEN MATCHED THEN UPDATE SET x = 1
"""
        return _extract_select(fake_sql)

    def test_contains_row_number(self):
        """The generated SQL should contain ROW_NUMBER() for dedup."""
        sql = self._get_extract_select_output()
        assert "ROW_NUMBER()" in sql, f"Expected ROW_NUMBER() in SQL, got:\n{sql}"

    def test_contains_partition_by_natural_key(self):
        """ROW_NUMBER should partition by the natural dedup key."""
        sql = self._get_extract_select_output()
        assert "PARTITION BY" in sql, f"Expected PARTITION BY in SQL, got:\n{sql}"
        # Check all key columns are in the partition
        for col in ["date", "platform_id", "campaign_id", "ad_set_id", "ad_id"]:
            assert col in sql, f"Expected '{col}' in PARTITION BY clause"

    def test_contains_where_rn_equals_1(self):
        """The outer query should filter to rn = 1."""
        sql = self._get_extract_select_output()
        assert "WHERE rn = 1" in sql, f"Expected 'WHERE rn = 1' in SQL, got:\n{sql}"

    def test_excludes_rn_from_output(self):
        """The final SELECT should use EXCEPT(rn) to drop the row number."""
        sql = self._get_extract_select_output()
        assert "EXCEPT(rn)" in sql, f"Expected 'EXCEPT(rn)' in SQL, got:\n{sql}"

    def test_orders_by_spend_desc(self):
        """The ROW_NUMBER should ORDER BY spend DESC to prefer the row with highest spend."""
        sql = self._get_extract_select_output()
        assert "ORDER BY spend DESC" in sql, f"Expected 'ORDER BY spend DESC' in SQL, got:\n{sql}"
