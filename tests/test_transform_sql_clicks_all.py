"""SQL-shape tests for the AI-102 clicks_all column + engagements remap.

The transform SQL itself executes in BigQuery, so these tests pin its shape
the same way test_performance_rf_stopgap.py pins the router SQL: by asserting
the load-bearing text of both transform files (daily + full-history must stay
mapping-identical) and of transformation.py's cross-region projection.

What is pinned, per the AI-102 labeled-coexistence design:

  1. Canonical `clicks` UNCHANGED on every platform —
     Meta: Link_Clicks; TikTok: Clicks_Destination; Snapchat: Swipes;
     Pinterest: Paid_Outbound_Clicks. (F1's Meta benchmark is calibrated to
     link-click CTR; switching definitions would flag healthy campaigns.)
  2. NEW `clicks_all` — Meta: Clicks_all; TikTok: Clicks_All; explicit
     NULL on the other six platforms (UNION ALL column-count safety).
  3. Meta `engagements` remapped Clicks_all → Post_Engagement (the all-click
     count was inflating displayed Eng. rate ~2x); TikTok `engagements` →
     NULL until its real engagement columns are mapped. Google / LinkedIn /
     Pinterest engagements untouched (genuine engagement-family metrics).
  4. clicks_all plumbed through enriched_data and all four MERGE sections
     (USING SELECT, UPDATE SET, INSERT columns, VALUES), and through
     transformation.py::_extract_select for the cross-region load path.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

SQL_DIR = Path(__file__).resolve().parent.parent / "ingestion" / "transformation"
DAILY_SQL = (SQL_DIR / "transform_funnel_to_unified.sql").read_text()
FULL_SQL = (SQL_DIR / "transform_funnel_to_unified_full_history.sql").read_text()

BOTH = [pytest.param(DAILY_SQL, id="daily"), pytest.param(FULL_SQL, id="full_history")]


# ── 1. Canonical clicks unchanged ────────────────────────────────────


@pytest.mark.parametrize("sql", BOTH)
def test_canonical_clicks_mappings_unchanged(sql):
    assert "CAST(Link_Clicks__Facebook_Ads AS INT64) AS clicks" in sql
    assert "CAST(Clicks_Destination__TikTok AS INT64) AS clicks" in sql
    assert "CAST(Swipes__Snapchat AS INT64) AS clicks" in sql
    assert "CAST(Paid_Outbound_Clicks__Pinterest AS INT64) AS clicks" in sql
    assert "CAST(Clicks__StackAdapt AS INT64) AS clicks" in sql
    assert "CAST(Clicks__LinkedIn AS INT64) AS clicks" in sql
    assert "CAST(Clicks__Reddit AS INT64) AS clicks" in sql
    # The regression AI-102 explicitly rejects: Meta clicks must never be
    # the all-clicks column.
    assert "CAST(Clicks_all__Facebook_Ads AS INT64) AS clicks\n" not in sql
    assert "CAST(Clicks_all__Facebook_Ads AS INT64) AS clicks," not in sql


# ── 2. clicks_all per platform ───────────────────────────────────────


@pytest.mark.parametrize("sql", BOTH)
def test_clicks_all_mapped_for_meta_and_tiktok(sql):
    assert "CAST(Clicks_all__Facebook_Ads AS INT64) AS clicks_all" in sql
    assert "CAST(Clicks_All__TikTok AS INT64) AS clicks_all" in sql


@pytest.mark.parametrize("sql", BOTH)
def test_clicks_all_null_on_remaining_platforms(sql):
    # 6 platforms without the concept: explicit NULL keeps the UNION ALL
    # column counts aligned (google_ads, stackadapt, snapchat, linkedin,
    # reddit, pinterest).
    nulls = len(re.findall(r"CAST\(NULL AS INT64\) AS clicks_all", sql))
    assert nulls == 6, f"expected 6 NULL clicks_all blocks, found {nulls}"


@pytest.mark.parametrize("sql", BOTH)
def test_each_platform_block_emits_clicks_all(sql):
    # Every one of the 8 UNION ALL platform blocks must project clicks_all:
    # 8 block projections + enriched (pd.clicks_all) + USING SELECT +
    # UPDATE SET + INSERT list + VALUES (source.clicks_all).
    assert len(re.findall(r"\bAS clicks_all\b", sql)) == 8


# ── 3. engagements remap ─────────────────────────────────────────────


@pytest.mark.parametrize("sql", BOTH)
def test_meta_engagements_remapped_to_post_engagement(sql):
    assert "CAST(Post_Engagement__Facebook_Ads AS INT64) AS engagements" in sql
    assert "CAST(Clicks_all__Facebook_Ads AS INT64) AS engagements" not in sql
    # post_engagement (diagnostic column, used by R1) keeps its own mapping.
    assert "CAST(Post_Engagement__Facebook_Ads AS INT64) AS post_engagement" in sql


@pytest.mark.parametrize("sql", BOTH)
def test_tiktok_engagements_nulled(sql):
    assert "CAST(Clicks_All__TikTok AS INT64) AS engagements" not in sql
    # The TikTok block carries the explicit NULL with the AI-102 comment.
    tiktok_block = sql.split("'tiktok' AS platform_id")[1].split("UNION ALL")[0]
    assert "CAST(NULL AS INT64) AS engagements" in tiktok_block


@pytest.mark.parametrize("sql", BOTH)
def test_genuine_engagement_mappings_untouched(sql):
    # Google / LinkedIn / Pinterest engagements are real engagement-family
    # metrics — AI-102 must not touch them.
    assert "CAST(Action_Clicks__LinkedIn AS INT64) AS engagements" in sql
    assert "CAST(Paid_engagements__Pinterest AS INT64) AS engagements" in sql
    assert re.search(r"Engagements__Google_Ads AS INT64\) AS engagements", sql)


# ── 4. Plumbing: enriched_data + MERGE + cross-region projection ─────


@pytest.mark.parametrize("sql", BOTH)
def test_clicks_all_plumbed_through_enriched_and_merge(sql):
    assert "pd.clicks_all," in sql                        # enriched_data
    assert "clicks_all = source.clicks_all," in sql       # UPDATE SET
    assert "source.clicks_all," in sql                    # VALUES
    # USING SELECT + INSERT column list: clicks_all directly after clicks.
    assert len(re.findall(r"\n    clicks,\n    clicks_all,\n", sql)) == 2


def test_daily_and_full_history_mappings_identical():
    """The two files may only differ in the date filter and header comments —
    the metric mappings must stay in lockstep (same guarantee the AI-102
    investigation relied on)."""
    def normalize(sql: str) -> list[str]:
        lines = []
        for line in sql.splitlines():
            s = line.strip()
            if not s.startswith("--") and ("AS clicks" in s or "AS engagements" in s):
                lines.append(s)
        return lines

    assert normalize(DAILY_SQL) == normalize(FULL_SQL)


def test_extract_select_projects_clicks_all():
    """transformation.py's cross-region path re-projects enriched_data; it
    must carry clicks_all or the WRITE_APPEND load drops it silently."""
    from backend.services.transformation import _extract_select

    fake_sql = "WITH enriched_data AS (SELECT 1) MERGE INTO dummy"
    projected = _extract_select(fake_sql)
    # clicks_all rides directly after clicks, before reach.
    assert re.search(r"clicks,\s*\n\s*clicks_all,\s*\n\s*reach,", projected), projected
