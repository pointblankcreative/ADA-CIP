"""SQL-shape tests for the Meta lead-form conversions fix (2026-06-04).

26018 collected 992 on-platform lead-form submissions that CIP showed as 0
conversions: the transform gated Meta conversions on
Campaign_Result_value__Facebook_Ads, which Funnel populates for some
objectives (e.g. OUTCOME_AWARENESS reach) but left at 0/NULL for the
OUTCOME_LEADS campaign — while the real count sat in Leads__Facebook_Ads on
the same rows.

Pinned shape: lead objectives take GREATEST(result_value, Leads) — the
result IS leads for those objectives, so this dedupes by construction;
CONVERSIONS-objective campaigns keep result_value only (a conversion
campaign's incidental leads must never override its purchases); awareness
objectives stay hard-zero. Daily + full-history must stay mapping-identical.
Snapchat precedent: Leads__Snapchat already maps straight to conversions.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

SQL_DIR = Path(__file__).resolve().parent.parent / "ingestion" / "transformation"
DAILY_SQL = (SQL_DIR / "transform_funnel_to_unified.sql").read_text()
FULL_SQL = (SQL_DIR / "transform_funnel_to_unified_full_history.sql").read_text()

BOTH = [pytest.param(DAILY_SQL, id="daily"), pytest.param(FULL_SQL, id="full_history")]


def _meta_conversions_case(sql: str) -> str:
    """Extract the Meta conversions CASE expression."""
    m = re.search(r"COALESCE\(\s*CASE\s*\n(.*?)\)\s+AS\s+conversions,", sql, re.DOTALL)
    assert m, "Meta conversions CASE not found"
    return m.group(0)


@pytest.mark.parametrize("sql", BOTH)
def test_lead_objectives_fall_back_to_leads_column(sql):
    case = _meta_conversions_case(sql)
    assert "'OUTCOME_LEADS', 'LEAD_GENERATION'" in case
    assert "GREATEST(" in case
    assert "Leads__Facebook_Ads" in case
    assert "Campaign_Result_value__Facebook_Ads" in case


@pytest.mark.parametrize("sql", BOTH)
def test_conversions_objective_does_not_use_leads_fallback(sql):
    """A CONVERSIONS campaign's purchases must never be overridden by
    incidental leads — its branch reads result_value only."""
    case = _meta_conversions_case(sql)
    conv_branch = case.split("WHEN Campaign_Objective__Facebook_Ads = 'CONVERSIONS'")[1]
    conv_branch = conv_branch.split("ELSE")[0]
    assert "Campaign_Result_value__Facebook_Ads" in conv_branch
    assert "Leads__Facebook_Ads" not in conv_branch


@pytest.mark.parametrize("sql", BOTH)
def test_awareness_objectives_stay_zero(sql):
    """No lead/result counting outside the explicit objective allowlist."""
    case = _meta_conversions_case(sql)
    assert re.search(r"ELSE 0\s*\n\s*END", case), case


@pytest.mark.parametrize("sql", BOTH)
def test_null_safety_both_sources(sql):
    """Both GREATEST arms are COALESCEd — GREATEST(NULL, x) is NULL in
    BigQuery, which would silently re-zero campaigns missing one source."""
    case = _meta_conversions_case(sql)
    greatest = case.split("GREATEST(")[1].split("WHEN Campaign_Objective")[0]
    assert greatest.count("COALESCE(") == 2, greatest


def test_daily_and_full_history_mapping_identical():
    assert _meta_conversions_case(DAILY_SQL) == _meta_conversions_case(FULL_SQL)
