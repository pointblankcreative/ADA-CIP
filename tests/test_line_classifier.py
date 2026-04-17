"""Unit tests for per-line campaign-type classification.

The mixed-campaign engine refactor (Build Plan §12) depends on correctly
classifying each media plan line as persuasion or conversion. These tests
cover the classifier's mapping from `objective_classifier.classify_objective()`
output onto `CampaignType`, plus convenience predicates and partition_lines().
"""

from __future__ import annotations

from datetime import date

from backend.services.diagnostics.line_classifier import (
    classify_campaign_name,
    classify_line,
    classify_objective_string,
    is_conversion,
    is_persuasion,
    partition_lines,
)
from backend.services.diagnostics.models import CampaignType, MediaPlanLine


# ── helpers ────────────────────────────────────────────────────────


def _line(objective: str | None, line_id: str = "line-x") -> MediaPlanLine:
    return MediaPlanLine(
        line_id=line_id,
        platform_id="facebook",
        planned_budget=1000.0,
        planned_impressions=100_000,
        flight_start=date(2026, 4, 1),
        flight_end=date(2026, 4, 30),
        objective=objective,
    )


# ── classify_objective_string ─────────────────────────────────────


class TestClassifyObjectiveString:
    def test_conversion_string_maps_to_conversion(self):
        assert classify_objective_string("Conversion") == CampaignType.CONVERSION

    def test_lead_gen_is_conversion(self):
        assert classify_objective_string("Lead Gen - Website Forms") == CampaignType.CONVERSION

    def test_retargeting_is_conversion(self):
        assert classify_objective_string("Retargeting from Meta & Stackadapt") == CampaignType.CONVERSION

    def test_remarketing_is_conversion(self):
        assert classify_objective_string("Remarketing") == CampaignType.CONVERSION

    def test_awareness_is_persuasion(self):
        assert classify_objective_string("Awareness (Video Views)") == CampaignType.PERSUASION

    def test_engagement_is_persuasion(self):
        assert classify_objective_string("Engagement (Comments & Likes)") == CampaignType.PERSUASION

    def test_reach_frequency_is_persuasion(self):
        assert classify_objective_string("Reach & Frequency (CTV)") == CampaignType.PERSUASION

    def test_empty_string_defaults_to_persuasion(self):
        """Conservative default: anything ambiguous → PERSUASION."""
        assert classify_objective_string("") == CampaignType.PERSUASION

    def test_none_defaults_to_persuasion(self):
        assert classify_objective_string(None) == CampaignType.PERSUASION

    def test_unknown_keyword_defaults_to_persuasion(self):
        """A line with no recognised keywords falls through to persuasion."""
        assert classify_objective_string("Xyzzy") == CampaignType.PERSUASION

    def test_campaign_name_fallback(self):
        """objective missing → campaign_name is consulted."""
        assert classify_objective_string(None, "25042 Retargeting") == CampaignType.CONVERSION
        assert classify_objective_string(None, "25042 Awareness F2") == CampaignType.PERSUASION

    def test_objective_takes_priority_over_campaign_name(self):
        """If objective classifies cleanly, campaign_name is not consulted."""
        assert (
            classify_objective_string("Conversion", "Awareness Brand Lift")
            == CampaignType.CONVERSION
        )


# ── classify_line / is_persuasion / is_conversion ────────────────


class TestClassifyLine:
    def test_persuasion_line(self):
        line = _line("Awareness (Video Views)")
        assert classify_line(line) == CampaignType.PERSUASION
        assert is_persuasion(line)
        assert not is_conversion(line)

    def test_conversion_line(self):
        line = _line("Retargeting")
        assert classify_line(line) == CampaignType.CONVERSION
        assert is_conversion(line)
        assert not is_persuasion(line)

    def test_line_with_no_objective_defaults_to_persuasion(self):
        """Null objective + no fallback → persuasion (conservative)."""
        line = _line(None)
        assert classify_line(line) == CampaignType.PERSUASION

    def test_line_with_whitespace_objective(self):
        """Whitespace-only objective is treated as null."""
        line = _line("   ")
        assert classify_line(line) == CampaignType.PERSUASION


# ── classify_campaign_name (used for fact_adset_daily rows) ──────


class TestClassifyCampaignName:
    def test_retargeting_campaign_name(self):
        assert (
            classify_campaign_name("25042 Retargeting Conversions Facebook")
            == CampaignType.CONVERSION
        )

    def test_awareness_campaign_name(self):
        assert (
            classify_campaign_name("25042 Flight 2 Member Mobilization Awareness")
            == CampaignType.PERSUASION
        )

    def test_none_campaign_name_is_persuasion(self):
        assert classify_campaign_name(None) == CampaignType.PERSUASION


# ── partition_lines — the key helper the engine relies on ─────────


class TestPartitionLines:
    def test_empty_input_returns_empty_buckets(self):
        out = partition_lines([])
        assert out[CampaignType.PERSUASION] == []
        assert out[CampaignType.CONVERSION] == []

    def test_pure_persuasion(self):
        lines = [
            _line("Awareness (Video Views)", "l1"),
            _line("Engagement (Comments & Likes)", "l2"),
            _line("Reach & Frequency (CTV)", "l3"),
        ]
        out = partition_lines(lines)
        assert len(out[CampaignType.PERSUASION]) == 3
        assert out[CampaignType.CONVERSION] == []

    def test_pure_conversion(self):
        lines = [
            _line("Conversion", "l1"),
            _line("Lead Gen - Website Forms", "l2"),
        ]
        out = partition_lines(lines)
        assert out[CampaignType.PERSUASION] == []
        assert len(out[CampaignType.CONVERSION]) == 2

    def test_osstf_25042_shape(self):
        """OSSTF 25042 — the canonical mixed shape: 3 persuasion + 1 conversion."""
        lines = [
            _line("Engagement (Comments & Likes)", "eng-f1"),
            _line("Awareness (Video Views)", "aware-f2"),
            _line("Retargeting", "retarget"),
            _line("Reach & Frequency (CTV)", "ctv"),
        ]
        out = partition_lines(lines)
        persuasion_ids = {l.line_id for l in out[CampaignType.PERSUASION]}
        conversion_ids = {l.line_id for l in out[CampaignType.CONVERSION]}
        assert persuasion_ids == {"eng-f1", "aware-f2", "ctv"}
        assert conversion_ids == {"retarget"}

    def test_both_buckets_always_present(self):
        """Partitioning a pure-persuasion list still returns both keys
        (conversion bucket empty). The engine relies on this to iterate
        the dict without KeyError handling."""
        lines = [_line("Awareness (Video Views)")]
        out = partition_lines(lines)
        assert CampaignType.PERSUASION in out
        assert CampaignType.CONVERSION in out
