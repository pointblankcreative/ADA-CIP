"""Tests for objective classification logic."""

from backend.services.objective_classifier import classify_objective, classify_project


class TestClassifyObjective:
    """classify_objective should pick the right category from text signals."""

    def test_awareness_from_media_plan(self):
        assert classify_objective("Awareness (Video Views)") == "awareness"

    def test_awareness_from_engagement_objective(self):
        assert classify_objective("Engagement (Comments & Likes)") == "awareness"

    def test_conversion_from_media_plan(self):
        assert classify_objective("Conversion") == "conversion"

    def test_conversion_from_leads(self):
        assert classify_objective("Lead Gen - Website Forms") == "conversion"

    def test_awareness_from_campaign_name(self):
        assert classify_objective(None, "25042 Flight 2 Member Mobilization Awareness") == "awareness"

    def test_conversion_from_campaign_name(self):
        assert classify_objective(None, "25042 Flight 2 Member Mobilization Conversion") == "conversion"

    def test_engagement_campaign_name_is_awareness(self):
        """Engagement campaigns are awareness, not conversion."""
        assert classify_objective(None, "V2 25042 Flight 1 Member Mobilization Engagement - Copy") == "awareness"

    def test_retargeting_is_conversion(self):
        assert classify_objective("Retargeting from Meta & Stackadapt") == "conversion"

    def test_reach_frequency_is_awareness(self):
        assert classify_objective("Reach & Frequency  (CTV)") == "awareness"

    def test_no_signals_returns_mixed(self):
        assert classify_objective(None, None) == "mixed"

    def test_objective_takes_priority_over_name(self):
        """Media plan objective is checked first."""
        assert classify_objective("Awareness (Video Views)", "Some Conversion Campaign") == "awareness"


class TestClassifyProject:
    """classify_project aggregates campaign-level objectives."""

    def test_all_awareness(self):
        assert classify_project(["awareness", "awareness"]) == "awareness"

    def test_all_conversion(self):
        assert classify_project(["conversion", "conversion"]) == "conversion"

    def test_mixed_objectives(self):
        assert classify_project(["awareness", "conversion"]) == "mixed"

    def test_mixed_with_mixed_entries(self):
        """'mixed' entries are ignored when others agree."""
        assert classify_project(["awareness", "mixed", "awareness"]) == "awareness"

    def test_empty_returns_mixed(self):
        assert classify_project([]) == "mixed"

    def test_osstf_current_state(self):
        """OSSTF with only engagement flights running should be awareness."""
        # Both campaigns have "Engagement" in the name → awareness
        objectives = ["awareness", "awareness"]
        assert classify_project(objectives) == "awareness"

    def test_osstf_future_state(self):
        """OSSTF with awareness + conversion flights should be mixed."""
        objectives = ["awareness", "awareness", "conversion"]
        assert classify_project(objectives) == "mixed"
