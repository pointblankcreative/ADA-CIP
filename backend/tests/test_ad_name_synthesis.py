"""Google Ads creative-name synthesis — ADA 1215990183023573.

Google responsive search ads (RSAs) report a null ad_name all the way into
fact_digital_daily, so every Google row in the Creative "Long Tables" drawer
(served by the /api/performance/{code}/ads endpoint) used to render blank and
the ads couldn't be told apart. `_display_ad_name` fills ONLY those genuinely
blank Google names from the ad group we already carry — 'Responsive search
ad — <ad group>' — and is inert for every other platform and for real names.

Hermetic: BigQuery is mocked (no GCP creds). The endpoint test stubs
`bq.run_query` with canned rows and passes every optional Query param as an
explicit None (otherwise FastAPI's Query(None) sentinels leak into the date
maths and raise before the ad_name logic runs).
"""
import asyncio
from unittest.mock import patch

from backend.routers.performance import _display_ad_name, get_ad_performance


# ── the pure helper ──────────────────────────────────────────────────


class TestDisplayAdName:
    def test_google_rsa_null_name_labels_from_ad_group(self):
        assert (
            _display_ad_name(None, "google_ads", "Brand Terms")
            == "Responsive search ad — Brand Terms"
        )

    def test_google_rsa_empty_name_labels_from_ad_group(self):
        assert (
            _display_ad_name("", "google_ads", "Brand Terms")
            == "Responsive search ad — Brand Terms"
        )

    def test_google_rsa_whitespace_name_labels_from_ad_group(self):
        assert (
            _display_ad_name("   ", "google_ads", "Brand Terms")
            == "Responsive search ad — Brand Terms"
        )

    def test_google_rsa_null_name_and_null_ad_group(self):
        # No ad group to borrow — no trailing separator.
        assert _display_ad_name(None, "google_ads", None) == "Responsive search ad"

    def test_google_rsa_null_name_and_blank_ad_group(self):
        assert _display_ad_name(None, "google_ads", "   ") == "Responsive search ad"

    def test_google_real_name_kept_unchanged(self):
        # Never overwrites a genuine name, even on Google (display/YouTube ads).
        assert (
            _display_ad_name("25013 - Spring Hero", "google_ads", "Brand Terms")
            == "25013 - Spring Hero"
        )

    def test_meta_null_name_stays_none(self):
        # Regression guard: other platforms are inert, blank stays blank.
        assert _display_ad_name(None, "meta", "Lookalike 1%") is None

    def test_meta_real_name_unchanged(self):
        assert _display_ad_name("Spring Hero 30s", "meta", "Lookalike 1%") == (
            "Spring Hero 30s"
        )


# ── the /ads endpoint wires the helper into the response ──────────────


class TestAdEndpointSynthesis:
    @patch("backend.routers.performance.bq")
    def test_google_row_filled_meta_row_untouched(self, mock_bq):
        rows = [
            {
                "ad_id": "g1",
                "ad_name": None,
                "ad_set_name": "Brand Terms",
                "platform_id": "google_ads",
                "campaign_name": "25013 Search",
            },
            {
                "ad_id": "m1",
                "ad_name": "Spring Hero 30s",
                "ad_set_name": "Lookalike 1%",
                "platform_id": "meta",
                "campaign_name": "25013 Awareness",
            },
        ]
        mock_bq.run_query.return_value = rows
        mock_bq.table.side_effect = lambda name: f"`proj.ds.{name}`"
        mock_bq.string_param.side_effect = lambda n, v: (n, v)
        mock_bq.date_param.side_effect = lambda n, v: (n, v)

        # Pass EVERY optional Query param as an explicit None — otherwise the
        # Query(None) sentinels bind and break the date maths before we reach
        # the ad_name logic (code-reviewer gotcha).
        resp = asyncio.run(
            get_ad_performance(
                "25013",
                start_date=None,
                end_date=None,
                days=None,
                platform=None,
            )
        )

        by_id = {a.ad_id: a for a in resp.ads}
        assert by_id["g1"].ad_name == "Responsive search ad — Brand Terms"
        assert by_id["m1"].ad_name == "Spring Hero 30s"
