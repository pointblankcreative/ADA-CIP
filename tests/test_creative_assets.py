"""Tests for the Phase 19 creative assets + audience targeting sync
(backend/services/creative_assets.py) and the additive endpoint fields it
feeds (image_url on rotation creatives; persona / pool_size / saturation
on matrix audiences).

Coverage:

  * Persona renderer — deterministic fragments in a fixed order (full
    spec, custom-audience-only, geo+age-only, country fallback), the
    lookalike naming convention, the interests cap, and None for an
    empty spec.
  * Variant matching — the sync reuses the creative router's alias
    resolution helper (identity-checked, plus the SQL shape it emits)
    and the matrix's audience slug function.
  * Image sync — Meta-first with image_url preferred over thumbnail,
    StackAdapt fallback, bytes stored at creative-assets/{sha1}.{ext},
    every attempt recorded in the creative_assets ledger (stored /
    no_match / fetch_failed), the once-per-day retry guard, and the
    empty-token no-op.
  * Targeting sync — rows keyed by the matrix endpoint's own slug,
    delivery_estimate parsing (estimate_dau, bounds midpoint).
  * Endpoints (additive) — rotation rows gain a signed image_url when a
    stored asset row exists; matrix audiences gain persona / pool_size /
    saturation (reach over pool), all None when either side is missing.

All network, GCS, and BigQuery access is mocked: bq is patched with the
QueryRecorder stub (test_creative_router.py pattern), platform listings
are patched at the fetch-helper boundary, and GCS upload/signing are
patched functions — nothing leaves the process.
"""

from __future__ import annotations

import hashlib
from datetime import date, datetime, timedelta, timezone
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import creative as creative_router
from backend.services import creative_assets as ca


# ── Helpers (mirroring test_creative_router.py) ───────────────────────


def _make_app() -> FastAPI:
    app = FastAPI()
    app.include_router(creative_router.router)
    return app


class QueryRecorder:
    """Stub for bq.run_query that records every call and returns canned rows."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, list]] = []
        self.responses: list[list[dict]] = []

    def __call__(self, sql: str, params=None):
        self.calls.append((sql, list(params or [])))
        if self.responses:
            return self.responses.pop(0)
        return []


def _string_param(name, value):
    return ("string", name, value)


def _date_param(name, value):
    return ("date", name, value)


def _array_param(name, type_, values):
    return ("array", name, type_, list(values))


def _scalar_param(name, type_, value):
    return ("scalar", name, type_, value)


def _table(name):
    return f"`dummy.{name}`"


def _bq_patches(module, rec):
    # ca.bq and creative_router.bq are the same module object, so patching
    # through either handle covers both (the alias probe inside
    # _alias_resolution runs through the router's binding).
    return [
        patch.object(module.bq, "run_query", side_effect=rec),
        patch.object(module.bq, "string_param", _string_param),
        patch.object(module.bq, "date_param", _date_param),
        patch.object(module.bq, "array_param", _array_param),
        patch.object(module.bq, "scalar_param", _scalar_param),
        patch.object(module.bq, "table", _table),
    ]


class _Patched:
    """Context manager applying the bq patch stack plus any extras."""

    def __init__(self, module, rec, *extras):
        self.patches = _bq_patches(module, rec) + list(extras)

    def __enter__(self):
        self.started = [p.start() for p in self.patches]
        return self.started

    def __exit__(self, *exc):
        for p in self.patches:
            p.stop()
        return False


# ── Persona renderer ──────────────────────────────────────────────────


class TestRenderPersona:
    def test_full_spec_renders_fragments_in_fixed_order(self):
        targeting = {
            "custom_audiences": [
                {"id": "1", "name": "Member List 2026"},
                {"id": "2", "name": "Lookalike (CA, 1%) - Member List 2026"},
            ],
            "geo_locations": {
                "regions": [{"name": "Ontario"}, {"name": "British Columbia"}],
            },
            "age_min": 35,
            "age_max": 64,
            "interests": [{"name": "Politics"}, {"name": "Labour unions"}],
            "flexible_spec": [
                {"interests": [
                    {"name": "News"}, {"name": "CBC News"}, {"name": "Economy"},
                ]},
            ],
            "publisher_platforms": ["facebook", "instagram"],
        }
        assert ca.render_persona(targeting) == (
            "Member List 2026 + 1% lookalike of Member List 2026"
            " · ON + BC · 35-64 · all genders"
            " · Politics, Labour unions, News, CBC News +1 more"
            " · Facebook + Instagram"
        )

    def test_custom_audience_only_is_just_the_audience_names(self):
        targeting = {
            "custom_audiences": [{"name": "Member List"}, {"name": "Donors 2025"}],
        }
        # No demographic targeting in the spec → no gender fragment.
        assert ca.render_persona(targeting) == "Member List + Donors 2025"

    def test_geo_and_age_only(self):
        targeting = {
            "geo_locations": {
                "regions": [{"name": "Ontario"}, {"name": "British Columbia"}],
            },
            "age_min": 35,
            "age_max": 64,
        }
        assert ca.render_persona(targeting) == "ON + BC · 35-64 · all genders"

    def test_country_fallback_open_age_and_explicit_gender(self):
        targeting = {
            "geo_locations": {"countries": ["CA"]},
            "age_min": 18,
            "genders": [2],
        }
        assert ca.render_persona(targeting) == "Canada · 18+ · women"

    def test_empty_spec_is_none(self):
        assert ca.render_persona(None) is None
        assert ca.render_persona({}) is None
        # Present-but-empty keys are still an empty spec.
        assert ca.render_persona({"custom_audiences": [], "geo_locations": {}}) is None


# ── Variant matching reuses the router's helpers ──────────────────────


class TestVariantMatching:
    def test_helpers_are_the_routers_not_copies(self):
        assert ca._alias_resolution is creative_router._alias_resolution
        assert ca._audience_id is creative_router._audience_id

    def test_variant_map_runs_the_router_normalization_sql(self):
        rec = QueryRecorder()
        rec.responses = [
            [],  # alias-table probe (no exception → alias join included)
            [{"ad_name": "26018 - Hero Video - 1080x1080",
              "creative_variant": "Hero Video"}],
        ]
        with _Patched(ca, rec):
            mapping = ca._variant_map("26018")

        assert mapping == {"26018 - Hero Video - 1080x1080": "Hero Video"}
        # Probe hit the alias table exactly like the router does.
        assert "creative_variant_aliases" in rec.calls[0][0]
        # The map query carries the router's alias join + regex
        # normalization (strip leading 5-digit code, trailing WxH).
        sql, params = rec.calls[1]
        assert "creative_variant_aliases" in sql
        assert r"\d{5}" in sql
        assert r"\d+x\d+" in sql
        assert ("string", "project_code", "26018") in params


# ── Image sync ────────────────────────────────────────────────────────


HERO_AD_NAME = "26018 - Hero Video - 1080x1080"
HERO_VARIANT = "Hero Video"
HERO_SHA1 = hashlib.sha1(HERO_VARIANT.encode()).hexdigest()


def _image_sync_responses(rec, states):
    """Canned responses in sync_creative_images' BQ call order:
    (1) active projects, (2) alias probe, (3) variant map, (4) ledger
    state. MERGE writes afterwards return [] from the recorder."""
    rec.responses = [
        [{"project_code": "26018"}],
        [],
        [{"ad_name": HERO_AD_NAME, "creative_variant": HERO_VARIANT}],
        states,
    ]


class TestImageSync:
    def test_meta_image_stored_full_size_preferred(self, monkeypatch):
        monkeypatch.setattr(ca.settings, "meta_access_token", "tok")
        monkeypatch.setattr(ca.settings, "stackadapt_api_key", "")
        rec = QueryRecorder()
        _image_sync_responses(rec, [])
        stored: dict[str, tuple[bytes, str]] = {}
        ads = [{
            "name": HERO_AD_NAME,
            "creative": {
                "image_url": "https://cdn.meta/full.jpg",
                "thumbnail_url": "https://cdn.meta/thumb.jpg",
            },
        }]
        with _Patched(
            ca, rec,
            patch.object(ca, "_meta_ad_accounts", return_value=["act_1"]),
            patch.object(ca, "_meta_ads", return_value=ads),
            patch.object(ca, "_download_image",
                         return_value=(b"IMG", "jpg", "image/jpeg")),
            patch.object(ca, "_store_bytes",
                         side_effect=lambda data, name, ct: stored.update({name: (data, ct)})),
        ) as started:
            result = ca.sync_creative_images()
            download = started[8]  # the _download_image mock (6 bq + 2 fetch patches first)

        assert result["status"] == "success"
        assert result["pending"] == 1
        assert result["stored"] == 1
        assert result["no_match"] == 0

        # Full-size image_url preferred over thumbnail_url.
        assert download.call_args[0][1] == "https://cdn.meta/full.jpg"
        # Bytes land at creative-assets/{sha1(variant)}.{ext}.
        assert list(stored) == [f"creative-assets/{HERO_SHA1}.jpg"]
        assert stored[f"creative-assets/{HERO_SHA1}.jpg"] == (b"IMG", "image/jpeg")

        # The ledger MERGE recorded the stored row.
        merge_sql, merge_params = rec.calls[-1]
        assert "MERGE" in merge_sql and "creative_assets" in merge_sql
        assert ("string", "variant", HERO_VARIANT) in merge_params
        assert ("string", "status", "stored") in merge_params
        assert ("scalar", "gcs_path", "STRING",
                f"creative-assets/{HERO_SHA1}.jpg") in merge_params
        assert ("scalar", "source_platform", "STRING", "meta") in merge_params
        assert ("scalar", "project_code", "STRING", "26018") in merge_params

    def test_stackadapt_fallback_when_meta_misses(self, monkeypatch):
        monkeypatch.setattr(ca.settings, "meta_access_token", "tok")
        monkeypatch.setattr(ca.settings, "stackadapt_api_key", "key")
        rec = QueryRecorder()
        _image_sync_responses(rec, [])
        with _Patched(
            ca, rec,
            patch.object(ca, "_meta_ad_accounts", return_value=[]),
            patch.object(ca, "_stackadapt_creatives",
                         return_value=[{"name": HERO_AD_NAME, "url": "https://sa/img.png"}]),
            patch.object(ca, "_download_image",
                         return_value=(b"PNG", "png", "image/png")),
            patch.object(ca, "_store_bytes", return_value=None),
        ):
            result = ca.sync_creative_images()

        assert result["stored"] == 1
        merge_sql, merge_params = rec.calls[-1]
        assert ("scalar", "source_platform", "STRING", "stackadapt") in merge_params
        assert ("scalar", "gcs_path", "STRING",
                f"creative-assets/{HERO_SHA1}.png") in merge_params

    def test_unmatched_variant_recorded_as_no_match(self, monkeypatch):
        monkeypatch.setattr(ca.settings, "meta_access_token", "tok")
        monkeypatch.setattr(ca.settings, "stackadapt_api_key", "")
        rec = QueryRecorder()
        _image_sync_responses(rec, [])
        with _Patched(
            ca, rec,
            patch.object(ca, "_meta_ad_accounts", return_value=["act_1"]),
            patch.object(ca, "_meta_ads",
                         return_value=[{"name": "Somebody else's ad", "creative": {}}]),
        ):
            result = ca.sync_creative_images()

        assert result["no_match"] == 1 and result["stored"] == 0
        merge_sql, merge_params = rec.calls[-1]
        assert "creative_assets" in merge_sql
        assert ("string", "status", "no_match") in merge_params
        assert ("scalar", "gcs_path", "STRING", None) in merge_params
        assert ("scalar", "source_platform", "STRING", None) in merge_params

    def test_failed_download_recorded_as_fetch_failed(self, monkeypatch):
        monkeypatch.setattr(ca.settings, "meta_access_token", "tok")
        monkeypatch.setattr(ca.settings, "stackadapt_api_key", "")
        rec = QueryRecorder()
        _image_sync_responses(rec, [])
        ads = [{"name": HERO_AD_NAME,
                "creative": {"thumbnail_url": "https://cdn.meta/thumb.jpg"}}]
        with _Patched(
            ca, rec,
            patch.object(ca, "_meta_ad_accounts", return_value=["act_1"]),
            patch.object(ca, "_meta_ads", return_value=ads),
            patch.object(ca, "_download_image", return_value=None),
        ):
            result = ca.sync_creative_images()

        assert result["fetch_failed"] == 1 and result["stored"] == 0
        merge_sql, merge_params = rec.calls[-1]
        assert ("string", "status", "fetch_failed") in merge_params
        assert ("scalar", "source_platform", "STRING", "meta") in merge_params

    def test_empty_tokens_no_op_without_any_queries(self, monkeypatch):
        monkeypatch.setattr(ca.settings, "meta_access_token", "")
        monkeypatch.setattr(ca.settings, "stackadapt_api_key", "")
        rec = QueryRecorder()
        with _Patched(ca, rec):
            images = ca.sync_creative_images()
            targeting = ca.sync_adset_targeting()
        assert images["status"] == "skipped"
        assert targeting["status"] == "skipped"
        assert rec.calls == []  # not a single BQ round trip

    def test_retry_guard_skips_attempts_already_made_today(self, monkeypatch):
        monkeypatch.setattr(ca.settings, "meta_access_token", "tok")
        monkeypatch.setattr(ca.settings, "stackadapt_api_key", "")
        rec = QueryRecorder()
        _image_sync_responses(rec, [{
            "variant": HERO_VARIANT, "status": "no_match", "gcs_path": None,
            "checked_at": datetime.now(timezone.utc),
        }])
        with _Patched(ca, rec):
            result = ca.sync_creative_images()
        assert result["pending"] == 0
        assert result["no_match"] == 0
        assert len(rec.calls) == 4  # state reads only, no MERGE writes

    def test_retry_guard_retries_misses_from_a_previous_day(self, monkeypatch):
        monkeypatch.setattr(ca.settings, "meta_access_token", "tok")
        monkeypatch.setattr(ca.settings, "stackadapt_api_key", "")
        rec = QueryRecorder()
        _image_sync_responses(rec, [{
            "variant": HERO_VARIANT, "status": "no_match", "gcs_path": None,
            "checked_at": datetime.now(timezone.utc) - timedelta(days=1),
        }])
        with _Patched(
            ca, rec,
            patch.object(ca, "_meta_ad_accounts", return_value=[]),
        ):
            result = ca.sync_creative_images()
        assert result["pending"] == 1
        assert result["no_match"] == 1  # attempted again, still no match

    def test_stored_variants_never_retried(self, monkeypatch):
        monkeypatch.setattr(ca.settings, "meta_access_token", "tok")
        monkeypatch.setattr(ca.settings, "stackadapt_api_key", "")
        rec = QueryRecorder()
        _image_sync_responses(rec, [{
            "variant": HERO_VARIANT, "status": "stored",
            "gcs_path": f"creative-assets/{HERO_SHA1}.jpg",
            "checked_at": datetime.now(timezone.utc) - timedelta(days=30),
        }])
        with _Patched(ca, rec):
            result = ca.sync_creative_images()
        assert result["pending"] == 0


# ── delivery_estimate parsing ─────────────────────────────────────────


class _FakeResp:
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload

    def raise_for_status(self):
        return None


class _FakeHttp:
    def __init__(self, payload):
        self.payload = payload
        self.requests: list[tuple[str, dict | None]] = []

    def get(self, url, params=None):
        self.requests.append((url, params))
        return _FakeResp(self.payload)


class TestPoolSize:
    def test_estimate_dau_preferred(self):
        http = _FakeHttp({"data": [{"estimate_dau": 1200,
                                    "users_lower_bound": 1, "users_upper_bound": 9}]})
        assert ca._adset_pool_size(http, "as1") == 1200

    def test_bounds_midpoint_fallback(self):
        http = _FakeHttp({"data": [{"users_lower_bound": 1000,
                                    "users_upper_bound": 3000}]})
        assert ca._adset_pool_size(http, "as1") == 2000

    def test_none_when_meta_returns_nothing(self):
        assert ca._adset_pool_size(_FakeHttp({"data": []}), "as1") is None


# ── Targeting sync ────────────────────────────────────────────────────


class TestTargetingSync:
    def test_rows_keyed_by_the_matrix_slug(self, monkeypatch):
        monkeypatch.setattr(ca.settings, "meta_access_token", "tok")
        rec = QueryRecorder()
        rec.responses = [[{"ad_set_name": "Members EN"}]]  # known fact-side ad sets
        targeting = {
            "custom_audiences": [{"name": "Member List"}],
            "age_min": 35, "age_max": 64,
        }
        # API name differs in case/whitespace — the fact-side name wins.
        adsets = [{"id": "as1", "name": "  members  en ", "targeting": targeting}]
        with _Patched(
            ca, rec,
            patch.object(ca, "_meta_ad_accounts", return_value=["act_1"]),
            patch.object(ca, "_meta_adsets", return_value=adsets),
            patch.object(ca, "_adset_pool_size", return_value=50_000),
        ):
            result = ca.sync_adset_targeting()

        assert result == {
            "status": "success", "complete": True, "matched": 1, "written": 1,
        }
        merge_sql, merge_params = rec.calls[-1]
        assert "adset_targeting" in merge_sql and "MERGE" in merge_sql
        # THE SAME slug the audiences/matrix endpoint computes.
        expected_key = creative_router._audience_id("Members EN", "meta")
        assert ("string", "audience_key", expected_key) in merge_params
        assert ("string", "platform_id", "meta") in merge_params
        assert ("scalar", "persona", "STRING",
                "Member List · 35-64 · all genders") in merge_params
        assert ("scalar", "pool_size", "INT64", 50_000) in merge_params

    def test_unknown_adsets_are_ignored(self, monkeypatch):
        monkeypatch.setattr(ca.settings, "meta_access_token", "tok")
        rec = QueryRecorder()
        rec.responses = [[{"ad_set_name": "Members EN"}]]
        adsets = [{"id": "as9", "name": "Some other client", "targeting": {"age_min": 18}}]
        with _Patched(
            ca, rec,
            patch.object(ca, "_meta_ad_accounts", return_value=["act_1"]),
            patch.object(ca, "_meta_adsets", return_value=adsets),
        ):
            result = ca.sync_adset_targeting()
        assert result == {
            "status": "success", "complete": True, "matched": 0, "written": 0,
        }
        assert len(rec.calls) == 1  # the known-adsets read, nothing written


# ── Endpoints: additive fields ────────────────────────────────────────


AS_OF = date(2026, 6, 8)

HERO_META = {
    "creative_variant": "Hero Video", "platform_id": "meta",
    "spend": 6000.0, "impressions": 500_000, "clicks": 1500,
    "conversions": 50.0, "engagements": 8000,
    "video_views": 200_000, "video_completions": 80_000,
    "video_views_3s": 150_000,
    "freq_weighted": 1_000_000.0, "freq_impressions": 500_000,
}
BANNER_META = {
    "creative_variant": "Static Banner", "platform_id": "meta",
    "spend": 2000.0, "impressions": 250_000, "clicks": 500,
    "conversions": 10.0, "engagements": 3000,
    "video_views": 0, "video_completions": 0, "video_views_3s": 0,
    "freq_weighted": 375_000.0, "freq_impressions": 250_000,
}

AUD_MEMBERS = {
    "platform_id": "meta", "ad_set_name": "Members EN",
    "spend": 5000.0, "impressions": 400_000, "clicks": 1200,
    "conversions": 20.0, "engagements": 5000,
    "video_views": 100_000, "video_completions": 50_000,
    "video_views_3s": 80_000,
    "freq_weighted": 1_000_000.0, "freq_impressions": 400_000,
}
AUD_PROSPECT_SA = {
    "platform_id": "stackadapt", "ad_set_name": "Prospecting Display",
    "spend": 2000.0, "impressions": 600_000, "clicks": 700,
    "conversions": 0.0, "engagements": 0,
    "video_views": 0, "video_completions": 0, "video_views_3s": 0,
    "freq_weighted": None, "freq_impressions": None,
}


def _get(rec, path):
    with _Patched(creative_router, rec):
        client = TestClient(_make_app())
        return client.get(path)


class TestRotationImageUrl:
    def _responses(self, asset_rows):
        """Rotation BQ call order: (1) MAX(date), (2) media-plan
        objectives, (3) campaigns, (4) alias probe, (5) cells, (6) daily
        trend, (7) creative_assets lookup (Phase 19, appended last)."""
        return [
            [{"max_date": AS_OF}],
            [{"platform_id": "meta", "objective": "Awareness"}],
            [{"campaign_name": "Awareness Video", "platform_id": "meta"}],
            [],
            [HERO_META, BANNER_META],
            [],
            asset_rows,
        ]

    def test_image_url_signed_when_asset_row_exists(self):
        rec = QueryRecorder()
        rec.responses = self._responses(
            [{"variant": "Hero Video", "gcs_path": f"creative-assets/{HERO_SHA1}.jpg"}]
        )
        with patch.object(ca, "signed_url",
                          side_effect=lambda p: f"https://signed/{p}"):
            resp = _get(rec, "/api/projects/26018/creative/rotation")
        assert resp.status_code == 200, resp.text
        rows = {c["variant"]: c for c in resp.json()["creatives"]}

        assert rows["Hero Video"]["image_url"] == (
            f"https://signed/creative-assets/{HERO_SHA1}.jpg"
        )
        # No stored asset → null, never a guessed URL.
        assert rows["Static Banner"]["image_url"] is None

        # The lookup only wants stored rows for this project's variants.
        sql, params = rec.calls[6]
        assert "creative_assets" in sql
        assert "status = 'stored'" in sql
        assert ("string", "project_code", "26018") in params
        assert ("array", "variants", "STRING",
                ["Hero Video", "Static Banner"]) in params

    def test_image_url_null_when_no_asset_rows(self):
        rec = QueryRecorder()
        rec.responses = self._responses([])
        resp = _get(rec, "/api/projects/26018/creative/rotation")
        assert resp.status_code == 200, resp.text
        assert all(c["image_url"] is None for c in resp.json()["creatives"])


class TestAudienceEnrichment:
    def _responses(self, targeting_rows, reach_rows):
        """Audiences BQ call order: (1) roles, (2) audience rollup,
        (3) frequency trend, (4) alias probe, (5) cells, then the Phase 19
        lookups appended last — (6) adset_targeting, (7) reach rollup."""
        return [
            [],
            [AUD_MEMBERS, AUD_PROSPECT_SA],
            [],
            [],
            [],
            targeting_rows,
            reach_rows,
        ]

    def test_persona_pool_and_saturation_attached(self):
        rec = QueryRecorder()
        rec.responses = self._responses(
            [{"audience_key": "members-en-meta",
              "persona": "Member list · ON · 35-64 · all genders",
              "pool_size": 1_000_000}],
            [{"platform_id": "meta", "ad_set_name": "Members EN",
              "reach": 400_000}],
        )
        resp = _get(rec, "/api/projects/26018/audiences/matrix")
        assert resp.status_code == 200, resp.text
        rows = {a["name"]: a for a in resp.json()["audiences"]}

        members = rows["Members EN"]
        assert members["persona"] == "Member list · ON · 35-64 · all genders"
        assert members["pool_size"] == 1_000_000
        assert members["saturation"] == pytest.approx(0.4)

        # Non-Meta ad set: the sync never wrote a row → all three null.
        prospecting = rows["Prospecting Display"]
        assert prospecting["persona"] is None
        assert prospecting["pool_size"] is None
        assert prospecting["saturation"] is None

        # The reach lookup keeps the AI-103 latest-snapshot + AI-120 shape.
        reach_sql, reach_params = rec.calls[6]
        assert "ORDER BY date DESC, loaded_at DESC" in reach_sql
        assert "platform_id NOT IN UNNEST(@rf_excluded)" in reach_sql
        # And the targeting lookup is keyed by the matrix's own slugs.
        targ_sql, targ_params = rec.calls[5]
        assert "adset_targeting" in targ_sql
        assert ("array", "audience_keys", "STRING",
                ["members-en-meta", "prospecting-display-stackadapt"]) in targ_params

    def test_saturation_null_when_either_side_missing(self):
        rec = QueryRecorder()
        rec.responses = self._responses(
            # Persona but no pool — saturation must stay null.
            [{"audience_key": "members-en-meta",
              "persona": "Member list", "pool_size": None}],
            [{"platform_id": "meta", "ad_set_name": "Members EN",
              "reach": 400_000}],
        )
        resp = _get(rec, "/api/projects/26018/audiences/matrix")
        assert resp.status_code == 200, resp.text
        members = {a["name"]: a for a in resp.json()["audiences"]}["Members EN"]
        assert members["persona"] == "Member list"
        assert members["pool_size"] is None
        assert members["saturation"] is None

    def test_pool_without_reach_keeps_saturation_null(self):
        rec = QueryRecorder()
        rec.responses = self._responses(
            [{"audience_key": "members-en-meta",
              "persona": "Member list", "pool_size": 1_000_000}],
            [],  # no reach snapshots at all
        )
        resp = _get(rec, "/api/projects/26018/audiences/matrix")
        assert resp.status_code == 200, resp.text
        members = {a["name"]: a for a in resp.json()["audiences"]}["Members EN"]
        assert members["pool_size"] == 1_000_000
        assert members["saturation"] is None
