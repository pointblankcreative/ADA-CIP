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
        # The StackAdapt branch resolves ad_id → variant via its own query
        # (alias probe + adid map) before fetching stills by id.
        rec.responses += [
            [],  # _stackadapt_adid_map alias-table probe
            [{"ad_id": "sa-1", "creative_variant": HERO_VARIANT}],
        ]
        with _Patched(
            ca, rec,
            patch.object(ca, "_meta_ad_accounts", return_value=[]),
            patch.object(ca, "_stackadapt_ad_stills",
                         return_value=[{"ad_id": "sa-1", "url": "https://sa/img.png"}]),
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

    def test_stackadapt_still_prefers_poster_then_image_never_raw_video(self):
        # Display / native image creative → its own s3Url.
        assert ca._stackadapt_still_from_ad(
            {"creativesConnection": {"nodes": [
                {"__typename": "ImageCreative", "s3Url": "https://sa/i.jpg"}]}}
        ) == "https://sa/i.jpg"
        # Video / CTV → the poster thumbnail, not the playable mp4.
        assert ca._stackadapt_still_from_ad(
            {"creativesConnection": {"nodes": [
                {"__typename": "UploadedVideo",
                 "thumbS3Url": "https://sa/poster.jpg",
                 "s3Url": "https://sa/clip.mp4"}]}}
        ) == "https://sa/poster.jpg"
        # A VAST tag carries only a non-image s3Url → nothing renderable.
        assert ca._stackadapt_still_from_ad(
            {"creativesConnection": {"nodes": [
                {"__typename": "VastCreative", "s3Url": "https://sa/vast.xml"}]}}
        ) is None
        # edges/node connection shape resolves the same way.
        assert ca._stackadapt_still_from_ad(
            {"creativesConnection": {"edges": [
                {"node": {"__typename": "ImageCreative", "s3Url": "https://sa/e.jpg"}}]}}
        ) == "https://sa/e.jpg"
        # Empty / missing connection → None, never a crash.
        assert ca._stackadapt_still_from_ad({}) is None
        assert ca._stackadapt_still_from_ad(
            {"creativesConnection": {"nodes": []}}) is None

    def test_stackadapt_adid_map_normalizes_and_pins_platform(self):
        rec = QueryRecorder()
        rec.responses = [
            [],  # alias-table probe (no exception → alias join included)
            [{"ad_id": "14223195",
              "creative_variant": "26018 CAPE Pre-Bargaining Awareness Ad B"}],
        ]
        with _Patched(ca, rec):
            mapping = ca._stackadapt_adid_map("26018")

        assert mapping == {
            "14223195": "26018 CAPE Pre-Bargaining Awareness Ad B"}
        sql, params = rec.calls[1]
        # Same alias join + regex normalization the router uses, but keyed
        # by ad_id and pinned to the StackAdapt platform.
        assert "creative_variant_aliases" in sql
        assert r"\d{5}" in sql and r"\d+x\d+" in sql
        assert "ad_agg.ad_id" in sql
        assert "platform_id = 'stackadapt'" in sql
        assert ("string", "project_code", "26018") in params

    def test_stackadapt_ad_stills_parses_ads_by_id(self, monkeypatch):
        monkeypatch.setattr(ca.settings, "stackadapt_api_key", "key")

        class _Resp:
            def raise_for_status(self):
                pass

            def json(self):
                return {"data": {"ads": {
                    "nodes": [{
                        "id": "14223195", "__typename": "DisplayAd",
                        "creativesConnection": {"nodes": [
                            {"__typename": "ImageCreative",
                             "s3Url": "https://sa/b.jpg"}]},
                    }],
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                }}}

        class _Http:
            def post(self, *a, **k):
                return _Resp()

        out = list(ca._stackadapt_ad_stills(_Http(), ["14223195"]))
        assert out == [{"ad_id": "14223195", "url": "https://sa/b.jpg"}]

    def test_stackadapt_ad_stills_raises_on_graphql_errors(self, monkeypatch):
        # An expired/limited token comes back as HTTP 200 + errors body —
        # must surface as a failure, never a silent empty (clean) scan.
        monkeypatch.setattr(ca.settings, "stackadapt_api_key", "expired")

        class _Resp:
            def raise_for_status(self):
                pass

            def json(self):
                return {"errors": [{"message": "The access token expired"}],
                        "data": None}

        class _Http:
            def post(self, *a, **k):
                return _Resp()

        with pytest.raises(RuntimeError):
            list(ca._stackadapt_ad_stills(_Http(), ["14223195"]))

    def test_stackadapt_token_error_does_not_record_no_match(self, monkeypatch):
        # End-to-end: when StackAdapt errors, pending variants stay pending
        # (source marked failed) rather than being written off as no_match.
        monkeypatch.setattr(ca.settings, "meta_access_token", "tok")
        monkeypatch.setattr(ca.settings, "stackadapt_api_key", "expired")
        rec = QueryRecorder()
        _image_sync_responses(rec, [])
        rec.responses += [
            [],  # _stackadapt_adid_map alias-table probe
            [{"ad_id": "sa-1", "creative_variant": HERO_VARIANT}],
        ]

        def _boom(*a, **k):
            raise RuntimeError("StackAdapt GraphQL error: token expired")

        with _Patched(
            ca, rec,
            patch.object(ca, "_meta_ad_accounts", return_value=[]),
            patch.object(ca, "_stackadapt_ad_stills", side_effect=_boom),
        ):
            result = ca.sync_creative_images()

        assert result["no_match"] == 0
        assert result["sources"]["stackadapt"] == "failed"
        assert result["complete"] is False
        # No MERGE write at all — nothing was concluded about the variant.
        assert not any("MERGE" in sql for sql, _ in rec.calls)

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

    def test_meta_image_hash_found_wherever_it_hides(self):
        assert ca._meta_image_hash({"image_hash": "h1"}) == "h1"
        assert ca._meta_image_hash(
            {"object_story_spec": {"link_data": {"image_hash": "h2"}}}
        ) == "h2"
        assert ca._meta_image_hash(
            {"object_story_spec": {"video_data": {"image_hash": "h3"}}}
        ) == "h3"
        assert ca._meta_image_hash(
            {"asset_feed_spec": {"images": [{"hash": "h4"}]}}
        ) == "h4"
        assert ca._meta_image_hash({"thumbnail_url": "x"}) is None
        assert ca._meta_image_hash(None) is None

    def test_hash_resolution_preferred_over_tiny_link_thumbnail(self, monkeypatch):
        # Link/conversion creatives: no image_url, 64px thumbnail — the
        # image hash must be resolved to the original upload instead.
        monkeypatch.setattr(ca.settings, "meta_access_token", "tok")
        monkeypatch.setattr(ca.settings, "stackadapt_api_key", "")
        rec = QueryRecorder()
        _image_sync_responses(rec, [])
        ads = [{
            "name": HERO_AD_NAME,
            "creative": {
                "thumbnail_url": "https://cdn.meta/p64x64.jpg",
                "object_story_spec": {"link_data": {"image_hash": "abc123",
                                                    "picture": "https://cdn.meta/p64x64.jpg"}},
            },
        }]
        with _Patched(
            ca, rec,
            patch.object(ca, "_meta_ad_accounts", return_value=["act_1"]),
            patch.object(ca, "_meta_ads", return_value=ads),
            patch.object(ca, "_resolve_image_hash",
                         return_value="https://cdn.meta/original_full.jpg"),
            patch.object(ca, "_download_image",
                         return_value=(b"BIG", "jpg", "image/jpeg")),
            patch.object(ca, "_store_bytes", return_value=None),
        ) as started:
            result = ca.sync_creative_images()
            resolve = started[8]   # _resolve_image_hash mock
            download = started[9]  # _download_image mock

        assert result["stored"] == 1
        assert resolve.call_args[0][2] == "abc123"
        assert download.call_args[0][1] == "https://cdn.meta/original_full.jpg"

    def test_hash_resolution_failure_falls_back_to_thumbnail(self, monkeypatch):
        monkeypatch.setattr(ca.settings, "meta_access_token", "tok")
        monkeypatch.setattr(ca.settings, "stackadapt_api_key", "")
        rec = QueryRecorder()
        _image_sync_responses(rec, [])
        ads = [{
            "name": HERO_AD_NAME,
            "creative": {
                "thumbnail_url": "https://cdn.meta/thumb.jpg",
                "image_hash": "abc123",
            },
        }]
        with _Patched(
            ca, rec,
            patch.object(ca, "_meta_ad_accounts", return_value=["act_1"]),
            patch.object(ca, "_meta_ads", return_value=ads),
            patch.object(ca, "_resolve_image_hash", return_value=None),
            patch.object(ca, "_download_image",
                         return_value=(b"IMG", "jpg", "image/jpeg")),
            patch.object(ca, "_store_bytes", return_value=None),
        ) as started:
            result = ca.sync_creative_images()
            download = started[9]

        assert result["stored"] == 1
        assert download.call_args[0][1] == "https://cdn.meta/thumb.jpg"

    def test_explicit_image_url_skips_hash_resolution(self, monkeypatch):
        monkeypatch.setattr(ca.settings, "meta_access_token", "tok")
        monkeypatch.setattr(ca.settings, "stackadapt_api_key", "")
        rec = QueryRecorder()
        _image_sync_responses(rec, [])
        ads = [{
            "name": HERO_AD_NAME,
            "creative": {"image_url": "https://cdn.meta/full.jpg",
                         "image_hash": "abc123"},
        }]
        with _Patched(
            ca, rec,
            patch.object(ca, "_meta_ad_accounts", return_value=["act_1"]),
            patch.object(ca, "_meta_ads", return_value=ads),
            patch.object(ca, "_resolve_image_hash", return_value=None),
            patch.object(ca, "_download_image",
                         return_value=(b"IMG", "jpg", "image/jpeg")),
            patch.object(ca, "_store_bytes", return_value=None),
        ) as started:
            ca.sync_creative_images()
            resolve = started[8]
            download = started[9]

        assert resolve.call_count == 0
        assert download.call_args[0][1] == "https://cdn.meta/full.jpg"

    def test_meta_ads_requests_full_size_thumbnails(self, monkeypatch):
        # thumbnail_url defaults to 64x64 — the request must carry the
        # field-expansion size modifiers or video/object_story_spec
        # creatives come back blurry.
        monkeypatch.setattr(ca.settings, "meta_access_token", "tok")
        captured: dict = {}

        def fake_paged(http, url, params):
            captured.update(params)
            return iter([])

        with patch.object(ca, "_meta_paged", side_effect=fake_paged):
            list(ca._meta_ads(None, "act_1"))

        fields = captured["fields"]
        assert "thumbnail_width(1080)" in fields
        assert "thumbnail_height(1080)" in fields
        assert "thumbnail_url" in fields and "image_url" in fields
        # Needed for the adimages hash-resolution path.
        assert "image_hash" in fields and "asset_feed_spec" in fields

    def _stored_meta_state(self):
        return [{
            "variant": HERO_VARIANT, "status": "stored",
            "gcs_path": f"creative-assets/{HERO_SHA1}.jpg",
            "source_platform": "meta",
            "checked_at": datetime.now(timezone.utc) - timedelta(days=30),
        }]

    def test_force_refreshes_stored_meta_stills(self, monkeypatch):
        monkeypatch.setattr(ca.settings, "meta_access_token", "tok")
        monkeypatch.setattr(ca.settings, "stackadapt_api_key", "")
        rec = QueryRecorder()
        _image_sync_responses(rec, self._stored_meta_state())
        ads = [{"name": HERO_AD_NAME,
                "creative": {"thumbnail_url": "https://cdn.meta/thumb_1080.jpg"}}]
        with _Patched(
            ca, rec,
            patch.object(ca, "_meta_ad_accounts", return_value=["act_1"]),
            patch.object(ca, "_meta_ads", return_value=ads),
            patch.object(ca, "_download_image",
                         return_value=(b"BIG", "jpg", "image/jpeg")),
            patch.object(ca, "_store_bytes", return_value=None),
        ):
            result = ca.sync_creative_images(force=True)

        assert result["pending"] == 1 and result["stored"] == 1
        merge_sql, merge_params = rec.calls[-1]
        assert "MERGE" in merge_sql
        assert ("string", "status", "stored") in merge_params

    def test_force_leaves_stackadapt_stored_alone(self, monkeypatch):
        # StackAdapt serves the original asset — nothing to heal.
        monkeypatch.setattr(ca.settings, "meta_access_token", "tok")
        monkeypatch.setattr(ca.settings, "stackadapt_api_key", "")
        rec = QueryRecorder()
        state = self._stored_meta_state()
        state[0]["source_platform"] = "stackadapt"
        _image_sync_responses(rec, state)
        with _Patched(ca, rec):
            result = ca.sync_creative_images(force=True)
        assert result["pending"] == 0

    def test_forced_refresh_miss_keeps_stored_row(self, monkeypatch):
        # A refresh that finds nothing must not flip stored → no_match.
        monkeypatch.setattr(ca.settings, "meta_access_token", "tok")
        monkeypatch.setattr(ca.settings, "stackadapt_api_key", "")
        rec = QueryRecorder()
        _image_sync_responses(rec, self._stored_meta_state())
        with _Patched(
            ca, rec,
            patch.object(ca, "_meta_ad_accounts", return_value=["act_1"]),
            patch.object(ca, "_meta_ads", return_value=[]),
        ):
            result = ca.sync_creative_images(force=True)

        assert result["pending"] == 1
        assert result["no_match"] == 0 and result["fetch_failed"] == 0
        assert len(rec.calls) == 4  # state reads only, no MERGE writes

    def test_forced_refresh_failed_download_keeps_stored_row(self, monkeypatch):
        # A refresh whose download fails keeps the working image too.
        monkeypatch.setattr(ca.settings, "meta_access_token", "tok")
        monkeypatch.setattr(ca.settings, "stackadapt_api_key", "")
        rec = QueryRecorder()
        _image_sync_responses(rec, self._stored_meta_state())
        ads = [{"name": HERO_AD_NAME,
                "creative": {"thumbnail_url": "https://cdn.meta/thumb_1080.jpg"}}]
        with _Patched(
            ca, rec,
            patch.object(ca, "_meta_ad_accounts", return_value=["act_1"]),
            patch.object(ca, "_meta_ads", return_value=ads),
            patch.object(ca, "_download_image", return_value=None),
        ):
            result = ca.sync_creative_images(force=True)

        assert result["fetch_failed"] == 1 and result["stored"] == 0
        assert len(rec.calls) == 4  # ledger untouched — stored row survives


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

    def test_image_url_is_proxy_path_when_asset_row_exists(self):
        """Phase 19 follow-up: images serve through the API's own proxy
        (the bucket blocks public access and signed URLs needed an IAM
        grant the staging SA doesn't hold)."""
        rec = QueryRecorder()
        rec.responses = self._responses(
            [{"variant": "Hero Video", "gcs_path": f"creative-assets/{HERO_SHA1}.jpg"}]
        )
        resp = _get(rec, "/api/projects/26018/creative/rotation")
        assert resp.status_code == 200, resp.text
        rows = {c["variant"]: c for c in resp.json()["creatives"]}

        assert rows["Hero Video"]["image_url"] == (
            "/api/projects/creative-assets/image?variant=Hero%20Video"
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


class TestImageSyncResilience:
    """Phase 19 follow-ups: force retries, source-failure honesty, and
    the image proxy endpoint that replaced signed URLs."""

    def test_force_retries_no_match_past_the_daily_guard(self, monkeypatch):
        monkeypatch.setattr(ca.settings, "meta_access_token", "tok")
        monkeypatch.setattr(ca.settings, "stackadapt_api_key", "")
        rec = QueryRecorder()
        _image_sync_responses(rec, [{
            "variant": HERO_VARIANT, "status": "no_match",
            "gcs_path": None, "checked_at": datetime.now(timezone.utc),
        }])
        ads = [{
            "name": HERO_AD_NAME,
            "creative": {"image_url": "https://cdn.meta/full.jpg"},
        }]
        with _Patched(
            ca, rec,
            patch.object(ca, "_meta_ad_accounts", return_value=["act_1"]),
            patch.object(ca, "_meta_ads", return_value=ads),
            patch.object(ca, "_download_image",
                         return_value=(b"IMG", "jpg", "image/jpeg")),
            patch.object(ca, "_store_bytes", return_value=None),
        ):
            # Without force: attempted today already, nothing pending.
            guarded = ca.sync_creative_images()
            # With force: the no_match row is retried and stores.
            # (Re-prime the recorder — each sync call consumes the four
            # canned BQ reads.)
            _image_sync_responses(rec, [{
                "variant": HERO_VARIANT, "status": "no_match",
                "gcs_path": None, "checked_at": datetime.now(timezone.utc),
            }])
            forced = ca.sync_creative_images(force=True)

        assert guarded["pending"] == 0
        assert forced["pending"] == 1
        assert forced["stored"] == 1

    def test_force_never_refetches_stored_variants(self, monkeypatch):
        monkeypatch.setattr(ca.settings, "meta_access_token", "tok")
        monkeypatch.setattr(ca.settings, "stackadapt_api_key", "")
        rec = QueryRecorder()
        _image_sync_responses(rec, [{
            "variant": HERO_VARIANT, "status": "stored",
            "gcs_path": "creative-assets/x.jpg",
            "checked_at": datetime.now(timezone.utc) - timedelta(days=30),
        }])
        with _Patched(
            ca, rec,
            patch.object(ca, "_meta_ad_accounts", return_value=["act_1"]),
            patch.object(ca, "_meta_ads", return_value=[]),
        ):
            result = ca.sync_creative_images(force=True)
        assert result["pending"] == 0
        assert result["complete"] is True

    def test_source_failure_blocks_no_match_and_is_reported(self, monkeypatch):
        """A broken source scan must not look like "this creative doesn't
        exist": no no_match rows, complete=False, source flagged."""
        monkeypatch.setattr(ca.settings, "meta_access_token", "tok")
        monkeypatch.setattr(ca.settings, "stackadapt_api_key", "")
        rec = QueryRecorder()
        _image_sync_responses(rec, [])
        with _Patched(
            ca, rec,
            patch.object(ca, "_meta_ad_accounts",
                         side_effect=RuntimeError("graph down")),
        ):
            result = ca.sync_creative_images()
        assert result["status"] == "success"
        assert result["complete"] is False
        assert result["sources"]["meta"] == "failed"
        assert result["no_match"] == 0
        # No ledger MERGE happened — the 4 reads are the only BQ calls.
        assert all("MERGE" not in sql for sql, _ in rec.calls)


class TestImageProxyEndpoint:
    def test_serves_stored_image_with_cache_headers(self):
        rec = QueryRecorder()
        rec.responses = [[{"gcs_path": "creative-assets/abc.jpg"}]]
        with patch.object(ca, "read_bytes", return_value=(b"IMGBYTES", "image/jpeg")):
            resp = _get(
                rec,
                "/api/projects/creative-assets/image?variant=Hero%20Video",
            )
        assert resp.status_code == 200, resp.text
        assert resp.content == b"IMGBYTES"
        assert resp.headers["content-type"].startswith("image/jpeg")
        assert resp.headers["cache-control"] == "public, max-age=86400"

    def test_404_when_variant_has_no_stored_asset(self):
        rec = QueryRecorder()
        rec.responses = [[]]
        resp = _get(
            rec, "/api/projects/creative-assets/image?variant=Nope"
        )
        assert resp.status_code == 404

    def test_404_when_object_unreadable(self):
        rec = QueryRecorder()
        rec.responses = [[{"gcs_path": "creative-assets/abc.jpg"}]]
        with patch.object(ca, "read_bytes", return_value=None):
            resp = _get(
                rec,
                "/api/projects/creative-assets/image?variant=Hero%20Video",
            )
        assert resp.status_code == 404
