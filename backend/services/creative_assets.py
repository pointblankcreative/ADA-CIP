"""Creative thumbnails + audience targeting sync (Phase 19).

Two daily best-effort syncs against the ad platforms' own APIs:

  1. Creative images — for tracked creative variants that don't yet have a
     stored still, find a matching ad on Meta (Graph API) or StackAdapt
     (GraphQL), download the image bytes, and park them in GCS under
     creative-assets/ in the shared resources bucket (the same bucket the
     alert charts use). Meta CDN URLs expire, so the BYTES are stored,
     never the URL. Every attempt is recorded in the creative_assets
     ledger; misses retry on later runs at most once per day.
  2. Ad-set targeting — per Meta ad account, pull each ad set's targeting
     spec and render it into a deterministic plain-English persona string
     (no LLM, no guessing: only what the spec says), plus a pool size from
     delivery_estimate. Rows land in adset_targeting keyed by the SAME
     audience slug the audiences/matrix endpoint serves, so the frontend
     join is free.

Variant matching reuses the creative router's alias resolution (the
creative_variant_aliases join + regex normalization) by importing its
helper — the ad_name → variant mapping here is byte-for-byte the one the
rotation endpoint serves.

Everything is log-and-continue: a failure on one ad or ad set never stops
the run, ``run_sync`` never raises out of the daily job, and the whole
thing no-ops gracefully when the tokens are unset (the settings default).
"""

import hashlib
import json
import logging
import re
import time
from collections.abc import Iterator
from datetime import datetime, timezone
from urllib.parse import urlparse

import httpx

from backend.config import settings
# Reused, not reimplemented: the alias join + regex normalization that maps
# ad_name → creative_variant, and the audience slug the matrix endpoint
# keys on. The router never imports this module at module level (it lazy
# imports inside its lookup helpers), so there is no circular import.
from backend.routers.creative import _alias_resolution, _audience_id
from backend.services import bigquery_client as bq

logger = logging.getLogger(__name__)

# GCS prefix inside settings.alert_charts_bucket. Objects are private (the
# bucket enforces Public Access Prevention); reads go through signed URLs.
ASSETS_PREFIX = "creative-assets/"

META_GRAPH_BASE = "https://graph.facebook.com"
META_PLATFORM_ID = "meta"  # fact_digital_daily's platform_id for Meta
STACKADAPT_PLATFORM_ID = "stackadapt"  # fact_digital_daily's platform_id for StackAdapt
STACKADAPT_GRAPHQL_URL = "https://api.stackadapt.com/graphql"

HTTP_TIMEOUT = 30.0
# Paging guard — a bad cursor must not loop forever inside the daily job.
MAX_PAGES = 50

_EXT_BY_CONTENT_TYPE = {
    "image/jpeg": "jpg",
    "image/jpg": "jpg",
    "image/png": "png",
    "image/gif": "gif",
    "image/webp": "webp",
}
_CONTENT_TYPE_BY_EXT = {
    "jpg": "image/jpeg",
    "png": "image/png",
    "gif": "image/gif",
    "webp": "image/webp",
}


def _http() -> httpx.Client:
    return httpx.Client(timeout=HTTP_TIMEOUT, follow_redirects=True)


# ── persona rendering (deterministic, no LLM) ─────────────────────────

# Meta's own naming convention for lookalike audiences, e.g.
# "Lookalike (CA, 1%) - Member List 2026" → "1% lookalike of Member List 2026".
_LOOKALIKE_NAME_RE = re.compile(
    r"^lookalike\s*\(\s*(?:[A-Z]{2}\s*,\s*)?(\d+(?:\.\d+)?)\s*%\s*\)\s*[-–:]?\s*(.*)$",
    re.IGNORECASE,
)

# Canadian campaigns are the norm — render provinces the way planners say
# them ("ON + BC"), fall back to the platform's full region name elsewhere.
_REGION_ABBR = {
    "Alberta": "AB",
    "British Columbia": "BC",
    "Manitoba": "MB",
    "New Brunswick": "NB",
    "Newfoundland and Labrador": "NL",
    "Northwest Territories": "NT",
    "Nova Scotia": "NS",
    "Nunavut": "NU",
    "Ontario": "ON",
    "Prince Edward Island": "PE",
    "Quebec": "QC",
    "Saskatchewan": "SK",
    "Yukon": "YT",
}
_COUNTRY_NAMES = {"CA": "Canada", "US": "United States"}

# Interests/behaviors fragment: first N names, then "+N more".
_MAX_INTEREST_NAMES = 4


def _audience_fragment(targeting: dict) -> str | None:
    """Custom audiences + lookalikes, joined with ' + '."""
    bits: list[str] = []
    for ca in targeting.get("custom_audiences") or []:
        name = ((ca or {}).get("name") or "").strip()
        if not name:
            continue
        m = _LOOKALIKE_NAME_RE.match(name)
        if m:
            ratio, origin = m.group(1), m.group(2).strip()
            bits.append(
                f"{ratio}% lookalike of {origin}" if origin else f"{ratio}% lookalike"
            )
        else:
            bits.append(name)
    # Defensive: some specs carry an explicit lookalike_spec instead of a
    # conventionally named custom audience.
    spec = targeting.get("lookalike_spec") or {}
    ratio = spec.get("ratio")
    if ratio:
        origins = spec.get("origin") or []
        origin_name = ((origins[0] or {}).get("name") or "").strip() if origins else ""
        pct = f"{ratio * 100:g}" if isinstance(ratio, float) and ratio <= 1 else f"{ratio:g}"
        bits.append(
            f"{pct}% lookalike of {origin_name}" if origin_name else f"{pct}% lookalike"
        )
    return " + ".join(bits) if bits else None


def _geo_fragment(targeting: dict) -> str | None:
    """Regions/cities joined with ' + ', country names as the fallback."""
    geo = targeting.get("geo_locations") or {}
    places = [
        _REGION_ABBR.get(r["name"], r["name"])
        for r in geo.get("regions") or []
        if (r or {}).get("name")
    ]
    places += [c["name"] for c in geo.get("cities") or [] if (c or {}).get("name")]
    if not places:
        places = [_COUNTRY_NAMES.get(c, c) for c in geo.get("countries") or [] if c]
    return " + ".join(places) if places else None


def _interest_fragment(targeting: dict) -> str | None:
    """Interest/behavior names (top level + flexible_spec), capped."""
    names: list[str] = []
    seen: set[str] = set()
    sources: list[dict] = [targeting] + [
        f for f in targeting.get("flexible_spec") or [] if isinstance(f, dict)
    ]
    for source in sources:
        for key in ("interests", "behaviors"):
            for item in source.get(key) or []:
                name = ((item or {}).get("name") or "").strip()
                if name and name.lower() not in seen:
                    seen.add(name.lower())
                    names.append(name)
    if not names:
        return None
    frag = ", ".join(names[:_MAX_INTEREST_NAMES])
    if len(names) > _MAX_INTEREST_NAMES:
        frag += f" +{len(names) - _MAX_INTEREST_NAMES} more"
    return frag


def render_persona(targeting: dict | None) -> str | None:
    """Deterministic plain-English read of a Meta targeting spec.

    Short fragments joined with " · " in a fixed order — audiences, geo,
    age, gender, interests, placements — e.g.
    "Member list + 1% lookalike of Member list · ON + BC · 35-64 · all genders".
    The gender fragment only appears when the spec carries demographic
    targeting at all (age bounds or an explicit genders key), so a pure
    custom-audience ad set reads as just its audience names. Returns None
    for an empty spec — the UI hides the persona slot entirely.
    """
    if not targeting:
        return None
    frags: list[str] = []

    audience = _audience_fragment(targeting)
    if audience:
        frags.append(audience)

    geo = _geo_fragment(targeting)
    if geo:
        frags.append(geo)

    age_min, age_max = targeting.get("age_min"), targeting.get("age_max")
    if age_min and age_max:
        frags.append(f"{age_min}-{age_max}")
    elif age_min:
        frags.append(f"{age_min}+")

    genders = targeting.get("genders")
    if age_min or age_max or genders is not None:
        if genders == [1]:
            frags.append("men")
        elif genders == [2]:
            frags.append("women")
        else:
            frags.append("all genders")

    interests = _interest_fragment(targeting)
    if interests:
        frags.append(interests)

    platforms = [
        p.replace("_", " ").title()
        for p in targeting.get("publisher_platforms") or []
        if p
    ]
    if platforms:
        frags.append(" + ".join(platforms))

    return " · ".join(frags) if frags else None


# ── Meta Graph API ─────────────────────────────────────────────────────


def _meta_paged(http: httpx.Client, url: str, params: dict | None) -> Iterator[dict]:
    """Yield Graph API list results across pages. ``paging.next`` carries
    the cursor AND the token, so params only apply to the first request."""
    pages = 0
    while url and pages < MAX_PAGES:
        resp = http.get(url, params=params)
        resp.raise_for_status()
        payload = resp.json()
        yield from payload.get("data") or []
        url = (payload.get("paging") or {}).get("next")
        params = None
        pages += 1


def _meta_ad_accounts(http: httpx.Client) -> list[str]:
    """Ad account ids ('act_…') visible to the system-user token."""
    url = f"{META_GRAPH_BASE}/{settings.meta_api_version}/me/adaccounts"
    params = {
        "fields": "id,name",
        "access_token": settings.meta_access_token,
        "limit": 100,
    }
    return [a["id"] for a in _meta_paged(http, url, params) if a.get("id")]


def _meta_ads(http: httpx.Client, account_id: str) -> Iterator[dict]:
    """Ads with creative image fields for one account, paged.

    thumbnail_url defaults to a 64x64 crop unless dimensions are asked
    for explicitly — the .param(value) field-expansion modifiers request
    a full-size still for the creatives (videos, object_story_spec
    statics) where image_url is absent and the thumbnail is all we get.
    """
    url = f"{META_GRAPH_BASE}/{settings.meta_api_version}/{account_id}/ads"
    params = {
        "fields": (
            "name,creative.thumbnail_width(1080).thumbnail_height(1080)"
            "{thumbnail_url,image_url,image_hash,object_story_spec,asset_feed_spec}"
        ),
        "access_token": settings.meta_access_token,
        "limit": 100,
    }
    yield from _meta_paged(http, url, params)


def _meta_adsets(http: httpx.Client, account_id: str) -> Iterator[dict]:
    """Ad sets with targeting specs for one account, paged."""
    url = f"{META_GRAPH_BASE}/{settings.meta_api_version}/{account_id}/adsets"
    params = {
        "fields": "name,targeting,id",
        "access_token": settings.meta_access_token,
        "limit": 100,
    }
    yield from _meta_paged(http, url, params)


def _meta_image_url(creative: dict | None) -> str | None:
    """Best directly-carried still on a Meta creative: image_url (full
    size) first, thumbnail_url second, then whatever the
    object_story_spec carries. Link/conversion creatives often have
    none of these at real size — see _meta_image_hash for those."""
    if not creative:
        return None
    url = creative.get("image_url") or creative.get("thumbnail_url")
    if url:
        return url
    spec = creative.get("object_story_spec") or {}
    link = spec.get("link_data") or {}
    video = spec.get("video_data") or {}
    return link.get("picture") or video.get("image_url") or None


def _meta_image_hash(creative: dict | None) -> str | None:
    """Image hash on a Meta creative, wherever it hides. Link-style
    conversion ads carry no image_url, and their thumbnails ignore the
    size modifiers (64x64 crops of link_data.picture) — but the hash
    resolves to the original upload via the account's adimages edge."""
    if not creative:
        return None
    if creative.get("image_hash"):
        return creative["image_hash"]
    spec = creative.get("object_story_spec") or {}
    for key in ("link_data", "video_data"):
        h = (spec.get(key) or {}).get("image_hash")
        if h:
            return h
    images = (creative.get("asset_feed_spec") or {}).get("images") or []
    if images and isinstance(images[0], dict) and images[0].get("hash"):
        return images[0]["hash"]
    return None


def _resolve_image_hash(
    http: httpx.Client,
    account_id: str,
    image_hash: str,
    cache: dict[str, str | None],
) -> str | None:
    """Full-size source URL for an image hash via /{account}/adimages.
    Cached per account scan; failures cache None so a broken hash costs
    one round-trip per run."""
    if image_hash in cache:
        return cache[image_hash]
    url = None
    try:
        resp = http.get(
            f"{META_GRAPH_BASE}/{settings.meta_api_version}/{account_id}/adimages",
            params={
                "hashes": json.dumps([image_hash]),
                "fields": "hash,url",
                "access_token": settings.meta_access_token,
            },
        )
        resp.raise_for_status()
        for row in (resp.json().get("data") or []):
            if row.get("hash") == image_hash and row.get("url"):
                url = row["url"]
                break
    except Exception:
        logger.warning("adimages lookup failed for hash %s", image_hash, exc_info=True)
    cache[image_hash] = url
    return url


def _meta_best_image(
    http: httpx.Client,
    account_id: str,
    creative: dict | None,
    hash_cache: dict[str, str | None],
) -> str | None:
    """The largest still we can get for one creative: explicit image_url,
    then the original upload via image-hash resolution, then the (size-
    modified) thumbnail and story-spec fallbacks."""
    if not creative:
        return None
    if creative.get("image_url"):
        return creative["image_url"]
    image_hash = _meta_image_hash(creative)
    if image_hash:
        resolved = _resolve_image_hash(http, account_id, image_hash, hash_cache)
        if resolved:
            return resolved
    return _meta_image_url(creative)


def _pool_from_estimate(est: dict) -> int | None:
    """Extract a pool size from one delivery_estimate entry. estimate_mau
    (monthly uniques) is the truest pool proxy; estimate_dau and the
    users bounds are fallbacks across Graph versions."""
    for key in ("estimate_mau", "estimate_dau"):
        if est.get(key):
            return int(est[key])
    lo, hi = est.get("users_lower_bound"), est.get("users_upper_bound")
    if lo is not None and hi is not None:
        return (int(lo) + int(hi)) // 2
    if lo is not None:
        return int(lo)
    if hi is not None:
        return int(hi)
    return None


def _adset_pool_size(http: httpx.Client, adset_id: str) -> int | None:
    """Pool size from the ad set's delivery_estimate. Some Graph
    versions 400 without an optimization_goal, so a bare call is tried
    first and retried with REACH before giving up. None when Meta
    returns nothing usable (common on paused or ended ad sets)."""
    url = f"{META_GRAPH_BASE}/{settings.meta_api_version}/{adset_id}/delivery_estimate"
    for params in (
        {"access_token": settings.meta_access_token},
        {
            "access_token": settings.meta_access_token,
            "optimization_goal": "REACH",
        },
    ):
        try:
            resp = http.get(url, params=params)
            resp.raise_for_status()
            data = resp.json().get("data") or []
            if not data:
                return None
            pool = _pool_from_estimate(data[0] or {})
            if pool is not None:
                return pool
        except Exception:
            continue
    logger.warning("delivery_estimate unusable for ad set %s", adset_id)
    return None


# ── StackAdapt GraphQL ─────────────────────────────────────────────────

# Verified live against api.stackadapt.com by schema introspection
# (2026-06-12). There is NO root `creatives` field — the prior query
# targeted a field that does not exist, so the StackAdapt path never
# resolved a single still and every SA static/video sat at no_match.
#
# The real entity is `ads` (AdConnection), filterable by id via
# AdFilters.ids: [ID!]. `Ad` is an interface; each concrete channel
# carries a `creativesConnection` whose leaf node holds the still:
#   DisplayAd / NativeAd → ImageCreative.s3Url
#   VideoAd  / CtvAd     → UploadedVideo.thumbS3Url (poster) ?? s3Url,
#                          or VastCreative.s3Url
# Funnel reports StackAdapt at the ad level, so fact_digital_daily.ad_id
# is the StackAdapt Ad.id — matching on that id (not the creative-library
# name) is what lets SA stills resolve. The parser stays tolerant of
# nodes/edges shape so a schema tweak degrades to no_match, never a crash.
_STACKADAPT_ADS_QUERY = """
query AdaAdStills($ids: [ID!], $after: String) {
  ads(filterBy: { ids: $ids }, first: 100, after: $after) {
    nodes {
      id
      __typename
      ... on DisplayAd { creativesConnection(first: 5) { nodes {
        __typename ... on ImageCreative { s3Url width height } } } }
      ... on NativeAd { creativesConnection(first: 5) { nodes {
        __typename ... on ImageCreative { s3Url width height } } } }
      ... on VideoAd { creativesConnection(first: 5) { nodes {
        __typename
        ... on UploadedVideo { thumbS3Url s3Url }
        ... on VastCreative { s3Url } } } }
      ... on CtvAd { creativesConnection(first: 5) { nodes {
        __typename
        ... on UploadedVideo { thumbS3Url s3Url }
        ... on VastCreative { s3Url } } } }
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""


def _stackadapt_still_from_ad(node: dict) -> str | None:
    """Best still URL for one StackAdapt ad node — always an image the
    Creative tab can render in an <img>: a video poster (thumbS3Url) or an
    ImageCreative's s3Url, never a raw video/VAST file. Tolerant of the
    per-channel union shapes and of the nodes/edges connection variants."""
    conn = node.get("creativesConnection") or {}
    nodes = conn.get("nodes")
    if nodes is None:  # edges/node fallback
        nodes = [e.get("node") for e in conn.get("edges") or [] if e and e.get("node")]
    fallback: str | None = None
    for cnode in nodes or []:
        cnode = cnode or {}
        thumb = cnode.get("thumbS3Url")
        if thumb:  # video / CTV poster — guaranteed an image
            return thumb
        if cnode.get("__typename") == "ImageCreative" and cnode.get("s3Url"):
            return cnode["s3Url"]
        # Defensive: an s3Url whose type we couldn't read (schema drift on
        # __typename) is a last resort only — the download step still
        # validates the content type before anything is stored.
        if fallback is None and cnode.get("s3Url") and not cnode.get("__typename"):
            fallback = cnode["s3Url"]
    return fallback


def _chunked(items: list, size: int) -> Iterator[list]:
    """Yield successive size-length slices — keeps the ads(ids:) filter
    under a sane query length when many variants are pending."""
    for i in range(0, len(items), size):
        yield items[i:i + size]


def _stackadapt_ad_stills(
    http: httpx.Client, ad_ids: list[str],
) -> Iterator[dict]:
    """Yield {ad_id, url} for StackAdapt ads that carry a still. Fetches
    only the requested ids (== Funnel ad_id) via AdFilters.ids, batched and
    paged. Each ad's attached creative is resolved to a renderable still."""
    if not ad_ids:
        return
    headers = {
        "Authorization": f"Bearer {settings.stackadapt_api_key}",
        "Content-Type": "application/json",
    }
    for batch in _chunked(list(ad_ids), 100):
        after: str | None = None
        for _ in range(MAX_PAGES):
            resp = http.post(
                STACKADAPT_GRAPHQL_URL,
                json={
                    "query": _STACKADAPT_ADS_QUERY,
                    "variables": {"ids": batch, "after": after},
                },
                headers=headers,
            )
            resp.raise_for_status()
            payload = resp.json()
            # StackAdapt returns auth/permission/schema problems as HTTP 200
            # with an `errors` body and data:null (e.g. "The access token
            # expired"). Treat that as a source failure — NOT a clean scan —
            # so the caller skips recording no_match for variants we never
            # actually got to look at.
            if payload.get("errors"):
                raise RuntimeError(
                    f"StackAdapt GraphQL error: {payload['errors']}"
                )
            conn = ((payload.get("data") or {}).get("ads")) or {}
            nodes = conn.get("nodes")
            if nodes is None:  # edges/node fallback
                nodes = [e.get("node") for e in conn.get("edges") or [] if e and e.get("node")]
            for node in nodes or []:
                node = node or {}
                ad_id = str(node.get("id") or "").strip()
                url = _stackadapt_still_from_ad(node)
                if ad_id and url:
                    yield {"ad_id": ad_id, "url": url}
            info = conn.get("pageInfo") or {}
            after = info.get("endCursor")
            if not info.get("hasNextPage") or not after:
                break


# ── GCS storage + signing ──────────────────────────────────────────────


def _asset_object_name(variant: str, ext: str) -> str:
    """creative-assets/{sha1(variant)}.{ext} — content-addressed by the
    variant so re-syncs overwrite in place and ad names never leak into
    object paths."""
    digest = hashlib.sha1(variant.encode("utf-8")).hexdigest()
    return f"{ASSETS_PREFIX}{digest}.{ext}"


def _download_image(http: httpx.Client, url: str) -> tuple[bytes, str, str] | None:
    """(bytes, extension, content_type) for an image URL, or None.

    Extension comes from the response content type first, the URL path
    second, jpg as the last resort — platform CDNs are inconsistent."""
    resp = http.get(url)
    resp.raise_for_status()
    if not resp.content:
        return None
    content_type = (resp.headers.get("content-type") or "").split(";")[0].strip().lower()
    ext = _EXT_BY_CONTENT_TYPE.get(content_type)
    if not ext:
        path_ext = urlparse(url).path.rsplit(".", 1)[-1].lower()
        ext = "jpg" if path_ext == "jpeg" else path_ext
        if ext not in _CONTENT_TYPE_BY_EXT:
            ext = "jpg"
        content_type = _CONTENT_TYPE_BY_EXT[ext]
    return resp.content, ext, content_type


def _store_bytes(data: bytes, object_name: str, content_type: str) -> None:
    """Upload to the shared resources bucket. Objects stay private — the
    bucket enforces Public Access Prevention — and reads are served
    through the backend image proxy with the runtime SA's ordinary
    storage access (signed URLs needed an IAM signBlob grant the staging
    SA doesn't hold, and the proxy removes that failure mode plus URL
    expiry entirely)."""
    from google.cloud import storage

    client = storage.Client(project=settings.gcp_project_id)
    blob = client.bucket(settings.alert_charts_bucket).blob(object_name)
    blob.cache_control = "private, max-age=604800"
    blob.upload_from_string(data, content_type=content_type)


def read_bytes(object_name: str) -> tuple[bytes, str] | None:
    """Download one stored asset for the image proxy endpoint.

    Returns (bytes, content_type), or None when the object is missing
    or unreadable. Best-effort by design: the Creative tab renders its
    placeholder frame when a thumbnail can't be served.
    """
    bucket_name = settings.alert_charts_bucket
    if not bucket_name or not object_name:
        return None
    from google.cloud import storage

    try:
        client = storage.Client(project=settings.gcp_project_id)
        blob = client.bucket(bucket_name).blob(object_name)
        data = blob.download_as_bytes()
        content_type = blob.content_type or "image/jpeg"
        return data, content_type
    except Exception:
        logger.warning("Asset read failed for %s", object_name, exc_info=True)
        return None


# ── BigQuery state ─────────────────────────────────────────────────────


def _tracked_projects() -> list[str]:
    """Project codes whose creatives get thumbnails: active flights plus
    anything that ended in the last 120 days. Ended campaigns still get
    looked at (retrospectives, client reporting, the Creative tab on a
    landed flight), and the image fetch is one-time-cheap — the daily
    retry guard keeps no_match variants from being hammered."""
    rows = bq.run_query(
        f"""
        SELECT project_code
        FROM {bq.table('dim_projects')}
        WHERE status = 'active'
           OR (end_date IS NOT NULL
               AND end_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 120 DAY))
        ORDER BY project_code
        """
    )
    return [r["project_code"] for r in rows if r.get("project_code")]


def _variant_map(project_code: str) -> dict[str, str]:
    """ad_name → creative_variant for one project, resolved by the SAME
    alias join + regex normalization the creative router uses (its
    _alias_resolution helper builds the SQL — reused, not reimplemented)."""
    alias_join, variant_expr = _alias_resolution("ad_agg")
    sql = f"""
        WITH ad_agg AS (
            SELECT f.ad_name, f.platform_id
            FROM {bq.table('fact_digital_daily')} f
            WHERE f.project_code = @project_code
              AND f.ad_name IS NOT NULL AND f.ad_name != ''
            GROUP BY f.ad_name, f.platform_id
        )
        SELECT
            ad_agg.ad_name,
            {variant_expr} AS creative_variant
        FROM ad_agg
        {alias_join}
    """
    rows = bq.run_query(sql, [bq.string_param("project_code", project_code)])
    return {
        r["ad_name"]: r["creative_variant"]
        for r in rows
        if r.get("ad_name") and r.get("creative_variant")
    }


def _stackadapt_adid_map(project_code: str) -> dict[str, str]:
    """StackAdapt ad_id → creative_variant for one project, resolved by the
    SAME alias join + regex normalization as _variant_map. StackAdapt's
    GraphQL keys ads by this id, so matching on it — not the creative-library
    name, which never lines up with the Funnel ad name — is what lets SA
    stills resolve. Several ad_ids (one per creative size) can map to one
    variant; the first that yields a still wins."""
    alias_join, variant_expr = _alias_resolution("ad_agg")
    sql = f"""
        WITH ad_agg AS (
            SELECT f.ad_id, f.ad_name, f.platform_id
            FROM {bq.table('fact_digital_daily')} f
            WHERE f.project_code = @project_code
              AND f.platform_id = '{STACKADAPT_PLATFORM_ID}'
              AND f.ad_id IS NOT NULL AND f.ad_id != ''
              AND f.ad_name IS NOT NULL AND f.ad_name != ''
            GROUP BY f.ad_id, f.ad_name, f.platform_id
        )
        SELECT ad_agg.ad_id, {variant_expr} AS creative_variant
        FROM ad_agg
        {alias_join}
    """
    rows = bq.run_query(sql, [bq.string_param("project_code", project_code)])
    return {
        r["ad_id"]: r["creative_variant"]
        for r in rows
        if r.get("ad_id") and r.get("creative_variant")
    }


def _asset_states() -> dict[str, dict]:
    """variant → latest creative_assets row. {} when the table is missing
    (first run before the migration lands — everything counts as pending)."""
    try:
        rows = bq.run_query(
            f"""
            SELECT variant, status, gcs_path, source_platform, checked_at
            FROM {bq.table('creative_assets')}
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY variant ORDER BY checked_at DESC
            ) = 1
            """
        )
        return {r["variant"]: r for r in rows if r.get("variant")}
    except Exception:
        logger.warning("creative_assets state read failed — treating all variants as pending", exc_info=True)
        return {}


def _needs_attempt(state: dict | None, now: datetime) -> bool:
    """A variant needs a fetch attempt unless it's already stored, or it
    was already attempted today — no_match / fetch_failed rows retry on
    later runs, capped at one attempt per UTC day."""
    if state is None:
        return True
    if state.get("status") == "stored" and state.get("gcs_path"):
        return False
    checked = state.get("checked_at")
    if checked is None or not hasattr(checked, "date"):
        return True
    return checked.date() < now.date()


def _record_asset(
    variant: str,
    project_code: str | None,
    source_platform: str | None,
    gcs_path: str | None,
    status: str,
) -> None:
    """Idempotent ledger write: MERGE on variant, latest attempt wins."""
    bq.run_query(
        f"""
        MERGE {bq.table('creative_assets')} t
        USING (SELECT @variant AS variant) s ON t.variant = s.variant
        WHEN MATCHED THEN UPDATE SET
            project_code = @project_code,
            source_platform = @source_platform,
            gcs_path = @gcs_path,
            status = @status,
            checked_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT
            (variant, project_code, source_platform, gcs_path, status, checked_at)
            VALUES (@variant, @project_code, @source_platform, @gcs_path, @status,
                    CURRENT_TIMESTAMP())
        """,
        [
            bq.string_param("variant", variant),
            bq.scalar_param("project_code", "STRING", project_code),
            bq.scalar_param("source_platform", "STRING", source_platform),
            bq.scalar_param("gcs_path", "STRING", gcs_path),
            bq.string_param("status", status),
        ],
    )


def _known_meta_adsets() -> dict[str, str]:
    """Normalized ad-set name → fact-side ad_set_name for Meta rows. The
    fact-side name is what the matrix slugs on, so it wins over whatever
    casing the API returns."""
    rows = bq.run_query(
        f"""
        SELECT DISTINCT ad_set_name
        FROM {bq.table('fact_digital_daily')}
        WHERE platform_id = @platform_id
          AND ad_set_name IS NOT NULL AND ad_set_name != ''
        """,
        [bq.string_param("platform_id", META_PLATFORM_ID)],
    )
    out: dict[str, str] = {}
    for r in rows:
        name = r.get("ad_set_name") or ""
        if name:
            out.setdefault(_normalize_adset_name(name), name)
    return out


def _normalize_adset_name(name: str) -> str:
    """Whitespace-collapsed, lowercased match key for ad-set names."""
    return " ".join(name.split()).lower()


def _record_targeting(
    audience_key: str,
    platform_id: str,
    persona: str | None,
    pool_size: int | None,
) -> None:
    """Idempotent targeting write: MERGE on (audience_key, platform_id)."""
    bq.run_query(
        f"""
        MERGE {bq.table('adset_targeting')} t
        USING (
            SELECT @audience_key AS audience_key, @platform_id AS platform_id
        ) s
          ON t.audience_key = s.audience_key
         AND t.platform_id = s.platform_id
        WHEN MATCHED THEN UPDATE SET
            persona = @persona,
            pool_size = @pool_size,
            fetched_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT
            (audience_key, platform_id, persona, pool_size, fetched_at)
            VALUES (@audience_key, @platform_id, @persona, @pool_size,
                    CURRENT_TIMESTAMP())
        """,
        [
            bq.string_param("audience_key", audience_key),
            bq.string_param("platform_id", platform_id),
            bq.scalar_param("persona", "STRING", persona),
            bq.scalar_param("pool_size", "INT64", pool_size),
        ],
    )


# ── time budget ────────────────────────────────────────────────────────
#
# The sync runs inside a Cloud Run request (300s service timeout). The
# first wide-scope run blew past it and the request died with nothing
# usable returned, so both syncs now work against a deadline: store as
# you scan, stop cleanly when time runs out, report complete=False, and
# let the next run (daily or manual) pick up where this one left off.

SYNC_TIME_BUDGET_SECS = 240.0


class _Deadline:
    def __init__(self, ends_at: float):
        self.ends_at = ends_at

    @classmethod
    def in_secs(cls, secs: float) -> "_Deadline":
        return cls(time.monotonic() + secs)

    def exceeded(self) -> bool:
        return time.monotonic() >= self.ends_at

    def remaining(self) -> float:
        return max(0.0, self.ends_at - time.monotonic())


# ── sync 1: creative images ────────────────────────────────────────────


def _store_variant_image(
    http: httpx.Client,
    variant: str,
    project_code: str,
    url: str,
    source: str,
    counts: dict,
) -> None:
    """Download → GCS → ledger for one matched variant. Inline during the
    scan so a timed-out run keeps everything it already found. Only
    unresolved variants reach here (stored stills are never refetched), so
    a failed download simply records fetch_failed for a later retry."""

    def _failed() -> None:
        counts["fetch_failed"] += 1
        _record_asset(variant, project_code, source, None, "fetch_failed")

    try:
        downloaded = _download_image(http, url)
        if not downloaded:
            _failed()
            return
        data, ext, content_type = downloaded
        object_name = _asset_object_name(variant, ext)
        _store_bytes(data, object_name, content_type)
        _record_asset(variant, project_code, source, object_name, "stored")
        counts["stored"] += 1
    except Exception:
        logger.warning("Creative image sync failed for variant %s", variant, exc_info=True)
        try:
            _failed()
        except Exception:
            logger.warning("Ledger write failed for variant %s", variant, exc_info=True)


def sync_creative_images(
    deadline: _Deadline | None = None,
    force: bool = False,
) -> dict:
    """Find and store stills for creative variants that lack one.

    Meta first, then StackAdapt for whatever's left. Matches are stored
    the moment they're found; `no_match` is only recorded when BOTH
    sources were fully scanned inside the budget AND neither source
    failed — running out of time or a broken source must not look like
    "this creative doesn't exist on the platforms". `force` retries
    no_match / fetch_failed variants regardless of the daily guard;
    already-stored stills are never refetched (both platforms hold the
    original asset, and re-downloading healthy images just hammers the
    APIs and starves the unresolved variants). Counts and per-source
    status come back for the admin endpoint; failures are logged per
    item and never raise.
    """
    deadline = deadline or _Deadline.in_secs(SYNC_TIME_BUDGET_SECS)
    sources = {
        "meta": "ok" if settings.meta_access_token else "skipped",
        "stackadapt": "ok" if settings.stackadapt_api_key else "skipped",
    }
    if not settings.meta_access_token and not settings.stackadapt_api_key:
        logger.info("Creative image sync skipped — no platform tokens configured")
        return {
            "status": "skipped", "reason": "no_tokens", "complete": True,
            "sources": sources,
            "pending": 0, "stored": 0, "no_match": 0, "fetch_failed": 0,
        }

    counts = {"stored": 0, "no_match": 0, "fetch_failed": 0}

    # ── what needs an image ─────────────────────────────────────────
    ad_name_to_variant: dict[str, tuple[str, str]] = {}
    try:
        for project_code in _tracked_projects():
            for ad_name, variant in _variant_map(project_code).items():
                ad_name_to_variant.setdefault(ad_name, (variant, project_code))
    except Exception:
        logger.warning("Creative image sync: variant enumeration failed", exc_info=True)
        return {
            "status": "error", "complete": False, "sources": sources,
            "pending": 0, **counts,
        }

    states = _asset_states()
    now = datetime.now(timezone.utc)
    pending: dict[str, str] = {}  # variant → project_code
    for variant, project_code in ad_name_to_variant.values():
        if variant in pending:
            continue
        state = states.get(variant)
        stored = bool(
            state and state.get("status") == "stored" and state.get("gcs_path")
        )
        if force:
            # Force retries no_match / fetch_failed past the once-per-day
            # guard, but never refetches a stored still — this matches the
            # /creative-assets/sync contract. Both Meta (full size since the
            # one-time 64px heal) and StackAdapt hold the original asset, so
            # re-downloading healthy images only hammers the platform APIs
            # and starves the unresolved variants force is actually for.
            if stored:
                continue
            pending[variant] = project_code
        elif _needs_attempt(state, now):
            pending[variant] = project_code

    if not pending:
        return {
            "status": "success", "complete": True, "sources": sources,
            "pending": 0, **counts,
        }

    # ── scan sources, storing matches as they appear ────────────────
    remaining = dict(pending)
    scanned_all = True
    with _http() as http:
        if settings.meta_access_token:
            try:
                for account_id in _meta_ad_accounts(http):
                    if deadline.exceeded() or not remaining:
                        scanned_all = scanned_all and not remaining
                        break
                    hash_cache: dict[str, str | None] = {}
                    try:
                        for ad in _meta_ads(http, account_id):
                            if deadline.exceeded():
                                scanned_all = False
                                break
                            mapped = ad_name_to_variant.get(ad.get("name") or "")
                            if not mapped:
                                continue
                            variant = mapped[0]
                            if variant not in remaining:
                                continue
                            url = _meta_best_image(
                                http, account_id, ad.get("creative"), hash_cache,
                            )
                            if url:
                                _store_variant_image(
                                    http, variant, remaining.pop(variant),
                                    url, "meta", counts,
                                )
                    except Exception:
                        logger.warning("Meta ads listing failed for %s", account_id, exc_info=True)
                        sources["meta"] = "failed"
                        scanned_all = False
            except Exception:
                logger.warning("Meta ad account enumeration failed", exc_info=True)
                sources["meta"] = "failed"
                scanned_all = False

        if settings.stackadapt_api_key and remaining:
            if deadline.exceeded():
                scanned_all = False
            else:
                try:
                    # StackAdapt is keyed by ad id, not creative name: build
                    # ad_id → variant for the projects that still have
                    # unresolved variants, then fetch exactly those ads.
                    adid_to_variant: dict[str, str] = {}
                    for project_code in set(remaining.values()):
                        for ad_id, variant in _stackadapt_adid_map(project_code).items():
                            if variant in remaining:
                                adid_to_variant[ad_id] = variant
                    for sa in _stackadapt_ad_stills(http, list(adid_to_variant)):
                        if deadline.exceeded():
                            scanned_all = False
                            break
                        variant = adid_to_variant.get(sa["ad_id"])
                        if not variant or variant not in remaining:
                            continue
                        _store_variant_image(
                            http, variant, remaining.pop(variant),
                            sa["url"], "stackadapt", counts,
                        )
                except Exception:
                    logger.warning("StackAdapt creative listing failed", exc_info=True)
                    sources["stackadapt"] = "failed"
                    scanned_all = False

        # ── no_match — only when the scan actually finished ─────────
        if scanned_all and not deadline.exceeded():
            for variant, project_code in remaining.items():
                try:
                    _record_asset(variant, project_code, None, None, "no_match")
                    counts["no_match"] += 1
                except Exception:
                    logger.warning("Ledger write failed for variant %s", variant, exc_info=True)
        elif remaining:
            logger.info(
                "Creative image sync stopped with %d variants unresolved "
                "(budget or source failure) — they stay pending for the "
                "next run", len(remaining),
            )

    return {
        "status": "success",
        "complete": scanned_all and not deadline.exceeded(),
        "sources": sources,
        "pending": len(pending),
        **counts,
    }


# ── sync 2: ad-set targeting personas ──────────────────────────────────


# Stop asking Meta for delivery estimates after this many consecutive
# failures in one run: when the API doesn't support it (or the ad sets
# are all ended), each refusal costs two HTTP round-trips and personas
# don't need it.
ESTIMATE_BREAKER_THRESHOLD = 6


def sync_adset_targeting(deadline: _Deadline | None = None) -> dict:
    """Render Meta targeting specs into personas for ADA's ad sets.

    Only ad sets whose names appear in fact_digital_daily (platform meta)
    are written — the matrix can't render targeting for ad sets it has
    never heard of. Keyed by the matrix endpoint's own audience slug.
    Writes happen per ad set, so a timed-out run keeps its progress.
    """
    deadline = deadline or _Deadline.in_secs(SYNC_TIME_BUDGET_SECS)
    if not settings.meta_access_token:
        logger.info("Ad-set targeting sync skipped — META_ACCESS_TOKEN not set")
        return {
            "status": "skipped", "reason": "no_token", "complete": True,
            "matched": 0, "written": 0,
        }

    try:
        known = _known_meta_adsets()
    except Exception:
        logger.warning("Ad-set targeting sync: known ad-set read failed", exc_info=True)
        return {"status": "error", "complete": False, "matched": 0, "written": 0}
    if not known:
        return {"status": "success", "complete": True, "matched": 0, "written": 0}

    matched = written = 0
    complete = True
    estimate_failures = 0
    with _http() as http:
        try:
            accounts = _meta_ad_accounts(http)
        except Exception:
            logger.warning("Meta ad account enumeration failed", exc_info=True)
            accounts = []
        for account_id in accounts:
            if deadline.exceeded():
                complete = False
                break
            try:
                for adset in _meta_adsets(http, account_id):
                    if deadline.exceeded():
                        complete = False
                        break
                    fact_name = known.get(_normalize_adset_name(adset.get("name") or ""))
                    if not fact_name:
                        continue
                    matched += 1
                    try:
                        persona = render_persona(adset.get("targeting"))
                        pool = None
                        if (
                            adset.get("id")
                            and estimate_failures < ESTIMATE_BREAKER_THRESHOLD
                            and deadline.remaining() > 15
                        ):
                            pool = _adset_pool_size(http, adset["id"])
                            estimate_failures = (
                                0 if pool is not None else estimate_failures + 1
                            )
                            if estimate_failures == ESTIMATE_BREAKER_THRESHOLD:
                                logger.info(
                                    "Delivery estimates unavailable %d times in a "
                                    "row — skipping for the rest of this run",
                                    ESTIMATE_BREAKER_THRESHOLD,
                                )
                        if persona is None and pool is None:
                            continue
                        _record_targeting(
                            _audience_id(fact_name, META_PLATFORM_ID),
                            META_PLATFORM_ID,
                            persona,
                            pool,
                        )
                        written += 1
                    except Exception:
                        logger.warning("Targeting write failed for ad set %s", fact_name, exc_info=True)
            except Exception:
                logger.warning("Meta ad set listing failed for %s", account_id, exc_info=True)

    return {
        "status": "success", "complete": complete,
        "matched": matched, "written": written,
    }


# ── orchestration ──────────────────────────────────────────────────────


def run_sync(
    budget_secs: float = SYNC_TIME_BUDGET_SECS,
    force: bool = False,
) -> dict:
    """Run both Phase 19 syncs inside one time budget. Images get the
    first ~55%, targeting runs to the end of the full budget — both
    report complete=False when they ran out of road, and repeated runs
    converge. `force` retries no_match/fetch_failed image variants past
    the daily guard and re-downloads Meta-sourced stored stills (64px
    thumbnail healing). Never raises: the daily pipeline and the admin
    endpoint both treat this as best-effort."""
    started = time.monotonic()
    image_deadline = _Deadline(started + budget_secs * 0.55)
    full_deadline = _Deadline(started + budget_secs)
    try:
        images = sync_creative_images(image_deadline, force=force)
    except Exception as e:
        logger.error("Creative image sync crashed: %s", e, exc_info=True)
        images = {"status": "error", "error": str(e)}
    try:
        targeting = sync_adset_targeting(full_deadline)
    except Exception as e:
        logger.error("Ad-set targeting sync crashed: %s", e, exc_info=True)
        targeting = {"status": "error", "error": str(e)}
    return {"images": images, "targeting": targeting}
