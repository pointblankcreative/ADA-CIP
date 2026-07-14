"""Tests for the in-process TTL cache on the project list/detail rollup
(ADA 1215989989045649).

Covers cache behaviour (cold miss, warm hit, TTL expiry, refresh bypass, key
isolation, per-code detail, TTL<=0 disables) and the synchronous router-side
invalidation that keeps the money numbers fresh after a mutation — the UAT gap:
update_line_is_direct, api_sync_media_plan and daily_run (admin router) plus
run_pacing_single and run_pacing (pacing router).

Deterministic: projects._now is monkeypatched to a controllable fake clock and
bq.run_query is a MagicMock with a call counter — no wall-clock, no BigQuery.
"""

import asyncio
from unittest.mock import MagicMock

import pytest

from backend.routers import admin as admin_router
from backend.routers import pacing as pacing_router
from backend.routers import projects
from backend.routers.admin import IsDirectOverrideUpdate


# ── Fixtures / helpers ───────────────────────────────────────────────


class FakeClock:
    """Deterministic monotonic clock seam for the TTL cache."""

    def __init__(self, t: float = 1000.0):
        self.t = t

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> None:
        self.t += dt


# Minimal rows — the endpoints only require project_code + project_name and
# .get() the rest with defaults.
ROWS = [{"project_code": "25042", "project_name": "Test Project"}]


@pytest.fixture(autouse=True)
def _reset_caches():
    """No cross-test cache leakage."""
    projects._LIST_CACHE.clear()
    projects._DETAIL_CACHE.clear()
    yield
    projects._LIST_CACHE.clear()
    projects._DETAIL_CACHE.clear()


@pytest.fixture
def clock(monkeypatch):
    c = FakeClock()
    monkeypatch.setattr(projects, "_now", c)
    return c


@pytest.fixture
def bq_mock(monkeypatch):
    m = MagicMock(return_value=ROWS)
    monkeypatch.setattr(projects.bq, "run_query", m)
    return m


def _set_ttl(monkeypatch, ttl):
    monkeypatch.setattr(projects.settings, "projects_cache_ttl_seconds", ttl)


def _list(status=None, include_recently_ended=True, refresh=False):
    return asyncio.run(
        projects.list_projects(
            status=status,
            include_recently_ended=include_recently_ended,
            refresh=refresh,
        )
    )


def _detail(code, refresh=False):
    return asyncio.run(projects.get_project(code, refresh=refresh))


# ── Cache behaviour ──────────────────────────────────────────────────


def test_cold_miss_queries_once(monkeypatch, clock, bq_mock):
    _set_ttl(monkeypatch, 60)
    res = _list()
    assert bq_mock.call_count == 1
    assert res[0].project_code == "25042"


def test_warm_hit_within_ttl(monkeypatch, clock, bq_mock):
    _set_ttl(monkeypatch, 60)
    _list()
    clock.advance(30)  # still < ttl
    res = _list()
    assert bq_mock.call_count == 1  # served from cache
    assert res[0].project_code == "25042"


def test_ttl_expiry_requeries(monkeypatch, clock, bq_mock):
    _set_ttl(monkeypatch, 60)
    _list()
    clock.advance(60)  # >= ttl → hit is invalid (strict <)
    _list()
    assert bq_mock.call_count == 2


def test_refresh_bypasses_and_repopulates(monkeypatch, clock, bq_mock):
    _set_ttl(monkeypatch, 60)
    _list()  # count 1, warm
    _list(refresh=True)  # bypass warm entry → count 2, overwrites
    assert bq_mock.call_count == 2
    _list()  # warm again (no clock advance) → hit
    assert bq_mock.call_count == 2


def test_key_isolation(monkeypatch, clock, bq_mock):
    _set_ttl(monkeypatch, 60)
    _list(status="active", include_recently_ended=True)   # key A → 1
    _list(status=None, include_recently_ended=True)        # key B → 2
    _list(status=None, include_recently_ended=False)       # key C → 3
    assert bq_mock.call_count == 3
    # every distinct key is now independently warm
    _list(status="active", include_recently_ended=True)
    _list(status=None, include_recently_ended=True)
    _list(status=None, include_recently_ended=False)
    assert bq_mock.call_count == 3


def test_detail_caches_per_code(monkeypatch, clock, bq_mock):
    _set_ttl(monkeypatch, 60)
    _detail("25042")
    assert bq_mock.call_count == 1
    _detail("25042")  # hit
    assert bq_mock.call_count == 1
    _detail("26009")  # different code → miss
    assert bq_mock.call_count == 2


def test_ttl_zero_disables_cache(monkeypatch, clock, bq_mock):
    _set_ttl(monkeypatch, 0)
    _list()
    _list()
    assert bq_mock.call_count == 2  # every call queries
    assert projects._LIST_CACHE == {}  # nothing stored
    _detail("25042")
    _detail("25042")
    assert bq_mock.call_count == 4
    assert projects._DETAIL_CACHE == {}


# ── Helper-level invalidation ────────────────────────────────────────


def test_invalidate_project_evicts_one_detail_and_clears_list(
    monkeypatch, clock, bq_mock
):
    _set_ttl(monkeypatch, 60)
    _detail("25042")   # 1
    _detail("26009")   # 2
    _list()            # 3
    assert bq_mock.call_count == 3

    projects.invalidate_project("25042")

    assert "25042" not in projects._DETAIL_CACHE
    assert "26009" in projects._DETAIL_CACHE   # unrelated detail survives
    assert projects._LIST_CACHE == {}          # list cleared wholesale

    _detail("26009")   # still a hit
    assert bq_mock.call_count == 3
    _detail("25042")   # evicted → re-queries
    assert bq_mock.call_count == 4
    _list()            # cleared → re-queries
    assert bq_mock.call_count == 5


def test_invalidate_all_clears_both(monkeypatch, clock, bq_mock):
    _set_ttl(monkeypatch, 60)
    _detail("25042")
    _detail("26009")
    _list()
    before = bq_mock.call_count

    projects.invalidate_all()

    assert projects._DETAIL_CACHE == {}
    assert projects._LIST_CACHE == {}
    _detail("25042")
    _detail("26009")
    _list()
    assert bq_mock.call_count == before + 3  # everything re-queried


# ── Router-path invalidation (the UAT gap) ───────────────────────────


def _prewarm(*codes, list_keys=((None, True),)):
    for code in codes:
        projects._DETAIL_CACHE[code] = (1000.0, "payload")
    for key in list_keys:
        projects._LIST_CACHE[key] = (1000.0, [])


def test_update_line_is_direct_invalidates_project(monkeypatch):
    _prewarm("25042", "26009")

    def qr(sql, params=None):
        # Only the line lookup SELECT should return a row.
        if "platform_id, budget, line_code" in sql:
            return [
                {"project_code": "25042", "platform_id": "",
                 "budget": 100.0, "line_code": ""}
            ]
        return []

    monkeypatch.setattr(admin_router.bq, "run_query", qr)
    # Re-pacer is imported inside the endpoint from backend.services.pacing.
    monkeypatch.setattr(
        "backend.services.pacing.run_pacing_for_project",
        lambda *a, **k: {"lines_processed": 0},
    )

    asyncio.run(
        admin_router.update_line_is_direct(
            "line-1", IsDirectOverrideUpdate(is_direct_override=True)
        )
    )

    assert "25042" not in projects._DETAIL_CACHE
    assert projects._LIST_CACHE == {}
    assert "26009" in projects._DETAIL_CACHE  # only the touched project evicted


def test_sync_media_plan_invalidates_project(monkeypatch):
    _prewarm("25042", "26009")
    monkeypatch.setattr(admin_router.bq, "run_query", lambda sql, params=None: [])
    monkeypatch.setattr(admin_router, "sync_media_plan", lambda **k: {"status": "ok"})

    asyncio.run(
        admin_router.api_sync_media_plan(
            sheet_id="sheet-1", project_code="25042", tab_name="Tab"
        )
    )

    assert "25042" not in projects._DETAIL_CACHE
    assert projects._LIST_CACHE == {}
    assert "26009" in projects._DETAIL_CACHE


def test_daily_run_invalidates_all(monkeypatch):
    _prewarm("25042", "26009")
    monkeypatch.setattr(admin_router, "run_daily_pipeline", lambda *a, **k: {"status": "ok"})

    asyncio.run(admin_router.daily_run())

    # A full pipeline re-paces every project → drop the whole cache.
    assert projects._DETAIL_CACHE == {}
    assert projects._LIST_CACHE == {}


def test_run_transformation_invalidates_all(monkeypatch):
    # "Full History Backfill" / daily transform reloads fact_digital_daily
    # (total_spend, the header "Spent $X"), which the rollup reads → drop all.
    _prewarm("25042", "26009")
    # FULL mode now checks ingestion_log for an in-flight run before starting
    # (ADA 1215990005858989); no active run → it starts normally.
    monkeypatch.setattr(admin_router.bq, "run_query", lambda *a, **k: [])
    monkeypatch.setattr(admin_router, "run_transformation", lambda *a, **k: {"status": "ok"})

    asyncio.run(admin_router.api_run_transformation("full"))

    assert projects._DETAIL_CACHE == {}
    assert projects._LIST_CACHE == {}


def test_run_pacing_single_invalidates_project(monkeypatch):
    _prewarm("25042", "26009")
    monkeypatch.setattr(
        pacing_router, "run_pacing_for_project", lambda *a, **k: {"lines_processed": 0}
    )

    asyncio.run(pacing_router.run_pacing_single("25042"))

    assert "25042" not in projects._DETAIL_CACHE
    assert projects._LIST_CACHE == {}
    assert "26009" in projects._DETAIL_CACHE


def test_run_pacing_bulk_invalidates_all(monkeypatch):
    _prewarm("25042", "26009")
    monkeypatch.setattr(pacing_router, "run_all_active", lambda *a, **k: {"projects": 0})

    asyncio.run(pacing_router.run_pacing())

    assert projects._DETAIL_CACHE == {}
    assert projects._LIST_CACHE == {}
