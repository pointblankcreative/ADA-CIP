"""Integration tests for the Orphan Auto-Discovery router.

Covers the HTTP surface, which is now read-only:
    - GET /api/orphan-projects (with include_dismissed query param)

Suppression is control-table-only — there are no dismiss/undismiss endpoints.
Service-layer BigQuery logic is covered in test_orphans_service.py; here the
service module is replaced with a recorder so we only test the router.
"""

from __future__ import annotations

from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import orphans as orphans_router


def _make_app() -> FastAPI:
    app = FastAPI()
    app.include_router(orphans_router.router)
    return app


class ServiceStub:
    """Swap for backend.services.orphans. Records calls + returns canned values."""

    def __init__(self):
        self.calls: list[tuple[str, dict]] = []
        self.scan_orphans_return: list[dict] = []

    def scan_orphans(self, include_dismissed: bool = False):
        self.calls.append(("scan_orphans", {"include_dismissed": include_dismissed}))
        return self.scan_orphans_return


@pytest.fixture
def stub(monkeypatch):
    s = ServiceStub()
    monkeypatch.setattr(orphans_router, "svc", s)
    return s


def _sample_orphan() -> dict:
    return {
        "project_code": "23061",
        "total_spend": 192069.10,
        "total_rows": 10627,
        "first_date": "2024-03-28",
        "last_date": "2026-03-28",
        "by_platform": [
            {"platform_id": "stackadapt", "spend": 89050.44, "row_count": 1737},
            {"platform_id": "meta", "spend": 72924.24, "row_count": 7264},
        ],
        "dismissed": False,
        "dismissed_at": None,
        "dismissed_by": None,
        "dismissed_reason": None,
        "level": None,
    }


# ── GET /api/orphan-projects ────────────────────────────────────────────────


def test_list_orphans_default_excludes_dismissed(stub):
    stub.scan_orphans_return = [_sample_orphan()]
    client = TestClient(_make_app())
    r = client.get("/api/orphan-projects")
    assert r.status_code == 200
    body = r.json()
    assert body["count"] == 1
    assert body["orphans"][0]["project_code"] == "23061"
    assert stub.calls == [("scan_orphans", {"include_dismissed": False})]


def test_list_orphans_with_include_dismissed(stub):
    stub.scan_orphans_return = []
    client = TestClient(_make_app())
    r = client.get("/api/orphan-projects?include_dismissed=true")
    assert r.status_code == 200
    assert r.json() == {"orphans": [], "count": 0}
    assert stub.calls == [("scan_orphans", {"include_dismissed": True})]


def test_no_write_endpoints_exist():
    """The dismiss/undismiss POST routes must be gone — suppression is table-only."""
    paths = {route.path for route in orphans_router.router.routes}
    assert not any("dismiss" in p for p in paths)


# ── Error wrapping ──────────────────────────────────────────────────────────


def test_list_orphans_wraps_service_exception_as_500(monkeypatch):
    class Boom:
        def scan_orphans(self, include_dismissed: bool = False):
            raise RuntimeError("bq down")

    monkeypatch.setattr(orphans_router, "svc", Boom())
    client = TestClient(_make_app())
    r = client.get("/api/orphan-projects")
    assert r.status_code == 500
    assert "bq down" in r.json()["detail"]
