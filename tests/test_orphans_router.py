"""Integration tests for the Orphan Auto-Discovery router.

Covers the HTTP surface:
    - GET /api/orphan-projects (with include_dismissed query param)
    - POST /api/orphan-projects/{code}/dismiss (reads email from request.state.user)
    - POST /api/orphan-projects/{code}/undismiss (404 if not found)

Service-layer BigQuery logic is covered in test_orphans_service.py; here the
service module is replaced with a recorder so we only test the router.
"""

from __future__ import annotations

from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import orphans as orphans_router


def _make_app(user_email: str | None = "frazer@pointblank.co") -> FastAPI:
    app = FastAPI()

    @app.middleware("http")
    async def _inject_user(request, call_next):
        if user_email is not None:
            request.state.user = {"uid": "test", "email": user_email}
        return await call_next(request)

    app.include_router(orphans_router.router)
    return app


class ServiceStub:
    """Swap for backend.services.orphans. Records calls + returns canned values."""

    def __init__(self):
        self.calls: list[tuple[str, dict]] = []
        self.scan_orphans_return: list[dict] = []
        self.dismiss_return: dict = {}
        self.undismiss_return: bool = True

    def _record(self, name: str, **kwargs: Any) -> None:
        self.calls.append((name, kwargs))

    def scan_orphans(self, include_dismissed: bool = False):
        self._record("scan_orphans", include_dismissed=include_dismissed)
        return self.scan_orphans_return

    def dismiss(self, **kwargs):
        self._record("dismiss", **kwargs)
        return self.dismiss_return

    def undismiss(self, project_code: str):
        self._record("undismiss", project_code=project_code)
        return self.undismiss_return


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


# ── POST /api/orphan-projects/{code}/dismiss ────────────────────────────────


def test_dismiss_reads_email_from_request_state(stub):
    dismissed = dict(_sample_orphan())
    dismissed.update({
        "dismissed": True,
        "dismissed_at": "2026-04-21T10:00:00+00:00",
        "dismissed_by": "frazer@pointblank.co",
        "dismissed_reason": "old test account",
    })
    stub.dismiss_return = dismissed

    client = TestClient(_make_app(user_email="frazer@pointblank.co"))
    r = client.post(
        "/api/orphan-projects/23061/dismiss",
        json={"reason": "old test account"},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["dismissed"] is True
    assert body["dismissed_by"] == "frazer@pointblank.co"

    _, kwargs = stub.calls[0]
    assert kwargs["project_code"] == "23061"
    assert kwargs["dismissed_by"] == "frazer@pointblank.co"
    assert kwargs["reason"] == "old test account"


def test_dismiss_with_no_reason(stub):
    stub.dismiss_return = _sample_orphan()
    client = TestClient(_make_app())
    r = client.post("/api/orphan-projects/23061/dismiss", json={})
    assert r.status_code == 200
    _, kwargs = stub.calls[0]
    assert kwargs["reason"] is None


def test_dismiss_without_authenticated_user(stub):
    """If request.state.user isn't set, dismissed_by should be None (not error)."""
    stub.dismiss_return = _sample_orphan()
    client = TestClient(_make_app(user_email=None))
    r = client.post("/api/orphan-projects/23061/dismiss", json={"reason": "x"})
    assert r.status_code == 200
    _, kwargs = stub.calls[0]
    assert kwargs["dismissed_by"] is None


# ── POST /api/orphan-projects/{code}/undismiss ──────────────────────────────


def test_undismiss_success(stub):
    stub.undismiss_return = True
    client = TestClient(_make_app())
    r = client.post("/api/orphan-projects/23061/undismiss")
    assert r.status_code == 200
    assert r.json() == {"status": "undismissed", "project_code": "23061"}
    assert stub.calls == [("undismiss", {"project_code": "23061"})]


def test_undismiss_404_when_not_found(stub):
    stub.undismiss_return = False
    client = TestClient(_make_app())
    r = client.post("/api/orphan-projects/99999/undismiss")
    assert r.status_code == 404


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
