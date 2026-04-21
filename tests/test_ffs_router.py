"""Integration tests for the FFS router HTTP surface.

Covers the thin layer between the FastAPI endpoints and the service:
    - Request body validation (Pydantic)
    - created_by is read from request.state.user["email"]
    - 404 on missing entries and lines
    - 400 when line override body is malformed
    - DELETE cleanup + APPLY reassignment routes

Service-layer behaviour is covered in test_ffs_entries.py; here the
service module is replaced with a recorder so we only test the router.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import ffs


# ── Test app that injects a stub user (mimics FirebaseAuthMiddleware) ───────


def _make_app(user_email: str | None = "frazer@pointblank.co") -> FastAPI:
    app = FastAPI()

    @app.middleware("http")
    async def _inject_user(request, call_next):
        if user_email is not None:
            request.state.user = {"uid": "test", "email": user_email}
        return await call_next(request)

    app.include_router(ffs.router)
    return app


class ServiceStub:
    """Swap for backend.services.ffs_entries. Records calls + returns canned values."""

    def __init__(self):
        self.calls: list[tuple[str, dict]] = []
        self.list_entries_return: list[dict] = []
        self.create_entry_return: dict = {}
        self.update_entry_return: dict | None = {}
        self.delete_entry_return: bool = True
        self.apply_to_lines_return: dict = {}
        self.set_line_override_return: dict = {}
        self.clear_line_override_return: dict = {}
        self.raise_value_error: bool = False

    def _record(self, name: str, **kwargs: Any) -> None:
        self.calls.append((name, kwargs))

    def list_entries(self, project_code):
        self._record("list_entries", project_code=project_code)
        return self.list_entries_return

    def create_entry(self, **kwargs):
        self._record("create_entry", **kwargs)
        return self.create_entry_return

    def update_entry(self, **kwargs):
        self._record("update_entry", **kwargs)
        return self.update_entry_return

    def delete_entry(self, project_code, entry_id):
        self._record("delete_entry", project_code=project_code, entry_id=entry_id)
        return self.delete_entry_return

    def apply_to_lines(self, **kwargs):
        self._record("apply_to_lines", **kwargs)
        if self.raise_value_error:
            raise ValueError("entry missing")
        return self.apply_to_lines_return

    def set_line_override(self, **kwargs):
        self._record("set_line_override", **kwargs)
        return self.set_line_override_return

    def clear_line_override(self, **kwargs):
        self._record("clear_line_override", **kwargs)
        return self.clear_line_override_return


@pytest.fixture
def stub(monkeypatch):
    s = ServiceStub()
    monkeypatch.setattr(ffs, "svc", s)
    return s


def _sample_response() -> dict:
    return {
        "entry_id": "entry-1",
        "project_code": "25042",
        "label": "underfunded.ca",
        "lp_url": "https://underfunded.ca",
        "is_platform_form": False,
        "platform_id": None,
        "ffs_inputs": {"field_count": 5, "required_fields": 3, "field_types": [],
                       "clicks_to_submit": 1, "below_fold_mobile": False,
                       "has_autofill": True, "is_platform_form": False},
        "ffs_score": 33.0,
        "created_at": "2026-04-20 10:00:00",
        "updated_at": "2026-04-20 10:00:00",
        "created_by": "frazer@pointblank.co",
        "linked_line_count": 0,
    }


def _sample_create_body() -> dict:
    return {
        "label": "underfunded.ca",
        "lp_url": "https://underfunded.ca",
        "is_platform_form": False,
        "platform_id": None,
        "ffs_inputs": {
            "field_count": 5,
            "required_fields": 3,
            "field_types": ["text_name", "text_email"],
            "clicks_to_submit": 1,
            "below_fold_mobile": False,
            "has_autofill": True,
            "is_platform_form": False,
        },
        "applied_line_ids": [],
    }


# ── GET /api/ffs/{project_code} ─────────────────────────────────────────────


def test_list_entries_returns_rows(stub):
    stub.list_entries_return = [_sample_response()]
    client = TestClient(_make_app())
    r = client.get("/api/ffs/25042")
    assert r.status_code == 200
    assert len(r.json()) == 1
    assert stub.calls == [("list_entries", {"project_code": "25042"})]


# ── POST /api/ffs/{project_code} ────────────────────────────────────────────


def test_create_reads_email_from_request_state(stub):
    stub.create_entry_return = _sample_response()
    client = TestClient(_make_app(user_email="frazer@pointblank.co"))
    r = client.post("/api/ffs/25042", json=_sample_create_body())
    assert r.status_code == 200, r.text
    _, kwargs = stub.calls[0]
    assert kwargs["created_by"] == "frazer@pointblank.co"


def test_create_forces_is_platform_form_in_inputs_when_top_level_true(stub):
    """Arch B consistency: if body says is_platform_form=TRUE, the inputs
    passed to compute_ffs must also carry that flag so the score reflects
    the -5 discount, even if the client forgot to set it in ffs_inputs."""
    stub.create_entry_return = _sample_response()
    body = _sample_create_body()
    body["is_platform_form"] = True
    body["ffs_inputs"]["is_platform_form"] = False  # client sent wrong flag
    client = TestClient(_make_app())
    client.post("/api/ffs/25042", json=body)
    _, kwargs = stub.calls[0]
    assert kwargs["ffs_inputs"]["is_platform_form"] is True


def test_create_rejects_missing_ffs_inputs(stub):
    client = TestClient(_make_app())
    r = client.post("/api/ffs/25042", json={"label": "oops"})
    assert r.status_code == 422  # Pydantic validation


# ── PATCH /api/ffs/{project_code}/{entry_id} ────────────────────────────────


def test_patch_returns_404_when_service_returns_none(stub):
    stub.update_entry_return = None
    client = TestClient(_make_app())
    r = client.patch("/api/ffs/25042/missing", json={"label": "new"})
    assert r.status_code == 404


def test_patch_partial_update(stub):
    stub.update_entry_return = _sample_response()
    client = TestClient(_make_app())
    r = client.patch("/api/ffs/25042/entry-1", json={"label": "renamed"})
    assert r.status_code == 200
    _, kwargs = stub.calls[0]
    assert kwargs["label"] == "renamed"
    # Unspecified fields should pass through as None so the service preserves them
    assert kwargs["lp_url"] is None
    assert kwargs["ffs_inputs"] is None


# ── DELETE /api/ffs/{project_code}/{entry_id} ───────────────────────────────


def test_delete_returns_404_when_service_returns_false(stub):
    stub.delete_entry_return = False
    client = TestClient(_make_app())
    r = client.delete("/api/ffs/25042/missing")
    assert r.status_code == 404


def test_delete_returns_status(stub):
    stub.delete_entry_return = True
    client = TestClient(_make_app())
    r = client.delete("/api/ffs/25042/entry-1")
    assert r.status_code == 200
    assert r.json() == {"status": "deleted", "entry_id": "entry-1"}


# ── POST /api/ffs/{project_code}/{entry_id}/apply ───────────────────────────


def test_apply_404_when_service_raises_value_error(stub):
    stub.raise_value_error = True
    client = TestClient(_make_app())
    r = client.post("/api/ffs/25042/missing/apply", json={"line_ids": ["a"]})
    assert r.status_code == 404


def test_apply_happy_path(stub):
    stub.apply_to_lines_return = {
        "entry_id": "entry-1", "linked_line_ids": ["a", "b"],
        "added": ["b"], "removed": [],
    }
    client = TestClient(_make_app())
    r = client.post("/api/ffs/25042/entry-1/apply", json={"line_ids": ["a", "b"]})
    assert r.status_code == 200
    assert r.json()["added"] == ["b"]


# ── POST /api/ffs/{project_code}/lines/{line_id}/override ───────────────────


def test_override_set_requires_ffs_inputs_when_not_clearing(stub):
    client = TestClient(_make_app())
    r = client.post("/api/ffs/25042/lines/line-x/override", json={"clear": False})
    assert r.status_code == 400


def test_override_clear_does_not_require_ffs_inputs(stub):
    stub.clear_line_override_return = {"line_id": "line-x", "ffs_score": None,
                                         "ffs_override": False,
                                         "resynced_from_entry": False}
    client = TestClient(_make_app())
    r = client.post("/api/ffs/25042/lines/line-x/override", json={"clear": True})
    assert r.status_code == 200
    assert stub.calls[0][0] == "clear_line_override"


def test_override_set_calls_service_with_inputs(stub):
    stub.set_line_override_return = {"line_id": "line-x", "ffs_score": 42.0,
                                      "ffs_override": True}
    body = {
        "ffs_inputs": {
            "field_count": 8, "required_fields": 6, "field_types": [],
            "clicks_to_submit": 1, "below_fold_mobile": True,
            "has_autofill": False, "is_platform_form": False,
        }
    }
    client = TestClient(_make_app())
    r = client.post("/api/ffs/25042/lines/line-x/override", json=body)
    assert r.status_code == 200
    name, kwargs = stub.calls[0]
    assert name == "set_line_override"
    assert kwargs["ffs_inputs"]["field_count"] == 8
