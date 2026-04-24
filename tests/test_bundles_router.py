"""Integration tests for the bundle Confirm/Clear endpoints (ADAC-54 follow-up).

Covers:
    POST   /api/admin/bundles/{bundle_id}/confirm
    DELETE /api/admin/bundles/{bundle_id}/override

The endpoints are inline SQL inside ``backend/routers/admin.py`` rather than
delegating to a service module, so these tests stub ``bq.run_query`` and
assert on the call sequence + parameters.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import admin as admin_router


# ── Helpers ──────────────────────────────────────────────────────────


def _make_app(user_email: str | None = "frazer@pointblank.co") -> FastAPI:
    """Minimal FastAPI app with the admin router and the user-injecting
    middleware that mirrors what FirebaseAuthMiddleware does in production."""
    app = FastAPI()

    @app.middleware("http")
    async def _inject_user(request, call_next):
        if user_email is not None:
            request.state.user = {"uid": "test", "email": user_email}
        return await call_next(request)

    app.include_router(admin_router.router)
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
    """Mimic backend.services.bigquery_client.string_param shape."""
    return ("string", name, value)


def _scalar_param(name, ptype, value):
    return ("scalar", name, ptype, value)


def _table(name):
    return f"`dummy.{name}`"


# ── confirm_bundle ──────────────────────────────────────────────────


def test_confirm_bundle_404_when_no_lines_match():
    """404 when no media_plan_lines row matches (project_code, bundle_id).
    Catches typos in URLs before they create dangling override rows."""
    rec = QueryRecorder()
    rec.responses = [[{"n": 0}]]  # _verify_bundle_exists returns 0

    with patch.object(admin_router.bq, "run_query", side_effect=rec), \
         patch.object(admin_router.bq, "string_param", _string_param), \
         patch.object(admin_router.bq, "scalar_param", _scalar_param), \
         patch.object(admin_router.bq, "table", _table):
        client = TestClient(_make_app())
        resp = client.post(
            "/api/admin/bundles/25034-meta-09/confirm?project_code=25034",
        )
    assert resp.status_code == 404
    assert "25034-meta-09" in resp.json()["detail"]


def test_confirm_bundle_writes_override_and_updates_lines():
    """Happy path: verifies bundle exists, MERGEs the override, then
    UPDATEs the live media_plan_lines rows. Three queries in order."""
    rec = QueryRecorder()
    rec.responses = [
        [{"n": 6}],   # _verify_bundle_exists: parent + 5 children
        [],           # MERGE returns nothing
        [],           # UPDATE returns nothing
    ]

    with patch.object(admin_router.bq, "run_query", side_effect=rec), \
         patch.object(admin_router.bq, "string_param", _string_param), \
         patch.object(admin_router.bq, "scalar_param", _scalar_param), \
         patch.object(admin_router.bq, "table", _table):
        client = TestClient(_make_app(user_email="frazer@pointblank.co"))
        resp = client.post(
            "/api/admin/bundles/25034-meta-09/confirm?project_code=25034",
        )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body == {
        "status": "confirmed",
        "project_code": "25034",
        "bundle_id": "25034-meta-09",
        "members_updated": 6,
    }

    # Three queries total: verify, MERGE, UPDATE.
    assert len(rec.calls) == 3

    # Verify SQL: SELECT COUNT(*) ... media_plan_lines
    assert "media_plan_lines" in rec.calls[0][0]
    assert "COUNT(*)" in rec.calls[0][0]

    # MERGE SQL writes to media_plan_bundle_overrides with role 'confirmed_parent'
    merge_sql = rec.calls[1][0]
    assert "media_plan_bundle_overrides" in merge_sql
    assert "MERGE" in merge_sql
    assert "'confirmed_parent'" in merge_sql

    # UPDATE SQL hits media_plan_lines with the parent/child CASE
    update_sql = rec.calls[2][0]
    assert "media_plan_lines" in update_sql
    assert "UPDATE" in update_sql
    assert "'confirmed_child'" in update_sql
    assert "'confirmed_parent'" in update_sql


def test_confirm_bundle_records_user_email_when_iap_passed_one():
    """The user's email from request.state.user lands on the override row
    so we have an audit trail when proper RBAC ships."""
    rec = QueryRecorder()
    rec.responses = [
        [{"n": 2}],
        [],
        [],
    ]

    with patch.object(admin_router.bq, "run_query", side_effect=rec), \
         patch.object(admin_router.bq, "string_param", _string_param), \
         patch.object(admin_router.bq, "scalar_param", _scalar_param), \
         patch.object(admin_router.bq, "table", _table):
        client = TestClient(_make_app(user_email="frazer@pointblank.co"))
        resp = client.post(
            "/api/admin/bundles/25034-meta-09/confirm?project_code=25034",
        )
    assert resp.status_code == 200

    # Find the updated_by param in the MERGE call
    merge_params = rec.calls[1][1]
    updated_by_param = next(
        p for p in merge_params if p[0] == "string" and p[1] == "updated_by"
    )
    assert updated_by_param[2] == "frazer@pointblank.co"


def test_confirm_bundle_handles_missing_user_gracefully():
    """When IAP isn't injecting a user (e.g. local dev without middleware),
    the endpoint still works — updated_by is recorded as empty string."""
    rec = QueryRecorder()
    rec.responses = [[{"n": 2}], [], []]

    with patch.object(admin_router.bq, "run_query", side_effect=rec), \
         patch.object(admin_router.bq, "string_param", _string_param), \
         patch.object(admin_router.bq, "scalar_param", _scalar_param), \
         patch.object(admin_router.bq, "table", _table):
        client = TestClient(_make_app(user_email=None))
        resp = client.post(
            "/api/admin/bundles/25034-meta-09/confirm?project_code=25034",
        )
    assert resp.status_code == 200

    merge_params = rec.calls[1][1]
    updated_by_param = next(
        p for p in merge_params if p[0] == "string" and p[1] == "updated_by"
    )
    assert updated_by_param[2] == ""


# ── clear_bundle_override ───────────────────────────────────────────


def test_clear_bundle_override_404_when_no_lines_match():
    rec = QueryRecorder()
    rec.responses = [[{"n": 0}]]

    with patch.object(admin_router.bq, "run_query", side_effect=rec), \
         patch.object(admin_router.bq, "string_param", _string_param), \
         patch.object(admin_router.bq, "scalar_param", _scalar_param), \
         patch.object(admin_router.bq, "table", _table):
        client = TestClient(_make_app())
        resp = client.delete(
            "/api/admin/bundles/25034-meta-09/override?project_code=25034",
        )
    assert resp.status_code == 404


def test_clear_bundle_override_deletes_and_reverts_lines():
    """Happy path: DELETE the override row, UPDATE live lines back to 'suggested_*'.

    DELETE is a no-op if the override didn't exist (which is fine — the
    UPDATE step still reverts any locally-applied confirmation). Endpoint
    is idempotent."""
    rec = QueryRecorder()
    rec.responses = [
        [{"n": 3}],   # _verify_bundle_exists
        [],           # DELETE
        [],           # UPDATE
    ]

    with patch.object(admin_router.bq, "run_query", side_effect=rec), \
         patch.object(admin_router.bq, "string_param", _string_param), \
         patch.object(admin_router.bq, "scalar_param", _scalar_param), \
         patch.object(admin_router.bq, "table", _table):
        client = TestClient(_make_app())
        resp = client.delete(
            "/api/admin/bundles/25034-meta-09/override?project_code=25034",
        )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body == {
        "status": "cleared",
        "project_code": "25034",
        "bundle_id": "25034-meta-09",
    }

    # Three queries: verify, DELETE, UPDATE.
    assert len(rec.calls) == 3
    delete_sql = rec.calls[1][0]
    assert "DELETE FROM" in delete_sql
    assert "media_plan_bundle_overrides" in delete_sql

    update_sql = rec.calls[2][0]
    assert "UPDATE" in update_sql
    assert "media_plan_lines" in update_sql
    # Reverts to 'suggested_*' (NOT 'confirmed_*')
    assert "'suggested_child'" in update_sql
    assert "'suggested_parent'" in update_sql
    assert "confirmed_" not in update_sql


def test_clear_bundle_override_idempotent_when_no_override_exists():
    """Calling clear when there's no override row to delete still
    succeeds — the DELETE is a no-op and the UPDATE makes sure local
    line state matches the parser's suggestion regardless. Lets the
    frontend retry safely."""
    rec = QueryRecorder()
    rec.responses = [[{"n": 1}], [], []]

    with patch.object(admin_router.bq, "run_query", side_effect=rec), \
         patch.object(admin_router.bq, "string_param", _string_param), \
         patch.object(admin_router.bq, "scalar_param", _scalar_param), \
         patch.object(admin_router.bq, "table", _table):
        client = TestClient(_make_app())
        resp = client.delete(
            "/api/admin/bundles/25034-meta-09/override?project_code=25034",
        )
    assert resp.status_code == 200


# ── _apply_bundle_overrides (sync re-application) ───────────────────


def test_apply_bundle_overrides_smoke():
    """The sync hook for re-applying overrides on every sync_media_plan run.

    We can't easily exercise the BQ logic from a unit test (it's a real
    UPDATE / DELETE pair), but we can make sure the function is wired in
    and tolerates the BQ table not existing yet (first deploy). Patches
    mtl.query to raise NotFound and asserts the function returns cleanly."""
    from unittest.mock import MagicMock
    import google.cloud.exceptions
    from backend.services import media_plan_sync

    mtl = MagicMock()
    mtl.query.side_effect = google.cloud.exceptions.NotFound("table missing")

    # Should not raise even though the BQ table doesn't exist.
    media_plan_sync._apply_bundle_overrides(mtl, "25034")

    # Two attempts: apply, then cleanup. Both swallowed.
    assert mtl.query.call_count == 2
