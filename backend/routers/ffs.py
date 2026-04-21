"""Form Friction Score (FFS) endpoints.

Six endpoints under ``/api/ffs/{project_code}`` power the FFS wizard in the
project Settings tab. Entries are stored in ``ffs_entries``; the cached
projection lives on ``media_plan_lines.ffs_{entry_id,score,inputs,override}``
so the diagnostic engine's F-pillar can read it without a join.

See ``/Projects--00002-ADA/FFS Wizard Spec.md`` for the full design.
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from backend.services import ffs_entries as svc

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/ffs", tags=["ffs"])


# ── Request/response schemas ────────────────────────────────────────────────


class FFSInputs(BaseModel):
    """Raw wizard answers. Mirrors the ``compute_ffs`` input contract."""

    field_count: int = 0
    required_fields: int = 0
    field_types: list[str] = Field(default_factory=list)
    clicks_to_submit: int = 1
    below_fold_mobile: bool = False
    has_autofill: bool = False
    is_platform_form: bool = False


class FFSEntryCreate(BaseModel):
    label: str | None = None
    lp_url: str | None = None
    is_platform_form: bool = False
    platform_id: str | None = None
    ffs_inputs: FFSInputs
    applied_line_ids: list[str] = Field(default_factory=list)


class FFSEntryUpdate(BaseModel):
    label: str | None = None
    lp_url: str | None = None
    is_platform_form: bool | None = None
    platform_id: str | None = None
    ffs_inputs: FFSInputs | None = None


class FFSApplyRequest(BaseModel):
    line_ids: list[str]


class FFSLineOverrideRequest(BaseModel):
    """Set or clear a line-level override.

    - ``clear=True``: remove the override; re-sync from entry if still linked.
    - ``clear=False`` (default): set custom ``ffs_inputs`` on the line.
    """

    ffs_inputs: FFSInputs | None = None
    clear: bool = False


class FFSEntryResponse(BaseModel):
    entry_id: str
    project_code: str
    label: str | None = None
    lp_url: str | None = None
    is_platform_form: bool = False
    platform_id: str | None = None
    ffs_inputs: dict[str, Any]
    ffs_score: float
    created_at: str | None = None
    updated_at: str | None = None
    created_by: str | None = None
    linked_line_count: int = 0
    linked_line_ids: list[str] = Field(default_factory=list)


# ── Endpoints ───────────────────────────────────────────────────────────────


@router.get("/{project_code}", response_model=list[FFSEntryResponse])
async def list_entries(project_code: str):
    """List all FFS entries for a project."""
    try:
        return svc.list_entries(project_code)
    except Exception as e:
        logger.exception("Failed to list FFS entries for %s", project_code)
        raise HTTPException(500, f"Failed to list FFS entries: {e}")


@router.post("/{project_code}", response_model=FFSEntryResponse)
async def create_entry(project_code: str, body: FFSEntryCreate, request: Request):
    """Create a new FFS entry and optionally apply it to media plan lines."""
    user = getattr(request.state, "user", None) or {}
    created_by = user.get("email") if isinstance(user, dict) else None

    # Arch B consistency: if is_platform_form is TRUE, auto-set the discount
    # in the inputs so the computed score is consistent with the wizard flow.
    inputs = body.ffs_inputs.model_dump()
    if body.is_platform_form:
        inputs["is_platform_form"] = True

    try:
        return svc.create_entry(
            project_code=project_code,
            label=body.label,
            lp_url=body.lp_url,
            is_platform_form=body.is_platform_form,
            platform_id=body.platform_id,
            ffs_inputs=inputs,
            applied_line_ids=body.applied_line_ids,
            created_by=created_by,
        )
    except Exception as e:
        logger.exception("Failed to create FFS entry for %s", project_code)
        raise HTTPException(500, f"Failed to create FFS entry: {e}")


@router.patch("/{project_code}/{entry_id}", response_model=FFSEntryResponse)
async def update_entry(project_code: str, entry_id: str, body: FFSEntryUpdate):
    """Update an entry. FFS recomputes + propagates to non-override lines."""
    try:
        result = svc.update_entry(
            project_code=project_code,
            entry_id=entry_id,
            label=body.label,
            lp_url=body.lp_url,
            is_platform_form=body.is_platform_form,
            platform_id=body.platform_id,
            ffs_inputs=body.ffs_inputs.model_dump() if body.ffs_inputs else None,
        )
    except Exception as e:
        logger.exception("Failed to update FFS entry %s", entry_id)
        raise HTTPException(500, f"Failed to update FFS entry: {e}")

    if result is None:
        raise HTTPException(404, "FFS entry not found")
    return result


@router.delete("/{project_code}/{entry_id}")
async def delete_entry(project_code: str, entry_id: str):
    """Delete an entry + clean up its cached projection on linked lines.

    Non-override linked lines have ffs_* nulled out. Override lines lose the
    link but keep their custom values.
    """
    try:
        ok = svc.delete_entry(project_code, entry_id)
    except Exception as e:
        logger.exception("Failed to delete FFS entry %s", entry_id)
        raise HTTPException(500, f"Failed to delete FFS entry: {e}")

    if not ok:
        raise HTTPException(404, "FFS entry not found")
    return {"status": "deleted", "entry_id": entry_id}


@router.post("/{project_code}/{entry_id}/apply")
async def apply_to_lines(project_code: str, entry_id: str, body: FFSApplyRequest):
    """Reassign which media plan lines this entry applies to."""
    try:
        return svc.apply_to_lines(
            project_code=project_code,
            entry_id=entry_id,
            line_ids=body.line_ids,
        )
    except ValueError as e:
        raise HTTPException(404, str(e))
    except Exception as e:
        logger.exception("Failed to apply FFS entry %s", entry_id)
        raise HTTPException(500, f"Failed to apply FFS entry: {e}")


@router.post("/{project_code}/lines/{line_id}/override")
async def line_override(project_code: str, line_id: str, body: FFSLineOverrideRequest):
    """Set or clear a line-level FFS override.

    Body:
    - ``{clear: true}``: drop the override; if still linked to an entry,
      re-sync from it. Otherwise null out ffs_score + ffs_inputs.
    - ``{ffs_inputs: {...}}``: set a custom FFS on the line.
    """
    try:
        if body.clear:
            return svc.clear_line_override(
                project_code=project_code, line_id=line_id,
            )
        if body.ffs_inputs is None:
            raise HTTPException(400, "ffs_inputs required when clear=false")
        return svc.set_line_override(
            project_code=project_code,
            line_id=line_id,
            ffs_inputs=body.ffs_inputs.model_dump(),
        )
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(404, str(e))
    except Exception as e:
        logger.exception("Failed to set FFS override on line %s", line_id)
        raise HTTPException(500, f"Failed to set FFS override: {e}")
