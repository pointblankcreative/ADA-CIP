"""FFS entries service — CRUD + propagation logic for Form Friction Score entries.

An ``ffs_entries`` row represents one form (landing page or platform lead form)
within a project. It carries the wizard inputs and the computed FFS score.

Each row may be linked to one or more ``media_plan_lines`` via
``media_plan_lines.ffs_entry_id``. Propagation rules:

* When a line's ``ffs_override`` is FALSE, the line's cached ``ffs_score`` and
  ``ffs_inputs`` mirror the linked entry — any entry update re-syncs the line.
* When ``ffs_override`` is TRUE, the line holds its own custom values and is
  never touched by entry-level writes.

See ``/Projects--00002-ADA/FFS Wizard Spec.md`` for the full design.
"""

from __future__ import annotations

import json
import logging
import uuid
from typing import Any

from backend.services import bigquery_client as bq
from backend.services.diagnostics.shared.form_friction import compute_ffs

logger = logging.getLogger(__name__)


# ── Read helpers ────────────────────────────────────────────────────────────


def list_entries(project_code: str) -> list[dict]:
    """List all FFS entries for a project with linked-line IDs + counts."""
    sql = f"""
        SELECT
          e.entry_id,
          e.project_code,
          e.label,
          e.lp_url,
          e.is_platform_form,
          e.platform_id,
          TO_JSON_STRING(e.ffs_inputs) AS ffs_inputs,
          e.ffs_score,
          CAST(e.created_at AS STRING) AS created_at,
          CAST(e.updated_at AS STRING) AS updated_at,
          e.created_by,
          ARRAY(
            SELECT l.line_id
            FROM {bq.table('media_plan_lines')} l
            WHERE l.ffs_entry_id = e.entry_id
            ORDER BY l.line_id
          ) AS linked_line_ids
        FROM {bq.table('ffs_entries')} e
        WHERE e.project_code = @project_code
        ORDER BY e.created_at
    """
    rows = bq.run_query(sql, [bq.string_param("project_code", project_code)])
    return [_hydrate_entry(r) for r in rows]


def get_entry(project_code: str, entry_id: str) -> dict | None:
    sql = f"""
        SELECT
          e.entry_id, e.project_code, e.label, e.lp_url, e.is_platform_form,
          e.platform_id,
          TO_JSON_STRING(e.ffs_inputs) AS ffs_inputs, e.ffs_score,
          CAST(e.created_at AS STRING) AS created_at,
          CAST(e.updated_at AS STRING) AS updated_at,
          e.created_by,
          ARRAY(
            SELECT l.line_id
            FROM {bq.table('media_plan_lines')} l
            WHERE l.ffs_entry_id = e.entry_id
            ORDER BY l.line_id
          ) AS linked_line_ids
        FROM {bq.table('ffs_entries')} e
        WHERE e.project_code = @project_code AND e.entry_id = @entry_id
    """
    rows = bq.run_query(sql, [
        bq.string_param("project_code", project_code),
        bq.string_param("entry_id", entry_id),
    ])
    return _hydrate_entry(rows[0]) if rows else None


def get_linked_line_ids(entry_id: str) -> list[str]:
    sql = f"""
        SELECT line_id
        FROM {bq.table('media_plan_lines')}
        WHERE ffs_entry_id = @entry_id
    """
    rows = bq.run_query(sql, [bq.string_param("entry_id", entry_id)])
    return [r["line_id"] for r in rows]


# ── Write: create / update / delete entry ───────────────────────────────────


def create_entry(
    *,
    project_code: str,
    label: str | None,
    lp_url: str | None,
    is_platform_form: bool,
    platform_id: str | None,
    ffs_inputs: dict[str, Any],
    applied_line_ids: list[str],
    created_by: str | None,
) -> dict:
    """Insert an entry + propagate to applied lines in one logical transaction.

    FFS is computed server-side from ffs_inputs. Clients never post a score.
    """
    entry_id = str(uuid.uuid4())
    score = compute_ffs(ffs_inputs)

    # Insert the entry row
    insert_sql = f"""
        INSERT INTO {bq.table('ffs_entries')}
          (entry_id, project_code, label, lp_url, is_platform_form, platform_id,
           ffs_inputs, ffs_score, created_by)
        VALUES
          (@entry_id, @project_code, @label, @lp_url, @is_platform_form, @platform_id,
           PARSE_JSON(@ffs_inputs_json), @ffs_score, @created_by)
    """
    bq.run_query(insert_sql, [
        bq.string_param("entry_id", entry_id),
        bq.string_param("project_code", project_code),
        bq.string_param("label", label or ""),
        bq.string_param("lp_url", lp_url or ""),
        bq.scalar_param("is_platform_form", "BOOL", is_platform_form),
        bq.string_param("platform_id", platform_id or ""),
        bq.string_param("ffs_inputs_json", json.dumps(ffs_inputs)),
        bq.scalar_param("ffs_score", "FLOAT64", score),
        bq.string_param("created_by", created_by or ""),
    ])

    # Propagate to applied lines
    if applied_line_ids:
        _link_lines_to_entry(
            project_code=project_code,
            entry_id=entry_id,
            line_ids=applied_line_ids,
            ffs_inputs=ffs_inputs,
            ffs_score=score,
        )

    entry = get_entry(project_code, entry_id)
    assert entry is not None, "Entry disappeared immediately after insert"
    return entry


def update_entry(
    *,
    project_code: str,
    entry_id: str,
    label: str | None = None,
    lp_url: str | None = None,
    is_platform_form: bool | None = None,
    platform_id: str | None = None,
    ffs_inputs: dict[str, Any] | None = None,
) -> dict | None:
    """Patch an entry's fields. If ffs_inputs changes, recompute + propagate.

    Only non-None kwargs are updated. Existing fields are preserved otherwise.
    Linked lines with ffs_override=TRUE are left alone; all others get the
    new ffs_score + ffs_inputs.
    """
    existing = get_entry(project_code, entry_id)
    if existing is None:
        return None

    # Merge provided fields on top of existing
    merged = {
        "label": label if label is not None else existing["label"],
        "lp_url": lp_url if lp_url is not None else existing["lp_url"],
        "is_platform_form": (
            is_platform_form if is_platform_form is not None
            else existing["is_platform_form"]
        ),
        "platform_id": (
            platform_id if platform_id is not None else existing["platform_id"]
        ),
        "ffs_inputs": (
            ffs_inputs if ffs_inputs is not None else existing["ffs_inputs"]
        ),
    }
    new_score = compute_ffs(merged["ffs_inputs"])

    update_sql = f"""
        UPDATE {bq.table('ffs_entries')}
        SET
          label            = @label,
          lp_url           = @lp_url,
          is_platform_form = @is_platform_form,
          platform_id      = @platform_id,
          ffs_inputs       = PARSE_JSON(@ffs_inputs_json),
          ffs_score        = @ffs_score,
          updated_at       = CURRENT_TIMESTAMP()
        WHERE entry_id = @entry_id AND project_code = @project_code
    """
    bq.run_query(update_sql, [
        bq.string_param("label", merged["label"] or ""),
        bq.string_param("lp_url", merged["lp_url"] or ""),
        bq.scalar_param("is_platform_form", "BOOL", bool(merged["is_platform_form"])),
        bq.string_param("platform_id", merged["platform_id"] or ""),
        bq.string_param("ffs_inputs_json", json.dumps(merged["ffs_inputs"])),
        bq.scalar_param("ffs_score", "FLOAT64", new_score),
        bq.string_param("entry_id", entry_id),
        bq.string_param("project_code", project_code),
    ])

    # Propagate to non-override linked lines
    _resync_non_override_linked_lines(
        entry_id=entry_id,
        ffs_inputs=merged["ffs_inputs"],
        ffs_score=new_score,
    )

    return get_entry(project_code, entry_id)


def delete_entry(project_code: str, entry_id: str) -> bool:
    """Delete an entry + clean up linked lines.

    For non-override linked lines: null out ffs_entry_id, ffs_score, ffs_inputs.
    For override linked lines: null out ffs_entry_id only; custom values remain.
    """
    existing = get_entry(project_code, entry_id)
    if existing is None:
        return False

    cleanup_sql = f"""
        UPDATE {bq.table('media_plan_lines')}
        SET
          ffs_entry_id = NULL,
          ffs_score    = IF(ffs_override = TRUE, ffs_score, NULL),
          ffs_inputs   = IF(ffs_override = TRUE, ffs_inputs, NULL)
        WHERE ffs_entry_id = @entry_id
    """
    bq.run_query(cleanup_sql, [bq.string_param("entry_id", entry_id)])

    delete_sql = f"""
        DELETE FROM {bq.table('ffs_entries')}
        WHERE entry_id = @entry_id AND project_code = @project_code
    """
    bq.run_query(delete_sql, [
        bq.string_param("entry_id", entry_id),
        bq.string_param("project_code", project_code),
    ])
    return True


# ── Write: reassign applied lines + line override ───────────────────────────


def apply_to_lines(
    *,
    project_code: str,
    entry_id: str,
    line_ids: list[str],
) -> dict:
    """Reassign which lines this entry applies to.

    * Lines removed from the set: null out ffs_entry_id. If ffs_override=FALSE
      also null out ffs_score + ffs_inputs. Override lines keep their custom
      values but lose the entry link.
    * Lines added to the set: link them and (if not override) copy the entry's
      score + inputs.
    """
    entry = get_entry(project_code, entry_id)
    if entry is None:
        raise ValueError(f"FFS entry {entry_id} not found in project {project_code}")

    current_line_ids = set(get_linked_line_ids(entry_id))
    new_line_ids = set(line_ids)

    to_remove = list(current_line_ids - new_line_ids)
    to_add = list(new_line_ids - current_line_ids)

    if to_remove:
        unlink_sql = f"""
            UPDATE {bq.table('media_plan_lines')}
            SET
              ffs_entry_id = NULL,
              ffs_score    = IF(ffs_override = TRUE, ffs_score, NULL),
              ffs_inputs   = IF(ffs_override = TRUE, ffs_inputs, NULL)
            WHERE ffs_entry_id = @entry_id
              AND line_id IN UNNEST(@line_ids)
        """
        bq.run_query(unlink_sql, [
            bq.string_param("entry_id", entry_id),
            bq.array_param("line_ids", "STRING", to_remove),
        ])

    if to_add:
        _link_lines_to_entry(
            project_code=project_code,
            entry_id=entry_id,
            line_ids=to_add,
            ffs_inputs=entry["ffs_inputs"],
            ffs_score=entry["ffs_score"],
        )

    return {
        "entry_id": entry_id,
        "linked_line_ids": sorted(new_line_ids),
        "added": to_add,
        "removed": to_remove,
    }


def set_line_override(
    *,
    project_code: str,
    line_id: str,
    ffs_inputs: dict[str, Any],
) -> dict:
    """Give one line a custom FFS that diverges from its linked entry.

    ``ffs_entry_id`` is retained (marks "this line used to sync from entry X")
    but ``ffs_override`` is set TRUE so entry updates no longer touch it.
    """
    score = compute_ffs(ffs_inputs)
    sql = f"""
        UPDATE {bq.table('media_plan_lines')}
        SET
          ffs_inputs   = PARSE_JSON(@ffs_inputs_json),
          ffs_score    = @ffs_score,
          ffs_override = TRUE
        WHERE line_id = @line_id AND project_code = @project_code
    """
    bq.run_query(sql, [
        bq.string_param("ffs_inputs_json", json.dumps(ffs_inputs)),
        bq.scalar_param("ffs_score", "FLOAT64", score),
        bq.string_param("line_id", line_id),
        bq.string_param("project_code", project_code),
    ])
    return {"line_id": line_id, "ffs_score": score, "ffs_override": True}


def clear_line_override(*, project_code: str, line_id: str) -> dict:
    """Drop a line's override. If still linked to an entry, re-sync from it."""
    linked = bq.run_query(
        f"""
        SELECT l.ffs_entry_id, e.ffs_score, TO_JSON_STRING(e.ffs_inputs) AS ffs_inputs
        FROM {bq.table('media_plan_lines')} l
        LEFT JOIN {bq.table('ffs_entries')} e ON e.entry_id = l.ffs_entry_id
        WHERE l.line_id = @line_id AND l.project_code = @project_code
        """,
        [bq.string_param("line_id", line_id),
         bq.string_param("project_code", project_code)],
    )
    if not linked:
        raise ValueError(f"Line {line_id} not found in project {project_code}")

    row = linked[0]
    still_linked = bool(row.get("ffs_entry_id"))

    if still_linked and row.get("ffs_inputs") is not None:
        inputs = json.loads(row["ffs_inputs"])
        sql = f"""
            UPDATE {bq.table('media_plan_lines')}
            SET
              ffs_override = FALSE,
              ffs_inputs   = PARSE_JSON(@ffs_inputs_json),
              ffs_score    = @ffs_score
            WHERE line_id = @line_id AND project_code = @project_code
        """
        bq.run_query(sql, [
            bq.string_param("ffs_inputs_json", json.dumps(inputs)),
            bq.scalar_param("ffs_score", "FLOAT64", float(row["ffs_score"])),
            bq.string_param("line_id", line_id),
            bq.string_param("project_code", project_code),
        ])
        return {"line_id": line_id, "ffs_score": float(row["ffs_score"]),
                "ffs_override": False, "resynced_from_entry": True}

    # Not linked to any entry — wipe the custom values
    sql = f"""
        UPDATE {bq.table('media_plan_lines')}
        SET ffs_override = FALSE, ffs_inputs = NULL, ffs_score = NULL
        WHERE line_id = @line_id AND project_code = @project_code
    """
    bq.run_query(sql, [
        bq.string_param("line_id", line_id),
        bq.string_param("project_code", project_code),
    ])
    return {"line_id": line_id, "ffs_score": None, "ffs_override": False,
            "resynced_from_entry": False}


# ── Internal helpers ────────────────────────────────────────────────────────


def _link_lines_to_entry(
    *,
    project_code: str,
    entry_id: str,
    line_ids: list[str],
    ffs_inputs: dict[str, Any],
    ffs_score: float,
) -> None:
    """Set ffs_entry_id + copy score/inputs on lines, but never clobber overrides."""
    if not line_ids:
        return
    sql = f"""
        UPDATE {bq.table('media_plan_lines')}
        SET
          ffs_entry_id = @entry_id,
          ffs_override = FALSE,
          ffs_inputs   = PARSE_JSON(@ffs_inputs_json),
          ffs_score    = @ffs_score
        WHERE project_code = @project_code
          AND line_id IN UNNEST(@line_ids)
          AND (ffs_override IS NULL OR ffs_override = FALSE)
    """
    bq.run_query(sql, [
        bq.string_param("entry_id", entry_id),
        bq.string_param("project_code", project_code),
        bq.array_param("line_ids", "STRING", line_ids),
        bq.string_param("ffs_inputs_json", json.dumps(ffs_inputs)),
        bq.scalar_param("ffs_score", "FLOAT64", ffs_score),
    ])
    # For override lines we still link them (so they show under the entry),
    # but preserve their custom ffs_score/ffs_inputs.
    link_only_sql = f"""
        UPDATE {bq.table('media_plan_lines')}
        SET ffs_entry_id = @entry_id
        WHERE project_code = @project_code
          AND line_id IN UNNEST(@line_ids)
          AND ffs_override = TRUE
    """
    bq.run_query(link_only_sql, [
        bq.string_param("entry_id", entry_id),
        bq.string_param("project_code", project_code),
        bq.array_param("line_ids", "STRING", line_ids),
    ])


def _resync_non_override_linked_lines(
    *,
    entry_id: str,
    ffs_inputs: dict[str, Any],
    ffs_score: float,
) -> None:
    sql = f"""
        UPDATE {bq.table('media_plan_lines')}
        SET
          ffs_inputs = PARSE_JSON(@ffs_inputs_json),
          ffs_score  = @ffs_score
        WHERE ffs_entry_id = @entry_id
          AND (ffs_override IS NULL OR ffs_override = FALSE)
    """
    bq.run_query(sql, [
        bq.string_param("entry_id", entry_id),
        bq.string_param("ffs_inputs_json", json.dumps(ffs_inputs)),
        bq.scalar_param("ffs_score", "FLOAT64", ffs_score),
    ])


def _hydrate_entry(row: dict) -> dict:
    """Coerce raw BQ row into a clean dict with parsed JSON + typed fields."""
    result = dict(row)
    raw_inputs = result.get("ffs_inputs")
    if isinstance(raw_inputs, str):
        try:
            result["ffs_inputs"] = json.loads(raw_inputs)
        except (TypeError, ValueError):
            result["ffs_inputs"] = {}
    elif raw_inputs is None:
        result["ffs_inputs"] = {}
    # Empty strings returned from BQ should be treated as NULL for nullable cols
    for col in ("label", "lp_url", "platform_id", "created_by"):
        if result.get(col) == "":
            result[col] = None
    if result.get("ffs_score") is not None:
        result["ffs_score"] = float(result["ffs_score"])
    if result.get("is_platform_form") is not None:
        result["is_platform_form"] = bool(result["is_platform_form"])
    # Normalise linked_line_ids → list[str] and derive count from it.
    raw_ids = result.get("linked_line_ids")
    if raw_ids is None:
        result["linked_line_ids"] = []
    else:
        result["linked_line_ids"] = [str(x) for x in raw_ids]
    result["linked_line_count"] = len(result["linked_line_ids"])
    return result
