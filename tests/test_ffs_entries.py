"""Unit tests for the FFS entries service.

Covers:
    - create_entry: insert + propagate to applied lines
    - update_entry: recompute score + re-sync non-override lines
    - delete_entry: null non-override lines, preserve override custom values
    - apply_to_lines: add/remove links honoring ffs_override
    - set_line_override / clear_line_override: toggle + re-sync
    - compute_ffs integration: score is server-computed, not client-provided

The service talks to BigQuery via ``backend.services.bigquery_client``;
tests swap ``bq.run_query`` for a recorder that captures SQL + params
and returns canned rows.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from backend.services import bigquery_client as bq
from backend.services import ffs_entries as svc


# ── Fake BQ layer ────────────────────────────────────────────────────────────


class FakeBQ:
    """Records every run_query call and returns canned responses.

    Use ``queue(rows)`` to enqueue the next response. SELECTs not matched by
    an explicit queue return an empty list.
    """

    def __init__(self):
        self.calls: list[tuple[str, dict]] = []
        self._queue: list[list[dict]] = []

    def queue(self, rows: list[dict]) -> None:
        self._queue.append(rows)

    def run_query(self, sql: str, params: list | None = None) -> list[dict]:
        param_dict: dict[str, Any] = {}
        for p in (params or []):
            # ScalarQueryParameter has .value; ArrayQueryParameter has .values
            param_dict[p.name] = getattr(p, "value", None)
            if param_dict[p.name] is None:
                param_dict[p.name] = getattr(p, "values", None)
        self.calls.append((sql, param_dict))
        # Only reads pop from the queue — writes always return [] regardless
        stripped = sql.lstrip()
        is_read = stripped.upper().startswith("SELECT") or stripped.upper().startswith("WITH")
        if is_read and self._queue:
            return self._queue.pop(0)
        return []

    # Call matchers — find the Nth call whose SQL contains a substring.
    def find(self, needle: str, n: int = 0) -> tuple[str, dict]:
        matches = [c for c in self.calls if needle in c[0]]
        assert matches, f"No call contained {needle!r}. Calls: {[c[0][:80] for c in self.calls]}"
        return matches[n]

    def count(self, needle: str) -> int:
        return sum(1 for c in self.calls if needle in c[0])


@pytest.fixture
def fake_bq(monkeypatch) -> FakeBQ:
    fb = FakeBQ()
    monkeypatch.setattr(bq, "run_query", fb.run_query)
    return fb


def _sample_inputs(**overrides: Any) -> dict:
    return {
        "field_count": 5,
        "required_fields": 3,
        "field_types": ["text_name", "text_email", "text_phone", "dropdown_simple", "text_address"],
        "clicks_to_submit": 1,
        "below_fold_mobile": False,
        "has_autofill": True,
        "is_platform_form": False,
        **overrides,
    }


def _entry_row(entry_id: str = "entry-1", **overrides: Any) -> dict:
    """Mimic a row as returned by _hydrate via run_query."""
    return {
        "entry_id": entry_id,
        "project_code": "25042",
        "label": "underfunded.ca main",
        "lp_url": "https://underfunded.ca",
        "is_platform_form": False,
        "platform_id": "",
        "ffs_inputs": json.dumps(_sample_inputs()),
        "ffs_score": 33.0,
        "created_at": "2026-04-20 10:00:00",
        "updated_at": "2026-04-20 10:00:00",
        "created_by": "frazer@pointblank.co",
        **overrides,
    }


# ── compute_ffs integration ──────────────────────────────────────────────────


class TestComputeIntegration:
    """FFS is computed server-side; clients never provide the score."""

    def test_create_computes_score_from_inputs(self, fake_bq):
        # Queue the post-insert get_entry fetch
        fake_bq.queue([_entry_row(ffs_score=33.0)])

        svc.create_entry(
            project_code="25042",
            label="underfunded.ca",
            lp_url="https://underfunded.ca",
            is_platform_form=False,
            platform_id=None,
            ffs_inputs=_sample_inputs(),
            applied_line_ids=[],
            created_by="frazer@pointblank.co",
        )

        _, insert_params = fake_bq.find("INSERT INTO")
        # The score written must be computed, not a random number
        assert isinstance(insert_params["ffs_score"], (int, float))
        assert 0 <= insert_params["ffs_score"] <= 100

    def test_score_changes_when_inputs_change(self, fake_bq):
        """Same project, two different inputs → two different scores."""
        low_friction = _sample_inputs(field_count=2, required_fields=1, has_autofill=True)
        high_friction = _sample_inputs(field_count=15, required_fields=15,
                                        below_fold_mobile=True, has_autofill=False)

        fake_bq.queue([_entry_row()])
        svc.create_entry(project_code="25042", label="lp1", lp_url="u1",
                         is_platform_form=False, platform_id=None,
                         ffs_inputs=low_friction, applied_line_ids=[],
                         created_by=None)
        low_score = fake_bq.calls[0][1]["ffs_score"]

        # Clear calls and run again with high-friction inputs
        fake_bq.calls.clear()
        fake_bq.queue([_entry_row()])
        svc.create_entry(project_code="25042", label="lp2", lp_url="u2",
                         is_platform_form=False, platform_id=None,
                         ffs_inputs=high_friction, applied_line_ids=[],
                         created_by=None)
        high_score = fake_bq.calls[0][1]["ffs_score"]

        assert high_score > low_score, f"{high_score=} should exceed {low_score=}"


# ── create_entry ─────────────────────────────────────────────────────────────


class TestCreateEntry:
    def test_propagates_to_applied_lines(self, fake_bq):
        fake_bq.queue([_entry_row()])  # final get_entry

        svc.create_entry(
            project_code="25042", label="lp", lp_url="u",
            is_platform_form=False, platform_id=None,
            ffs_inputs=_sample_inputs(),
            applied_line_ids=["line-a", "line-b"],
            created_by=None,
        )

        # Three writes: INSERT on entries, UPDATE non-override lines, UPDATE override-only
        assert fake_bq.count("INSERT INTO") == 1
        assert fake_bq.count("SET\n          ffs_entry_id = @entry_id") == 1
        _, p = fake_bq.find("SET\n          ffs_entry_id = @entry_id")
        assert set(p["line_ids"]) == {"line-a", "line-b"}

    def test_no_propagation_when_no_applied_lines(self, fake_bq):
        fake_bq.queue([_entry_row()])

        svc.create_entry(
            project_code="25042", label="lp", lp_url="u",
            is_platform_form=False, platform_id=None,
            ffs_inputs=_sample_inputs(),
            applied_line_ids=[],
            created_by=None,
        )

        # Only the INSERT + the final get_entry SELECT — no UPDATE on media_plan_lines
        assert fake_bq.count("UPDATE") == 0

    def test_override_lines_stay_linked_but_not_overwritten(self, fake_bq):
        """Linking an entry to an override line must set ffs_entry_id but NOT
        clobber that line's custom ffs_inputs / ffs_score."""
        fake_bq.queue([_entry_row()])

        svc.create_entry(
            project_code="25042", label="lp", lp_url="u",
            is_platform_form=False, platform_id=None,
            ffs_inputs=_sample_inputs(),
            applied_line_ids=["override-line"],
            created_by=None,
        )

        # Two line updates: one that writes score+inputs (non-override filter) and one
        # that only sets ffs_entry_id on override lines.
        non_override_sql, _ = fake_bq.find("(ffs_override IS NULL OR ffs_override = FALSE)")
        assert "ffs_inputs   = PARSE_JSON" in non_override_sql

        override_sql, _ = fake_bq.find("ffs_override = TRUE")
        assert "ffs_inputs" not in override_sql.split("WHERE")[0]
        assert "ffs_score"  not in override_sql.split("WHERE")[0]


# ── update_entry ─────────────────────────────────────────────────────────────


class TestUpdateEntry:
    def test_returns_none_when_entry_missing(self, fake_bq):
        # get_entry call returns empty → update returns None
        fake_bq.queue([])
        result = svc.update_entry(
            project_code="25042", entry_id="missing",
            ffs_inputs=_sample_inputs(),
        )
        assert result is None

    def test_propagates_to_non_override_lines_only(self, fake_bq):
        # 1st call: get_entry(existing) → return a row
        # 2nd call: UPDATE ffs_entries
        # 3rd call: UPDATE media_plan_lines (non-override only)
        # 4th call: get_entry(final) → return updated row
        fake_bq.queue([_entry_row()])
        fake_bq.queue([_entry_row(ffs_score=40.0)])

        svc.update_entry(
            project_code="25042", entry_id="entry-1",
            ffs_inputs=_sample_inputs(field_count=8),
        )

        resync_sql, _ = fake_bq.find("WHERE ffs_entry_id = @entry_id\n          AND (ffs_override IS NULL OR ffs_override = FALSE)")
        assert "ffs_inputs = PARSE_JSON" in resync_sql
        assert "ffs_score  = @ffs_score" in resync_sql

    def test_preserves_untouched_fields(self, fake_bq):
        """Patching only ffs_inputs should leave label/lp_url intact."""
        existing = _entry_row(label="keep-this-label", lp_url="https://keep.me")
        fake_bq.queue([existing])
        fake_bq.queue([existing])  # final get_entry

        svc.update_entry(
            project_code="25042", entry_id="entry-1",
            ffs_inputs=_sample_inputs(field_count=9),
        )

        _, p = fake_bq.find("SET\n          label            = @label")
        assert p["label"] == "keep-this-label"
        assert p["lp_url"] == "https://keep.me"


# ── delete_entry ─────────────────────────────────────────────────────────────


class TestDeleteEntry:
    def test_returns_false_when_missing(self, fake_bq):
        fake_bq.queue([])  # get_entry empty
        assert svc.delete_entry("25042", "missing") is False

    def test_nulls_non_override_lines_but_preserves_override_values(self, fake_bq):
        fake_bq.queue([_entry_row()])  # get_entry

        svc.delete_entry("25042", "entry-1")

        cleanup_sql, _ = fake_bq.find("SET\n          ffs_entry_id = NULL")
        # Non-override lines get fully nulled; override lines keep their values
        assert "IF(ffs_override = TRUE, ffs_score, NULL)" in cleanup_sql
        assert "IF(ffs_override = TRUE, ffs_inputs, NULL)" in cleanup_sql

        delete_sql, _ = fake_bq.find("DELETE FROM")
        assert "WHERE entry_id = @entry_id AND project_code = @project_code" in delete_sql


# ── apply_to_lines ───────────────────────────────────────────────────────────


class TestApplyToLines:
    def test_raises_when_entry_missing(self, fake_bq):
        fake_bq.queue([])  # get_entry empty
        with pytest.raises(ValueError):
            svc.apply_to_lines(project_code="25042", entry_id="missing", line_ids=["a"])

    def test_unlinks_removed_and_links_added(self, fake_bq):
        fake_bq.queue([_entry_row()])           # get_entry
        fake_bq.queue([{"line_id": "a"}, {"line_id": "b"}])  # get_linked_line_ids (current)

        svc.apply_to_lines(
            project_code="25042", entry_id="entry-1",
            line_ids=["b", "c"],  # drop a, keep b, add c
        )

        # Unlink should target removed lines
        _, unlink_params = fake_bq.find("SET\n              ffs_entry_id = NULL,\n              ffs_score    = IF(ffs_override = TRUE, ffs_score, NULL)")
        assert unlink_params["line_ids"] == ["a"]

        # Link should target added lines
        _, link_params = fake_bq.find("(ffs_override IS NULL OR ffs_override = FALSE)")
        assert link_params["line_ids"] == ["c"]

    def test_noop_when_set_is_unchanged(self, fake_bq):
        fake_bq.queue([_entry_row()])
        fake_bq.queue([{"line_id": "a"}, {"line_id": "b"}])

        result = svc.apply_to_lines(
            project_code="25042", entry_id="entry-1", line_ids=["a", "b"],
        )
        assert result["added"] == []
        assert result["removed"] == []


# ── Line overrides ───────────────────────────────────────────────────────────


class TestLineOverride:
    def test_set_override_writes_custom_values_and_flips_flag(self, fake_bq):
        svc.set_line_override(
            project_code="25042", line_id="line-x",
            ffs_inputs=_sample_inputs(field_count=12),
        )

        sql, p = fake_bq.find("ffs_override = TRUE")
        assert "ffs_inputs   = PARSE_JSON" in sql
        assert "ffs_score    = @ffs_score" in sql
        assert p["line_id"] == "line-x"

    def test_clear_override_resyncs_from_linked_entry(self, fake_bq):
        # Linked lookup row: still linked to an entry, returns entry ffs values
        fake_bq.queue([{
            "ffs_entry_id": "entry-1",
            "ffs_score": 33.0,
            "ffs_inputs": json.dumps(_sample_inputs()),
        }])

        result = svc.clear_line_override(project_code="25042", line_id="line-x")

        assert result["resynced_from_entry"] is True
        assert result["ffs_override"] is False
        sql, _ = fake_bq.find("ffs_override = FALSE,")
        assert "ffs_inputs   = PARSE_JSON" in sql

    def test_clear_override_nulls_values_when_unlinked(self, fake_bq):
        # Linked lookup returns no entry_id (ffs_entry_id is None/empty)
        fake_bq.queue([{
            "ffs_entry_id": None, "ffs_score": None, "ffs_inputs": None,
        }])

        result = svc.clear_line_override(project_code="25042", line_id="line-y")

        assert result["resynced_from_entry"] is False
        assert result["ffs_score"] is None
        sql, _ = fake_bq.find("SET ffs_override = FALSE, ffs_inputs = NULL, ffs_score = NULL")
        assert sql  # matched

    def test_clear_override_raises_when_line_missing(self, fake_bq):
        fake_bq.queue([])  # lookup returned nothing
        with pytest.raises(ValueError):
            svc.clear_line_override(project_code="25042", line_id="does-not-exist")


# ── Hydration (JSON parsing + null-empty coercion) ──────────────────────────


class TestHydration:
    def test_json_string_parsed_to_dict(self):
        raw = {"ffs_inputs": json.dumps({"field_count": 5}), "label": "x"}
        out = svc._hydrate_entry(raw)
        assert out["ffs_inputs"] == {"field_count": 5}

    def test_empty_strings_become_none(self):
        raw = {"label": "", "lp_url": "", "platform_id": "", "created_by": "",
               "ffs_inputs": "{}"}
        out = svc._hydrate_entry(raw)
        assert out["label"] is None
        assert out["lp_url"] is None
        assert out["platform_id"] is None
        assert out["created_by"] is None

    def test_ffs_score_coerced_to_float(self):
        raw = {"ffs_score": "42.5", "ffs_inputs": "{}"}
        out = svc._hydrate_entry(raw)
        assert out["ffs_score"] == 42.5
        assert isinstance(out["ffs_score"], float)

    def test_linked_line_ids_normalised_and_count_derived(self):
        """Array column coerced to list[str]; count is ARRAY_LENGTH, not extra query."""
        raw = {"ffs_inputs": "{}", "linked_line_ids": ["line-a", "line-b", "line-c"]}
        out = svc._hydrate_entry(raw)
        assert out["linked_line_ids"] == ["line-a", "line-b", "line-c"]
        assert out["linked_line_count"] == 3

    def test_linked_line_ids_missing_becomes_empty_list(self):
        """get_entry called before list_entries was fixed must not crash."""
        raw = {"ffs_inputs": "{}"}  # no linked_line_ids key
        out = svc._hydrate_entry(raw)
        assert out["linked_line_ids"] == []
        assert out["linked_line_count"] == 0


# ── list_entries (linked_line_ids surfaced) ─────────────────────────────────


class TestListEntries:
    def test_list_entries_returns_linked_line_ids(self, fake_bq):
        """The list query must pull linked_line_ids so the UI can show 'already
        linked to another entry' badges without an N+1 round-trip."""
        fake_bq.queue([
            _entry_row(entry_id="e1"),
            _entry_row(entry_id="e2", linked_line_ids=["line-a", "line-b"]),
        ])
        # Seed linked_line_ids on the first row too (the fixture doesn't include it).
        # Easiest: patch the queue item directly.
        fake_bq._queue[0][0]["linked_line_ids"] = []
        out = svc.list_entries("25042")
        assert out[0]["linked_line_ids"] == []
        assert out[0]["linked_line_count"] == 0
        assert out[1]["linked_line_ids"] == ["line-a", "line-b"]
        assert out[1]["linked_line_count"] == 2

    def test_list_sql_uses_array_agg_left_join(self, fake_bq):
        """Guard the CTE + LEFT JOIN shape.

        Earlier versions used a correlated ``ARRAY()`` subquery which
        BigQuery rejects with *"Correlated subqueries that reference other
        tables are not supported"*. The service must pre-aggregate in a CTE
        and LEFT JOIN, and must apply the standard ROW_NUMBER dedup on
        ``media_plan_lines`` so stale rows from prior syncs don't leak in.
        """
        fake_bq.queue([])
        svc.list_entries("25042")
        sql, _ = fake_bq.calls[0]
        assert "ARRAY_AGG" in sql
        assert "LEFT JOIN" in sql
        assert "ROW_NUMBER" in sql  # dedup on media_plan_lines
        assert "linked_line_ids" in sql
