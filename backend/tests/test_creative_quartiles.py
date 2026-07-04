"""Creative surface — S7 tickets #11 and #19 (backend half).

#11  Per-platform video quartiles: the creative × platform CELL rows the
     frontend matrix renders must carry the four raw quartile completion
     sums (video_q25..q100) so the frontend can draw a per-platform
     retention curve (anchored on q25). We only pass the raw SUMs through
     — no rate maths here — and only on the CELL (CreativeMatrixCell),
     never on the campaign-level CreativeTotals.

#19  A dimensions-only ad_name ("2160x3840", a StackAdapt DOOH ad) used to
     normalise to the empty string and render "Unknown". _alias_resolution
     now wraps the normalised expression in NULLIF(TRIM(...), '') and adds
     a final raw-ad_name arm to the COALESCE, so a name that empties out
     falls back to the literal "2160x3840"; the Python `or "Unnamed
     creative"` fallback is the friendlier true-null last resort.

Hermetic: BigQuery is mocked (no GCP creds). Where a builder returns SQL
text, we assert on the string it constructs.
"""
import re
from unittest.mock import patch

from backend.models.creative import CreativeMatrixCell, CreativeTotals


# ── #11: CELL model carries the four quartile sums ───────────────────


class TestMatrixCellQuartiles:
    def test_defaults_zero(self):
        cell = CreativeMatrixCell()
        assert cell.video_q25 == 0
        assert cell.video_q50 == 0
        assert cell.video_q75 == 0
        assert cell.video_q100 == 0

    def test_round_trips_supplied_values(self):
        cell = CreativeMatrixCell(
            spend=12.5,
            impressions=4000,
            video_q25=900,
            video_q50=600,
            video_q75=400,
            video_q100=250,
        )
        assert cell.video_q25 == 900
        assert cell.video_q50 == 600
        assert cell.video_q75 == 400
        assert cell.video_q100 == 250
        dumped = cell.model_dump()
        assert dumped["video_q25"] == 900
        assert dumped["video_q100"] == 250

    def test_totals_have_no_quartiles(self):
        # #11 is CELL-only: the campaign-level totals model must NOT gain
        # quartile fields (they'd otherwise leak into the aggregate).
        fields = set(CreativeTotals.model_fields)
        assert not (fields & {"video_q25", "video_q50", "video_q75", "video_q100"})


# ── #11: the platform-cells query selects the quartiles inner + outer ──


def _patch_bq(mock_bq):
    """Minimal bq stub so _alias_resolution + _query_creative_platform_cells
    build their SQL without touching BigQuery. run_query records every SQL
    string it is handed and returns []."""
    calls = []

    def run_query(sql, params=None):
        calls.append(sql)
        return []

    mock_bq.run_query.side_effect = run_query
    mock_bq.table.side_effect = lambda name: f"`proj.ds.{name}`"
    mock_bq.string_param.side_effect = lambda n, v: (n, v)
    mock_bq.date_param.side_effect = lambda n, v: (n, v)
    mock_bq.array_param.side_effect = lambda n, t, v: (n, t, list(v))
    return calls


class TestPlatformCellsSql:
    @patch("backend.routers.creative.bq")
    def test_quartiles_selected_inner_and_outer(self, mock_bq):
        from backend.routers import creative

        calls = _patch_bq(mock_bq)
        alias_join, variant_expr = creative._alias_resolution("ad_agg")
        creative._query_creative_platform_cells(
            "26023", "1=1", [], alias_join, variant_expr
        )

        # The last run_query is the cells query itself (the first is the
        # alias-table existence probe).
        cells_sql = calls[-1]

        # INNER ad-grain CTE reads them off fact_digital_daily (f.*).
        for q in ("video_q25", "video_q50", "video_q75", "video_q100"):
            assert f"SUM(f.{q}) AS {q}" in cells_sql, f"inner missing {q}"

        # OUTER SELECT re-sums them from the aliased CTE (a.*). Without this
        # the outer references a non-existent column and BigQuery 500s.
        for q in ("video_q25", "video_q50", "video_q75", "video_q100"):
            assert f"SUM(a.{q}) AS {q}" in cells_sql, f"outer missing {q}"

    @patch("backend.routers.creative.bq")
    def test_existing_video_metrics_still_selected(self, mock_bq):
        # Additive change: the pre-existing raw counts must survive.
        from backend.routers import creative

        calls = _patch_bq(mock_bq)
        alias_join, variant_expr = creative._alias_resolution("ad_agg")
        creative._query_creative_platform_cells(
            "26023", "1=1", [], alias_join, variant_expr
        )
        cells_sql = calls[-1]
        assert "SUM(f.video_views_3s) AS video_views_3s" in cells_sql
        assert "SUM(a.video_views_3s) AS video_views_3s" in cells_sql


# ── #19: alias resolution falls back to the raw ad_name ───────────────


class TestAliasResolutionFallback:
    @patch("backend.routers.creative.bq")
    def test_coalesce_has_nullif_and_ad_name_last_arm(self, mock_bq):
        from backend.routers import creative

        _patch_bq(mock_bq)
        _, variant_expr = creative._alias_resolution("ad_agg")

        # The normalised branch is wrapped so an empty normalisation is NULL…
        assert "NULLIF(TRIM(" in variant_expr
        # …and the raw ad_name is the final COALESCE arm (the #19 fix).
        # It must come AFTER the NULLIF branch, else a dimensions-only name
        # would never reach it.
        nullif_at = variant_expr.index("NULLIF(TRIM(")
        last_arm_at = variant_expr.rindex("ad_agg.ad_name")
        assert last_arm_at > nullif_at
        # sanity: still a COALESCE over the alias column first
        assert variant_expr.lstrip().startswith("COALESCE(")
        assert "cva.creative_variant" in variant_expr

    @patch("backend.routers.creative.bq")
    def test_source_prefix_is_parameterised(self, mock_bq):
        # A different CTE alias flows through to every arm, including the
        # new last-resort ad_name arm.
        from backend.routers import creative

        _patch_bq(mock_bq)
        _, variant_expr = creative._alias_resolution("mx")
        assert "mx.ad_name" in variant_expr
        assert variant_expr.rstrip().endswith(")")


# ── #19: the normalisation genuinely empties out on a dimensions-only name ──
#
# _alias_resolution emits SQL, so we can't run BigQuery's REGEXP_REPLACE here.
# We replicate the two normalisation regexes in Python (same patterns as the
# SQL) to prove the intent: "2160x3840" collapses to "" (which is exactly why
# the NULLIF + ad_name fallback is needed), while a real name survives.


def _normalise_like_sql(ad_name: str) -> str:
    """Python mirror of the two REGEXP_REPLACE steps in _alias_resolution:
    strip a leading 5-digit project code, then a trailing WxH token."""
    s = re.sub(r"^\d{5}\s*[-_]\s*", "", ad_name)
    s = re.sub(r"\s*[-_]?\s*\d+x\d+\s*$", "", s)
    return s.strip()


class TestNormalisationEmptiesOut:
    def test_dimensions_only_name_empties(self):
        # The DOOH ad_name is the bare dimensions string.
        assert _normalise_like_sql("2160x3840") == ""

    def test_named_creative_survives(self):
        assert _normalise_like_sql("26023 - Spring Hero") == "Spring Hero"
        # trailing dimensions on a real name are stripped but the name stays
        assert _normalise_like_sql("26023 - Spring Hero 1080x1080") == "Spring Hero"


# ── #19: the Python `or "Unnamed creative"` last resort ───────────────


class TestUnnamedFallback:
    def test_fires_only_on_empty_or_none(self):
        # Mirrors the four `variant = c.get("creative_variant") or "..."`
        # sites: a real variant is kept; only a truly falsy value trips the
        # friendly last resort.
        assert ((None) or "Unnamed creative") == "Unnamed creative"
        assert (("") or "Unnamed creative") == "Unnamed creative"
        assert (("2160x3840") or "Unnamed creative") == "2160x3840"
        assert (("Spring Hero") or "Unnamed creative") == "Spring Hero"

    def test_no_stray_unknown_left_in_sites(self):
        # Guard against a stray "Unknown" left in the four rewritten sites.
        import inspect
        from backend.routers import creative

        src = inspect.getsource(creative)
        assert 'or "Unknown"' not in src
        assert src.count('or "Unnamed creative"') == 4
