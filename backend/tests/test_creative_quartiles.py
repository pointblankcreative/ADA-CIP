"""Creative surface — S7 tickets #11 and #19 (backend half).

ADA 1216199318518420 (format-aware CTR grading) — appended below:
  A structurally-no-click placement (DOOH: its platform reports no clicks
  column at all, clicks IS NULL on every row) must not be graded on CTR. The
  four CTR-producing sites now key off a `clicks_reported` tally
  (COUNTIF(clicks IS NOT NULL)): ctr is None when nothing reported clicks,
  and 0.0 when clicks were reported as zero (a click-capable placement that
  genuinely earned none — the broken-tracking case that MUST still surface).


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


# ── ADA 1216199318518420: format-aware CTR (clicks_reported gate) ─────
#
# Four CTR-producing surfaces must agree on ONE structural signal:
#   1. rotation KPIs      → _rate_kpis
#   2. creative × platform → get_creative_matrix cell
#   3. audience row        → get_audience_matrix row
#   4. audience × creative → get_audience_matrix cell
# DOOH (no clicks column reported anywhere) ⇒ ctr None; a click-capable
# placement that reported zero clicks ⇒ ctr 0.0 (bottom-quartile flag kept).
import asyncio


def _cell(**over):
    """A creative × platform cell dict as _query_creative_platform_cells
    yields it. Defaults are past the 1,000-impression rate guard."""
    base = {
        "creative_variant": "Creative",
        "platform_id": "meta",
        "spend": 500.0,
        "impressions": 5000,
        "clicks": 0,
        "clicks_reported": 0,
        "conversions": 0.0,
        "engagements": 0,
        "video_views": 0,
        "video_completions": 0,
        "video_views_3s": 0,
        "video_q25": 0,
        "video_q50": 0,
        "video_q75": 0,
        "video_q100": 0,
    }
    base.update(over)
    return base


class TestClicksReportedSql:
    """The structural signal is threaded inner (COUNTIF) → outer (SUM) in
    every fact-reading query, mirroring the existing two-level SUM pattern."""

    @patch("backend.routers.creative.bq")
    def test_platform_cells_query_carries_clicks_reported(self, mock_bq):
        from backend.routers import creative

        calls = _patch_bq(mock_bq)
        alias_join, variant_expr = creative._alias_resolution("ad_agg")
        creative._query_creative_platform_cells(
            "26023", "1=1", [], alias_join, variant_expr
        )
        sql = calls[-1]
        assert "COUNTIF(f.clicks IS NOT NULL) AS clicks_reported_rows" in sql
        assert "SUM(a.clicks_reported_rows) AS clicks_reported" in sql


class TestRateKpisCtrGate:
    """Site #1 — the rotation rollup (_rate_kpis is a pure fn over an
    accumulator, so we assert its ctr directly)."""

    def _ctr(self, clicks, clicks_reported, impressions=5000):
        from backend.routers import creative

        agg = creative._new_accumulator()
        agg["impressions"] = impressions
        agg["clicks"] = clicks
        agg["clicks_reported"] = clicks_reported
        return creative._rate_kpis(agg, set(), set())["ctr"]

    def test_dooh_no_clicks_column_nulls_ctr(self):
        # No row ever reported a clicks value → no click path → no CTR grade.
        assert self._ctr(clicks=0, clicks_reported=0) is None

    def test_reported_zero_clicks_keeps_zero_ctr(self):
        # UAT REGRESSION GUARD: a click-capable placement that reported 0
        # clicks on real volume still reads a real 0.00% (bottom quartile),
        # so broken tracking / a dead link is never silently softened.
        assert self._ctr(clicks=0, clicks_reported=4) == 0.0

    def test_normal_clicks_unchanged(self):
        assert self._ctr(clicks=50, clicks_reported=4) == 50 / 5000


class TestCreativeMatrixCtrGate:
    """Site #2 — the creative × platform matrix cell."""

    @patch("backend.routers.creative._query_creative_platform_cells")
    @patch("backend.routers.creative.bq")
    def test_dooh_cell_null_reported_zero_keeps_zero(self, mock_bq, mock_cells):
        from backend.routers import creative

        _patch_bq(mock_bq)
        mock_cells.return_value = [
            _cell(creative_variant="DOOH Static", platform_id="stackadapt",
                  clicks=0, clicks_reported=0),
            _cell(creative_variant="Display Static", platform_id="meta",
                  clicks=0, clicks_reported=4),
            _cell(creative_variant="Feed Static", platform_id="google",
                  clicks=50, clicks_reported=4),
        ]
        resp = asyncio.run(creative.get_creative_matrix("26023"))
        assert resp.cells["DOOH Static"]["stackadapt"].ctr is None
        assert resp.cells["Display Static"]["meta"].ctr == 0.0
        assert resp.cells["Feed Static"]["google"].ctr == 50 / 5000


class TestAudienceMatrixCtrGate:
    """Sites #3 (audience row) and #4 (audience × creative cell)."""

    def _dispatch(self, sql, params=None):
        # Route each query by a distinguishing substring so order can't
        # break the test. Everything not asserted on returns [].
        if "adset_metrics AS" in sql:  # aud_sql (rollup rows)
            return [
                {"platform_id": "stackadapt", "ad_set_name": "DOOH Boards",
                 "spend": 500.0, "impressions": 5000, "clicks": 0,
                 "clicks_reported": 0, "conversions": 0.0, "engagements": 0,
                 "video_views": 0, "video_completions": 0, "video_views_3s": 0,
                 "freq_weighted": None, "freq_impressions": 0},
                {"platform_id": "meta", "ad_set_name": "Prospecting",
                 "spend": 500.0, "impressions": 5000, "clicks": 0,
                 "clicks_reported": 4, "conversions": 0.0, "engagements": 0,
                 "video_views": 0, "video_completions": 0, "video_views_3s": 0,
                 "freq_weighted": None, "freq_impressions": 0},
            ]
        if "GROUP BY a.creative_variant, a.platform_id, a.ad_set_name" in sql:
            # cell_sql (audience × creative cells)
            return [
                {"creative_variant": "DOOH Static", "platform_id": "stackadapt",
                 "ad_set_name": "DOOH Boards", "spend": 500.0,
                 "impressions": 5000, "clicks": 0, "clicks_reported": 0,
                 "conversions": 0.0, "engagements": 0, "video_views": 0,
                 "video_completions": 0, "video_views_3s": 0},
                {"creative_variant": "Display Static", "platform_id": "meta",
                 "ad_set_name": "Prospecting", "spend": 500.0,
                 "impressions": 5000, "clicks": 0, "clicks_reported": 4,
                 "conversions": 0.0, "engagements": 0, "video_views": 0,
                 "video_completions": 0, "video_views_3s": 0},
            ]
        return []

    @patch("backend.routers.creative.bq")
    def test_audience_row_and_cell_ctr_gate(self, mock_bq):
        from backend.routers import creative

        _patch_bq(mock_bq)
        mock_bq.run_query.side_effect = self._dispatch

        resp = asyncio.run(creative.get_audience_matrix("26023"))

        rows = {a.name: a for a in resp.audiences}
        # Site #3 — audience rows
        assert rows["DOOH Boards"].ctr is None
        assert rows["Prospecting"].ctr == 0.0

        # Site #4 — audience × creative cells (keyed by audience id → variant)
        dooh_aud = creative._audience_id("DOOH Boards", "stackadapt")
        disp_aud = creative._audience_id("Prospecting", "meta")
        assert resp.cells[dooh_aud]["DOOH Static"].ctr is None
        assert resp.cells[disp_aud]["Display Static"].ctr == 0.0
