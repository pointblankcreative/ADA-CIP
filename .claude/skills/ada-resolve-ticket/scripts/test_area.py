#!/usr/bin/env python3
"""Dependency-free tests for area.py (also pytest-compatible)."""
from area import classify, claim_area, is_park_path


def test_media_plan_sync_parks():
    assert is_park_path("backend/services/media_plan_sync.py")


def test_sql_migration_parks():
    assert is_park_path("infrastructure/bigquery/migrations/2026-06-buy-type.sql")


def test_ingestion_transform_parks():
    assert is_park_path("ingestion/transformation/ga4_transform.py")


def test_any_sql_file_parks():
    assert is_park_path("frontend/whatever.sql")  # a SQL file anywhere is a data change


def test_diagnostics_engine_parks():
    assert is_park_path("backend/services/diagnostics/engine.py")


def test_frontend_auto():
    decision, _ = classify(["frontend/src/lib/api.ts"])
    assert decision == "auto"


def test_isolated_router_auto():
    decision, _ = classify(["backend/routers/ga4.py"])
    assert decision == "auto"


def test_models_auto():
    decision, _ = classify(["backend/models/creative.py"])
    assert decision == "auto"


def test_config_py_auto():
    decision, _ = classify(["backend/config.py"])
    assert decision == "auto"


def test_buy_type_ticket_parks():
    # the real ticket touches media_plan_sync.py + its backend test
    decision, reasons = classify(
        ["backend/services/media_plan_sync.py", "backend/tests/test_media_plan_sync.py"]
    )
    assert decision == "park"
    assert any("media_plan_sync" in r for r in reasons)


def test_mixed_frontend_plus_bq_parks():
    decision, _ = classify(["frontend/src/x.tsx", "backend/services/media_plan_sync.py"])
    assert decision == "park"


def test_unknown_zone_parks_conservatively():
    decision, _ = classify(["backend/services/some_new_helper.py"])
    assert decision == "park"


def test_no_files_parks():
    decision, _ = classify([])
    assert decision == "park"


def test_claim_area_file_level():
    assert claim_area(["frontend/src/lib/api.ts"]) == ["frontend/src/lib/api.ts"]


def test_claim_area_unknown_is_coarse():
    assert claim_area([]) == ["backend/", "frontend/"]


def run_all():
    fns = [v for k, v in sorted(globals().items())
           if k.startswith("test_") and callable(v)]
    for fn in fns:
        fn()
        print(f"ok   {fn.__name__}")
    print(f"\n{len(fns)}/{len(fns)} passed")


if __name__ == "__main__":
    run_all()
