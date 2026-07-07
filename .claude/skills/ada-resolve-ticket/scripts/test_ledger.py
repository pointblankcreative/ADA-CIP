#!/usr/bin/env python3
"""
Dependency-free tests for ledger.py (also pytest-compatible).

Run either way:
    python test_ledger.py
    python -m pytest test_ledger.py -q
"""
import tempfile
from pathlib import Path

from ledger import Ledger, areas_overlap, paths_overlap


def _tmp_ledger():
    d = tempfile.mkdtemp()
    return Path(d) / "active-work.json"


# ---- overlap logic -----------------------------------------------------
def test_overlap_exact():
    assert paths_overlap("backend/services/media_plan_sync.py",
                         "backend/services/media_plan_sync.py")


def test_overlap_parent_child_both_directions():
    assert paths_overlap("backend/services", "backend/services/media_plan_sync.py")
    assert paths_overlap("backend/services/media_plan_sync.py", "backend/services")


def test_no_overlap_siblings():
    assert not paths_overlap("frontend/components/perf",
                             "backend/services/media_plan_sync.py")


def test_no_overlap_prefix_not_on_boundary():
    # "backend/serv" must NOT be treated as a parent of "backend/services"
    assert not paths_overlap("backend/serv", "backend/services/x.py")


def test_overlap_glob():
    assert paths_overlap("backend/**", "backend/services/media_plan_sync.py")
    assert paths_overlap("frontend/components/*", "frontend/components/glossary.tsx")
    assert not paths_overlap("frontend/**", "backend/services/x.py")


def test_areas_overlap_lists():
    a = ["frontend/components/perf", "frontend/lib/glossary.ts"]
    b = ["backend/services/media_plan_sync.py"]
    c = ["frontend/lib/glossary.ts"]
    assert not areas_overlap(a, b)
    assert areas_overlap(a, c)


# ---- claim lifecycle ---------------------------------------------------
def test_claim_on_empty_succeeds():
    with Ledger.locked(_tmp_ledger()) as led:
        claim = led.claim_if_free("1", "t1", ["backend/services"], "ada/t1", "/wt/1")
        assert claim is not None
        assert claim.status == "claimed"
        assert len(led.running()) == 1


def test_overlapping_area_is_refused():
    with Ledger.locked(_tmp_ledger()) as led:
        assert led.claim_if_free("1", "t1", ["backend/services"], "ada/t1", "/wt/1")
        # different ticket, overlapping module -> refused
        assert led.claim_if_free(
            "2", "t2", ["backend/services/media_plan_sync.py"], "ada/t2", "/wt/2"
        ) is None


def test_disjoint_areas_run_in_parallel():
    with Ledger.locked(_tmp_ledger()) as led:
        assert led.claim_if_free("1", "t1", ["backend/services"], "ada/t1", "/wt/1")
        assert led.claim_if_free(
            "2", "t2", ["frontend/components/perf"], "ada/t2", "/wt/2"
        ) is not None
        assert len(led.running()) == 2


def test_parked_ticket_still_reserves_its_area():
    with Ledger.locked(_tmp_ledger()) as led:
        led.claim_if_free("1", "t1", ["backend/services"], "ada/t1", "/wt/1")
        led.set_status("1", "parked")
        # a parked WIP still blocks an overlapping new ticket
        assert led.claim_if_free(
            "2", "t2", ["backend/services/x.py"], "ada/t2", "/wt/2"
        ) is None


def test_parked_ticket_can_be_resumed():
    with Ledger.locked(_tmp_ledger()) as led:
        led.claim_if_free("1", "t1", ["backend/services"], "ada/t1", "/wt/1")
        led.set_status("1", "parked")
        resumed = led.claim_if_free("1", "t1", ["backend/services"], "ada/t1", "/wt/1")
        assert resumed is not None
        assert resumed.status == "claimed"


def test_claimed_ticket_cannot_be_stolen():
    with Ledger.locked(_tmp_ledger()) as led:
        led.claim_if_free("1", "t1", ["backend/services"], "ada/t1", "/wt/1")
        # a second session trying the same live ticket is refused
        assert led.claim_if_free("1", "t1", ["backend/services"], "ada/t1", "/wt/1") is None


def test_remove_frees_the_area():
    with Ledger.locked(_tmp_ledger()) as led:
        led.claim_if_free("1", "t1", ["backend/services"], "ada/t1", "/wt/1")
        assert led.remove("1")
        assert led.claim_if_free(
            "2", "t2", ["backend/services/x.py"], "ada/t2", "/wt/2"
        ) is not None


# ---- self-heal ---------------------------------------------------------
def test_can_claim_is_nonmutating():
    with Ledger.locked(_tmp_ledger()) as led:
        assert led.can_claim("1", ["backend/services"]) is True
        assert led.get("1") is None      # peeking must not create a claim
        assert led.running() == []


def test_can_claim_agrees_with_conflicts():
    with Ledger.locked(_tmp_ledger()) as led:
        led.claim_if_free("1", "t1", ["backend/services"], "ada/t1", "/wt/1")
        assert led.can_claim("2", ["backend/services/x.py"]) is False  # area busy
        assert led.can_claim("3", ["frontend/x"]) is True              # free


def test_gc_reaps_dead_pid():
    p = _tmp_ledger()
    with Ledger.locked(p) as led:
        led.claim_if_free("1", "t1", ["backend/services"], "ada/t1", "/wt/1")
        # forge a crashed session: impossible pid + old timestamp
        rec = led.get("1")
        rec["pid"] = 2147480000
        rec["host"] = __import__("socket").gethostname()
        rec["updated_at"] = "2000-01-01T00:00:00+00:00"
        led._save()
    # next lock triggers gc_stale during load
    with Ledger.locked(p) as led:
        assert led.get("1") is None


# ---- locking -----------------------------------------------------------
def test_lock_is_exclusive():
    p = _tmp_ledger()
    with Ledger.locked(p):
        timed_out = False
        try:
            with Ledger.locked(p, timeout=0.4):
                pass
        except TimeoutError:
            timed_out = True
        assert timed_out, "a second lock on a held ledger must time out"


def run_all():
    fns = [
        v
        for k, v in sorted(globals().items())
        if k.startswith("test_") and callable(v)
    ]
    for fn in fns:
        fn()
        print(f"ok   {fn.__name__}")
    print(f"\n{len(fns)}/{len(fns)} passed")


if __name__ == "__main__":
    run_all()
