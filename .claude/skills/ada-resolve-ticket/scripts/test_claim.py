#!/usr/bin/env python3
"""Dependency-free tests for claim.py pure helpers (also pytest-compatible)."""
from claim import filter_candidates, notes_force_park, parse_declared_files

CFG = {"asana": {"fields": {
    "ready_for": {"gid": "RF", "agent": "AGENT", "frazer": "FRAZER"},
    "status": {"gid": "ST", "not_started": "NS", "in_progress": "IP", "completed": "CO"},
    "priority": {"gid": "PR", "high": "HI", "medium": "MD", "low": "LO"},
}}}

# trimmed from the real "Buy Type" ticket body
BUY_TYPE_NOTES = """
Current behaviour (origin/main, backend/services/media_plan_sync.py):
- Auto-classification is a proxy at ~L1290.
Add coverage to backend/tests/test_bcdirect.py and test_media_plan_sync.py.
"""


def _task(gid, ready, status=None, prio=None, completed=False, notes=""):
    return {
        "gid": gid,
        "name": f"t{gid}",
        "completed": completed,
        "notes": notes,
        "custom_fields": [
            {"gid": "RF", "enum_value": {"gid": ready} if ready else None},
            {"gid": "ST", "enum_value": {"gid": status} if status else None},
            {"gid": "PR", "enum_value": {"gid": prio} if prio else None},
        ],
    }


def test_parse_finds_prefixed_paths():
    files = parse_declared_files(BUY_TYPE_NOTES)
    assert "backend/services/media_plan_sync.py" in files
    assert "backend/tests/test_bcdirect.py" in files


def test_parse_ignores_unprefixed_bare_filename():
    # "test_media_plan_sync.py" has no dir prefix -> too ambiguous to claim
    assert "test_media_plan_sync.py" not in parse_declared_files(BUY_TYPE_NOTES)


def test_parse_touches_line():
    files = parse_declared_files("Touches: frontend/src/app/page.tsx, backend/config.py")
    assert "frontend/src/app/page.tsx" in files
    assert "backend/config.py" in files


def test_filter_keeps_only_agent_uncompleted():
    tasks = [
        _task("A", "AGENT", "NS", "HI"),
        _task("D", "FRAZER", "NS", "HI"),          # Frazer's court -> out
        _task("E", "AGENT", "CO", "HI"),           # status Completed -> out
        _task("F", "AGENT", "NS", "HI", completed=True),  # done -> out
    ]
    got = [t["gid"] for t in filter_candidates(tasks, CFG)]
    assert got == ["A"]


def test_filter_orders_priority_then_resume_first():
    tasks = [
        _task("low", "AGENT", "NS", "LO"),
        _task("hi_new", "AGENT", "NS", "HI"),
        _task("hi_resume", "AGENT", "IP", "HI"),
    ]
    got = [t["gid"] for t in filter_candidates(tasks, CFG)]
    # High before Low; within High, the in-progress resume before the new one
    assert got == ["hi_resume", "hi_new", "low"]


def test_notes_force_park_detects_review_flag():
    assert notes_force_park("A5 text renders in Slack, so this needs Frazer "
                            "review before STG.")
    assert notes_force_park("Note: review before staging please.")
    assert notes_force_park("Do not auto-promote — client-facing copy.")


def test_notes_force_park_ignores_ordinary_prose():
    assert not notes_force_park("Rewrite the diagnostic copy in plain language.")
    assert not notes_force_park("")


def run_all():
    fns = [v for k, v in sorted(globals().items())
           if k.startswith("test_") and callable(v)]
    for fn in fns:
        fn()
        print(f"ok   {fn.__name__}")
    print(f"\n{len(fns)}/{len(fns)} passed")


if __name__ == "__main__":
    run_all()
