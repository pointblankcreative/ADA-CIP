#!/usr/bin/env python3
"""area.py -- classify which part of the ADA codebase a ticket touches, and
decide whether a fix may auto-promote to staging or must PARK for Frazer.

Rule (deliberately conservative, matching Frazer's under-promise ethos): a ticket
auto-promotes only if EVERY file it touches lives in a known frontend / isolated-
backend zone. If any file is a BigQuery / schema / ingestion / transform path, or
lands in an unrecognised zone, the ticket parks. Frontend-only sails; anything
that can move data or change schema stops for a human.

The diagnostics engine (backend/services/diagnostics/) was moved from park ->
auto on 2026-07-07 at Frazer's request — he runs this skill only when he's
comfortable auto-promoting engine changes. Data-moving/transform files, any
.sql, and unrecognised zones still park; a ticket whose body explicitly asks
for review before staging also parks (see claim.py _notes_force_park).

Defaults below mirror config.json (the production source of truth); they are
duplicated here only so the module is self-contained and unit-testable.
"""
from __future__ import annotations

DEFAULT_PARK = {
    "dirs": ["ingestion/", "infrastructure/bigquery/", "scripts/"],
    "files": ["backend/services/transformation.py",
              "backend/services/media_plan_sync.py",
              "backend/services/daily_job.py",
              "backend/services/creative_assets.py"],
    "suffixes": [".sql"],
}
DEFAULT_AUTO = {
    "dirs": ["frontend/", "backend/routers/", "backend/models/",
             "backend/middleware/", "backend/services/diagnostics/",
             "tests/", "backend/tests/", "docs/"],
    "files": ["backend/config.py"],
}


def norm(p: str) -> str:
    p = (p or "").strip().replace("\\", "/")
    while p.startswith("./"):
        p = p[2:]
    return p.lstrip("/")


def _under(path: str, d: str) -> bool:
    d = d if d.endswith("/") else d + "/"
    return path == d[:-1] or path.startswith(d)


def is_park_path(path: str, park=DEFAULT_PARK) -> bool:
    p = norm(path)
    if not p:
        return False
    if p in set(park["files"]):
        return True
    if any(p.endswith(s) for s in park["suffixes"]):
        return True
    return any(_under(p, d) for d in park["dirs"])


def is_auto_path(path: str, auto=DEFAULT_AUTO) -> bool:
    p = norm(path)
    if not p:
        return False
    if p in set(auto["files"]):
        return True
    return any(_under(p, d) for d in auto["dirs"])


def classify(paths, park=DEFAULT_PARK, auto=DEFAULT_AUTO):
    """Return (decision, reasons). decision is 'auto' or 'park'.

    Any park path -> park. Otherwise every path must be auto-eligible, else park
    (an unrecognised zone is treated conservatively). reasons is a list of short
    strings suitable for pasting into a park comment."""
    paths = [norm(p) for p in paths if norm(p)]
    if not paths:
        return "park", ["no files declared on the ticket; cannot confirm frontend-only"]
    park_hits = [p for p in paths if is_park_path(p, park)]
    if park_hits:
        return "park", [f"{p} -> BigQuery/ingestion/schema path (needs your review)"
                        for p in park_hits]
    unknown = [p for p in paths if not is_auto_path(p, auto)]
    if unknown:
        return "park", [f"{p} -> unrecognised zone, not confirmed frontend-only"
                        for p in unknown]
    return "auto", [f"{p} -> frontend / isolated backend" for p in paths]


def claim_area(declared_files):
    """Area entries to reserve in the ledger. Claim at file granularity when the
    files are known (maximum parallelism); if the ticket declares no files, fall
    back to a coarse reservation of both source roots so an unknown-area job
    serialises against everything rather than silently colliding."""
    files = [norm(p) for p in (declared_files or []) if norm(p)]
    return files or ["backend/", "frontend/"]


def _load_cfg_lists():
    """Prefer config.json's park/auto lists (the production source of truth); fall
    back to the module defaults if config or common is unavailable."""
    try:
        import common
        cfg = common.load_config()
        return cfg.get("park", DEFAULT_PARK), cfg.get("auto", DEFAULT_AUTO)
    except Exception:
        return DEFAULT_PARK, DEFAULT_AUTO


if __name__ == "__main__":
    # CLI safety backstop: read changed paths (args or --stdin), print the decision,
    # exit 0 if the whole diff may auto-promote, 3 if any path must park.
    import sys

    _args = sys.argv[1:]
    if "--stdin" in _args:
        _paths = [ln.strip() for ln in sys.stdin.read().splitlines() if ln.strip()]
    else:
        _paths = [a for a in _args if not a.startswith("-")]
    _park, _auto = _load_cfg_lists()
    _decision, _reasons = classify(_paths, _park, _auto)
    sys.stderr.write("\n".join(_reasons) + "\n")
    print(_decision)
    sys.exit(0 if _decision == "auto" else 3)
