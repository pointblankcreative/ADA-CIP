#!/usr/bin/env python3
"""common.py -- shared config + repo-path resolution for the ADA resolver scripts.

The skill lives at <repo>/.claude/skills/ada-resolve-ticket/, so the repo root is
parents[4] of this file (scripts -> ada-resolve-ticket -> skills -> .claude -> repo).
Override with env ADA_REPO for testing or if the skill is installed elsewhere.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

_SKILL_DIR = Path(__file__).resolve().parents[1]  # .../ada-resolve-ticket


def repo_root() -> Path:
    env = os.environ.get("ADA_REPO")
    if env:
        return Path(env).expanduser().resolve()
    # scripts/ -> ada-resolve-ticket/ -> skills/ -> .claude/ -> <repo>
    return Path(__file__).resolve().parents[4]


def load_config() -> dict:
    override = os.environ.get("ADA_CONFIG")
    path = Path(override) if override else (_SKILL_DIR / "config.json")
    return json.loads(Path(path).read_text())


def venv_python() -> str:
    return str(repo_root() / ".venv" / "bin" / "python")


def asana_pat(cfg: dict) -> str:
    pat = os.environ.get(cfg["asana"]["pat_env"], "")
    if not pat:
        raise SystemExit(
            f"Missing Asana token: set ${cfg['asana']['pat_env']} in the environment."
        )
    return pat
