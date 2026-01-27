# -*- coding: utf-8 -*-
"""
monthly_spec.py

Env/config loader.

Folder convention (Dropbox):
<ROOT>/IN   : input
<ROOT>/DONE : processed inputs
<ROOT>/OUT  : outputs (final for last stage)

Roots (defaults):
- /00_inbox_raw
- /10_preformat_py
- /20_overview_api
- /30_personalize_py
System:
- /_system/state.json
- /_system/logs
"""
from __future__ import annotations

import os
from dataclasses import dataclass


<<<<<<< HEAD
def _getenv(name: str, default: str) -> str:
    v = os.getenv(name, "").strip()
    return v if v else default


@dataclass
class MonthlyCfg:
    # stage roots (NOT including /IN,/OUT,/DONE)
    inbox_root: str
    prep_root: str
    overview_root: str
    outbox_root: str

    state_path: str
    logs_dir: str
    mode: str = "multistage"

    @staticmethod
    def from_env() -> "MonthlyCfg":
        # IMPORTANT: roots only
        inbox_root = _getenv("MONTHLY_INBOX_PATH", "/00_inbox_raw")
        prep_root = _getenv("MONTHLY_PREP_DIR", "/10_preformat_py")
        overview_root = _getenv("MONTHLY_OVERVIEW_DIR", "/20_overview_api")
        outbox_root = _getenv("MONTHLY_OUTBOX_DIR", "/30_personalize_py")

        state_path = _getenv("MONTHLY_STATE_PATH", "/_system/state.json")
        logs_dir = _getenv("MONTHLY_LOGS_DIR", "/_system/logs")
        mode = _getenv("MONTHLY_MODE", "multistage")

        return MonthlyCfg(
=======
def _env(name: str, default: str = "") -> str:
    return (os.environ.get(name) or default).strip()


def _norm_root(p: str) -> str:
    p = (p or "").strip()
    if not p:
        return ""
    if not p.startswith("/"):
        p = "/" + p
    if len(p) > 1 and p.endswith("/"):
        p = p.rstrip("/")
    return p


def _join(root: str, sub: str) -> str:
    root = _norm_root(root)
    sub = (sub or "").strip().lstrip("/")
    if not root:
        return "/" + sub if sub else ""
    return root + ("/" + sub if sub else "")


@dataclass(frozen=True)
class MonthlyCfg:
    stage: str

    inbox_root: str
    prep_root: str
    overview_root: str
    outbox_root: str

    state_path: str
    logs_dir: str

    max_files_per_run: int
    max_input_chars: int

    openai_model: str
    depth: str

    @staticmethod
    def from_env() -> "MonthlyCfg":
        stage = (_env("MONTHLY_STAGE", "00") or "00").strip()
        if stage.isdigit() and len(stage) == 1:
            stage = f"0{stage}"

        inbox_root = _norm_root(_env("MONTHLY_INBOX_PATH", "/00_inbox_raw") or "/00_inbox_raw")
        prep_root = _norm_root(_env("MONTHLY_PREP_DIR", "/10_preformat_py") or "/10_preformat_py")
        overview_root = _norm_root(_env("MONTHLY_OVERVIEW_DIR", "/20_overview_api") or "/20_overview_api")
        outbox_root = _norm_root(_env("MONTHLY_OUTBOX_DIR", "/30_personalize_py") or "/30_personalize_py")

        state_path = _norm_root(_env("MONTHLY_STATE_PATH", _env("STATE_PATH", "/_system/state.json")) or "/_system/state.json")
        logs_dir = _norm_root(_env("MONTHLY_LOGS_DIR", _env("LOGS_DIR", "/_system/logs")) or "/_system/logs")

        max_files_per_run = int(_env("MAX_FILES_PER_RUN", "200"))
        max_input_chars = int(_env("MAX_INPUT_CHARS", "80000"))
        openai_model = _env("OPENAI_MODEL", "gpt-5-mini")
        depth = _env("DEPTH", "medium")

        return MonthlyCfg(
            stage=stage,
>>>>>>> dev
            inbox_root=inbox_root,
            prep_root=prep_root,
            overview_root=overview_root,
            outbox_root=outbox_root,
            state_path=state_path,
            logs_dir=logs_dir,
<<<<<<< HEAD
            mode=mode,
        )
=======
            max_files_per_run=max_files_per_run,
            max_input_chars=max_input_chars,
            openai_model=openai_model,
            depth=depth,
        )

    # derived folders
    def inbox_in(self) -> str: return _join(self.inbox_root, "IN")
    def inbox_done(self) -> str: return _join(self.inbox_root, "DONE")
    def inbox_out(self) -> str: return _join(self.inbox_root, "OUT")

    def prep_in(self) -> str: return _join(self.prep_root, "IN")
    def prep_done(self) -> str: return _join(self.prep_root, "DONE")
    def prep_out(self) -> str: return _join(self.prep_root, "OUT")

    def overview_in(self) -> str: return _join(self.overview_root, "IN")
    def overview_done(self) -> str: return _join(self.overview_root, "DONE")
    def overview_out(self) -> str: return _join(self.overview_root, "OUT")

    def outbox_in(self) -> str: return _join(self.outbox_root, "IN")
    def outbox_done(self) -> str: return _join(self.outbox_root, "DONE")
    def outbox_out(self) -> str: return _join(self.outbox_root, "OUT")
>>>>>>> dev
