# -*- coding: utf-8 -*-
from __future__ import annotations

import os
from dataclasses import dataclass


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
            inbox_root=inbox_root,
            prep_root=prep_root,
            overview_root=overview_root,
            outbox_root=outbox_root,
            state_path=state_path,
            logs_dir=logs_dir,
            mode=mode,
        )