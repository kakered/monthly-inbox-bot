# -*- coding: utf-8 -*-
"""
monthly_spec.py
環境変数から MonthlyCfg を組み立てる。

目的:
- stage00_in 等の属性欠落で落ちない
"""

from __future__ import annotations

import os
from dataclasses import dataclass


def _env(name: str, default: str = "") -> str:
    v = os.environ.get(name)
    if v is None:
        return default
    return str(v).strip() or default


@dataclass
class MonthlyCfg:
    # system
    state_path: str
    logs_dir: str
    monthly_stage: str  # "00".."40" or ""（auto）
    max_files_per_run: int

    # stage paths
    stage00_in: str
    stage00_out: str
    stage00_done: str

    stage10_in: str
    stage10_out: str
    stage10_done: str

    stage20_in: str
    stage20_out: str
    stage20_done: str

    stage30_in: str
    stage30_out: str
    stage30_done: str

    stage40_in: str
    stage40_out: str
    stage40_done: str

    @classmethod
    def from_env(cls) -> "MonthlyCfg":
        def _int(name: str, default: int) -> int:
            s = _env(name, str(default))
            try:
                return int(s)
            except Exception:
                return default

        return cls(
            state_path=_env("STATE_PATH", "/_system/state.json"),
            logs_dir=_env("LOGS_DIR", "/_system/logs"),
            monthly_stage=_env("MONTHLY_STAGE", "").zfill(2) if _env("MONTHLY_STAGE", "") else "",
            max_files_per_run=_int("MAX_FILES_PER_RUN", 200),

            stage00_in=_env("STAGE00_IN", "/00_inbox_raw/IN"),
            stage00_out=_env("STAGE00_OUT", "/00_inbox_raw/OUT"),
            stage00_done=_env("STAGE00_DONE", "/00_inbox_raw/DONE"),

            stage10_in=_env("STAGE10_IN", "/10_preformat_py/IN"),
            stage10_out=_env("STAGE10_OUT", "/10_preformat_py/OUT"),
            stage10_done=_env("STAGE10_DONE", "/10_preformat_py/DONE"),

            stage20_in=_env("STAGE20_IN", "/20_overview_api/IN"),
            stage20_out=_env("STAGE20_OUT", "/20_overview_api/OUT"),
            stage20_done=_env("STAGE20_DONE", "/20_overview_api/DONE"),

            stage30_in=_env("STAGE30_IN", "/30_personalize_py/IN"),
            stage30_out=_env("STAGE30_OUT", "/30_personalize_py/OUT"),
            stage30_done=_env("STAGE30_DONE", "/30_personalize_py/DONE"),

            stage40_in=_env("STAGE40_IN", "/40_trends_api/IN"),
            stage40_out=_env("STAGE40_OUT", "/40_trends_api/OUT"),
            stage40_done=_env("STAGE40_DONE", "/40_trends_api/DONE"),
        )