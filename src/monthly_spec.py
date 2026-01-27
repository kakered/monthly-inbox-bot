# -*- coding: utf-8 -*-
"""
monthly_spec.py

Monthly pipeline configuration loaded from environment variables.

Design:
- Keep it simple and robust for GitHub Actions.
- Prefer Dropbox refresh-token auth (handled in dropbox_io.py).
- Stage routing uses these env vars:
  STAGE00_IN/OUT/DONE, STAGE10_IN/OUT/DONE, ... STAGE40_...
- MONTHLY_STAGE selects which stage to run (00/10/20/30/40).
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Dict


def _env(name: str, default: str | None = None) -> str:
    v = os.getenv(name)
    if v is None:
        if default is None:
            raise RuntimeError(f"Missing required env var: {name}")
        return default
    return v


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or v.strip() == "":
        return default
    return int(v)


@dataclass(frozen=True)
class StagePaths:
    in_path: str
    out_path: str
    done_path: str


@dataclass(frozen=True)
class MonthlyCfg:
    # Basic
    state_path: str
    logs_dir: str

    # Stages
    stages: Dict[int, StagePaths]  # keys: 0,10,20,30,40
    monthly_stage: int             # 0,10,20,30,40

    # Runtime tuning
    max_files_per_run: int
    max_input_chars: int
    depth: str
    openai_model: str
    openai_timeout: int
    openai_max_retries: int
    openai_max_output_tokens: int

    @staticmethod
    def from_env() -> "MonthlyCfg":
        stages = {
            0: StagePaths(
                in_path=_env("STAGE00_IN"),
                out_path=_env("STAGE00_OUT"),
                done_path=_env("STAGE00_DONE"),
            ),
            10: StagePaths(
                in_path=_env("STAGE10_IN"),
                out_path=_env("STAGE10_OUT"),
                done_path=_env("STAGE10_DONE"),
            ),
            20: StagePaths(
                in_path=_env("STAGE20_IN"),
                out_path=_env("STAGE20_OUT"),
                done_path=_env("STAGE20_DONE"),
            ),
            30: StagePaths(
                in_path=_env("STAGE30_IN"),
                out_path=_env("STAGE30_OUT"),
                done_path=_env("STAGE30_DONE"),
            ),
            40: StagePaths(
                in_path=_env("STAGE40_IN"),
                out_path=_env("STAGE40_OUT"),
                done_path=_env("STAGE40_DONE"),
            ),
        }

        stage_raw = _env("MONTHLY_STAGE", "00").strip()
        # Accept "00" / "0" / "10" etc.
        stage_int = int(stage_raw)

        if stage_int not in stages:
            raise RuntimeError(f"Invalid MONTHLY_STAGE={stage_raw!r}. Must be one of {sorted(stages.keys())}")

        return MonthlyCfg(
            state_path=_env("STATE_PATH"),
            logs_dir=_env("LOGS_DIR"),
            stages=stages,
            monthly_stage=stage_int,
            max_files_per_run=_env_int("MAX_FILES_PER_RUN", 200),
            max_input_chars=_env_int("MAX_INPUT_CHARS", 80000),
            depth=_env("DEPTH", "medium"),
            openai_model=_env("OPENAI_MODEL", "gpt-5-mini"),
            openai_timeout=_env_int("OPENAI_TIMEOUT", 120),
            openai_max_retries=_env_int("OPENAI_MAX_RETRIES", 2),
            openai_max_output_tokens=_env_int("OPENAI_MAX_OUTPUT_TOKENS", 5000),
        )