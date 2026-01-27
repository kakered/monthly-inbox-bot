# -*- coding: utf-8 -*-
"""
monthly_spec.py
Configuration loader for the monthly multistage pipeline.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Dict


def _must_env(name: str) -> str:
    v = (os.getenv(name) or "").strip()
    if not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v


def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


@dataclass(frozen=True)
class StageCfg:
    in_dir: str
    out_dir: str
    done_dir: str


@dataclass(frozen=True)
class MonthlyCfg:
    stage: str
    state_path: str
    logs_dir: str
    stages: Dict[str, StageCfg]

    openai_model: str
    depth: str
    openai_timeout: int
    openai_max_retries: int
    openai_max_output_tokens: int
    max_files_per_run: int
    max_input_chars: int

    @classmethod
    def from_env(cls) -> "MonthlyCfg":
        stage = _env("MONTHLY_STAGE", "00") or "00"
        stage = stage.zfill(2)

        stages: Dict[str, StageCfg] = {}
        for k in ["00", "10", "20", "30", "40"]:
            stages[k] = StageCfg(
                in_dir=_must_env(f"STAGE{k}_IN"),
                out_dir=_must_env(f"STAGE{k}_OUT"),
                done_dir=_must_env(f"STAGE{k}_DONE"),
            )

        return cls(
            stage=stage,
            state_path=_must_env("STATE_PATH"),
            logs_dir=_must_env("LOGS_DIR"),
            stages=stages,
            openai_model=_env("OPENAI_MODEL", "gpt-5-mini"),
            depth=_env("DEPTH", "medium"),
            openai_timeout=int(_env("OPENAI_TIMEOUT", "120") or "120"),
            openai_max_retries=int(_env("OPENAI_MAX_RETRIES", "2") or "2"),
            openai_max_output_tokens=int(_env("OPENAI_MAX_OUTPUT_TOKENS", "5000") or "5000"),
            max_files_per_run=int(_env("MAX_FILES_PER_RUN", "200") or "200"),
            max_input_chars=int(_env("MAX_INPUT_CHARS", "80000") or "80000"),
        )

    def s(self, stage: str) -> StageCfg:
        stage = stage.zfill(2)
        return self.stages[stage]