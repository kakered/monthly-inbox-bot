# -*- coding: utf-8 -*-

from __future__ import annotations

import os
from dataclasses import dataclass


def _env(key: str, default: str = "") -> str:
    return os.environ.get(key, default).strip()


def _must_env(key: str) -> str:
    v = _env(key, "")
    if not v:
        raise RuntimeError(f"Missing required env: {key}")
    return v


def _env_int(key: str, default: int) -> int:
    v = _env(key, "")
    if not v:
        return int(default)
    return int(v)


@dataclass(frozen=True)
class MonthlyCfg:
    # system
    logs_dir: str
    state_path: str

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

    # controls
    monthly_stage: str  # "00"/"10"/"20"/"30"/"40"
    depth: str          # "light"/"medium"/"heavy" (運用側で自由)
    max_files_per_run: int
    max_input_chars: int

    # openai controls (pipeline側が読む想定)
    openai_model: str
    openai_timeout: int
    openai_max_retries: int
    openai_max_output_tokens: int

    @classmethod
    def from_env(cls) -> "MonthlyCfg":
        # logs/state
        logs_dir = _must_env("LOGS_DIR")
        state_path = _must_env("STATE_PATH")

        # stage paths
        return cls(
            logs_dir=logs_dir,
            state_path=state_path,

            stage00_in=_must_env("STAGE00_IN"),
            stage00_out=_must_env("STAGE00_OUT"),
            stage00_done=_must_env("STAGE00_DONE"),

            stage10_in=_must_env("STAGE10_IN"),
            stage10_out=_must_env("STAGE10_OUT"),
            stage10_done=_must_env("STAGE10_DONE"),

            stage20_in=_must_env("STAGE20_IN"),
            stage20_out=_must_env("STAGE20_OUT"),
            stage20_done=_must_env("STAGE20_DONE"),

            stage30_in=_must_env("STAGE30_IN"),
            stage30_out=_must_env("STAGE30_OUT"),
            stage30_done=_must_env("STAGE30_DONE"),

            stage40_in=_must_env("STAGE40_IN"),
            stage40_out=_must_env("STAGE40_OUT"),
            stage40_done=_must_env("STAGE40_DONE"),

            monthly_stage=_env("MONTHLY_STAGE", "00"),
            depth=_env("DEPTH", "medium"),
            max_files_per_run=_env_int("MAX_FILES_PER_RUN", 200),
            max_input_chars=_env_int("MAX_INPUT_CHARS", 80000),

            openai_model=_env("OPENAI_MODEL", "gpt-5-mini"),
            openai_timeout=_env_int("OPENAI_TIMEOUT", 120),
            openai_max_retries=_env_int("OPENAI_MAX_RETRIES", 2),
            openai_max_output_tokens=_env_int("OPENAI_MAX_OUTPUT_TOKENS", 5000),
        )