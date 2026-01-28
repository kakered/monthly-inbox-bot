# -*- coding: utf-8 -*-
"""monthly_spec.py
Env-driven config for monthly pipeline.
"""

from __future__ import annotations

from dataclasses import dataclass
import os


def _env(name: str, default: str | None = None) -> str | None:
    v = os.getenv(name)
    if v is None:
        return default
    v = v.strip()
    return v if v else default


def _must_env(name: str) -> str:
    v = _env(name)
    if v is None:
        raise RuntimeError(f"Missing required env var: {name}")
    return v


@dataclass(frozen=True)
class MonthlyCfg:
    # OpenAI (kept even if a stage doesn't use it yet)
    openai_api_key: str
    openai_model: str = "gpt-5-mini"
    depth: str = "medium"
    openai_timeout: int = 120
    openai_max_retries: int = 2
    openai_max_output_tokens: int = 5000

    # Dropbox auth
    dropbox_refresh_token: str | None = None
    dropbox_app_key: str | None = None
    dropbox_app_secret: str | None = None
    dropbox_access_token: str | None = None

    # Control
    monthly_stage: str = "00"  # stage selection start point: "00".."40"
    max_files_per_run: int = 200
    max_input_chars: int = 80000

    # Dropbox paths
    state_path: str = "/_system/state.json"
    logs_dir: str = "/_system/logs"

    stage00_in: str = "/00_inbox_raw/IN"
    stage00_out: str = "/00_inbox_raw/OUT"
    stage00_done: str = "/00_inbox_raw/DONE"

    stage10_in: str = "/10_preformat_py/IN"
    stage10_out: str = "/10_preformat_py/OUT"
    stage10_done: str = "/10_preformat_py/DONE"

    stage20_in: str = "/20_overview_api/IN"
    stage20_out: str = "/20_overview_api/OUT"
    stage20_done: str = "/20_overview_api/DONE"

    stage30_in: str = "/30_personalize_py/IN"
    stage30_out: str = "/30_personalize_py/OUT"
    stage30_done: str = "/30_personalize_py/DONE"

    stage40_in: str = "/40_trends_api/IN"
    stage40_out: str = "/40_trends_api/OUT"
    stage40_done: str = "/40_trends_api/DONE"

    @staticmethod
    def from_env() -> "MonthlyCfg":
        return MonthlyCfg(
            openai_api_key=_must_env("OPENAI_API_KEY"),
            openai_model=_env("OPENAI_MODEL", "gpt-5-mini") or "gpt-5-mini",
            depth=_env("DEPTH", "medium") or "medium",
            openai_timeout=int(_env("OPENAI_TIMEOUT", "120") or "120"),
            openai_max_retries=int(_env("OPENAI_MAX_RETRIES", "2") or "2"),
            openai_max_output_tokens=int(_env("OPENAI_MAX_OUTPUT_TOKENS", "5000") or "5000"),
            dropbox_refresh_token=_env("DROPBOX_REFRESH_TOKEN"),
            dropbox_app_key=_env("DROPBOX_APP_KEY"),
            dropbox_app_secret=_env("DROPBOX_APP_SECRET"),
            dropbox_access_token=_env("DROPBOX_ACCESS_TOKEN"),
            monthly_stage=_env("MONTHLY_STAGE", "00") or "00",
            max_files_per_run=int(_env("MAX_FILES_PER_RUN", "200") or "200"),
            max_input_chars=int(_env("MAX_INPUT_CHARS", "80000") or "80000"),
            state_path=_env("STATE_PATH", "/_system/state.json") or "/_system/state.json",
            logs_dir=_env("LOGS_DIR", "/_system/logs") or "/_system/logs",
            stage00_in=_env("STAGE00_IN", "/00_inbox_raw/IN") or "/00_inbox_raw/IN",
            stage00_out=_env("STAGE00_OUT", "/00_inbox_raw/OUT") or "/00_inbox_raw/OUT",
            stage00_done=_env("STAGE00_DONE", "/00_inbox_raw/DONE") or "/00_inbox_raw/DONE",
            stage10_in=_env("STAGE10_IN", "/10_preformat_py/IN") or "/10_preformat_py/IN",
            stage10_out=_env("STAGE10_OUT", "/10_preformat_py/OUT") or "/10_preformat_py/OUT",
            stage10_done=_env("STAGE10_DONE", "/10_preformat_py/DONE") or "/10_preformat_py/DONE",
            stage20_in=_env("STAGE20_IN", "/20_overview_api/IN") or "/20_overview_api/IN",
            stage20_out=_env("STAGE20_OUT", "/20_overview_api/OUT") or "/20_overview_api/OUT",
            stage20_done=_env("STAGE20_DONE", "/20_overview_api/DONE") or "/20_overview_api/DONE",
            stage30_in=_env("STAGE30_IN", "/30_personalize_py/IN") or "/30_personalize_py/IN",
            stage30_out=_env("STAGE30_OUT", "/30_personalize_py/OUT") or "/30_personalize_py/OUT",
            stage30_done=_env("STAGE30_DONE", "/30_personalize_py/DONE") or "/30_personalize_py/DONE",
            stage40_in=_env("STAGE40_IN", "/40_trends_api/IN") or "/40_trends_api/IN",
            stage40_out=_env("STAGE40_OUT", "/40_trends_api/OUT") or "/40_trends_api/OUT",
            stage40_done=_env("STAGE40_DONE", "/40_trends_api/DONE") or "/40_trends_api/DONE",
        )