# -*- coding: utf-8 -*-
"""
Local/Manual runner for monthly pipeline.

Examples:
  python -m src.run_monthly --stage 00
  python -m src.run_monthly --stage 10 --depth medium --model gpt-5-mini

This script sets env vars and then calls src.monthly_main.main().
GitHub Actions can continue to call `python -m src.monthly_main`.
"""

from __future__ import annotations

import argparse
import os
import sys

from src.monthly_main import main as monthly_main


def _set_if(provided: str | None, env_key: str) -> None:
    if provided is not None and str(provided).strip() != "":
        os.environ[env_key] = str(provided).strip()


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(add_help=True)
    p.add_argument("--stage", default="00", choices=["00", "10", "20", "30", "40"], help="Monthly stage to run")
    p.add_argument("--depth", default=None, help="DEPTH env (e.g., medium/heavy)")
    p.add_argument("--model", default=None, help="OPENAI_MODEL env (e.g., gpt-5-mini)")
    p.add_argument("--max-files", default=None, help="MAX_FILES_PER_RUN env")
    p.add_argument("--max-input-chars", default=None, help="MAX_INPUT_CHARS env")
    p.add_argument("--max-output-tokens", default=None, help="OPENAI_MAX_OUTPUT_TOKENS env")
    p.add_argument("--timeout", default=None, help="OPENAI_TIMEOUT env")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    ns = parse_args(argv or sys.argv[1:])

    # Mandatory for monthly_main: MONTHLY_STAGE
    os.environ["MONTHLY_STAGE"] = ns.stage

    # Optional controls
    _set_if(ns.depth, "DEPTH")
    _set_if(ns.model, "OPENAI_MODEL")
    _set_if(ns.max_files, "MAX_FILES_PER_RUN")
    _set_if(ns.max_input_chars, "MAX_INPUT_CHARS")
    _set_if(ns.max_output_tokens, "OPENAI_MAX_OUTPUT_TOKENS")
    _set_if(ns.timeout, "OPENAI_TIMEOUT")

    return int(monthly_main())


if __name__ == "__main__":
    raise SystemExit(main())