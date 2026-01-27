# -*- coding: utf-8 -*-
"""
monthly_main.py
Entry point for GitHub Actions.
"""
from __future__ import annotations

import sys
import traceback

from src.dropbox_io import DropboxIO
from src.monthly_spec import MonthlyCfg
from src.monthly_pipeline_MULTISTAGE import run_multistage


def main() -> int:
    try:
        cfg = MonthlyCfg.from_env()
        dbx = DropboxIO.from_env()
        run_multistage(dbx, cfg)
        return 0
    except Exception:
        print("[MONTHLY] Unhandled exception:", file=sys.stderr)
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())