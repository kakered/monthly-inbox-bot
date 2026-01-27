# -*- coding: utf-8 -*-
"""
monthly_main.py

Entry point for GitHub Actions (Monthly pipeline).
"""

from src.dropbox_io import DropboxIO
from src.monthly_spec import MonthlyCfg
from src.monthly_pipeline_MULTISTAGE import run_switch_stage


def main() -> int:
    dbx = DropboxIO.from_env()
    cfg = MonthlyCfg.from_env()
    run_switch_stage(dbx, cfg)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())