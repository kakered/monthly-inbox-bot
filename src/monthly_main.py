# -*- coding: utf-8 -*-
"""
monthly_main.py

Entry point for GitHub Actions.

Auth:
- Recommended: DROPBOX_REFRESH_TOKEN + DROPBOX_APP_KEY + DROPBOX_APP_SECRET
- Optional: DROPBOX_ACCESS_TOKEN

Stage:
- MONTHLY_STAGE=00 / 10 / 20
"""
from __future__ import annotations

from .dropbox_io import DropboxIO
from .monthly_spec import MonthlyCfg
from .monthly_pipeline_MULTISTAGE import run_switch_stage


def main() -> int:
    dbx = DropboxIO.from_env()
    cfg = MonthlyCfg.from_env()
    run_switch_stage(dbx, cfg)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())