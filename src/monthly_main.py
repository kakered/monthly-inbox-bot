# -*- coding: utf-8 -*-
"""
monthly_main.py

Entry point for GitHub Actions (Monthly pipeline).
- Loads config from env (MonthlyCfg)
- Builds Dropbox client (refresh token preferred)
- Runs stage switch pipeline
"""

from __future__ import annotations

from src.dropbox_io import DropboxIO
from src.monthly_spec import MonthlyCfg
from src.monthly_pipeline_MULTISTAGE import run_switch_stage


def main() -> int:
    dbx = DropboxIO.from_env()
    cfg = MonthlyCfg.from_env()
    processed = run_switch_stage(dbx, cfg)
    print(f"[MONTHLY] Done. stage={cfg.monthly_stage} processed={processed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())