　# -*- coding: utf-8 -*-
"""
monthly_main.py
GitHub Actions から呼ばれる entry point.

- DropboxIO.from_env()
- MonthlyCfg.from_env()
- run_id 生成
- monthly_pipeline_MULTISTAGE.run_multistage を呼ぶ
"""

from __future__ import annotations

from src.dropbox_io import DropboxIO
from src.monthly_spec import MonthlyCfg
from src.audit_logger import build_run_id
from src.monthly_pipeline_MULTISTAGE import run_multistage


def main() -> int:
    dbx = DropboxIO.from_env()
    cfg = MonthlyCfg.from_env()
    run_id = build_run_id()
    return int(run_multistage(dbx, cfg, run_id=run_id) or 0)


if __name__ == "__main__":
    raise SystemExit(main())