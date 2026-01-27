# -*- coding: utf-8 -*-
"""
monthly_main.py

GitHub Actions から起動される monthly パイプラインの entry point。

保証すること
- env（YAML/Secrets）から設定取得（触らない）
- entry を 1本化（src.monthly_main だけ）
- run_id を GitHub Actions の run に寄せる
"""

from __future__ import annotations

import os
import time
import traceback

from .dropbox_io import DropboxIO
from .monthly_spec import MonthlyCfg
from .monthly_pipeline_MULTISTAGE import run_multistage


def _run_id() -> str:
    rid = (os.getenv("GITHUB_RUN_ID") or "").strip()
    attempt = (os.getenv("GITHUB_RUN_ATTEMPT") or "").strip()
    if rid:
        return f"gh-{rid}" + (f"-{attempt}" if attempt else "")
    return time.strftime("%Y%m%d-%H%M%SZ", time.gmtime())


def main() -> int:
    cfg = MonthlyCfg.from_env()
    dbx = DropboxIO.from_env()
    rid = _run_id()

    try:
        return int(run_multistage(dbx, cfg, run_id=rid) or 0)
    except Exception:
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())