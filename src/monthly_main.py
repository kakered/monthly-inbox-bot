# -*- coding: utf-8 -*-
"""monthly_main.py
Entry point for GitHub Actions.

- builds config from env
- creates Dropbox client
- runs one-stage-per-run pipeline
"""

from __future__ import annotations

import os
import sys
import time
import traceback

from .dropbox_io import DropboxIO
from .monthly_spec import MonthlyCfg
from .monthly_pipeline_MULTISTAGE import run_multistage


def _run_id() -> str:
    # Prefer GitHub-provided IDs; fallback to timestamp
    run = (os.getenv("GITHUB_RUN_ID") or "").strip()
    attempt = (os.getenv("GITHUB_RUN_ATTEMPT") or "").strip()
    if run:
        return f"gh-{run}-{attempt or '1'}"
    return time.strftime("local-%Y%m%d-%H%M%S", time.gmtime())


def main() -> int:
    cfg = MonthlyCfg.from_env()
    dbx = DropboxIO.from_env()

    rid = _run_id()
    try:
        return int(run_multistage(dbx, cfg, run_id=rid) or 0)
    except Exception:
        traceback.print_exc(file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())