# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import uuid

from src.dropbox_io import DropboxIO
from src.monthly_cfg import MonthlyCfg
from src.monthly_pipeline_MULTISTAGE import run_multistage


def _must_env(key: str) -> str:
    v = os.environ.get(key, "").strip()
    if not v:
        raise RuntimeError(f"Missing required env: {key}")
    return v


def main() -> int:
    # Run ID
    run_id = os.environ.get("RUN_ID", "").strip() or f"gh-{uuid.uuid4().hex[:12]}"

    # Dropbox credentials (from GitHub Actions Secrets)
    refresh_token = _must_env("DROPBOX_REFRESH_TOKEN")
    app_key = _must_env("DROPBOX_APP_KEY")
    app_secret = _must_env("DROPBOX_APP_SECRET")

    io = DropboxIO(refresh_token=refresh_token, app_key=app_key, app_secret=app_secret)

    # Config (from env: paths, model, etc.)
    cfg = MonthlyCfg.from_env()

    return int(run_multistage(io, cfg, run_id=run_id) or 0)


if __name__ == "__main__":
    raise SystemExit(main())