# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import time
from datetime import datetime, timezone

import dropbox

from src.dropbox_io import DropboxIO
from src.monthly_spec import MonthlyCfg
from src.state_store import StateStore
from src.monthly_pipeline_MULTISTAGE import run_multistage


def _make_run_id() -> str:
    # GitHub Actions の run id が取れればそれを使い、なければUTC時刻
    rid = os.environ.get("GITHUB_RUN_ID")
    if rid:
        attempt = os.environ.get("GITHUB_RUN_ATTEMPT", "1")
        return f"gh-{rid}-{attempt}"
    return datetime.now(timezone.utc).strftime("utc-%Y%m%d-%H%M%S")


def main() -> int:
    cfg = MonthlyCfg.from_env()
    run_id = _make_run_id()

    dbx = dropbox.Dropbox(
        oauth2_refresh_token=os.environ["DROPBOX_REFRESH_TOKEN"],
        app_key=os.environ["DROPBOX_APP_KEY"],
        app_secret=os.environ["DROPBOX_APP_SECRET"],
    )
    io = DropboxIO(dbx)

    # state は pipeline 内で更新・保存する
    return int(run_multistage(io, cfg, run_id=run_id) or 0)


if __name__ == "__main__":
    raise SystemExit(main())