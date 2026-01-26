# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sys
import traceback

import dropbox

from .dropbox_io import DropboxIO
from .monthly_spec import MonthlyCfg
from .monthly_pipeline_MULTISTAGE import run_multistage


def _env(name: str, default: str = "") -> str:
    v = os.environ.get(name, default)
    return v.strip() if isinstance(v, str) else default


def _build_cfg() -> MonthlyCfg:
    # STAGE00/10/20/30 は GitHub Actions Variables で設定している前提
    inbox_path = _env("STAGE00_IN", "/00_inbox_raw/IN")
    prep_dir = _env("STAGE10_IN", "/10_preformat_py/IN")
    overview_dir = _env("STAGE20_IN", "/20_overview_api/IN")
    outbox_dir = _env("STAGE30_IN", "/30_personalize_py/IN")

    state_path = _env("STATE_PATH", "/_system/state.json")
    logs_dir = _env("LOGS_DIR", "/_system/logs")

    return MonthlyCfg(
        inbox_path=inbox_path,
        prep_dir=prep_dir,
        overview_dir=overview_dir,
        outbox_dir=outbox_dir,
        state_path=state_path,
        logs_dir=logs_dir,
        mode=_env("MONTHLY_MODE", "multistage"),
    )


def _build_dbx() -> DropboxIO:
    refresh = _env("DROPBOX_REFRESH_TOKEN")
    app_key = _env("DROPBOX_APP_KEY")
    app_secret = _env("DROPBOX_APP_SECRET")

    if not refresh or not app_key or not app_secret:
        raise RuntimeError(
            "Missing Dropbox credentials. Require DROPBOX_REFRESH_TOKEN, DROPBOX_APP_KEY, DROPBOX_APP_SECRET."
        )

    dbx = dropbox.Dropbox(
        oauth2_refresh_token=refresh,
        app_key=app_key,
        app_secret=app_secret,
    )
    return DropboxIO(dbx)


def main() -> int:
    cfg = _build_cfg()
    dbx = _build_dbx()

    # 実行（マルチステージ）
    run_multistage(dbx, cfg)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception:
        print("[MONTHLY] Unhandled exception:", file=sys.stderr)
        traceback.print_exc()
        raise SystemExit(1)