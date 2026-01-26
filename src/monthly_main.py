# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sys

from .dropbox_io import DropboxIO
from .monthly_spec import MonthlyCfg
from .monthly_pipeline_MULTISTAGE import run_multistage


def _must_env(name: str) -> str:
    v = os.getenv(name, "").strip()
    if not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v


def main() -> int:
    cfg = MonthlyCfg.from_env()

    # Dropbox: use an access token (short-lived or long-lived) supplied by Actions secrets/env
    # If you are using refresh tokens, convert to access token outside or extend this wrapper later.
    token = _must_env("DROPBOX_ACCESS_TOKEN")
    dbx = DropboxIO(token)

    if cfg.mode != "multistage":
        print(f"[MONTHLY] Unsupported MONTHLY_MODE={cfg.mode} (only 'multistage' supported)")
        return 2

    return run_multistage(dbx, cfg)


if __name__ == "__main__":
    raise SystemExit(main())