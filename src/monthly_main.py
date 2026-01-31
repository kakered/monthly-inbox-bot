# -*- coding: utf-8 -*-
"""
monthly_main.py

GitHub Actions から `python -m src.monthly_main` で引数なし実行されても落ちないようにする。
- 設定は env（monthly_spec.MonthlyCfg.from_env）から取得
- Dropbox 認証も env（dropbox_io.DropboxIO.from_env）から取得
- 実処理は monthly_pipeline_MULTISTAGE.run_multistage に委譲

Notes:
- 以前の版のように sys.argv[1] を必須にしない（= IndexError 回避）
- CLI引数はオプション扱い（将来のローカルデバッグ用途）
"""

from __future__ import annotations

import argparse
import os
import sys
import traceback

from .dropbox_io import DropboxIO
from .monthly_spec import MonthlyCfg
from .monthly_pipeline_MULTISTAGE import run_multistage


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(add_help=True)
    # 互換・将来用（現状は使わない想定）
    p.add_argument(
        "--stage",
        default=None,
        help="Override MONTHLY_STAGE (e.g. 00). If omitted, uses env MONTHLY_STAGE.",
    )
    p.add_argument(
        "--max-files",
        type=int,
        default=None,
        help="Override MAX_FILES_PER_RUN. If omitted, uses env MAX_FILES_PER_RUN.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="No-op placeholder (currently does not change behavior).",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)

    # ---- parse optional CLI args (should NOT be required) ----
    parser = _build_parser()
    args = parser.parse_args(argv)

    # ---- config from env ----
    cfg = MonthlyCfg.from_env()

    # Optional overrides (debug convenience)
    if args.stage:
        object.__setattr__(cfg, "monthly_stage", str(args.stage).strip())
    if args.max_files is not None:
        object.__setattr__(cfg, "max_files_per_run", int(args.max_files))

    # ---- dropbox client ----
    dbx = DropboxIO.from_env()

    # Ensure essential folders exist (safe)
    # (Dropbox create_folder is idempotent-ish in our wrapper)
    for path in [
        cfg.logs_dir,
        os.path.dirname(cfg.state_path) or "/_system",
        cfg.stage00_in,
        cfg.stage00_out,
        cfg.stage00_done,
        cfg.stage10_in,
        cfg.stage10_out,
        cfg.stage10_done,
        cfg.stage20_in,
        cfg.stage20_out,
        cfg.stage20_done,
        cfg.stage30_in,
        cfg.stage30_out,
        cfg.stage30_done,
        cfg.stage40_in,
        cfg.stage40_out,
        cfg.stage40_done,
    ]:
        try:
            dbx.ensure_folder(path)
        except Exception:
            # フォルダ作成失敗があっても、list/move/write の段階でエラーをログに落とす方針。
            pass

    # ---- run pipeline ----
    run_multistage(dbx, cfg)

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception:
        traceback.print_exc()
        raise SystemExit(1)