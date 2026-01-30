# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sys
import time
from typing import Tuple, Optional

# 既存プロジェクトのモジュール（あなたのrepoにある前提）
from .dropbox_io import DropboxIO, DbxEntry
from .state_store import StateStore
from .logger import JsonlLogger


def _env(name: str, default: str = "") -> str:
    return (os.environ.get(name, default) or "").strip()


def _require_env(name: str) -> str:
    v = _env(name, "")
    if not v:
        raise RuntimeError(f"Missing required env: {name}")
    return v


def _utc_stamp() -> str:
    return time.strftime("%Y%m%d-%H%M%S", time.gmtime())


def _stage_vars(stage: str) -> Tuple[str, str, str]:
    return (
        _env(f"STAGE{stage}_IN"),
        _env(f"STAGE{stage}_OUT"),
        _env(f"STAGE{stage}_DONE"),
    )


def _next_stage(stage: str) -> str:
    order = ["00", "10", "20", "30", "40"]
    i = order.index(stage)
    return order[i + 1] if i + 1 < len(order) else ""


def _is_xlsx(name: str) -> bool:
    n = name.lower()
    return n.endswith(".xlsx") or n.endswith(".xlsm") or n.endswith(".xls")


def _file_key(e: DbxEntry) -> str:
    if e.rev:
        return f"{e.path}@{e.rev}"
    return e.path


def stage_copy_forward(
    *,
    io: DropboxIO,
    logger: JsonlLogger,
    store: StateStore,
    stage: str,
    max_files: int,
) -> int:
    """
    Stage00/10/20/30/40 共通：INのxlsxをOUTにコピーしてDONEに退避し、次ステージINへもコピーする。
    OpenAIは不要（今のあなたのパイプライン前提：stage00で行分割準備など）
    """
    p_in, p_out, p_done = _stage_vars(stage)
    if not (p_in and p_out and p_done):
        raise RuntimeError(f"Stage{stage} paths are missing. IN/OUT/DONE must be set.")

    io.ensure_folder(p_in)
    io.ensure_folder(p_out)
    io.ensure_folder(p_done)

    state = store.load()
    bucket = store.get_stage_bucket(state, stage)
    bucket["last_run_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    entries = [e for e in io.list_folder(p_in) if e.is_file and _is_xlsx(e.name)]
    entries = entries[:max_files]

    processed = 0
    for e in entries:
        k = _file_key(e)
        if store.is_done(bucket, k):
            continue

        src_path = e.path
        base = e.name

        data = io.download(src_path)

        out_name = f"{os.path.splitext(base)[0]}__stage{stage}__{_utc_stamp()}{os.path.splitext(base)[1]}"
        out_path = f"{p_out}/{out_name}"
        io.upload_overwrite(out_path, data)

        rev = e.rev or "no-rev"
        done_name = f"{os.path.splitext(base)[0]}__rev-{rev}__{_utc_stamp()}{os.path.splitext(base)[1]}"
        done_path = f"{p_done}/{done_name}"
        io.move_replace(src_path, done_path)

        nxt = _next_stage(stage)
        if nxt:
            nxt_in, _, _ = _stage_vars(nxt)
            if nxt_in:
                io.ensure_folder(nxt_in)
                nxt_path = f"{nxt_in}/{base}"
                io.upload_overwrite(nxt_path, data)

        store.mark_done(bucket, k)
        store.save(state)

        logger.log(
            {
                "event": "file_processed",
                "stage": stage,
                "src": src_path,
                "out": out_path,
                "done": done_path,
                "size": len(data),
            }
        )

        processed += 1

    logger.log({"event": "stage_end", "stage": stage, "processed": processed, "in_count": len(entries)})
    return processed


def run_actions_pipeline() -> int:
    """
    GitHub Actions / workflow向け（argv不要）
    """
    tok = _require_env("DROPBOX_REFRESH_TOKEN")
    app_key = _require_env("DROPBOX_APP_KEY")
    app_secret = _require_env("DROPBOX_APP_SECRET")

    stage = _env("MONTHLY_STAGE", "00")
    if stage not in {"00", "10", "20", "30", "40"}:
        raise RuntimeError("MONTHLY_STAGE must be one of 00/10/20/30/40")

    max_files = int(_env("MAX_FILES_PER_RUN", "200") or "200")
    state_path = _env("STATE_PATH", "/_system/state.json")
    logs_dir = _env("LOGS_DIR", "/_system/logs")

    io = DropboxIO(refresh_token=tok, app_key=app_key, app_secret=app_secret)
    logger = JsonlLogger(io, logs_dir=logs_dir)
    store = StateStore(io=io, state_path=state_path)

    logger.log(
        {
            "event": "run_start",
            "stage": stage,
            "state_path": state_path,
            "logs_dir": logs_dir,
            "mode": "actions",
        }
    )

    processed = stage_copy_forward(io=io, logger=logger, store=store, stage=stage, max_files=max_files)

    logger.log({"event": "run_end", "stage": stage, "processed": processed, "mode": "actions"})
    return 0


def run_legacy_argv_mode(argv: list[str]) -> int:
    """
    旧仕様（sys.argv[1] 前提）を壊さないための温存口。
    ただし、Actionsでは使いません。

    ここはあなたの既存ローカル運用に合わせて後で中身を移植してください。
    """
    if len(argv) < 2:
        raise SystemExit("Usage: python -m src.monthly_main <input_file>")

    in_file = argv[1]
    # 旧処理があるならここで呼ぶ（現時点では入口だけ残す）
    # TODO: migrate old logic here if needed.
    print(f"[legacy] received input_file={in_file!r}")
    return 0


def main(argv: Optional[list[str]] = None) -> int:
    argv = argv if argv is not None else sys.argv

    # ★重要：Actionsは引数なしで動かすので、それをデフォルトにする
    # 旧仕様を使いたい場合だけ argv で分岐
    if len(argv) >= 2:
        return run_legacy_argv_mode(argv)

    return run_actions_pipeline()


if __name__ == "__main__":
    raise SystemExit(main())