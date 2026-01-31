# -*- coding: utf-8 -*-
"""
monthly_main.py (FULL OVERWRITE VERSION)

- GitHub Actions / runpy / python -m 実行に完全対応
- sys.argv に依存しない
- MONTHLY_STAGE 環境変数駆動
- Dropbox IN フォルダをスキャンして逐次処理
- state.json により再処理防止
"""

from __future__ import annotations

import os
import sys
import json
import time
import traceback
from datetime import datetime, timezone
from typing import List, Optional

from openai import OpenAI
import dropbox
from dropbox.files import FileMetadata
from dropbox.exceptions import ApiError

from state_store import StateStore
from utils_dropbox_item import download_file, upload_file, move_file


# =========================================================
# 基本設定
# =========================================================

UTC = timezone.utc

OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-5-mini")
DEPTH = os.environ.get("DEPTH", "medium")

MAX_FILES_PER_RUN = int(os.environ.get("MAX_FILES_PER_RUN", "200"))
MAX_INPUT_CHARS = int(os.environ.get("MAX_INPUT_CHARS", "80000"))

MONTHLY_STAGE = (os.environ.get("MONTHLY_STAGE", "00") or "00").strip()

LOGS_DIR = os.environ.get("LOGS_DIR", "/_system/logs")
STATE_PATH = os.environ.get("STATE_PATH", "/_system/state.json")


def stage_path(stage: str, kind: str) -> str:
    key = f"STAGE{stage}_{kind}"
    v = os.environ.get(key)
    if not v:
        raise RuntimeError(f"Missing env var: {key}")
    return v


STAGE_IN = stage_path(MONTHLY_STAGE, "IN")
STAGE_OUT = stage_path(MONTHLY_STAGE, "OUT")
STAGE_DONE = stage_path(MONTHLY_STAGE, "DONE")


# =========================================================
# 初期化
# =========================================================

def init_dropbox() -> dropbox.Dropbox:
    return dropbox.Dropbox(
        oauth2_refresh_token=os.environ["DROPBOX_REFRESH_TOKEN"],
        app_key=os.environ["DROPBOX_APP_KEY"],
        app_secret=os.environ["DROPBOX_APP_SECRET"],
    )


def init_openai() -> OpenAI:
    return OpenAI(api_key=os.environ["OPENAI_API_KEY"])


# =========================================================
# ログ
# =========================================================

def log_event(event: dict):
    today = datetime.now(UTC).strftime("%Y%m%d")
    ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    path = f"{LOGS_DIR}/{today}/run_{ts}.jsonl"

    event = dict(event)
    event["ts_utc"] = datetime.now(UTC).isoformat()

    upload_file(
        dbx,
        path,
        (json.dumps(event, ensure_ascii=False) + "\n").encode("utf-8"),
        mode="add",
    )


# =========================================================
# Dropbox ユーティリティ
# =========================================================

def list_inbox_files(dbx: dropbox.Dropbox) -> List[FileMetadata]:
    try:
        res = dbx.files_list_folder(STAGE_IN)
    except ApiError as e:
        raise RuntimeError(f"Dropbox list_folder failed: {e}")

    files = [
        e for e in res.entries
        if isinstance(e, FileMetadata)
    ]
    return files[:MAX_FILES_PER_RUN]


# =========================================================
# メイン処理（stage 00 用：Excel 前処理想定）
# =========================================================

def process_stage_00(file: FileMetadata):
    """
    Stage00:
    - Excel をそのまま OUT にコピー（前処理）
    """
    raw = download_file(dbx, file.path_lower)

    out_name = (
        os.path.splitext(file.name)[0]
        + f"__stage00__{datetime.now(UTC).strftime('%Y%m%d-%H%M%S')}.xlsx"
    )
    out_path = f"{STAGE_OUT}/{out_name}"

    upload_file(dbx, out_path, raw, mode="add")

    # DONE に移動
    move_file(dbx, file.path_lower, f"{STAGE_DONE}/{file.name}")


# =========================================================
# エントリポイント
# =========================================================

def main():
    global dbx

    start = time.time()
    processed = 0

    log_event({"event": "run_start", "stage": MONTHLY_STAGE})

    files = list_inbox_files(dbx)

    for f in files:
        if state.is_done(MONTHLY_STAGE, f.path_lower):
            continue

        try:
            if MONTHLY_STAGE == "00":
                process_stage_00(f)
            else:
                raise NotImplementedError(f"Stage {MONTHLY_STAGE} not implemented")

            state.mark_done(MONTHLY_STAGE, f.path_lower)
            processed += 1

        except Exception as e:
            log_event({
                "event": "file_error",
                "stage": MONTHLY_STAGE,
                "file": f.path_display,
                "error": repr(e),
                "traceback": traceback.format_exc(),
            })

    state.flush()

    log_event({
        "event": "run_end",
        "stage": MONTHLY_STAGE,
        "processed": processed,
        "elapsed_s": round(time.time() - start, 2),
    })


# =========================================================
# 実行
# =========================================================

if __name__ == "__main__":
    dbx = init_dropbox()
    state = StateStore(dbx, STATE_PATH)
    main()