# -*- coding: utf-8 -*-
"""
stage00.py
目的：
- 「本当に走った」を1回で証明する（marker を OUT に必ず作る）
- IN にある Excel を OUT にコピーし、元を DONE に移動する（最小のIN→OUT→DONE）
- ここで初めて“200回ループ”が止まる（観測可能な差分が出る）
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Dict

import dropbox
from dropbox.exceptions import ApiError


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")


def safe_mkdir(dbx: dropbox.Dropbox, path: str) -> None:
    if not path:
        return
    try:
        dbx.files_create_folder_v2(path)
    except Exception:
        pass


def list_files(dbx: dropbox.Dropbox, folder: str):
    res = dbx.files_list_folder(folder)
    return [e for e in res.entries if type(e).__name__ == "FileMetadata"]


def run(*, dbx, paths, state, audit, config: Dict[str, Any], **kwargs) -> int:
    # フォルダ前提チェック
    for k in ["in_path", "out_path", "done_path"]:
        v = getattr(paths, k, "")
        if not v:
            audit.write({
                "ts_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
                "event": "error",
                "where": "stage00",
                "message": f"missing required path: {k}",
            })
            return 2

    safe_mkdir(dbx, paths.out_path)
    safe_mkdir(dbx, paths.done_path)

    # 1) marker を必ず作る（RUNが実際に stage00 に入った証拠）
    marker_name = f"_stage00_marker__{utc_stamp()}.txt"
    marker_path = f"{paths.out_path.rstrip('/')}/{marker_name}"
    try:
        dbx.files_upload(
            f"stage00 alive at {utc_stamp()} UTC\n".encode("utf-8"),
            marker_path,
            mode=dropbox.files.WriteMode.add,
        )
        audit.write({
            "ts_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "event": "stage00_marker_written",
            "path": marker_path,
        })
    except Exception as e:
        audit.write({
            "ts_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "event": "warn",
            "where": "stage00.marker",
            "error": f"{type(e).__name__}: {e}",
        })
        # marker が書けないのは運用上致命なので落とす
        return 1

    # 2) IN のファイルを処理（最大 MAX_FILES_PER_RUN）
    try:
        files = list_files(dbx, paths.in_path)
    except ApiError as e:
        audit.write({
            "ts_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "event": "error",
            "where": "stage00.list",
            "error": str(e),
        })
        return 1

    max_n = int(str(config.get("MAX_FILES_PER_RUN", "200")))
    files = files[:max_n]

    processed = 0

    for f in files:
        src = f.path_display
        base = os.path.basename(src)

        # OUT は “コピー” として保存（名前に stage + timestamp）
        out_name = f"{os.path.splitext(base)[0]}__stage00__{utc_stamp()}{os.path.splitext(base)[1]}"
        out_path = f"{paths.out_path.rstrip('/')}/{out_name}"

        # DONE は “move” でアーカイブ（rev付きで衝突回避しやすい）
        done_name = f"{os.path.splitext(base)[0]}__rev-{getattr(f, 'rev', 'unknown')}__{utc_stamp()}{os.path.splitext(base)[1]}"
        done_path = f"{paths.done_path.rstrip('/')}/{done_name}"

        try:
            # copy -> OUT
            dbx.files_copy_v2(src, out_path, allow_shared_folder=True, autorename=True)
            # move -> DONE
            dbx.files_move_v2(src, done_path, allow_shared_folder=True, autorename=True)

            processed += 1
            audit.write({
                "ts_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
                "event": "stage00_processed",
                "src": src,
                "out": out_path,
                "done": done_path,
            })
        except Exception as e:
            audit.write({
                "ts_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
                "event": "stage00_error",
                "src": src,
                "error": f"{type(e).__name__}: {e}",
            })
            # 1件でも失敗したら失敗扱い（安全側）
            return 1

    # state に記録（最低限）
    try:
        state.stages.setdefault("00", {})
        state.stages["00"]["last_run_utc"] = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        state.stages["00"]["processed"] = processed
    except Exception:
        pass

    audit.write({
        "ts_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "event": "stage00_summary",
        "processed": processed,
    })
    return 0