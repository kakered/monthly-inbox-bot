# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Any

from src.dropbox_io import DropboxIO  # 既存を利用（なければ後で合わせます）

JST = timezone(timedelta(hours=9))


@dataclass(frozen=True)
class Paths:
    root: str
    stage00: str
    stage10: str
    logs: str


def now_jst_str() -> str:
    return datetime.now(JST).strftime("%Y%m%d_%H%M%S")


def safe_name(name: str) -> str:
    # Dropbox/Windows でも事故りにくい命名に寄せる
    name = name.strip()
    name = re.sub(r"[\\/:*?\"<>|]", "_", name)
    name = re.sub(r"\s+", " ", name)
    return name


def log_event(log_path_local: str, event: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(log_path_local), exist_ok=True)
    with open(log_path_local, "a", encoding="utf-8") as f:
        f.write(json.dumps(event, ensure_ascii=False) + "\n")


def env_or_default(key: str, default: str) -> str:
    v = os.environ.get(key)
    return v if v else default


def build_paths() -> Paths:
    # INBOX_PATH / OUTBOX_PATH はPDF側の名残があり得るので使いません（混乱防止）
    root = env_or_default("MONTHLY_ROOT", "/monthly-inbox-bot")
    root = root.rstrip("/")
    return Paths(
        root=root,
        stage00=f"{root}/00_inbox_raw",
        stage10=f"{root}/10_preformat_py",
        logs=f"{root}/logs",
    )


def list_stage00_xlsx(dbx: DropboxIO, stage00: str) -> List[Dict[str, Any]]:
    # DropboxIO 側の list_folder API に合わせて実装してください
    # ここでは “返り値が dict の配列（path, name, rev など）” を想定
    files = dbx.list_folder(stage00)
    out = []
    for it in files:
        name = it.get("name", "")
        if name.lower().endswith(".xlsx") and not name.startswith("~$"):
            out.append(it)
    return out


def main() -> int:
    paths = build_paths()
    run_id = now_jst_str()

    log_path_local = f"/tmp/monthly_{run_id}.jsonl"  # まずローカルに吐く
    started = {
        "ts": datetime.now(JST).isoformat(),
        "run_id": run_id,
        "stage": "bootstrap",
        "msg": "start",
        "paths": paths.__dict__,
    }
    log_event(log_path_local, started)

    # Dropbox 接続（既存の env を使う想定）
    dbx = DropboxIO.from_env()

    # 00 を列挙
    try:
        inputs = list_stage00_xlsx(dbx, paths.stage00)
        log_event(log_path_local, {
            "ts": datetime.now(JST).isoformat(),
            "run_id": run_id,
            "stage": "00_list",
            "count": len(inputs),
            "files": [x.get("name") for x in inputs],
        })
    except Exception as e:
        log_event(log_path_local, {
            "ts": datetime.now(JST).isoformat(),
            "run_id": run_id,
            "stage": "00_list",
            "level": "error",
            "error": repr(e),
        })
        dbx.upload_file(log_path_local, f"{paths.logs}/run_{run_id}.jsonl")
        return 1

    if not inputs:
        # 何もなければログだけ残して終了
        dbx.upload_file(log_path_local, f"{paths.logs}/run_{run_id}.jsonl")
        return 0

    # 00 -> 10 へコピー（加工はしない）
    processed = 0
    for it in inputs:
        src_path = it.get("path") or it.get("path_display") or f"{paths.stage00}/{it.get('name')}"
        src_name = it.get("name", "input.xlsx")
        base = safe_name(os.path.splitext(src_name)[0])
        dst_name = f"{base}__preformat.xlsx"
        dst_path = f"{paths.stage10}/{dst_name}"

        try:
            # DropboxIO に “copy” がなければ download->upload でもOK
            if hasattr(dbx, "copy"):
                dbx.copy(src_path, dst_path)
            else:
                data = dbx.download_bytes(src_path)
                dbx.upload_bytes(data, dst_path)

            processed += 1
            log_event(log_path_local, {
                "ts": datetime.now(JST).isoformat(),
                "run_id": run_id,
                "stage": "10_write",
                "src": src_path,
                "dst": dst_path,
                "status": "ok",
            })
        except Exception as e:
            log_event(log_path_local, {
                "ts": datetime.now(JST).isoformat(),
                "run_id": run_id,
                "stage": "10_write",
                "src": src_path,
                "dst": dst_path,
                "status": "error",
                "error": repr(e),
            })

    # ログをDropboxへ保存（最重要）
    try:
        dbx.upload_file(log_path_local, f"{paths.logs}/run_{run_id}.jsonl")
    except Exception as e:
        # ここで失敗しても処理自体は終わっているので終了コードは 0 に寄せる（お好みで1でもOK）
        pass

    return 0 if processed > 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())