# -*- coding: utf-8 -*-
"""
monthly_pipeline_MULTISTAGE.py

方針:
- 1 run = 1 stage 実行（one-stage-per-run）
- ただし「指定 stage の IN が空なら、次 stage の IN を見に行く」
  → YAML を触らなくても詰まりが自然に解消する
- stage10 は Excel を編集して 2派生を作る（overview / per_person）
- 監査ログは AuditLogger に積んで最後に flush（Dropboxに必ず残す）
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Tuple
import os

from dropbox.files import FileMetadata

from src.monthly_spec import MonthlyCfg
from src.dropbox_io import DropboxIO
from src.state_store import StateStore
from src.audit_logger import AuditLogger
from src.excel_exporter import process_monthly_workbook


def _basename(path: str) -> str:
    return path.split("/")[-1]


def _stem(name: str) -> str:
    if "." in name:
        return ".".join(name.split(".")[:-1])
    return name


def _stage_paths(cfg: MonthlyCfg) -> Dict[str, Dict[str, str]]:
    return {
        "00": {"IN": cfg.stage00_in, "OUT": cfg.stage00_out, "DONE": cfg.stage00_done, "NEXT_IN": cfg.stage10_in},
        "10": {"IN": cfg.stage10_in, "OUT": cfg.stage10_out, "DONE": cfg.stage10_done, "NEXT_IN": cfg.stage20_in},
        "20": {"IN": cfg.stage20_in, "OUT": cfg.stage20_out, "DONE": cfg.stage20_done, "NEXT_IN": cfg.stage30_in},
        "30": {"IN": cfg.stage30_in, "OUT": cfg.stage30_out, "DONE": cfg.stage30_done, "NEXT_IN": cfg.stage40_in},
        "40": {"IN": cfg.stage40_in, "OUT": cfg.stage40_out, "DONE": cfg.stage40_done, "NEXT_IN": None},
    }


def _list_xlsx(dbx: DropboxIO, in_path: str) -> List[FileMetadata]:
    items = dbx.list_folder(in_path)
    files: List[FileMetadata] = []
    for it in items:
        if isinstance(it, FileMetadata) and it.name.lower().endswith(".xlsx"):
            files.append(it)
    return files


def _select_stage_one_run(
    dbx: DropboxIO,
    cfg: MonthlyCfg,
    audit: AuditLogger,
) -> Tuple[str | None, List[FileMetadata], Dict[str, Dict[str, str]]]:
    sp = _stage_paths(cfg)
    order = ["00", "10", "20", "30", "40"]

    preferred = (cfg.monthly_stage or "00").strip()
    if preferred not in order:
        preferred = "00"

    start_idx = order.index(preferred)

    # まず preferred stage を見て、空なら次を探す
    for idx in range(start_idx, len(order)):
        st = order[idx]
        in_path = sp[st]["IN"]
        files = _list_xlsx(dbx, in_path)
        audit.event(stage=st, event="list", src_path=in_path, count=len(files))
        if files:
            return st, files, sp

    # preferred 以降が空なら、先頭からも探す（詰まり解消）
    for idx in range(0, start_idx):
        st = order[idx]
        in_path = sp[st]["IN"]
        files = _list_xlsx(dbx, in_path)
        audit.event(stage=st, event="list", src_path=in_path, count=len(files))
        if files:
            return st, files, sp

    return None, [], sp


def _copy_only_stage(
    *,
    dbx: DropboxIO,
    store: StateStore,
    audit: AuditLogger,
    stage: str,
    in_path: str,
    out_path: str,
    done_path: str,
    next_in: str | None,
    files: List[FileMetadata],
    max_files: int,
) -> int:
    n = 0
    for f in files[:max_files]:
        src = f.path_display or (in_path + "/" + f.name)
        if store.is_processed(stage, src):
            continue

        data = dbx.download_to_bytes(src)

        dst_out = f"{out_path}/{f.name}"
        dbx.write_file_bytes(dst_out, data, overwrite=True)
        audit.event(stage=stage, event="write", src_path=src, dst_path=dst_out, filename=f.name, size=len(data))

        dst_done = f"{done_path}/{f.name}"
        dbx.move(src, dst_done)
        audit.event(stage=stage, event="move", src_path=src, dst_path=dst_done, filename=f.name)

        store.mark_processed(stage, src)

        if next_in:
            dst_next = f"{next_in}/{f.name}"
            dbx.write_file_bytes(dst_next, data, overwrite=True)
            audit.event(stage=str(int(stage) + 10).zfill(2), event="write", src_path=dst_done, dst_path=dst_next,
                        filename=f.name, message=f"forward to stage{str(int(stage)+10).zfill(2)} IN", size=len(data))

        n += 1
    return n


def _stage10_preformat(
    *,
    dbx: DropboxIO,
    store: StateStore,
    audit: AuditLogger,
    in_path: str,
    out_path: str,
    done_path: str,
    next_in: str,
    files: List[FileMetadata],
    max_files: int,
    run_id: str,
) -> int:
    """
    stage10: 編集あり
    - INのxlsx 1つから overview/per_person の2派生を OUT に出す
    - 入力は DONE に退避
    - 派生2つを stage20 IN に送る
    """
    stage = "10"
    n = 0
    for f in files[:max_files]:
        src = f.path_display or (in_path + "/" + f.name)
        if store.is_processed(stage, src):
            continue

        xlsx = dbx.download_to_bytes(src)
        res = process_monthly_workbook(xlsx)

        base = _stem(f.name)
        out_over = f"{base}__00to10__overview__{run_id}.xlsx"
        out_pp = f"{base}__00to10__per_person__{run_id}.xlsx"

        dst_over = f"{out_path}/{out_over}"
        dbx.write_file_bytes(dst_over, res.overview_bytes, overwrite=True)
        audit.event(stage=stage, event="write", src_path=src, dst_path=dst_over, filename=out_over, size=len(res.overview_bytes))

        dst_pp = f"{out_path}/{out_pp}"
        dbx.write_file_bytes(dst_pp, res.per_person_bytes, overwrite=True)
        audit.event(stage=stage, event="write", src_path=src, dst_path=dst_pp, filename=out_pp, size=len(res.per_person_bytes))

        dst_done = f"{done_path}/{f.name}"
        dbx.move(src, dst_done)
        audit.event(stage=stage, event="move", src_path=src, dst_path=dst_done, filename=f.name)

        # forward to stage20 IN
        dst20_over = f"{next_in}/{out_over}"
        dbx.write_file_bytes(dst20_over, res.overview_bytes, overwrite=True)
        audit.event(stage="20", event="write", src_path=dst_over, dst_path=dst20_over, filename=out_over,
                    message="forward to stage20 IN", size=len(res.overview_bytes))

        dst20_pp = f"{next_in}/{out_pp}"
        dbx.write_file_bytes(dst20_pp, res.per_person_bytes, overwrite=True)
        audit.event(stage="20", event="write", src_path=dst_pp, dst_path=dst20_pp, filename=out_pp,
                    message="forward to stage20 IN", size=len(res.per_person_bytes))

        store.mark_processed(stage, src)
        n += 1

    return n


def run_multistage(dbx: DropboxIO, cfg: MonthlyCfg, run_id: str) -> int:
    """
    Entry point:
    - stageを自動選択（one-stage-per-run）
    - state.json 更新
    - audit log flush（必ずDropboxに残す）
    """
    audit = AuditLogger(logs_dir=cfg.logs_dir, run_id=run_id)
    store = StateStore.load(dbx, cfg.state_path)

    audit.event(stage="--", event="run_start", message="monthly pipeline start (one-stage-per-run; auto-skip-empty)")

    stage, files, sp = _select_stage_one_run(dbx, cfg, audit)
    if stage is None:
        audit.event(stage="--", event="run_end", message="no input files; nothing to do")
        # state もログも残す
        store.save(dbx, cfg.state_path)
        audit.event(stage="--", event="write_state", filename=cfg.state_path, message="state saved")
        audit.flush(dbx)
        return 0

    in_path = sp[stage]["IN"]
    out_path = sp[stage]["OUT"]
    done_path = sp[stage]["DONE"]
    next_in = sp[stage]["NEXT_IN"]

    processed = 0
    try:
        if stage == "10":
            processed = _stage10_preformat(
                dbx=dbx,
                store=store,
                audit=audit,
                in_path=in_path,
                out_path=out_path,
                done_path=done_path,
                next_in=next_in,  # type: ignore[arg-type]
                files=files,
                max_files=cfg.max_files_per_run,
                run_id=run_id,
            )
        else:
            processed = _copy_only_stage(
                dbx=dbx,
                store=store,
                audit=audit,
                stage=stage,
                in_path=in_path,
                out_path=out_path,
                done_path=done_path,
                next_in=next_in,
                files=files,
                max_files=cfg.max_files_per_run,
            )
    except Exception as e:
        audit.event(stage=stage, event="error", message=repr(e))
        # エラーでも state/log を残す
        store.save(dbx, cfg.state_path)
        audit.event(stage="--", event="write_state", filename=cfg.state_path, message="state saved")
        audit.flush(dbx)
        raise

    store.save(dbx, cfg.state_path)
    audit.event(stage="--", event="write_state", filename=cfg.state_path, message="state saved")
    audit.event(stage="--", event="run_end", message=f"monthly pipeline end; processed={processed}")
    audit.flush(dbx)
    return processed