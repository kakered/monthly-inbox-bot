# -*- coding: utf-8 -*-
"""monthly_pipeline_MULTISTAGE.py
Monthly pipeline runner.

Stability-first:
- one-stage-per-run (a single run processes only ONE stage)
- stage selection is automatic starting from cfg.monthly_stage:
    try 00; if empty then 10 -> 20 -> 30 -> 40
  => you usually DON'T need to edit workflow env each time.

Stages:
- 00: copy to OUT, move to DONE, forward to stage10/IN
- 10: read xlsx -> generate 2 xlsx -> write to OUT, move original to DONE, forward outputs to stage20/IN
- 20/30/40: copy-only passthrough (placeholder)
"""

from __future__ import annotations

import traceback
from typing import Any, Dict, Tuple

from dropbox.files import FileMetadata

from .dropbox_io import DropboxIO
from .excel_exporter import process_monthly_workbook
from .state_store import StateStore, stable_key


def _is_file(md: Any) -> bool:
    return isinstance(md, FileMetadata)


def _stage_paths(cfg) -> Dict[str, Dict[str, str]]:
    return {
        "00": {"IN": cfg.stage00_in, "OUT": cfg.stage00_out, "DONE": cfg.stage00_done, "NEXT_IN": cfg.stage10_in},
        "10": {"IN": cfg.stage10_in, "OUT": cfg.stage10_out, "DONE": cfg.stage10_done, "NEXT_IN": cfg.stage20_in},
        "20": {"IN": cfg.stage20_in, "OUT": cfg.stage20_out, "DONE": cfg.stage20_done, "NEXT_IN": cfg.stage30_in},
        "30": {"IN": cfg.stage30_in, "OUT": cfg.stage30_out, "DONE": cfg.stage30_done, "NEXT_IN": cfg.stage40_in},
        "40": {"IN": cfg.stage40_in, "OUT": cfg.stage40_out, "DONE": cfg.stage40_done, "NEXT_IN": ""},
    }


def _join(dir_path: str, filename: str) -> str:
    return f"{dir_path.rstrip('/')}/{filename}"


def _write_bytes(dbx: DropboxIO, dst: str, data: bytes) -> None:
    dbx.write_file_bytes(dst, data, overwrite=True)


def _copy_file(dbx: DropboxIO, src: str, dst: str) -> int:
    b = dbx.download_to_bytes(src)
    _write_bytes(dbx, dst, b)
    return len(b)


def _safe_basename(name: str) -> str:
    return name.replace("/", "_").strip()


def _split_ext(filename: str) -> Tuple[str, str]:
    if "." not in filename:
        return filename, ""
    base, ext = filename.rsplit(".", 1)
    return base, "." + ext


def _select_stage_one_run(dbx: DropboxIO, cfg, sp: Dict[str, Dict[str, str]]) -> tuple[str, list[FileMetadata], str]:
    order = ["00", "10", "20", "30", "40"]

    start = (cfg.monthly_stage or "00").strip()
    if start not in sp:
        start = "00"

    try:
        start_idx = order.index(start)
    except ValueError:
        start_idx = 0

    for st in order[start_idx:]:
        in_dir = sp[st]["IN"]
        files = [e for e in dbx.list_folder(in_dir) if _is_file(e)]
        files = sorted(files, key=lambda e: (e.name or "").lower())[: int(cfg.max_files_per_run or 200)]
        if files:
            return st, files, in_dir

    return start, [], sp[start]["IN"]


def run_multistage(dbx: DropboxIO, cfg, run_id: str) -> int:
    sp = _stage_paths(cfg)
    store = StateStore.load(dbx, cfg.state_path)

    store.log(run_id=run_id, stage="--", event="run_start", message="monthly pipeline start (one-stage-per-run; auto stage select)")

    stage, entries, in_dir = _select_stage_one_run(dbx, cfg, sp)
    store.log(run_id=run_id, stage=stage, event="list", src_path=in_dir, count=len(entries))

    processed_count = 0
    for md in entries:
        src_path = getattr(md, "path_display", None) or _join(in_dir, md.name)
        fid = getattr(md, "id", None)
        key = stable_key(stage, fid, src_path)

        if store.is_processed(key):
            continue

        try:
            if stage == "00":
                processed_count += _handle_stage00(dbx, store, run_id, sp, md, src_path)
            elif stage == "10":
                processed_count += _handle_stage10(dbx, store, run_id, sp, md, src_path)
            else:
                processed_count += _handle_copy_only(stage, dbx, store, run_id, sp, md, src_path)

            store.mark_processed(key, f"{stage}:{run_id}")
            store.add_done(src_path)

        except Exception as e:
            store.add_error({"stage": stage, "path": src_path, "error": repr(e), "traceback": traceback.format_exc()})
            store.log(run_id=run_id, stage=stage, event="error", src_path=src_path, message=repr(e))

    store.save(dbx)
    store.log(run_id=run_id, stage="--", event="write_state", filename=cfg.state_path, message="state saved")
    store.log(run_id=run_id, stage="--", event="run_end", message="monthly pipeline end")
    store.flush_audit_jsonl(dbx, cfg.logs_dir, run_id)

    return processed_count


def _handle_stage00(dbx: DropboxIO, store: StateStore, run_id: str, sp, md: FileMetadata, src_path: str) -> int:
    stage = "00"
    filename = _safe_basename(md.name)
    out_path = _join(sp[stage]["OUT"], filename)
    done_path = _join(sp[stage]["DONE"], filename)

    size = _copy_file(dbx, src_path, out_path)
    store.log(run_id=run_id, stage=stage, event="write", src_path=src_path, dst_path=out_path, filename=filename, size=size)

    dbx.move(src_path, done_path, overwrite=True)
    store.log(run_id=run_id, stage=stage, event="move", src_path=src_path, dst_path=done_path, filename=filename)

    next_in = sp[stage].get("NEXT_IN") or ""
    if next_in:
        next_path = _join(next_in, filename)
        size2 = _copy_file(dbx, done_path, next_path)
        store.log(run_id=run_id, stage="10", event="write", src_path=done_path, dst_path=next_path, filename=filename, message="forward to stage10 IN", size=size2)

    return 1


def _handle_stage10(dbx: DropboxIO, store: StateStore, run_id: str, sp, md: FileMetadata, src_path: str) -> int:
    stage = "10"
    filename = _safe_basename(md.name)
    base, ext = _split_ext(filename)

    if ext.lower() != ".xlsx":
        return _handle_copy_only(stage, dbx, store, run_id, sp, md, src_path)

    in_bytes = dbx.download_to_bytes(src_path)
    overview_bytes, per_person_bytes = process_monthly_workbook(in_bytes)

    tag = run_id.replace("/", "-")
    overview_name = f"{base}__00to10__overview__{tag}.xlsx"
    per_person_name = f"{base}__00to10__per_person__{tag}.xlsx"

    out_overview = _join(sp[stage]["OUT"], overview_name)
    out_per = _join(sp[stage]["OUT"], per_person_name)

    _write_bytes(dbx, out_overview, overview_bytes)
    store.log(run_id=run_id, stage=stage, event="write", src_path=src_path, dst_path=out_overview, filename=overview_name, size=len(overview_bytes))

    _write_bytes(dbx, out_per, per_person_bytes)
    store.log(run_id=run_id, stage=stage, event="write", src_path=src_path, dst_path=out_per, filename=per_person_name, size=len(per_person_bytes))

    done_path = _join(sp[stage]["DONE"], filename)
    dbx.move(src_path, done_path, overwrite=True)
    store.log(run_id=run_id, stage=stage, event="move", src_path=src_path, dst_path=done_path, filename=filename)

    next_in = sp[stage].get("NEXT_IN") or ""
    if next_in:
        next_overview = _join(next_in, overview_name)
        next_per = _join(next_in, per_person_name)

        _write_bytes(dbx, next_overview, overview_bytes)
        store.log(run_id=run_id, stage="20", event="write", src_path=done_path, dst_path=next_overview, filename=overview_name, message="forward to stage20 IN", size=len(overview_bytes))

        _write_bytes(dbx, next_per, per_person_bytes)
        store.log(run_id=run_id, stage="20", event="write", src_path=done_path, dst_path=next_per, filename=per_person_name, message="forward to stage20 IN", size=len(per_person_bytes))

    return 1


def _handle_copy_only(stage: str, dbx: DropboxIO, store: StateStore, run_id: str, sp, md: FileMetadata, src_path: str) -> int:
    filename = _safe_basename(md.name)
    out_path = _join(sp[stage]["OUT"], filename)
    done_path = _join(sp[stage]["DONE"], filename)

    size = _copy_file(dbx, src_path, out_path)
    store.log(run_id=run_id, stage=stage, event="write", src_path=src_path, dst_path=out_path, filename=filename, size=size)

    dbx.move(src_path, done_path, overwrite=True)
    store.log(run_id=run_id, stage=stage, event="move", src_path=src_path, dst_path=done_path, filename=filename)

    next_in = sp[stage].get("NEXT_IN") or ""
    if next_in:
        next_path = _join(next_in, filename)
        size2 = _copy_file(dbx, done_path, next_path)
        next_stage = str(int(stage) + 10).zfill(2)
        store.log(run_id=run_id, stage=next_stage, event="write", src_path=done_path, dst_path=next_path, filename=filename, message=f"forward to stage{next_stage} IN", size=size2)

    return 1