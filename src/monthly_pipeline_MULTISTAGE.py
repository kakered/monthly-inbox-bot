# -*- coding: utf-8 -*-
"""
monthly_pipeline_MULTISTAGE.py

One-stage-per-run multistage pipeline (currently COPY-ONLY).

Goals (now):
- stage auto-selection (00->40) based on which IN has files
- per stage: IN -> OUT (copy), then IN -> DONE (move)
- forward same bytes to next stage IN (copy)
- write JSONL audit logs to Dropbox

Later you can replace the "transform" per stage (Excel edit, API, etc.)
without changing the control flow.
"""
from __future__ import annotations

from typing import Dict, List, Optional, Tuple

from .dropbox_io import DropboxIO
from .monthly_spec import MonthlyCfg
from .utils_dropbox_item import is_file, get_path_lower
from .audit_logger import write_audit_record
from .state_store import StateStore


def _stage_paths(cfg: MonthlyCfg) -> Dict[str, Dict[str, Optional[str]]]:
    return {
        "00": {"IN": cfg.stage00_in, "OUT": cfg.stage00_out, "DONE": cfg.stage00_done, "NEXT_IN": cfg.stage10_in},
        "10": {"IN": cfg.stage10_in, "OUT": cfg.stage10_out, "DONE": cfg.stage10_done, "NEXT_IN": cfg.stage20_in},
        "20": {"IN": cfg.stage20_in, "OUT": cfg.stage20_out, "DONE": cfg.stage20_done, "NEXT_IN": cfg.stage30_in},
        "30": {"IN": cfg.stage30_in, "OUT": cfg.stage30_out, "DONE": cfg.stage30_done, "NEXT_IN": cfg.stage40_in},
        "40": {"IN": cfg.stage40_in, "OUT": cfg.stage40_out, "DONE": cfg.stage40_done, "NEXT_IN": None},
    }


def _next_stage(stage: str) -> str:
    order = ["00", "10", "20", "30", "40"]
    if stage not in order:
        return "--"
    i = order.index(stage)
    return order[i + 1] if i + 1 < len(order) else "--"


def _select_stage_one_run(dbx: DropboxIO, cfg: MonthlyCfg, run_id: str) -> Tuple[Optional[str], List[str], Dict[str, Dict[str, Optional[str]]]]:
    """
    Pick the first stage (00->40) that has at least 1 file in IN.
    Returns: (stage, file_paths_in, stage_paths_map)
    """
    sp = _stage_paths(cfg)
    for st in ["00", "10", "20", "30", "40"]:
        in_dir = sp[st]["IN"]
        if not in_dir:
            continue
        items = dbx.list_folder(in_dir)
        files = [get_path_lower(x) for x in items if is_file(x)]
        if files:
            return st, files, sp
    return None, [], sp


def run_multistage(dbx: DropboxIO, cfg: MonthlyCfg, run_id: str) -> int:
    write_audit_record(dbx, cfg.logs_dir, run_id, stage="--", event="run_start",
                       message="monthly pipeline start (one-stage-per-run; auto stage select)")

    store = StateStore(path=cfg.state_path)
    try:
        store.load(dbx)
    except Exception:
        pass

    stage, files, sp = _select_stage_one_run(dbx, cfg, run_id)

    if not stage:
        write_audit_record(dbx, cfg.logs_dir, run_id, stage="--", event="noop", message="no files in any IN")
        try:
            store.save(dbx)
        except Exception:
            pass
        write_audit_record(dbx, cfg.logs_dir, run_id, stage="--", event="run_end", message="monthly pipeline end")
        return 0

    in_dir = sp[stage]["IN"]
    out_dir = sp[stage]["OUT"]
    done_dir = sp[stage]["DONE"]
    next_in = sp[stage]["NEXT_IN"]

    write_audit_record(dbx, cfg.logs_dir, run_id, stage=stage, event="list", src_path=in_dir, count=len(files))

    processed = 0
    maxn = max(1, int(cfg.max_files_per_run))

    for src_path in files[:maxn]:
        filename = src_path.split("/")[-1]
        try:
            data = dbx.read_file_bytes(src_path)

            # OUT (copy-only)
            dst_out = f"{out_dir.rstrip('/')}/{filename}"
            dbx.write_file_bytes(dst_out, data, overwrite=True)
            write_audit_record(dbx, cfg.logs_dir, run_id, stage=stage, event="write",
                               src_path=src_path, dst_path=dst_out, filename=filename, size=len(data))

            # IN -> DONE
            dst_done = f"{done_dir.rstrip('/')}/{filename}"
            dbx.move(src_path, dst_done, overwrite=True)
            write_audit_record(dbx, cfg.logs_dir, run_id, stage=stage, event="move",
                               src_path=src_path, dst_path=dst_done, filename=filename)

            # forward to next stage IN
            if next_in:
                ns = _next_stage(stage)
                dst_next = f"{next_in.rstrip('/')}/{filename}"
                dbx.write_file_bytes(dst_next, data, overwrite=True)
                write_audit_record(dbx, cfg.logs_dir, run_id, stage=ns, event="write",
                                   src_path=dst_done, dst_path=dst_next, filename=filename, size=len(data),
                                   message=f"forward to stage{ns} IN")

            processed += 1

        except Exception as e:
            write_audit_record(dbx, cfg.logs_dir, run_id, stage=stage, event="error",
                               src_path=src_path, filename=filename, message=repr(e))

    try:
        store.data.setdefault("runs", [])
        store.data["runs"].append({"run_id": run_id, "stage": stage, "processed": processed})
        store.save(dbx)
        write_audit_record(dbx, cfg.logs_dir, run_id, stage="--", event="write_state",
                           filename=cfg.state_path, message="state saved")
    except Exception as e:
        write_audit_record(dbx, cfg.logs_dir, run_id, stage="--", event="error",
                           message=f"state_save_failed: {repr(e)}")

    write_audit_record(dbx, cfg.logs_dir, run_id, stage="--", event="run_end", message="monthly pipeline end")
    return processed