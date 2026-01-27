# -*- coding: utf-8 -*-
"""
monthly_pipeline_MULTISTAGE.py

"Switch-ON (semi-auto)" multistage pipeline.
- You choose which stage to run via env MONTHLY_STAGE (00/10/20/30/40)
- Each run processes files under that stage's IN folder.
- Outputs go to:
    - current stage OUT (for inspection)
    - next stage IN (for chaining without manual copy)
- Inputs are moved to current stage DONE (so you don't need to delete IN files).
"""
from __future__ import annotations

import json
import os
import time
from typing import Dict, List, Optional

from dropbox.files import FileMetadata

from .dropbox_io import DropboxIO
from .monthly_spec import MonthlyCfg


def _now_tag() -> str:
    return time.strftime("%Y%m%d-%H%M%S")


def _join(dir_path: str, name: str) -> str:
    dp = dir_path.rstrip("/")
    return f"{dp}/{name}"


def _is_xlsx(name: str) -> bool:
    return (name or "").lower().endswith(".xlsx")


def _load_state(dbx: DropboxIO, path: str) -> Dict[str, Dict[str, str]]:
    try:
        raw = dbx.download(path)
    except Exception:
        return {"processed": {}}
    try:
        return json.loads(raw.decode("utf-8"))
    except Exception:
        return {"processed": {}}


def _save_state(dbx: DropboxIO, path: str, state: Dict[str, Dict[str, str]]) -> None:
    dbx.ensure_folder(os.path.dirname(path).replace("\\", "/"))
    dbx.upload(path, json.dumps(state, ensure_ascii=False, indent=2).encode("utf-8"), overwrite=True)


def _key_for(md: FileMetadata) -> str:
    fid = getattr(md, "id", None) or ""
    rev = getattr(md, "rev", None) or ""
    pl = getattr(md, "path_lower", None) or ""
    return f"{fid}|{pl}|{rev}"


def _list_xlsx(dbx: DropboxIO, folder: str) -> List[FileMetadata]:
    items = dbx.list_folder(folder)
    out: List[FileMetadata] = []
    for it in items:
        if isinstance(it, FileMetadata) and _is_xlsx(it.name):
            out.append(it)
    return out


def _ensure_stage_dirs(dbx: DropboxIO, cfg: MonthlyCfg) -> None:
    dbx.ensure_folder(cfg.logs_dir)
    dbx.ensure_folder(os.path.dirname(cfg.state_path).replace("\\", "/") or "/")
    for s in cfg.stages.values():
        dbx.ensure_folder(s.in_dir)
        dbx.ensure_folder(s.out_dir)
        dbx.ensure_folder(s.done_dir)


def run_multistage(dbx: DropboxIO, cfg: MonthlyCfg) -> None:
    _ensure_stage_dirs(dbx, cfg)
    state = _load_state(dbx, cfg.state_path)
    processed: Dict[str, str] = state.get("processed", {})

    stage = cfg.stage
    if stage not in cfg.stages:
        raise RuntimeError(f"Invalid MONTHLY_STAGE={stage!r}. Expected one of: {sorted(cfg.stages.keys())}")

    if stage == "00":
        _stage_passthrough(dbx, cfg, processed, stage="00", next_stage="10")
    elif stage == "10":
        _stage_passthrough(dbx, cfg, processed, stage="10", next_stage="20")
    elif stage == "20":
        _stage20_overview_and_personalize(dbx, cfg, processed)
    elif stage == "30":
        _stage_passthrough(dbx, cfg, processed, stage="30", next_stage="40")
    elif stage == "40":
        _stage_passthrough(dbx, cfg, processed, stage="40", next_stage=None)

    state["processed"] = processed
    _save_state(dbx, cfg.state_path, state)


def _stage_passthrough(dbx: DropboxIO, cfg: MonthlyCfg, processed: Dict[str, str], stage: str, next_stage: Optional[str]) -> None:
    s = cfg.s(stage)
    files = _list_xlsx(dbx, s.in_dir)
    if not files:
        print(f"[MONTHLY] stage{stage}: no .xlsx in {s.in_dir}")
        return

    for md in files[: cfg.max_files_per_run]:
        key = _key_for(md)
        if processed.get(key):
            continue

        src = md.path_display or md.path_lower
        if not src:
            continue

        tag = _now_tag()
        base = md.name.rsplit(".", 1)[0]
        out_name = f"{base}__stage{stage}__{tag}.xlsx"

        out_path = _join(s.out_dir, out_name)
        dbx.copy(src, out_path)

        if next_stage:
            ns = cfg.s(next_stage)
            next_in_path = _join(ns.in_dir, out_name)
            dbx.copy(src, next_in_path)

        done_path = _join(s.done_dir, md.name)
        dbx.move(src, done_path)

        processed[key] = f"stage{stage}:{tag}"
        print(f"[MONTHLY] stage{stage}: {md.name} -> OUT/DONE" + (f" + next stage {next_stage} IN" if next_stage else ""))


def _stage20_overview_and_personalize(dbx: DropboxIO, cfg: MonthlyCfg, processed: Dict[str, str]) -> None:
    from .excel_exporter import process_monthly_workbook  # stage20 only

    s20 = cfg.s("20")
    s30 = cfg.s("30")

    files = _list_xlsx(dbx, s20.in_dir)
    if not files:
        print(f"[MONTHLY] stage20: no .xlsx in {s20.in_dir}")
        return

    for md in files[: cfg.max_files_per_run]:
        key = _key_for(md)
        if processed.get(key):
            continue

        src = md.path_display or md.path_lower
        if not src:
            continue

        xlsx = dbx.download(src)
        tag = _now_tag()
        base = md.name.rsplit(".", 1)[0]

        overview_bytes, per_person_bytes = process_monthly_workbook(
            xlsx,
            model=cfg.openai_model,
            depth=cfg.depth,
            timeout=cfg.openai_timeout,
            source_name=md.name,
        )

        ov_name = f"{base}__overview__{tag}.xlsx"
        pp_name = f"{base}__personalize__{tag}.xlsx"

        dbx.upload(_join(s20.out_dir, ov_name), overview_bytes, overwrite=True)
        dbx.upload(_join(s20.out_dir, pp_name), per_person_bytes, overwrite=True)

        dbx.upload(_join(s30.in_dir, ov_name), overview_bytes, overwrite=True)
        dbx.upload(_join(s30.in_dir, pp_name), per_person_bytes, overwrite=True)

        dbx.move(src, _join(s20.done_dir, md.name))

        processed[key] = f"stage20:{tag}"
        print(f"[MONTHLY] stage20: {md.name} -> OUT + stage30 IN; moved to DONE")