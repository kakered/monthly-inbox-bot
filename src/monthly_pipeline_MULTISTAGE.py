# -*- coding: utf-8 -*-
from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from typing import List, Tuple

from .dropbox_io import DropboxIO, DropboxItem
from .monthly_spec import MonthlyCfg
from .state import PipelineState
from .state_store import load_state, save_state


def _join(root: str, *parts: str) -> str:
    root = root.rstrip("/")
    p = "/".join([root] + [x.strip("/").strip() for x in parts if x is not None and str(x).strip() != ""])
    if not p.startswith("/"):
        p = "/" + p
    return p


def _sha1_bytes(b: bytes) -> str:
    return hashlib.sha1(b).hexdigest()


def _is_excel(name: str) -> bool:
    n = name.lower()
    return n.endswith(".xlsx") or n.endswith(".xlsm") or n.endswith(".xls")


def _ensure_stage_dirs(dbx: DropboxIO, root: str) -> None:
    dbx.ensure_folder(_join(root, "IN"))
    dbx.ensure_folder(_join(root, "OUT"))
    dbx.ensure_folder(_join(root, "DONE"))


def _debug_print_cfg(cfg: MonthlyCfg) -> None:
    print("[MONTHLY] MONTHLY_MODE=", cfg.mode)
    print("[MONTHLY] inbox_root=", cfg.inbox_root)
    print("[MONTHLY] prep_root=", cfg.prep_root)
    print("[MONTHLY] overview_root=", cfg.overview_root)
    print("[MONTHLY] outbox_root=", cfg.outbox_root)
    print("[MONTHLY] STATE_PATH=", "***")
    print("[MONTHLY] LOGS_DIR=", cfg.logs_dir)


def stage1_prep(dbx: DropboxIO, state: PipelineState, cfg: MonthlyCfg) -> None:
    """
    Stage1: consume Excel from inbox_root/IN -> write to prep_root/OUT (currently copy-only)
            then move original to inbox_root/DONE
    """
    _ensure_stage_dirs(dbx, cfg.inbox_root)
    _ensure_stage_dirs(dbx, cfg.prep_root)

    inbox_in = _join(cfg.inbox_root, "IN")
    inbox_done = _join(cfg.inbox_root, "DONE")
    prep_out = _join(cfg.prep_root, "OUT")

    items = dbx.list_folder(inbox_in)
    files = [it for it in items if it.is_file and _is_excel(it.name)]
    if not files:
        print(f"[MONTHLY] No Excel found under: {inbox_in}")
        return

    for it in files:
        src = _join(inbox_in, it.name)
        raw = dbx.read_bytes_or_none(src)
        if raw is None:
            continue

        key = f"inbox:{it.name}:{_sha1_bytes(raw)}"
        if key in state.done:
            # already processed, move to DONE to keep IN clean
            dst_done = _join(inbox_done, it.name)
            try:
                dbx.move(src, dst_done, overwrite=True)
            except Exception:
                pass
            continue

        # write to prep OUT with a stable name prefix to visualize flow
        out_name = f"00to10_prep_{it.name}"
        dst_out = _join(prep_out, out_name)
        dbx.write_bytes(dst_out, raw, mode="overwrite")

        # move original to DONE
        dst_done = _join(inbox_done, it.name)
        dbx.move(src, dst_done, overwrite=True)

        state.done.append(key)
        print(f"[MONTHLY] stage1_prep: {src} -> {dst_out} ; moved to DONE")


def stage2_api(dbx: DropboxIO, state: PipelineState, cfg: MonthlyCfg) -> None:
    """
    Stage2: consume from prep_root/OUT -> write to overview_root/OUT (copy-only)
            then move input to prep_root/DONE
    """
    _ensure_stage_dirs(dbx, cfg.prep_root)
    _ensure_stage_dirs(dbx, cfg.overview_root)

    prep_out = _join(cfg.prep_root, "OUT")
    prep_done = _join(cfg.prep_root, "DONE")
    overview_out = _join(cfg.overview_root, "OUT")

    items = dbx.list_folder(prep_out)
    files = [it for it in items if it.is_file and _is_excel(it.name)]
    if not files:
        print(f"[MONTHLY] No files found under: {prep_out}")
        return

    for it in files:
        src = _join(prep_out, it.name)
        raw = dbx.read_bytes_or_none(src)
        if raw is None:
            continue
        key = f"prep:{it.name}:{_sha1_bytes(raw)}"
        if key in state.done:
            # already processed; move to DONE
            dst_done = _join(prep_done, it.name)
            try:
                dbx.move(src, dst_done, overwrite=True)
            except Exception:
                pass
            continue

        out_name = f"10to20_overview_{it.name}"
        dst_out = _join(overview_out, out_name)
        dbx.write_bytes(dst_out, raw, mode="overwrite")

        dst_done = _join(prep_done, it.name)
        dbx.move(src, dst_done, overwrite=True)

        state.done.append(key)
        print(f"[MONTHLY] stage2_api: {src} -> {dst_out} ; moved to DONE")


def stage3_personalize(dbx: DropboxIO, state: PipelineState, cfg: MonthlyCfg) -> None:
    """
    Stage3: consume from overview_root/OUT -> write to outbox_root/OUT (copy-only)
            then move input to overview_root/DONE
    """
    _ensure_stage_dirs(dbx, cfg.overview_root)
    _ensure_stage_dirs(dbx, cfg.outbox_root)

    overview_out = _join(cfg.overview_root, "OUT")
    overview_done = _join(cfg.overview_root, "DONE")
    outbox_out = _join(cfg.outbox_root, "OUT")

    items = dbx.list_folder(overview_out)
    files = [it for it in items if it.is_file and _is_excel(it.name)]
    if not files:
        print(f"[MONTHLY] No files found under: {overview_out}")
        return

    for it in files:
        src = _join(overview_out, it.name)
        raw = dbx.read_bytes_or_none(src)
        if raw is None:
            continue
        key = f"overview:{it.name}:{_sha1_bytes(raw)}"
        if key in state.done:
            dst_done = _join(overview_done, it.name)
            try:
                dbx.move(src, dst_done, overwrite=True)
            except Exception:
                pass
            continue

        out_name = f"20to30_personalize_{it.name}"
        dst_out = _join(outbox_out, out_name)
        dbx.write_bytes(dst_out, raw, mode="overwrite")

        dst_done = _join(overview_done, it.name)
        dbx.move(src, dst_done, overwrite=True)

        state.done.append(key)
        print(f"[MONTHLY] stage3_personalize: {src} -> {dst_out} ; moved to DONE")


def run_multistage(dbx: DropboxIO, cfg: MonthlyCfg) -> int:
    _debug_print_cfg(cfg)

    # Ensure system dirs
    dbx.ensure_folder(cfg.logs_dir)
    dbx.ensure_folder(os.path.dirname(cfg.state_path) or "/_system")

    state = load_state(dbx, cfg.state_path)

    # Always keep stage dirs present
    _ensure_stage_dirs(dbx, cfg.inbox_root)
    _ensure_stage_dirs(dbx, cfg.prep_root)
    _ensure_stage_dirs(dbx, cfg.overview_root)
    _ensure_stage_dirs(dbx, cfg.outbox_root)

    # Run stages
    stage1_prep(dbx, state, cfg)
    stage2_api(dbx, state, cfg)
    stage3_personalize(dbx, state, cfg)

    save_state(dbx, cfg.state_path, state)
    print("[MONTHLY] DONE")
    return 0