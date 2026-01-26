# -*- coding: utf-8 -*-
from __future__ import annotations
import os
from typing import List
from .dropbox_io import DropboxIO
from .monthly_spec import MonthlyCfg


# ---------------- state ----------------
def _load_state(dbx: DropboxIO, path: str) -> dict:
    obj = dbx.read_json_or_none(path)
    return obj or {"done": []}


def _save_state(dbx: DropboxIO, path: str, state: dict):
    dbx.write_json(path, state)


# ---------------- helpers ----------------
def _iter_excel_under(dbx: DropboxIO, path: str) -> List[str]:
    items = dbx.list_folder(path)
    out = []
    for it in items:
        if hasattr(it, "name") and it.name.lower().endswith(".xlsx"):
            out.append(f"{path}/{it.name}")
    return out


# ---------------- stages ----------------
def stage1_prep(dbx: DropboxIO, state: dict, cfg: MonthlyCfg):
    excels = _iter_excel_under(dbx, cfg.inbox_path)
    if not excels:
        print(f"[MONTHLY] No Excel found directly under: {cfg.inbox_path}")
        return

    for src in excels:
        name = os.path.basename(src)
        dst = f"{cfg.prep_dir}/{name}"
        data = dbx.download(src)
        dbx.upload(dst, data, overwrite=True)
        state["done"].append(src)


def stage2_api(dbx: DropboxIO, state: dict, cfg: MonthlyCfg):
    excels = _iter_excel_under(dbx, cfg.prep_out_dir)
    for src in excels:
        name = os.path.basename(src)
        dst = f"{cfg.overview_dir}/{name}"
        data = dbx.download(src)
        dbx.upload(dst, data, overwrite=True)


def stage3_personalize(dbx: DropboxIO, state: dict, cfg: MonthlyCfg):
    excels = _iter_excel_under(dbx, cfg.overview_out_dir)
    for src in excels:
        name = os.path.basename(src)
        dst = f"{cfg.outbox_dir}/{name}"
        data = dbx.download(src)
        dbx.upload(dst, data, overwrite=True)


# ---------------- entry ----------------
def run_multistage(dbx: DropboxIO, cfg: MonthlyCfg):
    state = _load_state(dbx, cfg.state_path)

    stage1_prep(dbx, state, cfg)
    stage2_api(dbx, state, cfg)
    stage3_personalize(dbx, state, cfg)

    _save_state(dbx, cfg.state_path, state)