# -*- coding: utf-8 -*-
<<<<<<< HEAD
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
=======
"""
monthly_pipeline_MULTISTAGE.py

Switch-ON stage runner.

Stage mapping:
- MONTHLY_STAGE=00 : Excel (.xlsx) -> JSON (prep)  [00/IN -> 00/DONE, outputs -> 10/IN]
- MONTHLY_STAGE=10 : JSON (prep) -> Overview JSON  [10/IN -> 10/DONE, outputs -> 20/IN]
- MONTHLY_STAGE=20 : Overview JSON -> Markdown     [20/IN -> 20/DONE, outputs -> 30/OUT]

NOTE:
Stage10 (API) is currently a deterministic placeholder (no OpenAI call) to keep the pipeline running.
You can later replace _call_openai_overview() with your real API call module.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Dict, List, Tuple

from .dropbox_io import DropboxIO
from .excel_exporter import process_monthly_workbook
from .monthly_spec import MonthlyCfg
from .state_store import load_state, save_state
from .utils_dropbox_item import dbx_name, dbx_path


def _now_stamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _sha12(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()[:12]


def _ensure_stage_dirs(dbx: DropboxIO, cfg: MonthlyCfg) -> None:
    for p in [
        cfg.inbox_in(), cfg.inbox_done(), cfg.inbox_out(),
        cfg.prep_in(), cfg.prep_done(), cfg.prep_out(),
        cfg.overview_in(), cfg.overview_done(), cfg.overview_out(),
        cfg.outbox_in(), cfg.outbox_done(), cfg.outbox_out(),
        cfg.logs_dir,
    ]:
        try:
            dbx.ensure_folder(p)
        except Exception:
            pass
>>>>>>> dev

    inbox_in = _join(cfg.inbox_root, "IN")
    inbox_done = _join(cfg.inbox_root, "DONE")
    prep_out = _join(cfg.prep_root, "OUT")

<<<<<<< HEAD
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
=======
def _list_files(dbx: DropboxIO, folder: str) -> List[Tuple[str, str]]:
    items = dbx.list_folder(folder)
    out: List[Tuple[str, str]] = []
    for it in items:
        if getattr(it, "is_file", False):
            n = dbx_name(it)
            p = dbx_path(it)
            if n and p:
                out.append((n, p))
    return out


def _move_to_done(dbx: DropboxIO, src_path: str, done_dir: str) -> None:
    name = src_path.split("/")[-1]
    dbx.move(src_path, f"{done_dir}/{name}", overwrite=True)


def stage00_excel_to_prep(dbx: DropboxIO, cfg: MonthlyCfg, state: Dict) -> int:
    inbox_in = cfg.inbox_in()
    inbox_done = cfg.inbox_done()
    prep_in = cfg.prep_in()

    files = _list_files(dbx, inbox_in)
    xlsx = [(n, p) for (n, p) in files if n.lower().endswith(".xlsx")]
    if not xlsx:
        return 0

    done_keys = set(state.get("done", []))
    processed = 0

    for name, path in xlsx[: cfg.max_files_per_run]:
        raw = dbx.read_bytes_or_none(path)
        if raw is None:
            continue
        key = f"00:{name}:{_sha12(raw)}"
        if key in done_keys:
            continue

        base = name.rsplit(".", 1)[0]
        out_name = f"{base}__{_now_stamp()}__{_sha12(raw)}.prep.json"
        out_path = f"{prep_in}/{out_name}"

        prep_obj = process_monthly_workbook(raw, source_name=name)
        dbx.write_bytes(out_path, json.dumps(prep_obj, ensure_ascii=False, indent=2).encode("utf-8"), overwrite=True)

        _move_to_done(dbx, path, inbox_done)

        done_keys.add(key)
        processed += 1

    state["done"] = sorted(done_keys)
    return processed


def _call_openai_overview(prep_obj: Dict, cfg: MonthlyCfg) -> Dict:
    # placeholder (no API)
    return {
        "meta": {"model": cfg.openai_model, "depth": cfg.depth, "created_at": datetime.now().isoformat()},
        "source": prep_obj.get("source", {}),
        "sheets": prep_obj.get("sheets", []),
        "notes": ["API stage placeholder. Replace _call_openai_overview() with your real OpenAI call when ready."],
    }


def stage10_prep_to_overview(dbx: DropboxIO, cfg: MonthlyCfg, state: Dict) -> int:
    prep_in = cfg.prep_in()
    prep_done = cfg.prep_done()
    overview_in = cfg.overview_in()

    files = _list_files(dbx, prep_in)
    preps = [(n, p) for (n, p) in files if n.lower().endswith(".prep.json")]
    if not preps:
        return 0

    done_keys = set(state.get("done", []))
    processed = 0

    for name, path in preps[: cfg.max_files_per_run]:
        raw = dbx.read_bytes_or_none(path)
        if raw is None:
            continue
        key = f"10:{name}:{_sha12(raw)}"
        if key in done_keys:
            continue

        prep_obj = json.loads(raw.decode("utf-8"))
        overview_obj = _call_openai_overview(prep_obj, cfg)

        base = name.replace(".prep.json", "")
        out_name = f"{base}__{_now_stamp()}.overview.json"
        out_path = f"{overview_in}/{out_name}"
        dbx.write_bytes(out_path, json.dumps(overview_obj, ensure_ascii=False, indent=2).encode("utf-8"), overwrite=True)

        _move_to_done(dbx, path, prep_done)

        done_keys.add(key)
        processed += 1

    state["done"] = sorted(done_keys)
    return processed


def stage20_overview_to_markdown(dbx: DropboxIO, cfg: MonthlyCfg, state: Dict) -> int:
    overview_in = cfg.overview_in()
    overview_done = cfg.overview_done()
    out_dir = cfg.outbox_out()

    files = _list_files(dbx, overview_in)
    ov = [(n, p) for (n, p) in files if n.lower().endswith(".overview.json")]
    if not ov:
        return 0

    done_keys = set(state.get("done", []))
    processed = 0

    for name, path in ov[: cfg.max_files_per_run]:
        raw = dbx.read_bytes_or_none(path)
        if raw is None:
            continue
        key = f"20:{name}:{_sha12(raw)}"
        if key in done_keys:
            continue

        obj = json.loads(raw.decode("utf-8"))

        md = []
        md.append("# Monthly Overview\n\n")
        src = obj.get("source", {})
        md.append(f"- Source: {src.get('name','')}\n")
        md.append(f"- Generated: {obj.get('meta', {}).get('created_at', '')}\n\n")
        md.append("## Sheets\n")
        for sh in obj.get("sheets", []):
            md.append(f"- {sh.get('name','')}: {sh.get('n_rows','')} rows\n")
        md.append("\n## Notes\n")
        for n in obj.get("notes", []):
            md.append(f"- {n}\n")
        md_text = "".join(md)

        base = name.replace(".overview.json", "")
        out_name = f"{base}__{_now_stamp()}.md"
        out_path = f"{out_dir}/{out_name}"
        dbx.write_bytes(out_path, md_text.encode("utf-8"), overwrite=True)

        _move_to_done(dbx, path, overview_done)

        done_keys.add(key)
        processed += 1

    state["done"] = sorted(done_keys)
    return processed


def run_switch_stage(dbx: DropboxIO, cfg: MonthlyCfg) -> int:
    _ensure_stage_dirs(dbx, cfg)
    state = load_state(dbx, cfg.state_path)

    stage = (cfg.stage or "00").strip()
    if stage == "00":
        n = stage00_excel_to_prep(dbx, cfg, state)
    elif stage == "10":
        n = stage10_prep_to_overview(dbx, cfg, state)
    elif stage == "20":
        n = stage20_overview_to_markdown(dbx, cfg, state)
    else:
        n = 0

    save_state(dbx, cfg.state_path, state)
    return n
>>>>>>> dev
