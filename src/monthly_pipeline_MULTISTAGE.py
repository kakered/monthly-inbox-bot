# -*- coding: utf-8 -*-
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