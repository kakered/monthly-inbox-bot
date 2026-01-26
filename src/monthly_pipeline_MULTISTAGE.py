# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from .dropbox_io import DropboxIO
from .excel_exporter import process_monthly_workbook


def _ts() -> str:
    return datetime.now().strftime("%Y%m%d-%H%M%S")


def _norm(p: str) -> str:
    p = (p or "").strip()
    if p == "/":
        return ""
    if p == "":
        return ""
    if not p.startswith("/"):
        p = "/" + p
    if len(p) > 1 and p.endswith("/"):
        p = p[:-1]
    return p


def _sibling(p: str, leaf_from: str, leaf_to: str) -> str:
    """
    Replace last path segment if it matches leaf_from.
    e.g. /00_inbox_raw/IN -> /00_inbox_raw/DONE
    """
    p = _norm(p)
    if p == "":
        return p
    parts = p.split("/")
    if parts[-1] == leaf_from:
        parts[-1] = leaf_to
    return "/".join(parts)


def _join(a: str, b: str) -> str:
    a = _norm(a)
    b = (b or "").strip().lstrip("/")
    if a == "":
        return "/" + b if b else ""
    return a + ("/" + b if b else "")


@dataclass
class Cfg:
    inbox_in: str
    prep_in: str
    overview_in: str
    outbox_in: str
    logs_dir: str
    state_path: str
    max_files: int

    @classmethod
    def from_env(cls) -> "Cfg":
        inbox_in = os.getenv("MONTHLY_INBOX_PATH", "/00_inbox_raw/IN")
        prep_in = os.getenv("MONTHLY_PREP_DIR", "/10_preformat_py/IN")
        overview_in = os.getenv("MONTHLY_OVERVIEW_DIR", "/20_overview_api/IN")
        outbox_in = os.getenv("MONTHLY_OUTBOX_DIR", "/30_personalize_py/IN")
        logs_dir = os.getenv("LOGS_DIR", "/_system/logs")
        state_path = os.getenv("STATE_PATH", "/_system/state.json")
        max_files = int(os.getenv("MAX_FILES_PER_RUN", "200"))
        return cls(
            inbox_in=_norm(inbox_in),
            prep_in=_norm(prep_in),
            overview_in=_norm(overview_in),
            outbox_in=_norm(outbox_in),
            logs_dir=_norm(logs_dir),
            state_path=_norm(state_path),
            max_files=max_files,
        )


def _load_state(dbx: DropboxIO, path: str) -> Dict:
    raw = dbx.read_json_bytes_or_none(path)
    if not raw:
        return {}
    try:
        return json.loads(raw.decode("utf-8"))
    except Exception:
        return {}


def _save_state(dbx: DropboxIO, path: str, state: Dict) -> None:
    data = json.dumps(state, ensure_ascii=False, indent=2).encode("utf-8")
    dbx.ensure_folder(os.path.dirname(path) or "/_system")
    dbx.upload_bytes(path, data)


def _pick_xlsx_files(dbx: DropboxIO, folder: str, max_files: int) -> List[Tuple[str, str, str]]:
    """
    return list of (path, name, rev)
    """
    items = dbx.list_folder(folder)
    out: List[Tuple[str, str, str]] = []
    for it in items:
        name = getattr(it, "name", None)
        if not isinstance(name, str):
            continue
        low = name.lower()
        if not (low.endswith(".xlsx") or low.endswith(".xlsm") or low.endswith(".xls")):
            continue
        path = getattr(it, "path_display", None) or getattr(it, "path_lower", None)
        if not isinstance(path, str):
            continue
        rev = getattr(it, "rev", "") or ""
        out.append((path, name, rev))
        if len(out) >= max_files:
            break
    return out


def _stage_folders(in_path: str) -> Tuple[str, str, str]:
    """
    Given /XX_xxx/IN -> returns (IN, DONE, OUT) sibling folders
    """
    in_path = _norm(in_path)
    done_path = _sibling(in_path, "IN", "DONE")
    out_path = _sibling(in_path, "IN", "OUT")
    return in_path, done_path, out_path


def stage00_raw_to_prep(dbx: DropboxIO, cfg: Cfg, state: Dict) -> int:
    """
    00/IN から Excel を取り込み、加工済み(placeholder)を 10/IN へ出す。
    入力は 00/DONE へ move。
    """
    s_in, s_done, _ = _stage_folders(cfg.inbox_in)
    t_in, _, _ = _stage_folders(cfg.prep_in)

    dbx.ensure_folder(s_in)
    dbx.ensure_folder(s_done)
    dbx.ensure_folder(t_in)

    done_keys = set(state.get("stage00_done", []))

    files = _pick_xlsx_files(dbx, s_in, cfg.max_files)
    if not files:
        print(f"[MONTHLY][00] No Excel under: {s_in}")
        return 0

    n = 0
    for path, name, rev in files:
        key = f"{path}::{rev}"
        if key in done_keys:
            continue

        print(f"[MONTHLY][00] process: {path} (rev={rev})")
        xlsx_bytes = dbx.download_to_bytes(path)

        overview_bytes, per_person_bytes = process_monthly_workbook(xlsx_bytes=xlsx_bytes, password=None)

        stamp = _ts()
        base = os.path.splitext(name)[0]
        # 10/IN に「加工結果」2種を置く（必要なら後で1種に統合も可）
        out_over = f"{base}__00to10__overview__rev-{rev}__{stamp}.xlsx"
        out_per = f"{base}__00to10__per_person__rev-{rev}__{stamp}.xlsx"

        dbx.upload_bytes(_join(t_in, out_over), overview_bytes)
        dbx.upload_bytes(_join(t_in, out_per), per_person_bytes)

        # move input to DONE
        dbx.move(path, _join(s_done, f"{base}__rev-{rev}__{stamp}.xlsx"), overwrite=True)

        done_keys.add(key)
        n += 1

    state["stage00_done"] = sorted(done_keys)
    return n


def stage10_prep_to_overview(dbx: DropboxIO, cfg: Cfg, state: Dict) -> int:
    """
    10/IN から 20/IN へ"次段に回す"。
    （ここは今はコピー/移動のみ。後でAPI処理に置換する場所）
    """
    s_in, s_done, _ = _stage_folders(cfg.prep_in)
    t_in, _, _ = _stage_folders(cfg.overview_in)

    dbx.ensure_folder(s_in)
    dbx.ensure_folder(s_done)
    dbx.ensure_folder(t_in)

    done_keys = set(state.get("stage10_done", []))

    files = _pick_xlsx_files(dbx, s_in, cfg.max_files)
    if not files:
        print(f"[MONTHLY][10] No Excel under: {s_in}")
        return 0

    n = 0
    for path, name, rev in files:
        key = f"{path}::{rev}"
        if key in done_keys:
            continue

        stamp = _ts()
        base = os.path.splitext(name)[0]
        print(f"[MONTHLY][10] forward: {path} (rev={rev})")

        data = dbx.download_to_bytes(path)
        dbx.upload_bytes(_join(t_in, f"{base}__10to20__rev-{rev}__{stamp}.xlsx"), data)

        dbx.move(path, _join(s_done, f"{base}__rev-{rev}__{stamp}.xlsx"), overwrite=True)

        done_keys.add(key)
        n += 1

    state["stage10_done"] = sorted(done_keys)
    return n


def stage20_overview_to_personalize(dbx: DropboxIO, cfg: Cfg, state: Dict) -> int:
    """
    20/IN から 30/IN へ回す（今は移送のみ）
    """
    s_in, s_done, _ = _stage_folders(cfg.overview_in)
    t_in, _, _ = _stage_folders(cfg.outbox_in)

    dbx.ensure_folder(s_in)
    dbx.ensure_folder(s_done)
    dbx.ensure_folder(t_in)

    done_keys = set(state.get("stage20_done", []))

    files = _pick_xlsx_files(dbx, s_in, cfg.max_files)
    if not files:
        print(f"[MONTHLY][20] No Excel under: {s_in}")
        return 0

    n = 0
    for path, name, rev in files:
        key = f"{path}::{rev}"
        if key in done_keys:
            continue

        stamp = _ts()
        base = os.path.splitext(name)[0]
        print(f"[MONTHLY][20] forward: {path} (rev={rev})")

        data = dbx.download_to_bytes(path)
        dbx.upload_bytes(_join(t_in, f"{base}__20to30__rev-{rev}__{stamp}.xlsx"), data)

        dbx.move(path, _join(s_done, f"{base}__rev-{rev}__{stamp}.xlsx"), overwrite=True)

        done_keys.add(key)
        n += 1

    state["stage20_done"] = sorted(done_keys)
    return n


def stage30_finalize(dbx: DropboxIO, cfg: Cfg, state: Dict) -> int:
    """
    30/IN に来たものを 30/OUT に"完成物"として出す（今は同一コピー）。
    入力は 30/DONE に移動。
    """
    s_in, s_done, s_out = _stage_folders(cfg.outbox_in)

    dbx.ensure_folder(s_in)
    dbx.ensure_folder(s_done)
    dbx.ensure_folder(s_out)

    done_keys = set(state.get("stage30_done", []))

    files = _pick_xlsx_files(dbx, s_in, cfg.max_files)
    if not files:
        print(f"[MONTHLY][30] No Excel under: {s_in}")
        return 0

    n = 0
    for path, name, rev in files:
        key = f"{path}::{rev}"
        if key in done_keys:
            continue

        stamp = _ts()
        base = os.path.splitext(name)[0]
        print(f"[MONTHLY][30] finalize: {path} (rev={rev})")

        data = dbx.download_to_bytes(path)
        dbx.upload_bytes(_join(s_out, f"{base}__FINAL__rev-{rev}__{stamp}.xlsx"), data)
        dbx.move(path, _join(s_done, f"{base}__rev-{rev}__{stamp}.xlsx"), overwrite=True)

        done_keys.add(key)
        n += 1

    state["stage30_done"] = sorted(done_keys)
    return n


def run_multistage(dbx: Optional[DropboxIO] = None, cfg: Optional[Cfg] = None) -> None:
    dbx = dbx or DropboxIO.from_env()
    cfg = cfg or Cfg.from_env()

    # create system folders
    dbx.ensure_folder(cfg.logs_dir)
    dbx.ensure_folder(os.path.dirname(cfg.state_path) or "/_system")

    stage = (os.getenv("MONTHLY_STAGE") or "00").strip()
    state = _load_state(dbx, cfg.state_path)

    print(f"[MONTHLY] MONTHLY_STAGE={stage}")
    print(f"[MONTHLY] inbox_in={cfg.inbox_in}")
    print(f"[MONTHLY] prep_in={cfg.prep_in}")
    print(f"[MONTHLY] overview_in={cfg.overview_in}")
    print(f"[MONTHLY] outbox_in={cfg.outbox_in}")
    print(f"[MONTHLY] logs_dir={cfg.logs_dir}")
    print(f"[MONTHLY] state_path={cfg.state_path}")

    if stage == "00":
        ran = stage00_raw_to_prep(dbx, cfg, state)
    elif stage == "10":
        ran = stage10_prep_to_overview(dbx, cfg, state)
    elif stage == "20":
        ran = stage20_overview_to_personalize(dbx, cfg, state)
    elif stage == "30":
        ran = stage30_finalize(dbx, cfg, state)
    else:
        raise RuntimeError("MONTHLY_STAGE must be one of: 00, 10, 20, 30")

    _save_state(dbx, cfg.state_path, state)
    print(f"[MONTHLY] stage={stage} processed={ran}")