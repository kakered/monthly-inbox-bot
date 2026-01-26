# -*- coding: utf-8 -*-
"""
src/monthly_main.py

やること（重要）:
1) monthly_pipeline_MULTISTAGE.py が参照する cfg.inbox_path / cfg.prep_dir / cfg.overview_dir / cfg.outbox_dir を必ず持たせる
2) MONTHLY_* が空でも、STAGE00_IN / STAGE10_IN... などから復元して動くようにする
3) Dropbox が見えているパス空間のデバッグ出力を残す（secretsは出さない）

ポイント:
- これで "No Excel found under: /00_inbox_raw/IN" が出る場合、
  「Dropbox API から見えていない」か「ファイルがまだ同期中」かが切り分けできる。
"""

from __future__ import annotations

import os
import sys
import traceback
from dataclasses import dataclass
from typing import Any, List

from .dropbox_io import DropboxIO


# -------------------------
# env utils
# -------------------------

def _getenv_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v)


def _getenv_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    try:
        return int(v)
    except Exception:
        return default


def _norm_path(p: str) -> str:
    # Dropbox API のパスは先頭 "/" が基本
    if not p:
        return ""
    p = p.strip()
    if not p.startswith("/"):
        p = "/" + p
    # 末尾スラッシュは基本落とす（IN/DONE/OUT 等は呼び元でつける）
    if len(p) > 1 and p.endswith("/"):
        p = p[:-1]
    return p


def _stage_key(stage: str) -> str:
    # "0" -> "00" など
    s = str(stage).strip()
    if s.isdigit():
        return s.zfill(2)
    return s


def _pick_stage_paths(stage: str) -> tuple[str, str, str, str]:
    """
    MONTHLY_* が空だった時の救済:
    - STAGE00_IN / STAGE00_OUT / STAGE00_DONE ... のような repo variables を使って復元する
    """
    st = _stage_key(stage)

    # Stage00 の「入力」は STAGE00_IN を期待
    inbox = _getenv_str(f"STAGE{st}_IN", "")

    # 次段の IN は「その段の IN」だが、pipelineの用語としては
    # prep_dir/overview_dir/outbox_dir は「各段の IN」を指す運用にしているので
    # ここでは "ステージ全体のルート" ではなく "IN" を優先して使う。
    # ただし変数側が "/10_preformat_py/IN" のように IN 込みで入っている前提。
    prep = _getenv_str("STAGE10_IN", "")
    overview = _getenv_str("STAGE20_IN", "")
    outbox = _getenv_str("STAGE30_IN", "")

    return inbox, prep, overview, outbox


# -------------------------
# Config
# -------------------------

@dataclass
class MonthlyCfg:
    # pipeline側が期待している名前（重要）
    inbox_path: str = ""
    prep_dir: str = ""
    overview_dir: str = ""
    outbox_dir: str = ""

    logs_dir: str = ""
    state_path: str = ""

    monthly_stage: str = "00"
    max_files_per_run: int = 200

    # 互換（残しておく）
    monthly_mode: str = "multistage"

    def __post_init__(self) -> None:
        # normalize
        self.inbox_path = _norm_path(self.inbox_path)
        self.prep_dir = _norm_path(self.prep_dir)
        self.overview_dir = _norm_path(self.overview_dir)
        self.outbox_dir = _norm_path(self.outbox_dir)
        self.logs_dir = _norm_path(self.logs_dir)
        self.state_path = _norm_path(self.state_path)


def load_cfg_from_env() -> MonthlyCfg:
    stage = _getenv_str("MONTHLY_STAGE", "00")

    # まず MONTHLY_* を優先
    inbox = _getenv_str("MONTHLY_INBOX_PATH", "")
    prep = _getenv_str("MONTHLY_PREP_DIR", "")
    overview = _getenv_str("MONTHLY_OVERVIEW_DIR", "")
    outbox = _getenv_str("MONTHLY_OUTBOX_DIR", "")

    # 空なら STAGE??_IN 等から復元（今回のログはここが必要）
    if not any([inbox, prep, overview, outbox]):
        inbox2, prep2, overview2, outbox2 = _pick_stage_paths(stage)
        inbox = inbox or inbox2
        prep = prep or prep2
        overview = overview or overview2
        outbox = outbox or outbox2

    cfg = MonthlyCfg(
        inbox_path=inbox,
        prep_dir=prep,
        overview_dir=overview,
        outbox_dir=outbox,
        logs_dir=_getenv_str("LOGS_DIR", "/_system/logs"),
        state_path=_getenv_str("STATE_PATH", "/_system/state.json"),
        monthly_stage=stage,
        max_files_per_run=_getenv_int("MAX_FILES_PER_RUN", 200),
        monthly_mode=_getenv_str("MONTHLY_MODE", "multistage"),
    )
    return cfg


# -------------------------
# Debug helpers
# -------------------------

def _safe_print_kv(title: str, value: str) -> None:
    print(f"[MONTHLY] {title}={value}", flush=True)


def _list_folder_safe(dbx: DropboxIO, path: str, limit: int = 50) -> List[Any]:
    print(f"\n[MONTHLY][DBG] LIST {path!r}", flush=True)
    try:
        items = dbx.list_folder(path)
    except Exception as e:
        print(f"[MONTHLY][DBG]   ERROR: {repr(e)}", flush=True)
        return []
    shown = 0
    for it in items:
        n = getattr(it, "name", None)
        p = getattr(it, "path_display", None) or getattr(it, "path_lower", None)
        typ = type(it).__name__
        print(f"[MONTHLY][DBG]   - {typ}: {n} | {p}", flush=True)
        shown += 1
        if shown >= limit:
            break
    if len(items) > limit:
        print(f"[MONTHLY][DBG]   ... {len(items) - limit} more", flush=True)
    return items


def _looks_like_excel(name: str) -> bool:
    n = (name or "").lower()
    return n.endswith(".xlsx") or n.endswith(".xlsm") or n.endswith(".xls")


def _find_excels_in_folder(dbx: DropboxIO, folder: str, limit_list: int = 200) -> List[Any]:
    items = _list_folder_safe(dbx, folder, limit=limit_list)
    out: List[Any] = []
    for it in items:
        name = getattr(it, "name", "") or ""
        if _looks_like_excel(name):
            out.append(it)
    return out


def debug_dropbox_visibility(dbx: DropboxIO, cfg: MonthlyCfg) -> None:
    print("\n[MONTHLY] ===== Dropbox visibility debug (START) =====", flush=True)

    _safe_print_kv("MONTHLY_STAGE", cfg.monthly_stage)
    _safe_print_kv("inbox_path", cfg.inbox_path)
    _safe_print_kv("prep_dir", cfg.prep_dir)
    _safe_print_kv("overview_dir", cfg.overview_dir)
    _safe_print_kv("outbox_dir", cfg.outbox_dir)
    _safe_print_kv("LOGS_DIR", cfg.logs_dir)
    _safe_print_kv("STATE_PATH", "***" if cfg.state_path else "")
    _safe_print_kv("MONTHLY_MODE", cfg.monthly_mode)

    # どの世界が見えているか
    for c in ["", "/", "/Apps", "/Apps/monthly-inbox-bot", cfg.logs_dir]:
        if c:
            _list_folder_safe(dbx, c, limit=50)

    # ここが最重要：入力フォルダとINの中身
    if cfg.inbox_path:
        _list_folder_safe(dbx, cfg.inbox_path, limit=100)
        excels = _find_excels_in_folder(dbx, cfg.inbox_path, limit_list=200)
        if excels:
            print(f"\n[MONTHLY][DBG] Excel found under {cfg.inbox_path!r}: {len(excels)}", flush=True)
            for it in excels[:30]:
                n = getattr(it, "name", None)
                p = getattr(it, "path_display", None) or getattr(it, "path_lower", None)
                print(f"[MONTHLY][DBG]   * {n} | {p}", flush=True)
        else:
            print(f"\n[MONTHLY][DBG] No Excel found directly under: {cfg.inbox_path}", flush=True)
    else:
        print("\n[MONTHLY][DBG] inbox_path is empty (env not wired).", flush=True)

    print("[MONTHLY] ===== Dropbox visibility debug (END) =====\n", flush=True)


# -------------------------
# Pipeline dispatch
# -------------------------

def try_run_existing_pipeline(dbx: DropboxIO, cfg: MonthlyCfg) -> bool:
    """
    monthly_pipeline_MULTISTAGE があればそれを呼ぶ。
    （cfg に inbox_path 等が揃ったので、ここで落ちなくなるはず）
    """
    try:
        from .monthly_pipeline_MULTISTAGE import run_multistage  # type: ignore
    except ModuleNotFoundError:
        return False

    # state の読み込み（失敗しても空で続行）
    state = {}
    try:
        if cfg.state_path:
            state = dbx.read_json(cfg.state_path) or {}
    except Exception:
        state = {}

    # 関数シグネチャ差の吸収
    try:
        run_multistage(dbx, cfg)  # type: ignore
    except TypeError:
        run_multistage(dbx, state, cfg)  # type: ignore
    return True


def fallback_check_only(dbx: DropboxIO, cfg: MonthlyCfg) -> int:
    """
    pipeline が呼べない場合の最低限
    """
    if not cfg.inbox_path:
        print("[MONTHLY] inbox_path is empty. Check env wiring in workflow.", flush=True)
        return 0

    excels = _find_excels_in_folder(dbx, cfg.inbox_path, limit_list=200)
    if not excels:
        print(f"[MONTHLY] No Excel found under:\n{cfg.inbox_path}", flush=True)
        return 0

    print(f"[MONTHLY] Excel detected under {cfg.inbox_path}: {len(excels)} file(s).", flush=True)
    for it in excels[:20]:
        n = getattr(it, "name", None)
        p = getattr(it, "path_display", None) or getattr(it, "path_lower", None)
        print(f"[MONTHLY]   - {n} | {p}", flush=True)
    return 0


# -------------------------
# Entry
# -------------------------

def main() -> int:
    cfg = load_cfg_from_env()
    dbx = DropboxIO.from_env()

    debug_dropbox_visibility(dbx, cfg)

    ran = try_run_existing_pipeline(dbx, cfg)
    if ran:
        return 0

    return fallback_check_only(dbx, cfg)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception:
        print("[MONTHLY] Unhandled exception:", file=sys.stderr)
        traceback.print_exc()
        raise