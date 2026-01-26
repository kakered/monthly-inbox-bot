# -*- coding: utf-8 -*-
"""
src/monthly_main.py

目的:
- Dropbox APIが「実際に見えている」パス空間をログに出して、UIで見ている場所とのズレを可視化する。
- 既存のパイプライン実行（monthly_pipeline_MULTISTAGE/run_multistage 等）が存在するならそれを優先して呼ぶ。
- 存在しない場合でも、最低限「IN 配下に Excel が見えているか」をチェックして、見えていなければ安全に終了する。

注意:
- secrets は出さない（OPENAI_API_KEY 等は表示しない）。
- Dropbox のパス/ファイル名はデバッグのために出す（必要なら後でマスク可能）。
"""

from __future__ import annotations

import os
import sys
import traceback
from dataclasses import dataclass
from typing import Any, Iterable, Optional, List

from .dropbox_io import DropboxIO


# -------------------------
# Config (env)
# -------------------------

@dataclass
class MonthlyCfg:
    monthly_stage: str = "00"  # "00" / "10" / "20" / "30" / "40" etc.
    max_files_per_run: int = 200

    # stage-map後に bash がセットしてくる想定の「統一」環境変数
    monthly_inbox_path: str = ""
    monthly_prep_dir: str = ""
    monthly_overview_dir: str = ""
    monthly_outbox_dir: str = ""

    logs_dir: str = ""
    state_path: str = ""

    # 互換のため残す（既存コードが使ってる可能性）
    monthly_mode: str = "multistage"

    def validate(self) -> None:
        # 空でもクラッシュしない。後でデバッグ出力で分かるようにする。
        pass


def _getenv_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else str(v)


def _getenv_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    try:
        return int(v)
    except Exception:
        return default


def load_cfg_from_env() -> MonthlyCfg:
    cfg = MonthlyCfg(
        monthly_stage=_getenv_str("MONTHLY_STAGE", _getenv_str("MONTHLY_STAGE", "00")),
        max_files_per_run=_getenv_int("MAX_FILES_PER_RUN", 200),
        monthly_inbox_path=_getenv_str("MONTHLY_INBOX_PATH", ""),
        monthly_prep_dir=_getenv_str("MONTHLY_PREP_DIR", ""),
        monthly_overview_dir=_getenv_str("MONTHLY_OVERVIEW_DIR", ""),
        monthly_outbox_dir=_getenv_str("MONTHLY_OUTBOX_DIR", ""),
        logs_dir=_getenv_str("LOGS_DIR", ""),
        state_path=_getenv_str("STATE_PATH", ""),
        monthly_mode=_getenv_str("MONTHLY_MODE", "multistage"),
    )
    cfg.validate()
    return cfg


# -------------------------
# Debug helpers (Dropbox visibility)
# -------------------------

def _safe_print_kv(title: str, value: str) -> None:
    # secrets をここに渡さない前提。パスだけ。
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
    n = name.lower()
    return n.endswith(".xlsx") or n.endswith(".xlsm") or n.endswith(".xls")


def _find_excels_in_folder(dbx: DropboxIO, folder: str, limit_scan: int = 500) -> List[Any]:
    items = _list_folder_safe(dbx, folder, limit=min(200, limit_scan))
    excels = []
    for it in items:
        name = getattr(it, "name", "") or ""
        if _looks_like_excel(name):
            excels.append(it)
    return excels


def debug_dropbox_visibility(dbx: DropboxIO, cfg: MonthlyCfg) -> None:
    """
    ここが「ズレ」特定のための主役。
    - ルート("", "/") や /Apps 周りを試し、どの世界が見えているかをログに残す。
    - 実際に処理対象の inbox_path も必ず list する。
    """
    print("\n[MONTHLY] ===== Dropbox visibility debug (START) =====", flush=True)

    # 重要: いま Python が見ている env を明示（値そのものはパスだけなのでOK）
    _safe_print_kv("MONTHLY_STAGE", cfg.monthly_stage)
    _safe_print_kv("MONTHLY_INBOX_PATH", cfg.monthly_inbox_path)
    _safe_print_kv("MONTHLY_PREP_DIR", cfg.monthly_prep_dir)
    _safe_print_kv("MONTHLY_OVERVIEW_DIR", cfg.monthly_overview_dir)
    _safe_print_kv("MONTHLY_OUTBOX_DIR", cfg.monthly_outbox_dir)
    _safe_print_kv("LOGS_DIR", cfg.logs_dir)
    _safe_print_kv("STATE_PATH", cfg.state_path)
    _safe_print_kv("MONTHLY_MODE", cfg.monthly_mode)

    # いくつかの定番候補を順に試す（Appフォルダ型だと見えないものもある）
    candidates: List[str] = []
    candidates += ["", "/"]
    candidates += ["/Apps", "/Apps/monthly-inbox-bot"]

    # あなたの運用フォルダ候補（stage-map後の値）
    if cfg.monthly_inbox_path:
        candidates += [cfg.monthly_inbox_path]
        # INフォルダも念のため
        if not cfg.monthly_inbox_path.rstrip("/").endswith("/IN"):
            candidates += [cfg.monthly_inbox_path.rstrip("/") + "/IN"]

    # stage dirs も一応
    for p in [cfg.monthly_prep_dir, cfg.monthly_overview_dir, cfg.monthly_outbox_dir, cfg.logs_dir]:
        if p:
            candidates.append(p)

    # 重複除去（順序維持）
    seen = set()
    uniq_candidates: List[str] = []
    for c in candidates:
        if c in seen:
            continue
        seen.add(c)
        uniq_candidates.append(c)

    for c in uniq_candidates:
        _list_folder_safe(dbx, c, limit=50)

    # inbox_path 直下で Excel を探す（見つからない問題の核心）
    inbox = cfg.monthly_inbox_path or ""
    if inbox:
        excels = _find_excels_in_folder(dbx, inbox)
        if excels:
            print(f"\n[MONTHLY][DBG] Excel found under {inbox!r}: {len(excels)}", flush=True)
            for it in excels[:30]:
                n = getattr(it, "name", None)
                p = getattr(it, "path_display", None) or getattr(it, "path_lower", None)
                print(f"[MONTHLY][DBG]   * {n} | {p}", flush=True)
        else:
            print(f"\n[MONTHLY][DBG] No Excel found directly under: {inbox}", flush=True)

    print("[MONTHLY] ===== Dropbox visibility debug (END) =====\n", flush=True)


# -------------------------
# Pipeline dispatch
# -------------------------

def try_run_existing_pipeline(dbx: DropboxIO, cfg: MonthlyCfg) -> bool:
    """
    既存の pipeline 実装があるなら呼ぶ。
    失敗した場合は例外を握りつぶさず、上位で落として良い（原因がログに出るため）。
    戻り値:
      True  = pipeline 実行に移行した（=この関数内で呼んだ）
      False = pipeline モジュールが見つからない等で呼べなかった
    """
    # 1) monthly_pipeline_MULTISTAGE.py があるケース
    try:
        from .monthly_pipeline_MULTISTAGE import run_multistage  # type: ignore
        # 既存関数の引数形が (dbx, cfg) か (dbx, state, cfg) か不明なので吸収
        try:
            run_multistage(dbx, cfg)  # type: ignore
        except TypeError:
            # state を Dropbox 上の JSON として扱っている可能性
            state = {}
            try:
                if cfg.state_path:
                    state = dbx.read_json(cfg.state_path) or {}
            except Exception:
                state = {}
            run_multistage(dbx, state, cfg)  # type: ignore
        return True
    except ModuleNotFoundError:
        pass
    except Exception:
        # 実装が存在していて内部で落ちた場合は、そのまま上に投げる
        raise

    # 2) もし monthly_pipeline.py / monthly_pipeline_SINGLE.py があるケース（保険）
    for mod_name, fn_name in [
        (".monthly_pipeline", "run"),
        (".monthly_pipeline", "main"),
        (".monthly_pipeline_SINGLE", "run_single"),
        (".monthly_pipeline_SINGLE", "run"),
    ]:
        try:
            mod = __import__(f"src{mod_name}", fromlist=[fn_name])
            fn = getattr(mod, fn_name, None)
            if fn is None:
                continue
            try:
                fn(dbx, cfg)  # type: ignore
            except TypeError:
                state = {}
                try:
                    if cfg.state_path:
                        state = dbx.read_json(cfg.state_path) or {}
                except Exception:
                    state = {}
                fn(dbx, state, cfg)  # type: ignore
            return True
        except ModuleNotFoundError:
            continue
        except Exception:
            raise

    return False


def fallback_stage00_check_only(dbx: DropboxIO, cfg: MonthlyCfg) -> int:
    """
    既存 pipeline が無い/呼べない場合の最低限動作:
    - inbox_path を見に行き、Excel が無ければ 0（成功）で終了
    - Excel があれば「見えている」ことだけログに出して 0 で終了（まだ処理はしない）
    """
    inbox = cfg.monthly_inbox_path
    if not inbox:
        print("[MONTHLY] inbox path is empty. (MONTHLY_INBOX_PATH not set?)", flush=True)
        return 0

    excels = _find_excels_in_folder(dbx, inbox)
    if not excels:
        print(f"[MONTHLY] No Excel found under:\n{inbox}", flush=True)
        return 0

    print(f"[MONTHLY] Excel detected under {inbox}: {len(excels)} file(s).", flush=True)
    for it in excels[:20]:
        n = getattr(it, "name", None)
        p = getattr(it, "path_display", None) or getattr(it, "path_lower", None)
        print(f"[MONTHLY]   - {n} | {p}", flush=True)

    print("[MONTHLY] (fallback) Not processing in this mode; pipeline module not found.", flush=True)
    return 0


# -------------------------
# Entry
# -------------------------

def main() -> int:
    cfg = load_cfg_from_env()
    dbx = DropboxIO.from_env()

    # まず「見えている世界」を出す（ここが今回の目的）
    debug_dropbox_visibility(dbx, cfg)

    # 既存 pipeline があればそれを優先して実行
    ran = try_run_existing_pipeline(dbx, cfg)
    if ran:
        return 0

    # 無ければ最低限のチェックだけ
    return fallback_stage00_check_only(dbx, cfg)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception:
        print("[MONTHLY] Unhandled exception:", file=sys.stderr)
        traceback.print_exc()
        raise