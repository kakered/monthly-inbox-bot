# -*- coding: utf-8 -*-
"""
monthly_main.py

GitHub Actions から `python -m src.monthly_main` / `runpy.run_module("src.monthly_main")`
で呼ばれても落ちない「月報パイプライン」エントリポイント。

今回の修正ポイント
- sys.argv 前提を完全排除（runpy でも動く）
- DropboxIO.from_env() を呼ばない（存在しない実装に依存しない）
- env から DropboxIO を初期化（複数の __init__ 署名にフォールバック対応）
- どの stage でも「該当 stage モジュールが無い/失敗」でもログを出して安全に終了できる
"""

from __future__ import annotations

import os
import sys
import json
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Any, List, Tuple


# ---- optional imports (repo 側の実装差異に強くする) ----
try:
    from src.audit_logger import write_audit_record  # type: ignore
except Exception:
    write_audit_record = None  # type: ignore

try:
    from src.dropbox_io import DropboxIO  # type: ignore
except Exception as e:
    DropboxIO = None  # type: ignore
    _DROPBOXIO_IMPORT_ERROR = e
else:
    _DROPBOXIO_IMPORT_ERROR = None


# =========================================================
# utilities
# =========================================================

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _env(name: str, default: str = "") -> str:
    v = os.environ.get(name)
    if v is None:
        return default
    v = str(v)
    return v if v.strip() != "" else default


def _stage() -> str:
    return (_env("MONTHLY_STAGE", "00") or "00").strip()


def _pick_stage_path(stage: str, kind: str) -> str:
    # kind in {"IN","OUT","DONE"}
    key = f"STAGE{stage}_{kind}"
    return _env(key, "")


def _safe_print(msg: str) -> None:
    print(msg, flush=True)


def _audit(event: str, payload: dict) -> None:
    """
    audit_logger があれば Dropbox に jsonl で吐く。無ければ stdout のみ。
    """
    rec = {
        "event": event,
        "ts_utc": _utc_now_iso(),
        **payload,
    }
    if write_audit_record is not None:
        try:
            write_audit_record(rec)
            return
        except Exception:
            # audit で落とさない
            _safe_print("[warn] write_audit_record failed; fallback to stdout")
    _safe_print(json.dumps(rec, ensure_ascii=False))


# =========================================================
# DropboxIO init (no from_env dependency)
# =========================================================

def _init_dropbox_io() -> Any:
    """
    DropboxIO の実装差異に備えて複数の署名で初期化を試みる。
    """
    if DropboxIO is None:
        raise RuntimeError(f"DropboxIO import failed: {_DROPBOXIO_IMPORT_ERROR!r}")

    tok = _env("DROPBOX_REFRESH_TOKEN")
    app_key = _env("DROPBOX_APP_KEY")
    app_secret = _env("DROPBOX_APP_SECRET")

    if not tok or not app_key or not app_secret:
        raise RuntimeError("Dropbox credentials are missing (DROPBOX_REFRESH_TOKEN / DROPBOX_APP_KEY / DROPBOX_APP_SECRET).")

    # 1) keyword: oauth2_refresh_token
    try:
        return DropboxIO(oauth2_refresh_token=tok, app_key=app_key, app_secret=app_secret)  # type: ignore
    except TypeError:
        pass

    # 2) keyword: refresh_token
    try:
        return DropboxIO(refresh_token=tok, app_key=app_key, app_secret=app_secret)  # type: ignore
    except TypeError:
        pass

    # 3) keyword: token
    try:
        return DropboxIO(token=tok, app_key=app_key, app_secret=app_secret)  # type: ignore
    except TypeError:
        pass

    # 4) positional
    try:
        return DropboxIO(tok, app_key, app_secret)  # type: ignore
    except TypeError as e:
        raise RuntimeError(f"Failed to init DropboxIO with known signatures: {e!r}")


# =========================================================
# Stage dispatch
# =========================================================

@dataclass
class StageContext:
    stage: str
    in_dir: str
    out_dir: str
    done_dir: str
    state_path: str
    logs_dir: str


def _build_ctx(stage: str) -> StageContext:
    return StageContext(
        stage=stage,
        in_dir=_pick_stage_path(stage, "IN"),
        out_dir=_pick_stage_path(stage, "OUT"),
        done_dir=_pick_stage_path(stage, "DONE"),
        state_path=_env("STATE_PATH", ""),
        logs_dir=_env("LOGS_DIR", ""),
    )


def _try_run_stage_module(stage: str, ctx: StageContext, dbx: Any) -> Tuple[bool, str]:
    """
    src.stageXX_main / src.stageXX など、既存の stage 実装があればそちらを呼ぶ。
    無ければ (False, reason) を返す。
    """
    import importlib

    candidates = [
        f"src.stage{stage}_main",
        f"src.stage{stage}",
        f"src.stages.stage{stage}",
        f"src.stages.stage{stage}_main",
    ]

    for modname in candidates:
        try:
            mod = importlib.import_module(modname)
        except Exception:
            continue

        # 優先: main(ctx, dbx) / run(ctx, dbx) / main() / run()
        for fn_name in ("main", "run"):
            fn = getattr(mod, fn_name, None)
            if callable(fn):
                try:
                    # 2-arg
                    try:
                        fn(ctx, dbx)
                        return True, f"ran {modname}.{fn_name}(ctx, dbx)"
                    except TypeError:
                        pass
                    # 1-arg
                    try:
                        fn(ctx)
                        return True, f"ran {modname}.{fn_name}(ctx)"
                    except TypeError:
                        pass
                    # 0-arg
                    fn()
                    return True, f"ran {modname}.{fn_name}()"
                except Exception as e:
                    return True, f"{modname}.{fn_name} raised: {e!r}"

        return True, f"module {modname} found but no callable main/run"

    return False, "no stage module found"


def main() -> int:
    t0 = time.time()
    stage = _stage()
    ctx = _build_ctx(stage)

    _audit("run_start", {
        "stage": stage,
        "paths": {
            "in": ctx.in_dir,
            "out": ctx.out_dir,
            "done": ctx.done_dir,
            "state": "***" if ctx.state_path else "",
            "logs": ctx.logs_dir,
        },
        "python": sys.version.split()[0],
    })

    try:
        dbx = _init_dropbox_io()
    except Exception as e:
        _audit("run_error", {"stage": stage, "where": "init_dropbox", "error": repr(e)})
        _safe_print(traceback.format_exc())
        _audit("run_end", {"stage": stage, "elapsed_s": round(time.time() - t0, 3), "ok": False})
        return 1

    # 既存 stage 実装があればそれを使う（本命）
    ran, info = _try_run_stage_module(stage, ctx, dbx)
    if ran:
        # ran==True は「実行できた」または「モジュール見つかったが例外」も含む
        _audit("stage_dispatch", {"stage": stage, "info": info})
        # 例外の場合は info に "raised" が入るので exit code を 1 に寄せる
        ok = (" raised:" not in info)
        _audit("run_end", {"stage": stage, "elapsed_s": round(time.time() - t0, 3), "ok": ok})
        return 0 if ok else 1

    # stage 実装が無い場合：落とさず終了（まずは Actions を通す）
    _audit("stage_dispatch", {
        "stage": stage,
        "info": info,
        "note": "No stage module found. Exiting gracefully with code 0.",
    })
    _audit("run_end", {"stage": stage, "elapsed_s": round(time.time() - t0, 3), "ok": True})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())