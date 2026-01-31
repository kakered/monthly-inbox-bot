# -*- coding: utf-8 -*-
"""
monthly_main.py
- MONTHLY_STAGE を読み、対応 stage を import して run() を呼ぶ
- stage が無い場合は「成功終了(0)」にせず fail-fast で落とす（200回ループ防止）
- Dropbox logs_dir に jsonl で監査ログを書けるようにする（失敗しても stdout にフォールバック）
"""

from __future__ import annotations

import importlib
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import dropbox
from dropbox.exceptions import ApiError

from .state_store import StateStore


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def jst_date_yyyymmdd() -> str:
    # runner は UTC だが、フォルダは JST 揃えが分かりやすい前提（必要ならUTCに変えてOK）
    # ただしここはローカル時刻にせず、UTC+9 を明示して計算する
    import datetime as _dt

    tz = _dt.timezone(_dt.timedelta(hours=9))
    return _dt.datetime.now(tz).strftime("%Y%m%d")


def safe_env(key: str, default: str = "") -> str:
    v = os.environ.get(key, "")
    return v if v is not None and str(v).strip() != "" else default


@dataclass
class Paths:
    in_path: str
    out_path: str
    done_path: str
    state_path: str
    logs_dir: str


class AuditLogger:
    """
    Dropboxに jsonl を書く。失敗したら stdout にフォールバック。
    1行=1イベント。
    """
    def __init__(self, dbx: dropbox.Dropbox, logs_dir: str):
        self.dbx = dbx
        self.logs_dir = logs_dir or ""
        self.buf: List[str] = []
        self.log_path: Optional[str] = None

    def _ensure_log_path(self) -> Optional[str]:
        if not self.logs_dir:
            return None
        if self.log_path:
            return self.log_path

        day = jst_date_yyyymmdd()
        folder = f"{self.logs_dir.rstrip('/')}/{day}"
        # folder create (ignore if exists)
        try:
            self.dbx.files_create_folder_v2(folder)
        except Exception:
            pass

        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        self.log_path = f"{folder}/run_{run_id}.jsonl"
        return self.log_path

    def write(self, event: Dict[str, Any]) -> None:
        line = json.dumps(event, ensure_ascii=False)
        self.buf.append(line)

        # 小さくても都度 flush（「途中で死んでもログが残る」優先）
        self.flush()

    def flush(self) -> None:
        path = self._ensure_log_path()
        if not path:
            # logs_dir が無いなら stdout に出すだけ
            for line in self.buf:
                print(line, flush=True)
            self.buf = []
            return

        payload = ("\n".join(self.buf) + "\n").encode("utf-8")
        self.buf = []

        try:
            import dropbox as _dropbox
            # append したいが Dropbox は append API が弱いので「download+concat+overwrite」は避ける
            # 代わりに upload で "add" する（毎回別runファイルなのでOK）
            # ここは「ファイルが存在しない前提」なので overwrite でよい
            self.dbx.files_upload(payload, path, mode=_dropbox.files.WriteMode.overwrite)
        except Exception:
            # 最後の砦：stdout
            print("[warn] write_audit_record failed; fallback to stdout", file=sys.stderr, flush=True)
            print(payload.decode("utf-8", errors="replace"), flush=True)


def stage_paths(stage: str) -> Paths:
    def pick(kind: str) -> str:
        return safe_env(f"STAGE{stage}_{kind}")

    return Paths(
        in_path=pick("IN"),
        out_path=pick("OUT"),
        done_path=pick("DONE"),
        state_path=safe_env("STATE_PATH"),
        logs_dir=safe_env("LOGS_DIR"),
    )


def resolve_stage_module_candidates(stage: str) -> List[str]:
    # 互換候補を複数見に行く（あなたのログと一致）
    s = stage.zfill(2)
    return [
        f"src.stage{s}",
        f"src.stages.stage{s}",
        f"src.monthly_stage{s}",
        f"src.monthly_stages.stage{s}",
        f"src.pipeline.stage{s}",
        f"src.pipeline_stages.stage{s}",
    ]


def import_stage_module(stage: str) -> Any:
    errors = []
    for mod in resolve_stage_module_candidates(stage):
        try:
            return importlib.import_module(mod)
        except Exception as e:
            errors.append(f"{mod}: {type(e).__name__}: {e}")
    raise ModuleNotFoundError("no stage module found:\n" + "\n".join(errors))


def main() -> int:
    stage = safe_env("MONTHLY_STAGE", "00").strip()
    paths = stage_paths(stage)

    tok = os.environ["DROPBOX_REFRESH_TOKEN"]
    app_key = os.environ["DROPBOX_APP_KEY"]
    app_secret = os.environ["DROPBOX_APP_SECRET"]

    dbx = dropbox.Dropbox(oauth2_refresh_token=tok, app_key=app_key, app_secret=app_secret)
    audit = AuditLogger(dbx, paths.logs_dir)

    t0 = time.time()
    audit.write({
        "ts_utc": utc_now_iso(),
        "event": "run_start",
        "stage": stage,
        "paths": {
            "in": paths.in_path, "out": paths.out_path, "done": paths.done_path,
            "state": "***" if paths.state_path else "",
            "logs": paths.logs_dir
        },
        "python": sys.version.split()[0],
    })

    # state load (warn はログに残す)
    state = StateStore()
    if paths.state_path:
        try:
            state = StateStore.load(dbx, paths.state_path)
        except Exception as e:
            audit.write({
                "ts_utc": utc_now_iso(),
                "event": "warn",
                "where": "StateStore.load",
                "error": f"{type(e).__name__}: {e}",
            })
            state = StateStore()

    # stage import
    try:
        mod = import_stage_module(stage)
    except Exception as e:
        audit.write({
            "ts_utc": utc_now_iso(),
            "event": "stage_dispatch",
            "stage": stage,
            "ok": False,
            "info": "no stage module found",
            "error": f"{type(e).__name__}: {e}",
            "note": "Fail-fast: exiting with code 2.",
        })
        audit.write({
            "ts_utc": utc_now_iso(),
            "event": "run_end",
            "stage": stage,
            "elapsed_s": round(time.time() - t0, 3),
            "ok": False,
        })
        return 2

    # stage run
    config = {
        "DEPTH": safe_env("DEPTH", "medium"),
        "OPENAI_MODEL": safe_env("OPENAI_MODEL", "gpt-5-mini"),
        "OPENAI_TIMEOUT": safe_env("OPENAI_TIMEOUT", "120"),
        "OPENAI_MAX_RETRIES": safe_env("OPENAI_MAX_RETRIES", "2"),
        "OPENAI_MAX_OUTPUT_TOKENS": safe_env("OPENAI_MAX_OUTPUT_TOKENS", "5000"),
        "MAX_FILES_PER_RUN": safe_env("MAX_FILES_PER_RUN", "200"),
        "MAX_INPUT_CHARS": safe_env("MAX_INPUT_CHARS", "80000"),
    }

    audit.write({
        "ts_utc": utc_now_iso(),
        "event": "stage_start",
        "stage": stage,
        "module": getattr(mod, "__name__", ""),
        "entrypoint": "run",
        "config": config,
    })

    rc = 1
    try:
        # stage 側は柔軟に受けられるよう **kwargs で渡す
        rc = int(mod.run(
            dbx=dbx,
            paths=paths,
            state=state,
            audit=audit,
            config=config,
        ) or 0)
        ok = (rc == 0)
        audit.write({
            "ts_utc": utc_now_iso(),
            "event": "stage_end",
            "stage": stage,
            "ok": ok,
            "return_code": rc,
            "elapsed_s": round(time.time() - t0, 3),
        })

        # state save（必要なら stage 側で更新している前提）
        try:
            if paths.state_path:
                state.updated_at_utc = utc_now_iso()
                state.save(dbx, paths.state_path)
        except Exception as e:
            audit.write({
                "ts_utc": utc_now_iso(),
                "event": "warn",
                "where": "StateStore.save",
                "error": f"{type(e).__name__}: {e}",
            })

        audit.write({
            "ts_utc": utc_now_iso(),
            "event": "run_end",
            "stage": stage,
            "elapsed_s": round(time.time() - t0, 3),
            "ok": ok,
        })
        return rc
    except Exception as e:
        audit.write({
            "ts_utc": utc_now_iso(),
            "event": "stage_exception",
            "stage": stage,
            "error": f"{type(e).__name__}: {e}",
        })
        audit.write({
            "ts_utc": utc_now_iso(),
            "event": "run_end",
            "stage": stage,
            "elapsed_s": round(time.time() - t0, 3),
            "ok": False,
        })
        return 1


if __name__ == "__main__":
    raise SystemExit(main())