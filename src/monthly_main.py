# -*- coding: utf-8 -*-
"""
monthly_main.py

- GitHub Actions から `python -m src.monthly_main` で引数なし起動しても落ちない
- MONTHLY_STAGE (00/10/20/30/40) に応じて Dropbox の IN を処理する
- 現時点は「stage copy-forward」:
    INのExcelを OUTへコピー保存 → 元INを DONEへ移動 → 次ステージINへコピー（あれば）
- state.json に処理済みを記録して再処理を避ける
- logs_dir に jsonl を書く
"""

from __future__ import annotations

import os
import json
import time
import hashlib
from dataclasses import dataclass
from typing import Dict, Any, List, Optional, Tuple

import dropbox
from dropbox.exceptions import ApiError


def _env(name: str, default: str = "") -> str:
    return (os.environ.get(name, default) or "").strip()


def _utc_now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _utc_stamp() -> str:
    return time.strftime("%Y%m%d-%H%M%S", time.gmtime())


def _today_utc_yyyymmdd() -> str:
    return time.strftime("%Y%m%d", time.gmtime())


def _safe_int(name: str, default: int) -> int:
    v = _env(name, "")
    if not v:
        return default
    try:
        return int(v)
    except Exception:
        return default


def _is_excel(name: str) -> bool:
    n = name.lower()
    return n.endswith(".xlsx") or n.endswith(".xlsm") or n.endswith(".xls")


def _stage_vars(stage: str) -> Tuple[str, str, str]:
    return (
        _env(f"STAGE{stage}_IN"),
        _env(f"STAGE{stage}_OUT"),
        _env(f"STAGE{stage}_DONE"),
    )


def _next_stage(stage: str) -> str:
    order = ["00", "10", "20", "30", "40"]
    if stage not in order:
        return ""
    i = order.index(stage)
    return order[i + 1] if i + 1 < len(order) else ""


def _file_key(path: str, rev: str) -> str:
    return f"{path}@{rev}" if rev else path


def _sha256_12(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:12]


@dataclass
class DbxEntry:
    path: str
    name: str
    is_file: bool
    size: int = 0
    rev: str = ""


class DropboxIO:
    def __init__(self, *, refresh_token: str, app_key: str, app_secret: str):
        if not refresh_token:
            raise RuntimeError("DROPBOX_REFRESH_TOKEN is missing")
        if not app_key:
            raise RuntimeError("DROPBOX_APP_KEY is missing")
        if not app_secret:
            raise RuntimeError("DROPBOX_APP_SECRET is missing")

        self.dbx = dropbox.Dropbox(
            oauth2_refresh_token=refresh_token,
            app_key=app_key,
            app_secret=app_secret,
        )

    def ensure_folder(self, path: str) -> None:
        if not path:
            return
        if not path.startswith("/"):
            raise RuntimeError(f"Dropbox path must start with '/': {path!r}")

        try:
            self.dbx.files_get_metadata(path)
            return
        except ApiError:
            pass

        try:
            self.dbx.files_create_folder_v2(path)
        except ApiError as e:
            msg = str(e).lower()
            if "conflict" in msg:
                return
            raise

    def list_folder(self, path: str) -> List[DbxEntry]:
        if not path:
            return []
        try:
            res = self.dbx.files_list_folder(path)
        except ApiError as e:
            raise RuntimeError(f"Dropbox list_folder failed: {path!r} {e}") from e

        out: List[DbxEntry] = []
        for ent in res.entries:
            t = type(ent).__name__
            if t == "FileMetadata":
                out.append(
                    DbxEntry(
                        path=ent.path_display or "",
                        name=ent.name or "",
                        is_file=True,
                        size=getattr(ent, "size", 0) or 0,
                        rev=getattr(ent, "rev", "") or "",
                    )
                )
            elif t == "FolderMetadata":
                out.append(DbxEntry(path=ent.path_display or "", name=ent.name or "", is_file=False))
        return out

    def download(self, path: str) -> bytes:
        try:
            _, resp = self.dbx.files_download(path)
            return resp.content
        except ApiError as e:
            raise RuntimeError(f"Dropbox download failed: {path!r} {e}") from e

    def upload_overwrite(self, path: str, data: bytes) -> None:
        try:
            self.dbx.files_upload(data, path, mode=dropbox.files.WriteMode.overwrite, mute=True)
        except ApiError as e:
            raise RuntimeError(f"Dropbox upload failed: {path!r} {e}") from e

    def move_replace(self, src: str, dst: str) -> None:
        try:
            self.dbx.files_move_v2(src, dst, autorename=False, allow_shared_folder=True)
            return
        except ApiError:
            pass

        try:
            self.dbx.files_delete_v2(dst)
        except ApiError:
            pass

        try:
            self.dbx.files_move_v2(src, dst, autorename=False, allow_shared_folder=True)
        except ApiError as e:
            raise RuntimeError(f"Dropbox move failed: {src!r} -> {dst!r} {e}") from e

    def read_json(self, path: str) -> Optional[Dict[str, Any]]:
        try:
            data = self.download(path)
        except Exception:
            return None
        try:
            return json.loads(data.decode("utf-8"))
        except Exception:
            return None

    def write_json_overwrite(self, path: str, obj: Dict[str, Any]) -> None:
        raw = (json.dumps(obj, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
        self.upload_overwrite(path, raw)


class StateStore:
    def __init__(self, *, io: DropboxIO, state_path: str):
        self.io = io
        self.state_path = state_path

    def load(self) -> Dict[str, Any]:
        obj = self.io.read_json(self.state_path)
        if isinstance(obj, dict) and isinstance(obj.get("stages"), dict):
            return obj
        return {"updated_at_utc": _utc_now_iso(), "stages": {}}

    def save(self, state: Dict[str, Any]) -> None:
        state["updated_at_utc"] = _utc_now_iso()
        self.io.write_json_overwrite(self.state_path, state)

    def bucket(self, state: Dict[str, Any], stage: str) -> Dict[str, Any]:
        stages = state.setdefault("stages", {})
        b = stages.get(stage)
        if not isinstance(b, dict):
            b = {}
            stages[stage] = b
        if "done" not in b or not isinstance(b.get("done"), dict):
            b["done"] = {}
        return b

    def is_done(self, bucket: Dict[str, Any], key: str) -> bool:
        return bool(bucket.get("done", {}).get(key))

    def mark_done(self, bucket: Dict[str, Any], key: str) -> None:
        bucket.setdefault("done", {})
        bucket["done"][key] = True


class JsonlLogger:
    def __init__(self, *, io: DropboxIO, logs_dir: str):
        self.io = io
        self.logs_dir = logs_dir or "/_system/logs"
        self.run_folder = f"{self.logs_dir}/{_today_utc_yyyymmdd()}"
        self.run_file = f"{self.run_folder}/run_{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}.jsonl"
        self._lines: List[str] = []

    def log(self, obj: Dict[str, Any]) -> None:
        x = dict(obj)
        x.setdefault("ts_utc", _utc_now_iso())
        self._lines.append(json.dumps(x, ensure_ascii=False))

    def flush(self) -> None:
        self.io.ensure_folder(self.logs_dir)
        self.io.ensure_folder(self.run_folder)
        data = ("\n".join(self._lines) + "\n").encode("utf-8")
        self.io.upload_overwrite(self.run_file, data)


def stage_copy_forward(
    *,
    io: DropboxIO,
    store: StateStore,
    logger: JsonlLogger,
    stage: str,
    max_files: int,
) -> int:
    p_in, p_out, p_done = _stage_vars(stage)
    if not (p_in and p_out and p_done):
        raise RuntimeError(f"Stage{stage} paths missing: STAGE{stage}_IN/OUT/DONE")

    io.ensure_folder(p_in)
    io.ensure_folder(p_out)
    io.ensure_folder(p_done)

    state = store.load()
    bucket = store.bucket(state, stage)
    bucket["last_run_utc"] = _utc_now_iso()

    entries = [e for e in io.list_folder(p_in) if e.is_file and _is_excel(e.name)]
    entries = entries[:max_files]

    processed = 0
    for e in entries:
        key = _file_key(e.path, e.rev)
        if store.is_done(bucket, key):
            continue

        src_path = e.path
        base = e.name
        root, ext = os.path.splitext(base)

        data = io.download(src_path)

        out_name = f"{root}__stage{stage}__{_utc_stamp()}{ext}"
        out_path = f"{p_out}/{out_name}"
        io.upload_overwrite(out_path, data)

        rev = e.rev or "no-rev"
        done_name = f"{root}__rev-{rev}__{_utc_stamp()}{ext}"
        done_path = f"{p_done}/{done_name}"
        io.move_replace(src_path, done_path)

        nxt = _next_stage(stage)
        copied_to_next = False
        if nxt:
            nxt_in, _, _ = _stage_vars(nxt)
            if nxt_in:
                io.ensure_folder(nxt_in)
                nxt_path = f"{nxt_in}/{base}"
                io.upload_overwrite(nxt_path, data)
                copied_to_next = True

        store.mark_done(bucket, key)
        store.save(state)

        logger.log(
            {
                "event": "file_processed",
                "stage": stage,
                "src": src_path,
                "out": out_path,
                "done": done_path,
                "copied_to_next_in": copied_to_next,
                "size": len(data),
            }
        )
        processed += 1

    logger.log({"event": "stage_end", "stage": stage, "processed": processed, "in_count": len(entries)})
    return processed


def main() -> int:
    tok = _env("DROPBOX_REFRESH_TOKEN")
    app_key = _env("DROPBOX_APP_KEY")
    app_secret = _env("DROPBOX_APP_SECRET")

    stage = _env("MONTHLY_STAGE", "00")
    max_files = _safe_int("MAX_FILES_PER_RUN", 200)

    state_path = _env("STATE_PATH", "/_system/state.json")
    logs_dir = _env("LOGS_DIR", "/_system/logs")

    if stage not in {"00", "10", "20", "30", "40"}:
        raise RuntimeError("MONTHLY_STAGE must be one of 00/10/20/30/40")

    io = DropboxIO(refresh_token=tok, app_key=app_key, app_secret=app_secret)
    store = StateStore(io=io, state_path=state_path)
    logger = JsonlLogger(io=io, logs_dir=logs_dir)

    logger.log(
        {
            "event": "run_start",
            "stage": stage,
            "state_path_len": len(state_path),
            "state_path_sha256_12": _sha256_12(state_path) if state_path else "EMPTY",
            "logs_dir": logs_dir,
            "max_files": max_files,
            "has_openai_key": bool(_env("OPENAI_API_KEY")),
            "openai_model": _env("OPENAI_MODEL", ""),
            "depth": _env("DEPTH", ""),
        }
    )

    processed = stage_copy_forward(io=io, store=store, logger=logger, stage=stage, max_files=max_files)

    logger.log({"event": "run_end", "stage": stage, "processed": processed})
    logger.flush()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())