# -*- coding: utf-8 -*-
"""
monthly_main.py
Entry point for the monthly multi-stage pipeline.

Goals
- Run on GitHub Actions without requiring CLI args.
- Stage is selected by env var MONTHLY_STAGE (00/10/20/30/40).
- Find and dispatch to a stage module if present.
- Keep robust audit logging even if a stage fails.
- Avoid tight coupling to the exact DropboxIO implementation (older/newer variants).

Conventions
- Stage module candidates (first match wins):
    1) env override: STAGE_MODULE_00 / STAGE_MODULE_10 ... (full import path)
    2) src.stage00, src.stage10, ...
    3) src.stages.stage00, ...
    4) src.monthly_stages.stage00, ...
    5) auto-discovery by scanning package directory for modules containing 'stage{NN}'
- Stage module interface (supported in this order):
    - main(**kwargs) -> int | None
    - run(**kwargs)  -> int | None
    - process(**kwargs) -> int | None

Each stage receives:
  dbx: DropboxIO (or compatible object)
  stage: '00'..'40'
  paths: {'in':..., 'out':..., 'done':..., 'state':..., 'logs':...}
  state: StateStore (if available; otherwise None)
  run_id: str (UTC timestamp)
  config: dict (misc controls)

If stage module is not found:
- Exit code 0 (graceful) but emits an audit record explaining what was searched.
"""

from __future__ import annotations

import importlib
import json
import os
import pkgutil
import sys
import time
import traceback
from dataclasses import dataclass
from types import ModuleType
from typing import Any, Callable, Optional

# Optional (but recommended) local helpers
try:
    from .state_store import StateStore  # type: ignore
except Exception:  # pragma: no cover
    StateStore = None  # type: ignore

try:
    from .dropbox_io import DropboxIO  # type: ignore
except Exception:  # pragma: no cover
    DropboxIO = None  # type: ignore


# ----------------------------
# Utilities
# ----------------------------

def now_utc_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def run_id_utc() -> str:
    # 20260131T035605Z
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())


def env_str(key: str, default: str = "") -> str:
    v = os.environ.get(key, default)
    return (v or default).strip()


def stage_norm(stage: str) -> str:
    s = (stage or "").strip()
    if len(s) == 1:
        s = "0" + s
    return s


def pick_paths(stage: str) -> dict[str, str]:
    stage = stage_norm(stage)

    def pick(kind: str) -> str:
        return env_str(f"STAGE{stage}_{kind}", "")

    return {
        "in": pick("IN"),
        "out": pick("OUT"),
        "done": pick("DONE"),
        "state": env_str("STATE_PATH", ""),
        "logs": env_str("LOGS_DIR", ""),
    }


def safe_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), default=str)


# ----------------------------
# Audit sink (best-effort)
# ----------------------------

@dataclass
class Audit:
    events: list[dict[str, Any]]

    def log(self, **event: Any) -> None:
        self.events.append({"ts_utc": now_utc_iso(), **event})

    def to_jsonl(self) -> bytes:
        lines = [json.dumps(e, ensure_ascii=False) for e in self.events]
        return ("\n".join(lines) + "\n").encode("utf-8")


def _dbx_write_bytes(dbx: Any, path: str, body: bytes) -> None:
    """
    Try common write APIs across DropboxIO variants.
    """
    if not path:
        raise ValueError("empty path")

    # 1) our DropboxIO wrapper (newer)
    if hasattr(dbx, "write_file_bytes"):
        dbx.write_file_bytes(path, body, overwrite=True)  # type: ignore
        return

    # 2) alternative naming
    if hasattr(dbx, "upload_bytes"):
        dbx.upload_bytes(path, body, overwrite=True)  # type: ignore
        return

    # 3) raw SDK
    raw = getattr(dbx, "dbx", None) or getattr(dbx, "_dbx", None) or getattr(dbx, "client", None)
    if raw is None:
        raw = dbx
    if hasattr(raw, "files_upload"):
        import dropbox  # local import
        raw.files_upload(body, path, mode=dropbox.files.WriteMode.overwrite)  # type: ignore
        return

    raise AttributeError("No known write method on dbx")


def write_audit_record(dbx: Any, logs_dir: str, run_id: str, audit: Audit) -> Optional[str]:
    """
    Writes JSONL to:
      {LOGS_DIR}/{YYYYMMDD}/run_{run_id}.jsonl
    Returns written path or None.
    """
    if not logs_dir:
        return None
    day = run_id[:8]
    out_path = f"{logs_dir.rstrip('/')}/{day}/run_{run_id}.jsonl"
    try:
        _dbx_write_bytes(dbx, out_path, audit.to_jsonl())
        return out_path
    except Exception:
        return None


# ----------------------------
# DropboxIO factory (compat)
# ----------------------------

def make_dbx_from_env() -> Any:
    """
    Tries to build DropboxIO in a backward-compatible way.

    Order:
      1) DropboxIO.from_env() if exists
      2) DropboxIO(oauth2_refresh_token=..., app_key=..., app_secret=...)
      3) DropboxIO(refresh_token=..., app_key=..., app_secret=...)
      4) raw dropbox.Dropbox(...)
    """
    tok = env_str("DROPBOX_REFRESH_TOKEN", "")
    app_key = env_str("DROPBOX_APP_KEY", "")
    app_secret = env_str("DROPBOX_APP_SECRET", "")

    if not tok or not app_key or not app_secret:
        raise RuntimeError("Missing Dropbox credentials in env (DROPBOX_REFRESH_TOKEN / DROPBOX_APP_KEY / DROPBOX_APP_SECRET).")

    # 1) DropboxIO.from_env
    if DropboxIO is not None and hasattr(DropboxIO, "from_env"):
        try:
            return DropboxIO.from_env()  # type: ignore
        except Exception:
            pass

    # 2/3) constructor
    if DropboxIO is not None:
        for kwargs in (
            {"oauth2_refresh_token": tok, "app_key": app_key, "app_secret": app_secret},
            {"refresh_token": tok, "app_key": app_key, "app_secret": app_secret},
        ):
            try:
                return DropboxIO(**kwargs)  # type: ignore
            except Exception:
                continue

    # 4) raw SDK fallback
    import dropbox
    return dropbox.Dropbox(oauth2_refresh_token=tok, app_key=app_key, app_secret=app_secret)


# ----------------------------
# Stage module discovery
# ----------------------------

def _candidate_module_names(stage: str) -> list[str]:
    s = stage_norm(stage)
    env_override = env_str(f"STAGE_MODULE_{s}", "")
    names: list[str] = []
    if env_override:
        names.append(env_override)

    # common patterns
    names.extend([
        f"src.stage{s}",
        f"src.stages.stage{s}",
        f"src.monthly_stage{s}",
        f"src.monthly_stages.stage{s}",
        f"src.pipeline.stage{s}",
        f"src.pipeline_stages.stage{s}",
    ])
    return names


def _discover_by_scan(stage: str) -> list[str]:
    """
    Scan the directory where this file lives and a few common subpackages for modules
    containing 'stage{NN}'.
    """
    s = stage_norm(stage)
    needle = f"stage{s}"
    found: list[str] = []

    # Try scanning src package directory
    base_dir = os.path.dirname(__file__)
    # candidates: src/, src/stages/, src/monthly_stages/
    scan_dirs = [
        base_dir,
        os.path.join(base_dir, "stages"),
        os.path.join(base_dir, "monthly_stages"),
        os.path.join(base_dir, "pipeline_stages"),
    ]

    for d in scan_dirs:
        if not os.path.isdir(d):
            continue
        pkg_prefix = "src"
        if os.path.basename(d) != "src":
            pkg_prefix = f"src.{os.path.basename(d)}"
        for m in pkgutil.iter_modules([d]):
            if needle in m.name.lower():
                found.append(f"{pkg_prefix}.{m.name}")

    # de-dup preserve order
    out: list[str] = []
    for x in found:
        if x not in out:
            out.append(x)
    return out


def import_stage_module(stage: str) -> tuple[Optional[ModuleType], list[str], list[str]]:
    """
    Returns (module or None, tried_names, errors)
    """
    tried: list[str] = []
    errors: list[str] = []

    candidates = _candidate_module_names(stage) + _discover_by_scan(stage)
    # de-dup preserve order
    uniq: list[str] = []
    for n in candidates:
        if n and n not in uniq:
            uniq.append(n)

    for name in uniq:
        tried.append(name)
        try:
            mod = importlib.import_module(name)
            return mod, tried, errors
        except Exception as e:
            errors.append(f"{name}: {type(e).__name__}: {e}")

    return None, tried, errors


def resolve_stage_callable(mod: ModuleType) -> tuple[Optional[Callable[..., Any]], str]:
    for fn_name in ("main", "run", "process"):
        fn = getattr(mod, fn_name, None)
        if callable(fn):
            return fn, fn_name
    return None, ""


# ----------------------------
# Main
# ----------------------------

def main() -> int:
    stage = stage_norm(env_str("MONTHLY_STAGE", "00"))
    paths = pick_paths(stage)

    rid = run_id_utc()
    audit = Audit(events=[])

    audit.log(
        event="run_start",
        stage=stage,
        paths={k: ("***" if k == "state" and v else v) for k, v in paths.items()},
        python=sys.version.split()[0],
    )

    # init dropbox + state
    dbx: Any = None
    state: Any = None

    try:
        dbx = make_dbx_from_env()
    except Exception as e:
        audit.log(event="fatal", where="make_dbx_from_env", error=f"{type(e).__name__}: {e}")
        # cannot write audit to dropbox; fallback stdout
        print("[fatal] cannot init dropbox:", safe_json(audit.events[-1]), file=sys.stderr, flush=True)
        return 1

    # load state (optional)
    if StateStore is not None and paths.get("state"):
        try:
            state = StateStore.load(dbx, paths["state"])  # type: ignore
        except Exception as e:
            audit.log(event="warn", where="StateStore.load", error=f"{type(e).__name__}: {e}")
            state = None

    # dispatch stage module
    mod, tried, import_errors = import_stage_module(stage)
    if mod is None:
        audit.log(
            event="stage_dispatch",
            stage=stage,
            ok=False,
            info="no stage module found",
            tried=tried,
            errors=import_errors[:10],
            note="No stage module found. Exiting gracefully with code 0.",
        )
        # write audit (best-effort)
        outp = write_audit_record(dbx, paths.get("logs", ""), rid, audit)
        if outp is None:
            print("[warn] write_audit_record failed; fallback to stdout", flush=True)
            for e in audit.events:
                print(safe_json(e), flush=True)
        return 0

    fn, fn_name = resolve_stage_callable(mod)
    if fn is None:
        audit.log(
            event="stage_dispatch",
            stage=stage,
            ok=False,
            module=mod.__name__,
            note="Stage module has no callable main/run/process. Exiting with code 1.",
        )
        outp = write_audit_record(dbx, paths.get("logs", ""), rid, audit)
        if outp is None:
            print("[warn] write_audit_record failed; fallback to stdout", flush=True)
            for e in audit.events:
                print(safe_json(e), flush=True)
        return 1

    # build config payload
    config: dict[str, Any] = {
        "DEPTH": env_str("DEPTH", "medium"),
        "OPENAI_MODEL": env_str("OPENAI_MODEL", ""),
        "OPENAI_TIMEOUT": env_str("OPENAI_TIMEOUT", ""),
        "OPENAI_MAX_RETRIES": env_str("OPENAI_MAX_RETRIES", ""),
        "OPENAI_MAX_OUTPUT_TOKENS": env_str("OPENAI_MAX_OUTPUT_TOKENS", ""),
        "MAX_FILES_PER_RUN": env_str("MAX_FILES_PER_RUN", ""),
        "MAX_INPUT_CHARS": env_str("MAX_INPUT_CHARS", ""),
    }

    audit.log(
        event="stage_start",
        stage=stage,
        module=mod.__name__,
        entrypoint=fn_name,
        config={k: v for k, v in config.items() if v},
    )

    rc = 0
    t0 = time.time()
    try:
        ret = fn(dbx=dbx, stage=stage, paths=paths, state=state, run_id=rid, config=config)  # type: ignore[arg-type]
        if isinstance(ret, int):
            rc = ret
        else:
            rc = 0
        audit.log(event="stage_end", stage=stage, ok=(rc == 0), return_code=rc, elapsed_s=round(time.time() - t0, 3))
    except SystemExit as e:
        rc = int(e.code) if isinstance(e.code, int) else 1
        audit.log(
            event="stage_end",
            stage=stage,
            ok=(rc == 0),
            return_code=rc,
            elapsed_s=round(time.time() - t0, 3),
            system_exit=True,
        )
        raise
    except Exception as e:
        rc = 1
        audit.log(event="stage_error", stage=stage, ok=False, error=f"{type(e).__name__}: {e}", tb=traceback.format_exc(limit=50))
    finally:
        # flush state if the stage mutated it
        if state is not None and hasattr(state, "save"):
            try:
                state.save(dbx)  # type: ignore
                audit.log(event="state_saved", stage=stage, ok=True)
            except Exception as e:
                audit.log(event="state_saved", stage=stage, ok=False, error=f"{type(e).__name__}: {e}")

        outp = write_audit_record(dbx, paths.get("logs", ""), rid, audit)
        if outp is None:
            print("[warn] write_audit_record failed; fallback to stdout", flush=True)
            for e in audit.events:
                print(safe_json(e), flush=True)

    return rc


if __name__ == "__main__":
    raise SystemExit(main())