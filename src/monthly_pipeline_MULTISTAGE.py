# -*- coding: utf-8 -*-
"""
monthly_pipeline_MULTISTAGE.py

方針:
- 1回のrunで1ステージだけ処理（one-stage-per-run）
- どのステージを処理するかは "IN が空でない最初のステージ" を自動選択
- 各ファイルについて audit JSONL を Dropbox(/_system/logs/) に必ず残す
- state.json を必ず保存（落ちても原因追跡可能）

注意:
- ここではまだ「編集/AI処理」は入れない（まず搬送 + 監査ログ + 安定性）
"""

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Dict, Any, List, Tuple, Optional

from src.dropbox_io import DropboxIO
from src.state_store import StateStore
from src.monthly_spec import MonthlyCfg


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _log_path(cfg: MonthlyCfg, run_id: str) -> str:
    # 1 run = 1 jsonl
    return f"{cfg.logs_dir.rstrip('/')}/monthly_audit__{run_id}.jsonl"


def _audit_append(dbx: DropboxIO, cfg: MonthlyCfg, run_id: str, rec: Dict[str, Any]) -> None:
    """
    JSONL を Dropbox 上の1ファイルに追記。
    Dropboxは追記APIがやや面倒なので、サイズが小さい前提で「read→append→overwrite」。
    監査ログが目的なので、失敗しても本体を止めない。
    """
    try:
        path = _log_path(cfg, run_id)
        line = json.dumps(rec, ensure_ascii=False) + "\n"
        try:
            prev = dbx.read_file_bytes(path)
            new = prev + line.encode("utf-8")
        except Exception:
            new = line.encode("utf-8")
        dbx.write_file_bytes(path, new, overwrite=True)
    except Exception:
        return


def _stage_paths(cfg: MonthlyCfg) -> Dict[str, Dict[str, str]]:
    return {
        "00": {"IN": cfg.stage00_in, "OUT": cfg.stage00_out, "DONE": cfg.stage00_done, "NEXT_IN": cfg.stage10_in},
        "10": {"IN": cfg.stage10_in, "OUT": cfg.stage10_out, "DONE": cfg.stage10_done, "NEXT_IN": cfg.stage20_in},
        "20": {"IN": cfg.stage20_in, "OUT": cfg.stage20_out, "DONE": cfg.stage20_done, "NEXT_IN": cfg.stage30_in},
        "30": {"IN": cfg.stage30_in, "OUT": cfg.stage30_out, "DONE": cfg.stage30_done, "NEXT_IN": cfg.stage40_in},
        "40": {"IN": cfg.stage40_in, "OUT": cfg.stage40_out, "DONE": cfg.stage40_done, "NEXT_IN": ""},  # last
    }


def _select_stage_one_run(dbx: DropboxIO, cfg: MonthlyCfg, run_id: str) -> Tuple[Optional[str], List[Dict[str, Any]], Dict[str, Dict[str, str]]]:
    sp = _stage_paths(cfg)

    # monthly_stage が指定されていればそこから優先（ただし空なら auto）
    order = ["00", "10", "20", "30", "40"]
    if cfg.monthly_stage and cfg.monthly_stage in order:
        # 指定ステージから順に見る（例: 00固定でもOK）
        start = order.index(cfg.monthly_stage)
        order = order[start:] + order[:start]

    for st in order:
        res = dbx.list_folder(sp[st]["IN"])
        entries = res.get("entries", [])
        files = [e for e in entries if e.get(".tag") == "file"]
        _audit_append(dbx, cfg, run_id, {
            "timestamp": _utc_now(),
            "run_id": run_id,
            "stage": st,
            "event": "list",
            "src_path": sp[st]["IN"],
            "count": len(files),
        })
        if files:
            return st, files, sp

    return None, [], sp


def run_multistage(dbx: DropboxIO, cfg: MonthlyCfg, run_id: str) -> int:
    store = StateStore.load(dbx, cfg.state_path)

    _audit_append(dbx, cfg, run_id, {
        "timestamp": _utc_now(),
        "run_id": run_id,
        "stage": "--",
        "event": "run_start",
        "message": "monthly pipeline start (one-stage-per-run; auto stage select)",
    })

    stage, files, sp = _select_stage_one_run(dbx, cfg, run_id)
    if not stage:
        _audit_append(dbx, cfg, run_id, {
            "timestamp": _utc_now(),
            "run_id": run_id,
            "stage": "--",
            "event": "run_end",
            "message": "no input files in any IN folder",
        })
        # state も保存しておく（監査上）
        store.save(dbx)
        return 0

    paths = sp[stage]
    processed = 0

    for f in files[: cfg.max_files_per_run]:
        name = f.get("name", "")
        src = f"{paths['IN'].rstrip('/')}/{name}"
        out = f"{paths['OUT'].rstrip('/')}/{name}"
        done = f"{paths['DONE'].rstrip('/')}/{name}"
        next_in = paths.get("NEXT_IN") or ""
        next_dst = f"{next_in.rstrip('/')}/{name}" if next_in else ""

        try:
            # IN -> OUT (copy)
            content = dbx.read_file_bytes(src)
            dbx.write_file_bytes(out, content, overwrite=True)
            _audit_append(dbx, cfg, run_id, {
                "timestamp": _utc_now(),
                "run_id": run_id,
                "stage": stage,
                "event": "write",
                "src_path": src,
                "dst_path": out,
                "filename": name,
                "size": len(content),
            })

            # IN -> DONE (move)
            dbx.move(src, done, overwrite=True)
            _audit_append(dbx, cfg, run_id, {
                "timestamp": _utc_now(),
                "run_id": run_id,
                "stage": stage,
                "event": "move",
                "src_path": src,
                "dst_path": done,
                "filename": name,
            })

            # forward to next stage IN (copy from DONE)
            if next_dst:
                content2 = dbx.read_file_bytes(done)
                dbx.write_file_bytes(next_dst, content2, overwrite=True)
                _audit_append(dbx, cfg, run_id, {
                    "timestamp": _utc_now(),
                    "run_id": run_id,
                    "stage": (f"{int(stage)+10:02d}" if stage != "40" else "40"),
                    "event": "write",
                    "src_path": done,
                    "dst_path": next_dst,
                    "filename": name,
                    "message": f"forward to stage{next_in.split('/')[1][:2]} IN" if "/" in next_in else "forward",
                    "size": len(content2),
                })

            # state 更新（最小）
            store.data.setdefault("processed", {})
            store.data["processed"].setdefault(stage, [])
            store.data["processed"][stage].append({"name": name, "ts": _utc_now()})

            processed += 1

        except Exception as e:
            _audit_append(dbx, cfg, run_id, {
                "timestamp": _utc_now(),
                "run_id": run_id,
                "stage": stage,
                "event": "error",
                "src_path": src,
                "message": f"{type(e).__name__}({e!r})",
            })
            # 1件失敗しても run 自体は続けず止める（デバッグ優先）
            break

    # state 保存（ここで malformed_path が出ないように dropbox_io/state_store を直した）
    try:
        store.save(dbx)
        _audit_append(dbx, cfg, run_id, {
            "timestamp": _utc_now(),
            "run_id": run_id,
            "stage": "--",
            "event": "write_state",
            "filename": cfg.state_path,
            "message": "state saved",
        })
    except Exception as e:
        _audit_append(dbx, cfg, run_id, {
            "timestamp": _utc_now(),
            "run_id": run_id,
            "stage": "--",
            "event": "error",
            "message": f"state_save_failed: {type(e).__name__}({e!r})",
        })
        return 1

    _audit_append(dbx, cfg, run_id, {
        "timestamp": _utc_now(),
        "run_id": run_id,
        "stage": "--",
        "event": "run_end",
        "message": "monthly pipeline end",
    })

    # 何か失敗して break していれば processed は 0/途中、でも state 保存できてれば rc=0 に寄せる
    return 0