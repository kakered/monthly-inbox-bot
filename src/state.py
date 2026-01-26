# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List


@dataclass
class PipelineState:
    """
    Minimal persistent state to avoid re-processing the same inputs.
    """
    done: List[str] = field(default_factory=list)   # list of "keys" for processed sources
    meta: Dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> Dict:
        return {"done": list(self.done), "meta": dict(self.meta)}

    @staticmethod
    def from_dict(d: Dict | None) -> "PipelineState":
        if not isinstance(d, dict):
            return PipelineState()
        done = d.get("done")
        meta = d.get("meta")
        if not isinstance(done, list):
            done = []
        if not isinstance(meta, dict):
            meta = {}
        # ensure string lists
        done = [str(x) for x in done]
        meta = {str(k): str(v) for k, v in meta.items()}
        return PipelineState(done=done, meta=meta)