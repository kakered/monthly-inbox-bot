# -*- coding: utf-8 -*-
from __future__ import annotations
from dataclasses import dataclass


@dataclass
class MonthlyCfg:
    # Dropbox paths
    inbox_path: str
    prep_dir: str
    overview_dir: str
    outbox_dir: str

    # system
    state_path: str
    logs_dir: str
    mode: str = "multistage"

    # ---- compatibility aliases（重要）----
    @property
    def prep_out_dir(self) -> str:
        # stage2 が参照していた旧名
        return self.prep_dir

    @property
    def overview_out_dir(self) -> str:
        return self.overview_dir