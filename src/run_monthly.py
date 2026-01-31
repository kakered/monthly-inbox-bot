# -*- coding: utf-8 -*-
"""
run_monthly.py
将来 `python -m src.run_monthly` を入口にしたい場合の薄いラッパー。
"""

from __future__ import annotations

from .monthly_main import main

if __name__ == "__main__":
    raise SystemExit(main())