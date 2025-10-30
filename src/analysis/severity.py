# -*- coding: utf-8 -*-
from __future__ import annotations
import numpy as np
import pandas as pd
from pathlib import Path
from src.utils.config_loader import DATA_PROCESSED

EPS = 1e-9

def _safe_max(x: pd.Series) -> float:
    v = pd.to_numeric(x, errors="coerce").abs().max()
    return float(v) if np.isfinite(v) and v > 0 else 1.0

def attach_severity(in_path: Path | None = None, out_path: Path | None = None) -> Path:
    """
    读取 alerts.csv 或 alerts_all.csv，计算严重度分数与等级，写回 CSV。
    评分要素（都归一化到 0~1 后加权）：
      - 深度：season 用 -min_dev，slope 用 -min_drop
      - 持续：duration_days
      - 面积：deficit（∑|dev|）
      - 干段占比：dry_days_share（越干越严重）
      - 云量占比：high_cloud_share（越多越不可靠 -> 扣分）
    """
    if in_path is None:
        in_path = DATA_PROCESSED / ("alerts_all.csv" if (DATA_PROCESSED / "alerts_all.csv").exists()
                                    else "alerts.csv")
    df = pd.read_csv(in_path)
    if df.empty:
        # 空文件也照样回写
        if out_path is None: out_path = in_path
        df.to_csv(out_path, index=False)
        return out_path

    # 统一“深度”
    depth = pd.Series(dtype=float, index=df.index)
    if "min_dev" in df.columns:
        depth = depth.fillna(-pd.to_numeric(df["min_dev"], errors="coerce"))
    if "min_drop" in df.columns:
        depth = depth.fillna(-pd.to_numeric(df["min_drop"], errors="coerce"))
    depth = depth.fillna(0.0).clip(lower=0)

    dur = pd.to_numeric(df.get("duration_days", 0), errors="coerce").fillna(0)
    area = pd.to_numeric(df.get("deficit", 0), errors="coerce").fillna(0).clip(lower=0)
    dry  = pd.to_numeric(df.get("dry_days_share", 0), errors="coerce").fillna(0).clip(0, 1)
    cld  = pd.to_numeric(df.get("high_cloud_share", 0), errors="coerce").fillna(0).clip(0, 1)

    # 归一化（自适应到当前文件的尺度）
    depth_n = depth / (_safe_max(depth) + EPS)
    dur_n   = dur   / (_safe_max(dur) + EPS)
    area_n  = area  / (_safe_max(area) + EPS)
    dry_n   = dry   # 已是 0-1
    cld_n   = cld   # 已是 0-1

    # 加权：深度0.4 持续0.3 面积0.2 干旱0.1，云量作为扣分0.1
    score = (0.40*depth_n + 0.30*dur_n + 0.20*area_n + 0.10*dry_n - 0.10*cld_n)
    score = score.clip(0, 1)

    # 等级
    bins = [0.0, 0.4, 0.7, 1.0]
    names = ["minor", "moderate", "major"]
    level = pd.cut(score, bins=bins, labels=names, include_lowest=True)

    out = df.copy()
    out["severity_score"] = score.round(3)
    out["severity_level"] = level.astype(str)

    if out_path is None:
        out_path = in_path
    out.to_csv(out_path, index=False)
    return out_path
