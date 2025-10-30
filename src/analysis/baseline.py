# -*- coding: utf-8 -*-
"""
src/analysis/baseline.py
- 基于 merged.csv 构建 DOY 季节基线（分位带，环形平滑）
- 基于阈值与连续天数识别异常事件（含严重程度评分）
- 生成可视化图
输出：
  data/processed/ndvi_baseline.csv
  data/processed/alerts.csv
  assets/ndvi_baseline_alerts.png（由 plot_baseline_with_alerts 生成）
"""
from __future__ import annotations
from pathlib import Path
from typing import List, Tuple, Dict, Optional

import numpy as np
import pandas as pd

from src.utils.config_loader import MERGED_CSV, DATA_PROCESSED, ASSETS


# ---------- 工具函数 ----------
def _drop_leap_day(df: pd.DataFrame) -> pd.DataFrame:
    """去掉 2/29，避免 DOY 混乱"""
    return df[~((df.index.month == 2) & (df.index.day == 29))]


def _cyclic_rolling_mean(s: pd.Series, window: int = 15) -> pd.Series:
    """对 DOY 序列做环形平滑（首尾相接），避免年初/年末边缘效应"""
    if window <= 1:
        return s
    pad = window // 2
    s_ext = pd.concat([s.iloc[-pad:], s, s.iloc[:pad]])
    sm = s_ext.rolling(window=window, center=True, min_periods=1).mean()
    sm = sm.iloc[pad:-pad]
    sm.index = s.index
    return sm


def _find_runs(mask: pd.Series, min_run: int) -> List[Tuple[pd.Timestamp, pd.Timestamp]]:
    """从布尔序列中找 >=min_run 的连续 True 区段，返回[(start, end)]"""
    runs: List[Tuple[pd.Timestamp, pd.Timestamp]] = []
    if mask.empty:
        return runs
    mask = mask.copy()
    mask.index = pd.to_datetime(mask.index)
    mask = mask.sort_index()

    start = None
    prev = None
    for t, v in mask.items():
        if v and start is None:
            start = t
        # 断开：遇到 False 或 日期不连续
        if ((not v) or (prev is not None and (t - prev).days > 1)) and start is not None:
            end = prev
            if end is not None and (end - start).days + 1 >= min_run:
                runs.append((start, end))
            start = t if v else None
        prev = t
    # 收尾
    if start is not None and prev is not None:
        end = prev
        if (end - start).days + 1 >= min_run:
            runs.append((start, end))
    return runs


# ---------- 基线 ----------
def build_ndvi_baseline(window: int = 15, train_years: Optional[List[int]] = None) -> Path:
    """
    按 DOY 聚合 ndvi_mean_daily → p10/25/50/75/90，并进行环形平滑。
    train_years: 只使用这些年份的数据来训练基线；为 None 时使用全部年份。
    """
    df = pd.read_csv(MERGED_CSV, parse_dates=["date"]).set_index("date").sort_index()
    df = _drop_leap_day(df)
    if train_years is not None:
        df = df[df.index.year.isin(train_years)]
    nd = df["ndvi_mean_daily"].astype(float)

    # 生成 DOY 聚合
    doy = df.index.dayofyear
    g = nd.groupby(doy)

    base = pd.DataFrame({
        "doy": sorted(g.groups.keys()),
        "p10": g.quantile(0.10).values,
        "p25": g.quantile(0.25).values,
        "p50": g.quantile(0.50).values,
        "p75": g.quantile(0.75).values,
        "p90": g.quantile(0.90).values,
        "n": g.size().values
    }).set_index("doy").sort_index()

    # 环形平滑
    for c in ["p10", "p25", "p50", "p75", "p90"]:
        base[c] = _cyclic_rolling_mean(base[c], window=window)

    DATA_PROCESSED.mkdir(parents=True, exist_ok=True)
    out = DATA_PROCESSED / "ndvi_baseline.csv"
    base.to_csv(out, float_format="%.4f")
    return out


# ---------- 告警 ----------
def detect_alerts(
    dev_thresh: float = -0.08,    # 触发阈值：低于基线中位线多少视为异常
    min_run: int = 5,             # 至少连续天数
    precip7_max: float | None = None,  # 可选：仅在干段触发（如 5 mm）
    dry_thresh: float = 5.0,      # 干段定义（用于指标）
    target_years: Optional[List[int]] = None,  # 只在这些年份里触发/输出事件
) -> Path:
    """把低于基线阈值的连续区段压缩为事件，并给出严重程度分级。"""
    df = pd.read_csv(MERGED_CSV, parse_dates=["date"]).set_index("date").sort_index()
    df = _drop_leap_day(df)

    # 读基线并映射
    base = pd.read_csv(DATA_PROCESSED / "ndvi_baseline.csv", index_col="doy")
    df["doy"] = df.index.dayofyear
    for c in ["p10", "p25", "p50", "p75", "p90"]:
        df[f"base_{c}"] = df["doy"].map(base[c])

    # 偏离量
    df["dev"] = df["ndvi_mean_daily"] - df["base_p50"]

    # 目标年份过滤只影响触发与输出（基线仍用训练期）
    df_eval = df.copy()
    if target_years is not None:
        df_eval = df_eval[df_eval.index.year.isin(target_years)]

    # 触发条件
    cond = df_eval["ndvi_mean_daily"].notna() & (df_eval["dev"] <= dev_thresh)
    if precip7_max is not None and "precip_7d" in df_eval.columns:
        cond &= (df_eval["precip_7d"] <= float(precip7_max))

    runs = _find_runs(cond, min_run=min_run)

    # 分级辅助
    def _depth_level(abs_min_dev: float) -> int:
        if abs_min_dev < 0.06: return 0  # info
        if abs_min_dev < 0.08: return 1  # minor
        if abs_min_dev < 0.12: return 2  # moderate
        return 3                          # severe

    name_map = ["info", "minor", "moderate", "severe"]

    events = []
    for i, (s, e) in enumerate(runs, 1):
        seg = df.loc[s:e].copy()  # 用原 df（含所有列）取片段
        duration = len(seg)

        # 最严重的一天
        j = seg["dev"].idxmin()
        min_dev = float(seg.loc[j, "dev"])
        abs_min_dev = abs(min_dev)
        lower_band = float(max(seg.loc[j, "base_p50"] - seg.loc[j, "base_p10"], 0.02))  # 防极小除数
        depth_std = float(abs_min_dev / lower_band)

        # 面积（负偏离积分）
        deficit = float(seg["dev"].clip(upper=0).abs().sum())

        # 可靠性/干段
        raw_avail_share = float(seg["ndvi_mean"].notna().mean()) if "ndvi_mean" in seg.columns else np.nan
        high_cloud_share = float((seg.get("cloud_frac", pd.Series(index=seg.index)) > 0.6).mean()) \
            if "cloud_frac" in seg.columns else 0.0
        dry_days_share = float((seg.get("precip_7d", pd.Series(index=seg.index, data=np.inf)) <= dry_thresh).mean()) \
            if "precip_7d" in seg.columns else np.nan

        # 分级：深度为基，持续加权，可靠性折扣
        lvl = _depth_level(abs_min_dev)
        if duration >= 15: lvl += 2
        elif duration >= 10: lvl += 1
        if (high_cloud_share > 0.4) and (raw_avail_share < 0.5):  # 云多且原始点少 → 降级
            lvl -= 1
        lvl = int(max(0, min(3, lvl)))

        # 综合分值（0-1）
        depth_score = min(depth_std / 2.0, 1.0)
        area_score  = min(deficit / 1.5, 1.0)     # 约等于 0.1×15 天
        dur_score   = min(duration / 20.0, 1.0)
        severity_score = round(0.5*depth_score + 0.3*area_score + 0.2*dur_score, 3)

        events.append({
            "event_id": i,
            "start_date": s.date().isoformat(),
            "end_date": e.date().isoformat(),
            "duration_days": duration,
            "min_date": j.date().isoformat(),
            "min_dev": min_dev,
            "depth_std": depth_std,
            "deficit": deficit,
            "raw_avail_share": raw_avail_share,
            "high_cloud_share": high_cloud_share,
            "dry_days_share": dry_days_share,
            "severity_level": lvl,
            "severity_name": name_map[lvl],
            "severity_score": severity_score,
            "ndvi_at_min": float(seg.loc[j, "ndvi_mean_daily"]),
            "base50_at_min": float(seg.loc[j, "base_p50"]),
            "precip7_mean": float(seg.get("precip_7d", pd.Series(dtype=float)).mean())
        })

    # 即使没有事件也写空表（含表头），避免下游崩
    cols = ["event_id","start_date","end_date","duration_days","min_date","min_dev",
            "depth_std","deficit","raw_avail_share","high_cloud_share","dry_days_share",
            "severity_level","severity_name","severity_score","ndvi_at_min","base50_at_min","precip7_mean"]
    out = DATA_PROCESSED / "alerts.csv"
    pd.DataFrame(events, columns=cols).to_csv(out, index=False, float_format="%.4f")
    return out


# ---------- 包装器 ----------
def baseline_and_alerts(
    smooth_window: int = 15,
    dev_thresh: float = -0.08,
    min_run: int = 5,
    precip7_max: float | None = None,
    train_years: Optional[List[int]] = None,
    target_years: Optional[List[int]] = None,
) -> Dict[str, Path]:
    """先算基线（可限定训练年份），再在目标年份上判告警。"""
    b = build_ndvi_baseline(window=smooth_window, train_years=train_years)
    a = detect_alerts(dev_thresh=dev_thresh, min_run=min_run,
                      precip7_max=precip7_max, target_years=target_years)
    return {"baseline_csv": b, "alerts_csv": a}


# ---------- 画图 ----------
def plot_baseline_with_alerts(target_years: Optional[List[int]] = None) -> Path:
    """画“分位带 + 曲线 + 告警区段”，可只对 target_years 着色。"""
    import matplotlib.pyplot as plt

    ASSETS.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(MERGED_CSV, parse_dates=["date"]).set_index("date").sort_index()
    df = _drop_leap_day(df)

    base = pd.read_csv(DATA_PROCESSED / "ndvi_baseline.csv", index_col="doy")
    df["doy"] = df.index.dayofyear
    for c in ["p10", "p25", "p50", "p75", "p90"]:
        df[f"base_{c}"] = df["doy"].map(base[c])

    fig, ax = plt.subplots(figsize=(12, 5))
    ax.fill_between(df.index, df["base_p10"], df["base_p90"], alpha=0.15, label="P10–P90")
    ax.fill_between(df.index, df["base_p25"], df["base_p75"], alpha=0.25, label="P25–P75")
    ax.plot(df.index, df["base_p50"], lw=1.2, label="Baseline P50")
    ax.plot(df.index, df["ndvi_mean_daily"], lw=1.5, color="#ff7f0e", label="NDVI (daily)")
    ax.set_ylabel("NDVI"); ax.set_title("NDVI Baseline & Alerts")

    alerts_path = DATA_PROCESSED / "alerts.csv"
    if alerts_path.exists():
        al = pd.read_csv(alerts_path, parse_dates=["start_date", "end_date"])
        if target_years is not None and not al.empty:
            al = al[al["start_date"].dt.year.isin(target_years)]
        for _, r in al.iterrows():
            ax.axvspan(r["start_date"], r["end_date"], color="red", alpha=0.12)
            xm = r["start_date"] + (r["end_date"] - r["start_date"]) / 2
            ax.text(xm, ax.get_ylim()[1]-0.01, f"#{int(r['event_id'])}",
                    ha="center", va="top", fontsize=8, color="red")

    ax.legend(loc="upper left")
    fig.tight_layout()
    out = ASSETS / "ndvi_baseline_alerts.png"
    fig.savefig(out, dpi=150)
    return out
# === 新增：斜率型告警（10 天跌幅 > 0.08）==============================
def detect_slope_alerts(
    drop_days: int = 10,
    drop_thresh: float = -0.08,         # 10 天跌幅阈值
    min_run: int = 3,                   # 连续满足天数
    target_years: list[int] | None = None,
    harvest_windows: list[tuple[int,int]] | None = None  # 可选：收割窗口屏蔽，如 [(230,260)]
) -> Path:
    df = pd.read_csv(MERGED_CSV, parse_dates=["date"]).set_index("date").sort_index()
    if target_years is not None:
        df = df[df.index.year.isin(target_years)]
    nd = df["ndvi_mean_daily"].astype(float)
    drop = nd - nd.shift(drop_days)     # 10日差
    df["drop"] = drop

    cond = drop <= drop_thresh
    # 可选：收割窗口不触发
    if harvest_windows:
        doy = df.index.dayofyear
        mask_harv = False
        for a,b in harvest_windows:
            mask_harv |= (doy>=a) & (doy<=b)
        cond &= ~mask_harv

    runs = _find_runs(cond.fillna(False), min_run=min_run)
    rows = []
    for i,(s,e) in enumerate(runs,1):
        seg = df.loc[s:e]
        j = seg["drop"].idxmin()
        rows.append({
            "event_id": i,
            "type": "slope",
            "start_date": s.date().isoformat(),
            "end_date": e.date().isoformat(),
            "duration_days": len(seg),
            "min_date": j.date().isoformat(),
            "drop_days": drop_days,
            "min_drop": float(seg.loc[j,"drop"]),
        })
    out = DATA_PROCESSED / "alerts_slope.csv"
    pd.DataFrame(rows).to_csv(out, index=False, float_format="%.4f")
    return out

# === 新增：合并两类告警为一个清单 ==============================
def union_alerts() -> Path:
    """合并季节偏离 alerts.csv 与斜率型 alerts_slope.csv 到 alerts_all.csv"""
    p_a = DATA_PROCESSED / "alerts.csv"
    p_s = DATA_PROCESSED / "alerts_slope.csv"
    da = pd.read_csv(p_a) if p_a.exists() else pd.DataFrame()
    ds = pd.read_csv(p_s) if p_s.exists() else pd.DataFrame()
    if not da.empty:
        da.insert(1, "type", "season")  # 标注类型
    cols = sorted(set(da.columns).union(ds.columns))
    out = pd.concat([da.reindex(columns=cols), ds.reindex(columns=cols)], ignore_index=True)
    out_path = DATA_PROCESSED / "alerts_all.csv"
    out.to_csv(out_path, index=False)
    return out_path
def export_daily_flags(dev_thresh=-0.05, target_years=None):
    df = pd.read_csv(MERGED_CSV, parse_dates=["date"]).set_index("date").sort_index()
    base = pd.read_csv(DATA_PROCESSED/"ndvi_baseline.csv", index_col="doy")
    if target_years is not None:
        df = df[df.index.year.isin(target_years)]
    df["doy"] = df.index.dayofyear
    df["base_p50"] = df["doy"].map(base["p50"])
    df["dev"] = df["ndvi_mean_daily"] - df["base_p50"]
    df["is_anom"] = (df["dev"] <= dev_thresh).astype(int)
    out = DATA_PROCESSED / "alerts_daily.csv"
    df[["is_anom","dev","ndvi_mean_daily","base_p50","precip_7d"]].to_csv(out, float_format="%.4f")
    return out
