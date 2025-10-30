# -*- coding: utf-8 -*-
"""
scripts/qa_plots.py
- 读取 data/processed/merged.csv
- 画两张质检图：时间序列 & 散点
- 打印关键质量指标
"""
# --- 把项目根加入 sys.path（手动法） ---
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
# ---------------------------------------

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from src.utils.config_loader import MERGED_CSV, ASSETS

ASSETS.mkdir(parents=True, exist_ok=True)

# 读取
df = pd.read_csv(MERGED_CSV, parse_dates=["date"]).set_index("date").sort_index()

# 质量掩膜（与合并时一致）：低云 & 有观测
obs_mask = (df.get("cloud_frac", 1) <= 0.6) & df["ndvi_mean"].notna()

# ---------- 图 1：时间序列质检 ----------
fig, (ax0, ax1) = plt.subplots(2, 1, figsize=(12, 6), sharex=True)

# 降水累计（7d）
ax0.bar(df.index, df.get("precip_7d", pd.Series(index=df.index, dtype=float)), width=1.0)
ax0.set_ylabel("Precip 7d (mm)")
ax0.set_title("Weather × NDVI QC")

# NDVI（实线）+ 原始观测点（散点）
line_ndvi, = ax1.plot(df.index, df["ndvi_mean_daily"], lw=1.5, label="NDVI (daily)")
pts = ax1.scatter(df.index[obs_mask], df.loc[obs_mask, "ndvi_mean"],
                  s=12, alpha=0.7, label="NDVI window-center")

# 右轴：温度（虚线）——并将左右轴图例合并
handles = [line_ndvi, pts]
labels = ["NDVI (daily)", "NDVI window-center"]
if "tmean_7d" in df.columns:
    ax2 = ax1.twinx()
    line_t, = ax2.plot(df.index, df["tmean_7d"], lw=1.0, linestyle="--", label="Tmean 7d")
    ax2.set_ylabel("Tmean 7d (°C)")
    handles.append(line_t); labels.append("Tmean 7d")

ax1.set_ylabel("NDVI")
ax1.legend(handles, labels, loc="upper left")

# （可选）给高云窗口打底色，阅读更直观：取消下面三行注释即可
# if {"cloud_frac", "window_start", "window_end"} <= set(df.columns):
#     bad = (df["cloud_frac"] > 0.6) & df["window_start"].notna()
#     for ws, we in df.loc[bad, ["window_start", "window_end"]].dropna().itertuples(index=False):
#         ax1.axvspan(pd.to_datetime(ws), pd.to_datetime(we), alpha=0.08)

fig.tight_layout()
p1 = ASSETS / "qc_timeseries.png"
fig.savefig(p1, dpi=150)

# ---------- 图 2：散点（快速相关感） ----------
fig2, ax = plt.subplots(figsize=(6, 5))
x = df.get("tmean_7d")
y = df["ndvi_mean_daily"]
m = x.notna() & y.notna()
ax.scatter(x[m], y[m], s=10, alpha=0.5)
ax.set_xlabel("Tmean 7d (°C)")
ax.set_ylabel("NDVI (daily)")
ax.set_title("NDVI vs 7-day mean temperature")
fig2.tight_layout()
p2 = ASSETS / "qc_scatter.png"
fig2.savefig(p2, dpi=150)

# ---------- 质检摘要 ----------
total_days = len(df)
have_raw_ndvi = df["ndvi_mean"].notna().sum()
high_cloud = (df.get("cloud_frac", 0) > 0.6).sum()
corr = np.corrcoef(x[m], y[m])[0, 1] if m.sum() > 5 else np.nan

print("\n=== QC 摘要 ===")
print(f"总天数：{total_days}")
print(f"原始NDVI可用天数（窗口中点）：{have_raw_ndvi}")
print(f"高云窗口天数（cloud_frac>0.6）：{high_cloud}")
print(f"NDVI(daily) 与 Tmean_7d 相关系数：{corr:.3f}")
print("导出图像：")
print(" -", p1)
print(" -", p2)
