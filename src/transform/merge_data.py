# -*- coding: utf-8 -*-
"""
把 data/raw/weather.csv 与 data/raw/ndvi.csv 按 date 合并，输出 data/processed/merged.csv
流程：
1) 读入并统一日期类型
2) 过滤高云量 NDVI（cloud_frac > 阈值 -> 视为缺测）
3) 以天气日表为主轴 left join NDVI
4) 可选：按时间插值把 NDVI 补到每天
5) 派生几个常用滚动特征（示例）
"""
from __future__ import annotations
from pathlib import Path
import pandas as pd

from src.utils.config_loader import (
    CFG, DATA_RAW, DATA_PROCESSED, WEATHER_CSV, NDVI_CSV, MERGED_CSV
)

def merge_weather_ndvi(
    cloud_frac_max: float = 0.6,
    interpolate_ndvi: bool = True,
    clip_ndvi: tuple[float, float] = (-0.2, 0.95),
) -> Path:
    # 1) 读入
    w = pd.read_csv(WEATHER_CSV)
    n = pd.read_csv(NDVI_CSV)

    # 2) 日期 → datetime（只取日期部分）
    w["date"] = pd.to_datetime(w["date"]).dt.date
    n["date"] = pd.to_datetime(n["date"]).dt.date

    # 3) NDVI 质量控制：大云量认为无效
    if "cloud_frac" in n.columns:
        n.loc[n["cloud_frac"] > cloud_frac_max, ["ndvi_mean","ndvi_p10","ndvi_p90"]] = pd.NA

    # 4) 只保留我们需要的列
    keep = ["date","ndvi_mean","ndvi_p10","ndvi_p90","n_obs","cloud_frac",
            "window_start","window_end"]
    n = n[[c for c in keep if c in n.columns]].copy()

    # 5) left join（天气为主轴）
    df = pd.merge(w, n, on="date", how="left")  # how="left" = 左连接（保留天气表全部日期）

    # 6) 索引化日期，排序
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").set_index("date")

    # 7) 可选：把 NDVI 插值到逐日
    if interpolate_ndvi:
        # time/linear 插值，随后裁剪到物理范围
        ndvi_daily = df["ndvi_mean"].interpolate(method="time").ffill().bfill()
        lo, hi = clip_ndvi
        df["ndvi_mean_daily"] = ndvi_daily.clip(lo, hi)
    else:
        df["ndvi_mean_daily"] = df["ndvi_mean"]

    # 8) 示例派生特征：7 日降水累计、7 日均温
    if "precipitation_sum" in df.columns:
        df["precip_7d"] = df["precipitation_sum"].rolling(7, min_periods=1).sum()
    if {"temperature_2m_max","temperature_2m_min"} <= set(df.columns):
        df["tmean"] = (df["temperature_2m_max"] + df["temperature_2m_min"]) / 2.0
        df["tmean_7d"] = df["tmean"].rolling(7, min_periods=1).mean()

    # 9) 输出
    DATA_PROCESSED.mkdir(parents=True, exist_ok=True)
    df.to_csv(MERGED_CSV, index=True, encoding="utf-8", float_format="%.4f")
    return MERGED_CSV

if __name__ == "__main__":
    out = merge_weather_ndvi()
    print("写入：", out)
