# -*- coding: utf-8 -*-
"""
Utility to merge weather and remote‑sensing indices.

This module defines a function ``merge_weather_ndvi`` that reads the raw
weather data (typically downloaded via Open‑Meteo) and the remote
monitoring indices exported from Google Earth Engine or other sources.  It
harmonises dates, optionally masks NDVI values with high cloud fraction,
retains additional indices when available (NDMI, NDRE, EVI, GNDVI, MSI),
and left‑joins on date to produce a unified daily table.  The resulting
CSV will be written to ``MERGED_CSV`` as defined in the configuration.

This version differs from the original implementation in that it no longer
assumes the presence of a ``cloud_frac`` column or NDVI percentiles; it
automatically adapts to whichever columns are present in the indices file.

Example usage (from ``scripts/build_merged.py``)::

    from src.transform.merge_data import merge_weather_ndvi
    merge_weather_ndvi(cloud_frac_max=0.6, interpolate_ndvi=True)

"""

from __future__ import annotations

from pathlib import Path
import pandas as pd

from src.utils.config_loader import (
    CFG,
    DATA_RAW,
    DATA_PROCESSED,
    WEATHER_CSV,
    INDICES_CSV,
    NDVI_CSV,
    MERGED_CSV,
)


def merge_weather_ndvi(
    cloud_frac_max: float = 0.6,
    interpolate_ndvi: bool = True,
    clip_ndvi: tuple[float, float] = (-0.2, 0.95),
) -> Path:
    """
    Merge daily weather data with remote‑sensing indices.

    This function reads the raw weather table (``WEATHER_CSV``) and the
    remote‑sensing indices table (``NDVI_CSV``).  Historically the second file
    contained NDVI only, but since v0.2.0 it may include additional indices
    such as NDMI, NDRE, EVI, GNDVI and MSI.  The merger performs the
    following steps:

    1.  Read the CSVs and normalise the ``date`` column to ``datetime.date``.
    2.  Apply cloud‑fraction filtering *only if* a ``cloud_frac`` column is
        present.  When absent (as in the new indices table) no filtering is
        applied.
    3.  Select a subset of useful columns.  In addition to the legacy
        ``ndvi_mean`` and its percentiles, any of ``ndmi_mean``,
        ``ndre_mean``, ``evi_mean``, ``gndvi_mean`` and ``msi_mean`` present in
        the file will be retained.  Missing columns are ignored.
    4.  Left‑join the weather table on date (weather is the primary key), sort
        by date, and set the ``date`` as the index.
    5.  Optionally interpolate ``ndvi_mean`` onto a daily grid and clip to a
        physical range.  Additional indices are *not* interpolated to
        preserve their original temporal resolution.
    6.  Derive a few simple rolling features (7‑day precipitation and mean
        temperature).
    7.  Write the merged table to ``MERGED_CSV`` and return its path.

    Parameters
    ----------
    cloud_frac_max : float, default 0.6
        Cloud‑fraction threshold above which NDVI values are invalidated.
        Ignored when the ``cloud_frac`` column is absent.
    interpolate_ndvi : bool, default True
        If ``True``, linearly interpolate ``ndvi_mean`` to a daily series.
    clip_ndvi : tuple(float, float), default (-0.2, 0.95)
        The lower and upper bounds used to clip the interpolated NDVI values.

    Returns
    -------
    pathlib.Path
        The path to the written ``merged.csv`` file.
    """

    # 1) Read the weather and remote‑sensing tables.  We support both
    # legacy NDVI-only files and newer multi-index files.  The underlying
    # config loader ensures that NDVI_CSV and INDICES_CSV point to the
    # appropriate file on disk.
    w = pd.read_csv(WEATHER_CSV)
    # Use INDICES_CSV for clarity (identical to NDVI_CSV)
    n = pd.read_csv(INDICES_CSV)

    # 2) 日期 → datetime（只取日期部分）
    w["date"] = pd.to_datetime(w["date"]).dt.date
    n["date"] = pd.to_datetime(n["date"]).dt.date

    # 3) NDVI quality control: only mask when a cloud_frac column exists.
    # This keeps backward compatibility with older NDVI exports while not
    # requiring this column in newer multi-index tables.
    if "cloud_frac" in n.columns:
        cols_to_mask = [
            c
            for c in (
                "ndvi_mean",
                "ndvi_p10",
                "ndvi_p90",
            )
            if c in n.columns
        ]
        n.loc[n["cloud_frac"] > cloud_frac_max, cols_to_mask] = pd.NA

    # 4) Only retain the columns we care about.  We drop intermediate
    # Sentinel‑2 metadata (n_obs, window_start, etc.) because the new
    # indices export does not provide them.  If these columns exist they
    # will be kept; otherwise they are silently ignored.
    base_cols = [
        "date",
        "ndvi_mean",
        "ndvi_p10",
        "ndvi_p90",
    ]
    extra_indices = ["ndmi_mean", "ndre_mean", "evi_mean", "gndvi_mean", "msi_mean"]
    keep = base_cols + extra_indices
    n = n[[c for c in keep if c in n.columns]].copy()

    # 5) left join（天气为主轴）
    df = pd.merge(w, n, on="date", how="left")  # how="left" = 左连接（保留天气表全部日期）

    # 6) 索引化日期，排序
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").set_index("date")

    # 7) 可选：把 NDVI 插值到逐日
    if interpolate_ndvi and "ndvi_mean" in df.columns:
        # time/linear 插值，随后裁剪到物理范围
        ndvi_daily = df["ndvi_mean"].interpolate(method="time").ffill().bfill()
        lo, hi = clip_ndvi
        df["ndvi_mean_daily"] = ndvi_daily.clip(lo, hi)
    elif "ndvi_mean" in df.columns:
        df["ndvi_mean_daily"] = df["ndvi_mean"]

    # 8) 派生气象特征：滚动降水、均温、相对湿度
    # 7 日降水累计
    if "precipitation_sum" in df.columns:
        df["precip_7d"] = df["precipitation_sum"].rolling(7, min_periods=1).sum()

    # 7 日平均气温
    if {"temperature_2m_max", "temperature_2m_min"} <= set(df.columns):
        df["tmean"] = (df["temperature_2m_max"] + df["temperature_2m_min"]) / 2.0
        df["tmean_7d"] = df["tmean"].rolling(7, min_periods=1).mean()

    # 7 日平均相对湿度（若存在）
    if "relative_humidity_2m_mean" in df.columns:
        df["rh_7d"] = df["relative_humidity_2m_mean"].rolling(7, min_periods=1).mean()

    # 9) 输出
    DATA_PROCESSED.mkdir(parents=True, exist_ok=True)
    df.to_csv(MERGED_CSV, index=True, encoding="utf-8", float_format="%.4f")
    return MERGED_CSV
