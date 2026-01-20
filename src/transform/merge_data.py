"""
Utility to merge weather and remote-sensing indices.

This module defines a function ``merge_weather_ndvi`` that reads the raw
weather data (typically downloaded via Open-Meteo) and the remote
monitoring indices exported from Google Earth Engine or other sources. It
harmonises dates, optionally masks NDVI values with high cloud fraction,
retains additional indices when available (NDMI, NDRE, EVI, GNDVI, MSI),
and left-joins on date to produce a unified daily table. The resulting
CSV will be written to ``MERGED_CSV`` as defined in the configuration.

This version differs from the original implementation in that it no longer
assumes the presence of a ``cloud_frac`` column or NDVI percentiles; it
automatically adapts to whichever columns are present in the indices file.
It also emits explicit observation vs fill columns (``*_obs`` / ``*_fill``)
so readers can distinguish real observations from daily filled values.

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

INDEX_NAMES = ["ndvi", "ndmi", "ndre", "evi", "gndvi", "msi"]


def merge_weather_ndvi(
    cloud_frac_max: float = 0.6,
    interpolate_ndvi: bool = True,
    clip_ndvi: tuple[float, float] = (-0.2, 0.95),
) -> Path:
    """
    Merge daily weather data with remote-sensing indices.

    This function reads the raw weather table (``WEATHER_CSV``) and the
    remote-sensing indices table (``NDVI_CSV``). Historically the second file
    contained NDVI only, but since v0.2.0 it may include additional indices
    such as NDMI, NDRE, EVI, GNDVI and MSI. The merger performs the
    following steps:

    1. Read the CSVs and normalise the ``date`` column to ``datetime.date``.
    2. Apply cloud-fraction filtering *only if* a ``cloud_frac`` column is
       present. When absent (as in the new indices table) no filtering is
       applied.
    3. Select a subset of useful columns. In addition to the legacy
       ``ndvi_mean`` and its percentiles, any of ``ndmi_mean``, ``ndre_mean``,
       ``evi_mean``, ``gndvi_mean`` and ``msi_mean`` present in the file will
       be retained. Missing columns are ignored.
    4. Left-join the weather table on date (weather is the primary key), sort
       by date, and set the ``date`` as the index.
    5. Optionally interpolate remote-sensing indices onto a daily grid and
       clip NDVI to a physical range. The interpolated series are stored in
       ``*_fill`` columns (and also mirrored to ``ndvi_mean_daily`` for
       backward compatibility). Raw observation values remain in
       ``*_mean`` and ``*_obs`` columns.
    6. Derive rolling features (7-day precipitation, mean temperature, RH).
    7. Write the merged table to ``MERGED_CSV`` and return its path.

    Parameters
    ----------
    cloud_frac_max : float, default 0.6
        Cloud-fraction threshold above which NDVI values are invalidated.
        Ignored when the ``cloud_frac`` column is absent.
    interpolate_ndvi : bool, default True
        If ``True``, linearly interpolate remote-sensing indices to a daily
        series. NDVI is clipped to ``clip_ndvi``.
    clip_ndvi : tuple(float, float), default (-0.2, 0.95)
        The lower and upper bounds used to clip the interpolated NDVI values.

    Returns
    -------
    pathlib.Path
        The path to the written ``01_merged.csv`` file.
    """

    w = pd.read_csv(WEATHER_CSV)
    n = pd.read_csv(INDICES_CSV)

    w["date"] = pd.to_datetime(w["date"]).dt.date
    n["date"] = pd.to_datetime(n["date"]).dt.date

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

    base_cols = [
        "date",
        "ndvi_mean",
        "ndvi_p10",
        "ndvi_p90",
    ]
    extra_indices = ["ndmi_mean", "ndre_mean", "evi_mean", "gndvi_mean", "msi_mean"]
    keep = base_cols + extra_indices
    n = n[[c for c in keep if c in n.columns]].copy()

    df = pd.merge(w, n, on="date", how="left")

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").set_index("date")

    for name in INDEX_NAMES:
        mean_col = f"{name}_mean"
        if mean_col in df.columns:
            df[f"{name}_obs"] = df[mean_col]

    if interpolate_ndvi:
        for name in INDEX_NAMES:
            obs_col = f"{name}_obs"
            if obs_col not in df.columns:
                continue
            fill = df[obs_col].interpolate(method="time").ffill().bfill()
            if name == "ndvi":
                lo, hi = clip_ndvi
                fill = fill.clip(lo, hi)
            df[f"{name}_fill"] = fill
    else:
        for name in INDEX_NAMES:
            obs_col = f"{name}_obs"
            if obs_col in df.columns:
                df[f"{name}_fill"] = df[obs_col]

    if "ndvi_fill" in df.columns:
        df["ndvi_mean_daily"] = df["ndvi_fill"]

    obs_cols = [f"{name}_obs" for name in INDEX_NAMES if f"{name}_obs" in df.columns]
    if obs_cols:
        obs_flag = df[obs_cols].notna().any(axis=1)
    else:
        obs_flag = pd.Series(False, index=df.index)
    df["obs_or_fill"] = obs_flag
    dates = df.index.to_numpy()
    last_obs_series = pd.Series(dates).where(obs_flag.to_numpy()).ffill()
    df["last_rs_date"] = pd.to_datetime(last_obs_series).dt.date.to_numpy()
    rs_age = (pd.Series(dates) - last_obs_series).dt.days
    df["rs_age"] = rs_age.fillna(9999).astype(int).to_numpy()

    if "precipitation_sum" in df.columns:
        df["precip_7d"] = df["precipitation_sum"].rolling(7, min_periods=1).sum()

    if {"temperature_2m_max", "temperature_2m_min"} <= set(df.columns):
        df["tmean"] = (df["temperature_2m_max"] + df["temperature_2m_min"]) / 2.0
        df["tmean_7d"] = df["tmean"].rolling(7, min_periods=1).mean()

    if "relative_humidity_2m_mean" in df.columns:
        df["rh_7d"] = df["relative_humidity_2m_mean"].rolling(7, min_periods=1).mean()

    DATA_PROCESSED.mkdir(parents=True, exist_ok=True)
    df.to_csv(MERGED_CSV, index=True, encoding="utf-8", float_format="%.4f")

    return MERGED_CSV
