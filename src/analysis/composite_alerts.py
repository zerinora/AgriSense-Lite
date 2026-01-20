from __future__ import annotations

from pathlib import Path
import pandas as pd
import numpy as np

try:
    from src.utils.config_loader import (
        CFG,
        MERGED_CSV,
        ALERTS_GATED_CSV,
        ALERTS_RAW_CSV,
        ALERTS_MERGED_CSV,
        RS_DEBUG_CSV,
    )
except ImportError:
    from utils.config_loader import (
        CFG,
        MERGED_CSV,
        ALERTS_GATED_CSV,
        ALERTS_RAW_CSV,
        ALERTS_MERGED_CSV,
        RS_DEBUG_CSV,
    )

MERGED = MERGED_CSV
OUT = ALERTS_GATED_CSV
OUT_RAW = ALERTS_RAW_CSV
OUT_MERGED = ALERTS_MERGED_CSV
OUT_DEBUG = RS_DEBUG_CSV

_ALERT_CFG = CFG.get("composite_alerts", {}) if isinstance(CFG, dict) else {}
_RS_CFG = CFG.get("remote_sensing", {}) if isinstance(CFG, dict) else {}
_GATING_CFG = CFG.get("gating", {}) if isinstance(CFG, dict) else {}


def _cfg_value(cfg: dict, key: str, default):
    value = cfg.get(key, default) if isinstance(cfg, dict) else default
    return default if value is None else value


NDVI_CROP = float(_cfg_value(_ALERT_CFG, "ndvi_crop", 0.45))
EVI_CROP = float(_cfg_value(_ALERT_CFG, "evi_crop", 0.35))
RS_MAX_AGE = int(_cfg_value(_ALERT_CFG, "rs_max_age", 5))

WINDOW_HALF_DAYS = int(_cfg_value(_RS_CFG, "window_half_days", RS_MAX_AGE))
WINDOW_MODE = str(_cfg_value(_RS_CFG, "window_mode", "symmetric")).strip().lower()
SUPPORT_PICK = str(_cfg_value(_RS_CFG, "support_pick", "nearest")).strip().lower()

GATING_MODE = str(
    _cfg_value(_GATING_CFG, "mode", _cfg_value(_ALERT_CFG, "gating_mode", "both"))
).strip().lower()
CANOPY_OBS_MIN = int(
    _cfg_value(_GATING_CFG, "canopy_obs_min", _cfg_value(_ALERT_CFG, "canopy_obs_min", 2))
)
_canopy_ndvi = _cfg_value(_GATING_CFG, "canopy_ndvi_min", NDVI_CROP)
_canopy_evi = _cfg_value(_GATING_CFG, "canopy_evi_min", EVI_CROP)
CANOPY_NDVI_MIN = float(_canopy_ndvi)
CANOPY_EVI_MIN = float(_canopy_evi)

_gating_months = _cfg_value(
    _GATING_CFG,
    "months",
    _cfg_value(_ALERT_CFG, "gating_months", [4, 5, 6, 7, 8, 9, 10]),
)
if isinstance(_gating_months, (list, tuple)):
    GATING_MONTHS = [int(m) for m in _gating_months]
else:
    GATING_MONTHS = [int(_gating_months)]

NDMI_DRY = float(_cfg_value(_ALERT_CFG, "ndmi_dry", 0.20))
MSI_DRY = float(_cfg_value(_ALERT_CFG, "msi_dry", 1.50))
PRECIP_LOW7 = float(_cfg_value(_ALERT_CFG, "precip_low7", 15.0))
NDMI_WET = float(_cfg_value(_ALERT_CFG, "ndmi_wet", 0.45))
PRECIP_HIGH7 = float(_cfg_value(_ALERT_CFG, "precip_high7", 60.0))

HEAT_TMEAN7 = float(_cfg_value(_ALERT_CFG, "heat_tmean7", 30.0))
HEAT_RH7 = float(_cfg_value(_ALERT_CFG, "heat_rh7", 60.0))
COLD_TMIN7 = float(_cfg_value(_ALERT_CFG, "cold_tmin7", 3.0))

NDRE_LOW = float(_cfg_value(_ALERT_CFG, "ndre_low", 0.30))
GNDVI_LOW = float(_cfg_value(_ALERT_CFG, "gndvi_low", 0.50))

SLOPE7_DROP = float(_cfg_value(_ALERT_CFG, "slope7_drop", -0.03))
MERGE_GAP_DAYS = int(_cfg_value(_ALERT_CFG, "merge_gap_days", 1))

METRIC_DEFS = {
    "ndvi": {
        "obs": ["ndvi_obs", "ndvi_mean"],
        "fill": ["ndvi_fill", "ndvi_mean_daily", "ndvi_mean"],
    },
    "evi": {
        "obs": ["evi_obs", "evi_mean"],
        "fill": ["evi_fill", "evi_mean"],
    },
    "ndmi": {
        "obs": ["ndmi_obs", "ndmi_mean"],
        "fill": ["ndmi_fill", "ndmi_mean"],
    },
    "ndre": {
        "obs": ["ndre_obs", "ndre_mean"],
        "fill": ["ndre_fill", "ndre_mean"],
    },
    "gndvi": {
        "obs": ["gndvi_obs", "gndvi_mean"],
        "fill": ["gndvi_fill", "gndvi_mean"],
    },
    "msi": {
        "obs": ["msi_obs", "msi_mean"],
        "fill": ["msi_fill", "msi_mean"],
    },
}


def _ensure_metric_columns(df: pd.DataFrame) -> pd.DataFrame:
    for meta in METRIC_DEFS.values():
        obs_col = meta["obs"][0]
        if obs_col not in df.columns:
            for candidate in meta["obs"][1:]:
                if candidate in df.columns:
                    df[obs_col] = df[candidate]
                    break
            else:
                df[obs_col] = np.nan
        fill_col = meta["fill"][0]
        if fill_col not in df.columns:
            for candidate in meta["fill"][1:]:
                if candidate in df.columns:
                    df[fill_col] = df[candidate]
                    break
            else:
                df[fill_col] = np.nan
    return df


def _finite_df(df: pd.DataFrame, cols: list[str]) -> pd.Series:
    if not cols:
        return pd.Series(True, index=df.index)
    values = df[cols]
    return values.notna().all(axis=1) & np.isfinite(values).all(axis=1)


def _finite_row(row: pd.Series, *cols: str) -> bool:
    for c in cols:
        v = row.get(c, np.nan)
        if not (pd.notna(v) and np.isfinite(v)):
            return False
    return True


def _pick_support_date(
    target: pd.Timestamp,
    obs_dates: list[pd.Timestamp],
    window_half_days: int,
    mode: str,
    support_pick: str,
) -> pd.Timestamp | None:
    if not obs_dates:
        return None

    if mode == "past_only":
        candidates = [
            d
            for d in obs_dates
            if d <= target and 0 <= (target - d).days <= window_half_days
        ]
    else:
        candidates = [d for d in obs_dates if abs((target - d).days) <= window_half_days]

    if not candidates:
        return None

    deltas = [abs((target - d).days) for d in candidates]
    min_delta = min(deltas)
    closest = [d for d, delta in zip(candidates, deltas) if delta == min_delta]

    if support_pick == "prefer_past":
        past = [d for d in closest if d <= target]
        if past:
            return max(past)
    return min(closest)


def _canopy_ok(row: pd.Series) -> bool:
    ndvi = row.get("ndvi_fill", np.nan)
    evi = row.get("evi_fill", np.nan)
    ok = (pd.notna(ndvi) and ndvi >= NDVI_CROP) or (pd.notna(evi) and evi >= EVI_CROP)
    return bool(ok)


def _gating_mask(df: pd.DataFrame, mode: str) -> pd.Series:
    if mode == "off":
        return pd.Series(True, index=df.index)
    if mode == "month_window":
        return df["month_ok"].fillna(False)
    if mode == "both":
        return df["month_ok"].fillna(False) & df["canopy_obs_ready"].fillna(False)
    return df["canopy_obs_ready"].fillna(False)


def _classify_row(row: pd.Series, apply_gating: bool) -> tuple[str | None, str]:
    if not row.get("qc_ok", False):
        return None, ""
    if apply_gating and not row.get("gating_ok", False):
        return None, ""

    ndvi = row.get("ndvi_fill", np.nan)
    evi = row.get("evi_fill", np.nan)
    ndmi = row.get("ndmi_fill", np.nan)
    msi = row.get("msi_fill", np.nan)
    ndre = row.get("ndre_fill", np.nan)
    gnd = row.get("gndvi_fill", np.nan)
    p7 = row.get("precip_7d", np.nan)
    t7 = row.get("tmean_7d", np.nan)
    rh7 = row.get("rh_7d", np.nan)
    tmin7 = row.get("tmin_7d", np.nan)
    slope7 = row.get("ndvi_slope7", np.nan)
    canopy = _canopy_ok(row)

    trig = []

    if canopy and _finite_row(row, "ndmi_fill", "msi_fill", "precip_7d"):
        if (ndmi < NDMI_DRY or msi > MSI_DRY) and (p7 < PRECIP_LOW7):
            trig.append(("drought", f"NDMI={ndmi:.3f}/MSI={msi:.3f}; precip_7d={p7:.1f}"))

    if canopy and _finite_row(row, "ndmi_fill", "precip_7d", "evi_fill", "ndvi_fill"):
        if (ndmi > NDMI_WET) and (p7 > PRECIP_HIGH7) and ((evi < EVI_CROP) or (ndvi < NDVI_CROP)):
            trig.append(
                (
                    "waterlogging",
                    f"NDMI={ndmi:.3f}; precip_7d={p7:.1f}; EVI={evi:.3f}, NDVI={ndvi:.3f}",
                )
            )

    if canopy and _finite_row(row, "tmean_7d", "rh_7d", "evi_fill", "ndvi_slope7"):
        if (t7 >= HEAT_TMEAN7) and (rh7 <= HEAT_RH7) and ((evi < EVI_CROP) or (slope7 <= SLOPE7_DROP)):
            trig.append(
                (
                    "heat_stress",
                    f"tmean_7d={t7:.1f}C, RH7={rh7:.0f}%, slope7={slope7:.3f}, EVI={evi:.3f}",
                )
            )

    if canopy and _finite_row(row, "tmin_7d", "evi_fill", "ndvi_fill", "ndvi_slope7"):
        if (tmin7 <= COLD_TMIN7) and ((evi < 0.40) or (ndvi < 0.50) or (slope7 <= SLOPE7_DROP)):
            trig.append(
                (
                    "cold_stress",
                    f"tmin_7d={tmin7:.1f}?C, EVI={evi:.3f}, NDVI={ndvi:.3f}, slope7={slope7:.3f}",
                )
            )

    if canopy and _finite_row(row, "ndre_fill", "gndvi_fill", "ndmi_fill"):
        if ((ndre < NDRE_LOW) or (gnd < GNDVI_LOW)) and (ndmi >= NDMI_DRY):
            trig.append(("nutrient_or_pest", f"NDRE={ndre:.3f}, GNDVI={gnd:.3f}, NDMI={ndmi:.3f}"))

    if len(trig) == 0:
        return None, ""
    if len(trig) >= 2:
        return "composite", " + ".join(k for k, _ in trig)
    return trig[0][0], trig[0][1]


def _obs_streak(obs_ok: pd.Series, obs_flag: pd.Series) -> pd.Series:
    count = 0
    out = []
    for ok, is_obs in zip(obs_ok, obs_flag):
        if is_obs:
            count = count + 1 if ok else 0
        out.append(count)
    return pd.Series(out, index=obs_ok.index)


def _merge_events(alerts: pd.DataFrame, df: pd.DataFrame) -> pd.DataFrame:
    if alerts.empty:
        return pd.DataFrame(
            columns=[
                "event_type",
                "start_date",
                "end_date",
                "duration_days",
                "peak_date",
                "peak_value",
                "peak_metric",
                "reason_summary",
            ]
        )

    joined = alerts.merge(df, on="date", how="left")

    def _intensity(row: pd.Series) -> tuple[float, str]:
        et = row["event_type"]
        if et == "drought" and _finite_row(row, "ndmi_fill", "precip_7d"):
            return (NDMI_DRY - row["ndmi_fill"]), "ndmi_fill"
        if et == "waterlogging" and _finite_row(row, "ndmi_fill", "precip_7d"):
            return (row["ndmi_fill"] - NDMI_WET), "ndmi_fill"
        if et == "heat_stress" and _finite_row(row, "tmean_7d"):
            return (row["tmean_7d"] - HEAT_TMEAN7), "tmean_7d"
        if et == "cold_stress" and _finite_row(row, "tmin_7d"):
            return (COLD_TMIN7 - row["tmin_7d"]), "tmin_7d"
        if et == "nutrient_or_pest" and _finite_row(row, "ndre_fill"):
            return (NDRE_LOW - row["ndre_fill"]), "ndre_fill"
        return (np.nan, "na")

    joined[["intensity", "peak_metric"]] = joined.apply(
        lambda r: pd.Series(_intensity(r)), axis=1
    )

    rows = []
    for et, sub in joined.groupby("event_type"):
        sub = sub.sort_values("date")
        start = sub.iloc[0]["date"]
        last = start
        bucket = [sub.iloc[0]]
        for _, r in sub.iloc[1:].iterrows():
            gap = (r["date"] - last).days
            if gap <= MERGE_GAP_DAYS + 1:
                bucket.append(r)
            else:
                rows.append((et, bucket))
                bucket = [r]
            last = r["date"]
        rows.append((et, bucket))

    out_rows = []
    for et, bucket in rows:
        b = pd.DataFrame(bucket)
        start = b["date"].min()
        end = b["date"].max()
        duration = (end - start).days + 1
        if b["intensity"].notna().any():
            idx = b["intensity"].idxmax()
            peak = b.loc[idx]
        else:
            peak = b.iloc[0]
        reasons = b["reason"].dropna().unique().tolist() if "reason" in b.columns else []
        reason_summary = " | ".join(reasons[:2])
        out_rows.append(
            {
                "event_type": et,
                "start_date": start.date().isoformat(),
                "end_date": end.date().isoformat(),
                "duration_days": int(duration),
                "peak_date": peak["date"].date().isoformat(),
                "peak_value": float(peak["intensity"]) if pd.notna(peak["intensity"]) else np.nan,
                "peak_metric": peak["peak_metric"],
                "reason_summary": reason_summary,
            }
        )

    return pd.DataFrame(out_rows).sort_values(["start_date", "event_type"])


def detect_composite_alerts(
    df: pd.DataFrame, gating_mode: str = "both", apply_gating: bool = True
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if "date" not in df.columns:
        raise ValueError("df must contain 'date'")
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df.sort_values("date", inplace=True)

    df = _ensure_metric_columns(df)

    if "precip_7d" not in df.columns and "precipitation_sum" in df.columns:
        df["precip_7d"] = df["precipitation_sum"].rolling(7, min_periods=1).sum()

    if "tmean_7d" not in df.columns:
        if {"temperature_2m_max", "temperature_2m_min"} <= set(df.columns):
            df["tmean"] = (df["temperature_2m_max"] + df["temperature_2m_min"]) / 2.0
            df["tmean_7d"] = df["tmean"].rolling(7, min_periods=1).mean()

    if "rh_7d" not in df.columns and "relative_humidity_2m_mean" in df.columns:
        df["rh_7d"] = df["relative_humidity_2m_mean"].rolling(7, min_periods=1).mean()

    if "tmin_7d" not in df.columns:
        if "temperature_2m_min" in df.columns:
            df["tmin_7d"] = df["temperature_2m_min"].rolling(7, min_periods=3).min()
        elif "tmean_7d" in df.columns:
            df["tmin_7d"] = df["tmean_7d"]

    if "ndvi_slope7" not in df.columns:
        df["ndvi_slope7"] = df["ndvi_fill"] - df["ndvi_fill"].shift(7)

    obs_cols = [meta["obs"][0] for meta in METRIC_DEFS.values() if meta["obs"][0] in df.columns]
    obs_flag = df[obs_cols].notna().any(axis=1) if obs_cols else pd.Series(False, index=df.index)
    df["real_obs_day"] = obs_flag

    obs_ok = (df["ndvi_obs"] >= CANOPY_NDVI_MIN) | (df["evi_obs"] >= CANOPY_EVI_MIN)
    obs_ok = obs_ok.fillna(False)
    df["canopy_obs_streak"] = _obs_streak(obs_ok, obs_flag)
    df["canopy_obs_ready"] = df["canopy_obs_streak"] >= CANOPY_OBS_MIN
    df["month_ok"] = df["date"].dt.month.isin(list(GATING_MONTHS))

    obs_dates = sorted(df.loc[obs_flag, "date"].tolist())
    support_dates = [
        _pick_support_date(d, obs_dates, WINDOW_HALF_DAYS, WINDOW_MODE, SUPPORT_PICK)
        for d in df["date"]
    ]
    support = pd.to_datetime(pd.Series(support_dates, index=df.index))

    df["rs_support_date"] = support.dt.date
    support_age = (df["date"] - support).abs().dt.days
    df["rs_support_age"] = support_age.fillna(9999).astype(int)
    df["rs_window_ok"] = support.notna() & (df["rs_support_age"] <= WINDOW_HALF_DAYS)

    weather_cols = [
        c for c in ("precip_7d", "tmean_7d", "rh_7d", "tmin_7d") if c in df.columns
    ]
    metric_cols = [meta["fill"][0] for meta in METRIC_DEFS.values() if meta["fill"][0] in df.columns]

    weather_ok = _finite_df(df, weather_cols)
    metric_ok = _finite_df(df, metric_cols)

    df["missing_weather"] = ~weather_ok
    df["missing_remote"] = ~df["rs_window_ok"]
    df["qc_ok"] = df["rs_window_ok"] & weather_ok & metric_ok

    df["skip_reason"] = np.where(
        df["missing_remote"],
        "missing_remote",
        np.where(df["missing_weather"], "missing_weather", np.where(~metric_ok, "nonfinite", "ok")),
    )

    df["gating_ok"] = _gating_mask(df, gating_mode)
    df["allow_alert"] = df["qc_ok"] & df["gating_ok"]

    rows = []
    for _, r in df.iterrows():
        et, reason = _classify_row(r, apply_gating=apply_gating)
        if et:
            rows.append({"date": r["date"], "event_type": et, "reason": reason})
    out = pd.DataFrame(rows)
    if not out.empty:
        out.sort_values("date", inplace=True)

    debug_cols = [
        "date",
        "real_obs_day",
        "rs_support_date",
        "rs_support_age",
        "rs_window_ok",
        "missing_remote",
        "missing_weather",
        "qc_ok",
        "skip_reason",
        "canopy_obs_streak",
        "canopy_obs_ready",
        "month_ok",
        "gating_ok",
        "allow_alert",
    ]
    debug = df[debug_cols].copy()
    return out, debug


def run(infile: Path = MERGED, outfile: Path = OUT) -> Path:
    df = pd.read_csv(infile, parse_dates=["date"])
    df = _ensure_metric_columns(df)

    alerts_raw, _ = detect_composite_alerts(df, gating_mode="off", apply_gating=False)
    alerts_gated, debug = detect_composite_alerts(df, gating_mode=GATING_MODE, apply_gating=True)
    merged_events = _merge_events(alerts_gated, df)

    outfile.parent.mkdir(parents=True, exist_ok=True)
    alerts_raw.to_csv(OUT_RAW, index=False)
    alerts_gated.to_csv(outfile, index=False)
    merged_events.to_csv(OUT_MERGED, index=False)
    debug.to_csv(OUT_DEBUG, index=False)

    return outfile
