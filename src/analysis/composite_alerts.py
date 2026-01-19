from pathlib import Path
import pandas as pd
import numpy as np

try:
    from src.utils.config_loader import CFG
except ImportError:
    from utils.config_loader import CFG

ROOT = Path(__file__).resolve().parents[2]
MERGED = ROOT / "data/processed/merged.csv"
OUT = ROOT / "data/processed/alerts_composite.csv"
OUT_RAW = ROOT / "data/processed/alerts_composite_raw.csv"
OUT_MERGED = ROOT / "data/processed/alerts_composite_merged.csv"
OUT_DEBUG = ROOT / "data/processed/rs_debug.csv"

_ALERT_CFG = CFG.get("composite_alerts", {})


def _cfg_value(key: str, default):
    value = _ALERT_CFG.get(key, default)
    return default if value is None else value


NDVI_CROP = float(_cfg_value("ndvi_crop", 0.45))
EVI_CROP = float(_cfg_value("evi_crop", 0.35))
RS_MAX_AGE = int(_cfg_value("rs_max_age", 5))

NDMI_DRY = float(_cfg_value("ndmi_dry", 0.20))
MSI_DRY = float(_cfg_value("msi_dry", 1.50))
PRECIP_LOW7 = float(_cfg_value("precip_low7", 15.0))
NDMI_WET = float(_cfg_value("ndmi_wet", 0.45))
PRECIP_HIGH7 = float(_cfg_value("precip_high7", 60.0))

HEAT_TMEAN7 = float(_cfg_value("heat_tmean7", 30.0))
HEAT_RH7 = float(_cfg_value("heat_rh7", 60.0))
COLD_TMIN7 = float(_cfg_value("cold_tmin7", 3.0))

NDRE_LOW = float(_cfg_value("ndre_low", 0.30))
GNDVI_LOW = float(_cfg_value("gndvi_low", 0.50))

SLOPE7_DROP = float(_cfg_value("slope7_drop", -0.03))

GATING_MODE = str(_cfg_value("gating_mode", "canopy_obs")).strip().lower()
CANOPY_OBS_MIN = int(_cfg_value("canopy_obs_min", 2))
_gating_months = _cfg_value("gating_months", [4, 5, 6, 7, 8, 9, 10])
if isinstance(_gating_months, (list, tuple)):
    GATING_MONTHS = [int(m) for m in _gating_months]
else:
    GATING_MONTHS = [int(_gating_months)]
MERGE_GAP_DAYS = int(_cfg_value("merge_gap_days", 1))

REMOTE_OBS_COLS = (
    "ndvi_mean",
    "evi_mean",
    "ndmi_mean",
    "ndre_mean",
    "gndvi_mean",
    "msi_mean",
)

def _finite(row: pd.Series, *cols: str) -> bool:
    for c in cols:
        v = row.get(c, np.nan)
        if not (pd.notna(v) and np.isfinite(v)):
            return False
    return True


def _canopy_ok(row: pd.Series) -> bool:
    ndvi = row.get("ndvi_mean_daily", np.nan)
    evi = row.get("evi_mean", np.nan)
    age = row.get("rs_age", 9999)
    ok = (pd.notna(ndvi) and ndvi >= NDVI_CROP) or (pd.notna(evi) and evi >= EVI_CROP)
    return bool(ok and (age <= RS_MAX_AGE))

def _is_winter_fallow(row: pd.Series) -> bool:
    m = row["date"].month
    if m in (11, 12, 1, 2, 3):
        return not _canopy_ok(row)
    return False


def _gating_ok(row: pd.Series, mode: str) -> bool:
    if mode == "off":
        return True
    rs_ok = row.get("rs_age", 9999) <= RS_MAX_AGE
    if mode == "month_window":
        months = row.get("date").month
        return bool(rs_ok and (months in set(GATING_MONTHS)))
    if mode == "both":
        return bool(rs_ok and row.get("canopy_obs_ready", False) and row.get("month_ok", False))
    return bool(rs_ok and row.get("canopy_obs_ready", False))

def _classify_row(row: pd.Series, apply_gating: bool, gating_mode: str) -> tuple[str | None, str]:
    if apply_gating and not row.get("gating_ok", False):
        return None, ""

    if _is_winter_fallow(row):
        return None, ""

    ndvi = row["ndvi_mean_daily"]
    evi = row["evi_mean"]
    ndmi = row["ndmi_mean"]
    msi = row["msi_mean"]
    ndre = row["ndre_mean"]
    gnd = row["gndvi_mean"]
    p7 = row["precip_7d"]
    t7 = row["tmean_7d"]
    rh7 = row["rh_7d"]
    tmin7 = row.get("tmin_7d", np.nan)
    slope7 = row["ndvi_slope7"]
    canopy = _canopy_ok(row)

    trig = []

    if canopy and _finite(row, "ndmi_mean", "msi_mean", "precip_7d"):
        if (ndmi < NDMI_DRY or msi > MSI_DRY) and (p7 < PRECIP_LOW7):
            trig.append(("drought", f"NDMI={ndmi:.3f}/MSI={msi:.3f}; precip_7d={p7:.1f}"))

    if canopy and _finite(row, "ndmi_mean", "precip_7d", "evi_mean", "ndvi_mean_daily"):
        if (ndmi > NDMI_WET) and (p7 > PRECIP_HIGH7) and ((evi < EVI_CROP) or (ndvi < NDVI_CROP)):
            trig.append(("waterlogging", f"NDMI={ndmi:.3f}; precip_7d={p7:.1f}; EVI={evi:.3f}, NDVI={ndvi:.3f}"))

    if canopy and _finite(row, "tmean_7d", "rh_7d", "evi_mean", "ndvi_slope7"):
        if (t7 >= HEAT_TMEAN7) and (rh7 <= HEAT_RH7) and ((evi < EVI_CROP) or (slope7 <= SLOPE7_DROP)):
            trig.append(("heat_stress", f"tmean_7d={t7:.1f}°C, RH7={rh7:.0f}%, slope7={slope7:.3f}, EVI={evi:.3f}"))

    if canopy and _finite(row, "tmin_7d", "evi_mean", "ndvi_mean_daily", "ndvi_slope7"):
        if (tmin7 <= COLD_TMIN7) and ((evi < 0.40) or (ndvi < 0.50) or (slope7 <= SLOPE7_DROP)):
            trig.append(("cold_stress", f"tmin_7d={tmin7:.1f}°C, EVI={evi:.3f}, NDVI={ndvi:.3f}, slope7={slope7:.3f}"))

    if canopy and _finite(row, "ndre_mean", "gndvi_mean", "ndmi_mean"):
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
        if et == "drought" and _finite(row, "ndmi_mean", "precip_7d"):
            return (NDMI_DRY - row["ndmi_mean"]), "ndmi_mean"
        if et == "waterlogging" and _finite(row, "ndmi_mean", "precip_7d"):
            return (row["ndmi_mean"] - NDMI_WET), "ndmi_mean"
        if et == "heat_stress" and _finite(row, "tmean_7d"):
            return (row["tmean_7d"] - HEAT_TMEAN7), "tmean_7d"
        if et == "cold_stress" and _finite(row, "tmin_7d"):
            return (COLD_TMIN7 - row["tmin_7d"]), "tmin_7d"
        if et == "nutrient_or_pest" and _finite(row, "ndre_mean"):
            return (NDRE_LOW - row["ndre_mean"]), "ndre_mean"
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
        reasons = (
            b["reason"].dropna().unique().tolist()
            if "reason" in b.columns
            else []
        )
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
    df: pd.DataFrame, gating_mode: str = "canopy_obs", apply_gating: bool = True
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if "date" not in df.columns:
        raise ValueError("df must contain 'date'")
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df.sort_values("date", inplace=True)

    needed_cols = [
        "ndvi_mean_daily",
        "ndvi_mean",
        "evi_mean",
        "ndmi_mean",
        "msi_mean",
        "ndre_mean",
        "gndvi_mean",
        "precip_7d",
        "tmean_7d",
        "rh_7d",
    ]
    for c in needed_cols:
        if c not in df.columns:
            df[c] = np.nan

    df["ndvi_slope7"] = df["ndvi_mean_daily"] - df["ndvi_mean_daily"].shift(7)

    if "temperature_2m_min" in df.columns:
        df["tmin_7d"] = df["temperature_2m_min"].rolling(7, min_periods=3).min()
    else:
        df["tmin_7d"] = df["tmean_7d"]

    obs_cols = [c for c in REMOTE_OBS_COLS if c in df.columns]
    obs_flag = df[obs_cols].notna().any(axis=1) if obs_cols else pd.Series(False, index=df.index)
    obs_ok = (df["ndvi_mean"] >= NDVI_CROP) | (df["evi_mean"] >= EVI_CROP)
    obs_ok = obs_ok.fillna(False)
    df["canopy_obs_streak"] = _obs_streak(obs_ok, obs_flag)
    df["canopy_obs_ready"] = df["canopy_obs_streak"] >= CANOPY_OBS_MIN
    df["month_ok"] = df["date"].dt.month.isin(list(GATING_MONTHS))

    last = df["date"].where(obs_flag).ffill()
    df["last_rs_date"] = last.dt.date
    df["rs_age"] = (df["date"] - last).dt.days.fillna(9999).astype(int)
    df["gating_ok"] = df.apply(lambda r: _gating_ok(r, gating_mode), axis=1)

    missing_weather = df[["precip_7d", "tmean_7d", "rh_7d", "tmin_7d"]].isna().any(axis=1)
    missing_remote = ~obs_flag
    df["skip_reason"] = np.where(
        df["rs_age"] > RS_MAX_AGE,
        "rs_max_age",
        np.where(missing_remote, "missing_remote", np.where(missing_weather, "missing_weather", "ok")),
    )
    df["allow_alert"] = (df["skip_reason"] == "ok") & (df["gating_ok"])

    rows = []
    for _, r in df.iterrows():
        et, reason = _classify_row(r, apply_gating=apply_gating, gating_mode=gating_mode)
        if et:
            rows.append({"date": r["date"], "event_type": et, "reason": reason})
    out = pd.DataFrame(rows)
    if not out.empty:
        out.sort_values("date", inplace=True)

    debug_cols = [
        "date",
        "last_rs_date",
        "rs_age",
        "canopy_obs_streak",
        "canopy_obs_ready",
        "month_ok",
        "gating_ok",
        "skip_reason",
        "allow_alert",
    ]
    debug = df[debug_cols].copy()
    return out, debug

def run(infile: Path = MERGED, outfile: Path = OUT) -> Path:
    df = pd.read_csv(infile, parse_dates=["date"])
    alerts_raw, debug_raw = detect_composite_alerts(df, gating_mode="off", apply_gating=False)
    alerts_gated, debug = detect_composite_alerts(df, gating_mode=GATING_MODE, apply_gating=True)
    merged_events = _merge_events(alerts_gated, df)

    outfile.parent.mkdir(parents=True, exist_ok=True)
    alerts_raw.to_csv(OUT_RAW, index=False)
    alerts_gated.to_csv(outfile, index=False)
    merged_events.to_csv(OUT_MERGED, index=False)
    debug.to_csv(OUT_DEBUG, index=False)
    return outfile
