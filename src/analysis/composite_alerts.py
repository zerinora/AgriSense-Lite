# -*- coding: utf-8 -*-
from pathlib import Path
import pandas as pd
import numpy as np

ROOT = Path(__file__).resolve().parents[2]
MERGED = ROOT / "data/processed/merged.csv"
OUT = ROOT / "data/processed/alerts_composite.csv"

# —— 阈值（可按地面经验调） ——
NDVI_CROP = 0.45
EVI_CROP  = 0.35
RS_MAX_AGE = 5  # 遥感“过期”天数：>5天视为不可用

NDMI_DRY  = 0.20
MSI_DRY   = 1.50
PRECIP_LOW7  = 15.0
NDMI_WET  = 0.45
PRECIP_HIGH7 = 60.0

HEAT_TMEAN7 = 30.0
HEAT_RH7    = 60.0
COLD_TMIN7  = 3.0

NDRE_LOW  = 0.30
GNDVI_LOW = 0.50

SLOPE7_DROP = -0.03  # NDVI近7天跌幅阈值

def _canopy_ok(row: pd.Series) -> bool:
    ndvi = row["ndvi_mean_daily"]
    evi  = row["evi_mean"]
    age  = row["rs_age"]
    ok = (pd.notna(ndvi) and ndvi >= NDVI_CROP) or (pd.notna(evi) and evi >= EVI_CROP)
    return bool(ok and (age <= RS_MAX_AGE))

def _is_winter_fallow(row: pd.Series) -> bool:
    m = row["date"].month
    if m in (11,12,1,2,3):
        # 冬闲/油菜期：若无可靠冠层，则视为正常
        return not _canopy_ok(row)
    return False

def _classify_row(row: pd.Series) -> tuple[str | None, str]:
    if _is_winter_fallow(row):
        return None, ""

    ndvi = row["ndvi_mean_daily"]; evi = row["evi_mean"]
    ndmi = row["ndmi_mean"]; msi = row["msi_mean"]
    ndre = row["ndre_mean"]; gnd = row["gndvi_mean"]
    p7   = row["precip_7d"];  t7  = row["tmean_7d"]
    rh7  = row["rh_7d"];      tmin7 = row.get("tmin_7d", np.nan)
    slope7 = row["ndvi_slope7"]
    canopy = _canopy_ok(row)

    trig = []

    # 干旱：需有冠层 + 水分信号 + 少雨
    if canopy and pd.notna(ndmi) and ((ndmi < NDMI_DRY) or (pd.notna(msi) and msi > MSI_DRY)) and (pd.notna(p7) and p7 < PRECIP_LOW7):
        trig.append(("drought", f"NDMI={ndmi:.3f}/MSI={msi:.3f}; precip_7d={p7:.1f}"))

    # 水涝：需有冠层 + 过湿 + 大雨 + 植被低
    if canopy and pd.notna(ndmi) and (ndmi > NDMI_WET) and (pd.notna(p7) and p7 > PRECIP_HIGH7) and ((pd.notna(evi) and evi < EVI_CROP) or (pd.notna(ndvi) and ndvi < NDVI_CROP)):
        trig.append(("waterlogging", f"NDMI={ndmi:.3f}; precip_7d={p7:.1f}; EVI={evi:.3f}, NDVI={ndvi:.3f}"))

    # 热胁迫：需有冠层 + 高温 + 干/或快速下跌
    if canopy and pd.notna(t7) and (t7 >= HEAT_TMEAN7) and (pd.notna(rh7) and rh7 <= HEAT_RH7) and ((pd.notna(evi) and evi < EVI_CROP) or (pd.notna(slope7) and slope7 <= SLOPE7_DROP)):
        trig.append(("heat_stress", f"tmean_7d={t7:.1f}°C, RH7={rh7:.0f}%, slope7={slope7:.3f}, EVI={evi:.3f}"))

    # 冷胁迫：需有冠层 + 低温阈值（7日最低气温） + 指数低/下跌；1–3月只对确有冠层的冬作物生效
    if canopy and pd.notna(tmin7) and (tmin7 <= COLD_TMIN7) and ((pd.notna(evi) and evi < 0.40) or (pd.notna(ndvi) and ndvi < 0.50) or (pd.notna(slope7) and slope7 <= SLOPE7_DROP)):
        trig.append(("cold_stress", f"tmin_7d={tmin7:.1f}°C, EVI={evi:.3f}, NDVI={ndvi:.3f}, slope7={slope7:.3f}"))

    # 营养/病虫：需有冠层 + 叶绿素指数低 + 水分不干
    if canopy and ((pd.notna(ndre) and ndre < NDRE_LOW) or (pd.notna(gnd) and gnd < GNDVI_LOW)) and (pd.notna(ndmi) and ndmi >= NDMI_DRY):
        trig.append(("nutrient_or_pest", f"NDRE={ndre:.3f}, GNDVI={gnd:.3f}, NDMI={ndmi:.3f}"))

    if len(trig) == 0:
        return None, ""
    if len(trig) >= 2:
        return "composite", " + ".join(k for k,_ in trig)
    return trig[0][0], trig[0][1]

def detect_composite_alerts(df: pd.DataFrame) -> pd.DataFrame:
    if "date" not in df.columns:
        raise ValueError("df must contain 'date'")
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df.sort_values("date", inplace=True)

    # 7日滚动指标
    if "ndvi_mean_daily" not in df.columns: df["ndvi_mean_daily"] = np.nan
    df["ndvi_slope7"] = df["ndvi_mean_daily"] - df["ndvi_mean_daily"].shift(7)

    if "temperature_2m_min" in df.columns:
        df["tmin_7d"] = df["temperature_2m_min"].rolling(7, min_periods=3).min()
    else:
        df["tmin_7d"] = df["tmean_7d"]  # 兜底

    # 遥感观测时效：距离最近一次有效 NDVI 的天数
    v = df["ndvi_mean_daily"].notna()
    last = df["date"].where(v).ffill()
    df["rs_age"] = (df["date"] - last).dt.days.fillna(9999).astype(int)

    rows = []
    for _, r in df.iterrows():
        et, reason = _classify_row(r)
        if et:
            rows.append({"date": r["date"], "event_type": et, "reason": reason})
    out = pd.DataFrame(rows)
    if not out.empty:
        out.sort_values("date", inplace=True)
    return out

def run(infile: Path = MERGED, outfile: Path = OUT) -> Path:
    df = pd.read_csv(infile, parse_dates=["date"])
    out = detect_composite_alerts(df)
    outfile.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(outfile, index=False)
    return outfile
