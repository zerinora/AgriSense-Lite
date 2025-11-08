# src/transform/composite_alerts.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import pandas as pd
import numpy as np
from pathlib import Path
from dataclasses import dataclass

DATA_DIR = Path("data")
RAW = DATA_DIR/"raw"
PROC = DATA_DIR/"processed"

@dataclass
class Season:
    # 月-日：包含端点；用于成都平原 Cwa 的稻—油/稻—麦+稻渔
    rice_start: str = "04-15"   # 插秧/返青起
    rice_end:   str = "10-15"   # 收割止
    oil_start:  str = "11-01"   # 播栽起（油菜/麦）
    oil_end:    str = "03-15"   # 越冬-抽薹期

SEASON = Season()

def in_range(md: str, start: str, end: str) -> bool:
    """md=MM-DD；支持跨年窗口"""
    if start <= end:
        return start <= md <= end
    return (md >= start) or (md <= end)

def season_mask(d: pd.Series) -> pd.DataFrame:
    md = d.dt.strftime("%m-%d")
    rice = md.apply(lambda x: in_range(x, SEASON.rice_start, SEASON.rice_end))
    oil  = md.apply(lambda x: in_range(x, SEASON.oil_start,  SEASON.oil_end))
    return pd.DataFrame({"is_rice": rice, "is_oil": oil})

def ensure_cols(df: pd.DataFrame) -> pd.DataFrame:
    """补全常用气象与滚动统计"""
    if "tmean" not in df:
        if {"temperature_2m_max","temperature_2m_min"} <= set(df.columns):
            df["tmean"] = (df["temperature_2m_max"] + df["temperature_2m_min"]) / 2.0
        else:
            df["tmean"] = np.nan
    if "precipitation_sum" not in df:
        df["precipitation_sum"] = 0.0

    # 滚动统计（保守最小窗口=7天）
    if "precip_7d" not in df:
        df["precip_7d"] = df["precipitation_sum"].rolling(7, min_periods=1).sum()
    if "tmean_7d" not in df:
        df["tmean_7d"] = df["tmean"].rolling(7, min_periods=1).mean()
    # EVI/NDVI 近7日趋势，用于跌落判据
    if "evi_mean" in df:
        df["evi_7d_ma"] = df["evi_mean"].rolling(7, min_periods=1).mean()
        df["evi_dev"]   = df["evi_mean"] - df["evi_7d_ma"]
    else:
        df["evi_dev"] = np.nan
    return df

def load_merged() -> pd.DataFrame:
    fp = PROC/"merged.csv"
    df = pd.read_csv(fp)
    df["date"] = pd.to_datetime(df["date"])
    df.sort_values("date", inplace=True)
    df = ensure_cols(df)
    # 可容错：没有就填 NaN
    for col in ["ndvi_mean","ndmi_mean","ndre_mean","evi_mean","gndvi_mean","msi_mean",
                "relative_humidity_2m_mean","wind_speed_10m_max"]:
        if col not in df:
            df[col] = np.nan
    # 云干扰（可选）：有就用，无就跳过
    if "cloud_frac" not in df:
        df["cloud_frac"] = np.nan
    return df

def mk_reason(**kvs) -> str:
    # 把关键数值拼进说明
    parts = []
    for k, v in kvs.items():
        if v is None or (isinstance(v, float) and np.isnan(v)):
            continue
        if isinstance(v, float):
            parts.append(f"{k}={v:.3f}")
        else:
            parts.append(f"{k}={v}")
    return ", ".join(parts)

def detect_events(df: pd.DataFrame) -> pd.DataFrame:
    sm = season_mask(df["date"])
    df = pd.concat([df.reset_index(drop=True), sm], axis=1)

    out = []

    for i, r in df.iterrows():
        md = r["date"].strftime("%m-%d")
        # 基础可用性：云干扰严重则跳过（如果有 cloud_frac）
        if pd.notna(r["cloud_frac"]) and r["cloud_frac"] > 0.6:
            continue

        # -------- 1) 干旱 / 水分胁迫（作物期限定：水稻季）
        # 证据：NDMI 偏低 + 7日降水偏少 + MSI 偏高 + EVI 在“有作物覆盖”的合理区间（过滤冬闲地）
        if r["is_rice"]:
            if (r["ndmi_mean"] < 0.10) and (r["precip_7d"] < 10) and \
               ((r["msi_mean"] if pd.notna(r["msi_mean"]) else 1.0) > 1.2) and \
               (0.25 <= (r["evi_mean"] if pd.notna(r["evi_mean"]) else 0.0) <= 0.85):
                reason = "drought," + mk_reason(
                    NDMI=r["ndmi_mean"], precip_7d=r["precip_7d"],
                    MSI=r["msi_mean"], EVI=r["evi_mean"]
                ) + " (rice season)"
                out.append((r["date"].date().isoformat(), "drought", reason))

        # -------- 2) 渍害 / 涝情（作物期，强降水+地表湿信号偏高）
        if r["is_rice"]:
            if (r["ndmi_mean"] > 0.30) and ((r["precip_7d"] > 80) or (r["precipitation_sum"] > 40)):
                # EVI 短期走弱可作为加分证据，但不强制
                reason = "waterlogging," + mk_reason(
                    NDMI=r["ndmi_mean"], precip_7d=r["precip_7d"], EVIdev=r["evi_dev"]
                ) + " (rice season)"
                out.append((r["date"].date().isoformat(), "waterlogging", reason))

        # -------- 3) 高温胁迫（稻季）：高温+少雨+生长走弱
        if r["is_rice"]:
            if (r["tmean_7d"] > 28) and (r["precip_7d"] < 15) and (r["evi_dev"] < -0.03):
                reason = "heat_stress," + mk_reason(tmean_7d=r["tmean_7d"],
                                                    precip_7d=r["precip_7d"],
                                                    EVIdev=r["evi_dev"]) + " (rice season)"
                out.append((r["date"].date().isoformat(), "heat_stress", reason))

        # -------- 4) 冷害（油菜/小麦季）：低温+高湿+生长走弱
        if r["is_oil"]:
            tmin = min(r.get("temperature_2m_min", np.nan), r.get("tmean", np.nan))
            if (tmin < 0.0) and (r["relative_humidity_2m_mean"] > 70) and (r["evi_dev"] < -0.03):
                reason = "cold_stress," + mk_reason(tmin=tmin,
                                                    RH=r["relative_humidity_2m_mean"],
                                                    EVIdev=r["evi_dev"]) + " (oil/wheat season)"
                out.append((r["date"].date().isoformat(), "cold_stress", reason))

        # -------- 5) 营养不足 / 病虫害可疑（温暖高湿+叶绿素指数低）
        # 证据：NDRE/GNDVI 偏低 +（可选）EVI下降 + 温暖高湿（利于病害）；
        # 注意：这是“可疑”类，需要后续人工复核/实地
        warm_humid = (20 <= r["tmean_7d"] <= 32) and (r["relative_humidity_2m_mean"] >= 75)
        chlor_low = ((r["ndre_mean"] < 0.28) if pd.notna(r["ndre_mean"]) else False) and \
                    ((r["gndvi_mean"] < 0.55) if pd.notna(r["gndvi_mean"]) else False)
        evi_drop = pd.notna(r["evi_dev"]) and (r["evi_dev"] < -0.04)
        if (r["is_rice"] or r["is_oil"]) and warm_humid and chlor_low and evi_drop:
            reason = "nutrient_or_pest," + mk_reason(NDRE=r["ndre_mean"],
                                                     GNDVI=r["gndvi_mean"],
                                                     EVIdev=r["evi_dev"],
                                                     tmean_7d=r["tmean_7d"],
                                                     RH=r["relative_humidity_2m_mean"])
            out.append((r["date"].date().isoformat(), "nutrient_or_pest", reason))

    res = pd.DataFrame(out, columns=["date","event_type","reason"])
    return res

def run() -> Path:
    df = load_merged()
    alerts = detect_events(df)
    out_fp = PROC/"alerts_composite.csv"
    alerts.to_csv(out_fp, index=False)
    print(f"[OK] Composite alerts saved → {out_fp}")
    print(alerts["event_type"].value_counts(dropna=False))
    return out_fp

if __name__ == "__main__":
    run()
