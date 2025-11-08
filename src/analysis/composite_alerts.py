"""
Simple rule‑based composite alert detection.

This module classifies each day in a merged dataset into one of several
stress categories (drought, waterlogging, heat stress, cold stress,
nutrient/pest) or normal.  If multiple stress flags are triggered the
``event_type`` becomes ``composite``.  Thresholds are based on typical
agronomic ranges and may be adjusted via keyword arguments.  Off‑season
dates (November–March) are always marked as normal.
"""

from __future__ import annotations

import pandas as pd
from typing import Dict, List, Optional


def _classify_row(
    row: pd.Series,
    ndvi_thresh: float,
    ndmi_thresh: float,
    ndmi_strong_thresh: float,
    msi_thresh: float,
    msi_strong_thresh: float,
    ndre_thresh: float,
    ndre_strong_thresh: float,
    evi_thresh: float,
    gndvi_thresh: float,
    precip_thresh: float,
    rainlong_thresh: float,
    humidity_thresh: float,
    ndmi_wet_thresh: float,
    tmean_hot_thresh: float = 30.0,
    tmean_cold_thresh: float = 5.0,
    rh_high_thresh: float = 0.75,
    rh_low_thresh: float = 0.30,
    evi_cover_thresh: float = 0.20,
    ) -> Dict[str, str]:
    """Classify a single day's conditions into stress categories.

    Each row's vegetation indices and simple weather features are compared
    to a set of thresholds.  Flags are raised for drought, waterlogging,
    heat stress, cold stress and nutrient/pest; if more than one flag
    triggers the result is ``composite``.  Off‑season dates (Nov–Mar)
    return ``normal`` regardless of values.
    """
    reasons: List[str] = []

    # Extract values with fallback to NaN
    ndvi = row.get("ndvi_mean_daily")
    ndmi = row.get("ndmi_mean")
    msi = row.get("msi_mean")
    ndre = row.get("ndre_mean")
    evi = row.get("evi_mean")
    gndvi = row.get("gndvi_mean")
    precip = row.get("precip_7d")
    tmean = row.get("tmean_7d")
    rh = row.get("relative_humidity_2m_mean")

    # Season check: off‑season between 11‑01 and 03‑31 considered normal
    date_val = row.get("date")
    if pd.notna(date_val):
        month_day = date_val.strftime("%m-%d")
        # If date is in winter (Nov–Mar), skip anomaly classification
        if ("11-01" <= month_day <= "12-31") or ("01-01" <= month_day <= "03-31"):
            return {"event_type": "normal", "reason": "off‑season"}

    # Initialise flags
    drought_flag = False
    waterlog_flag = False
    heat_flag = False
    cold_flag = False
    nutrient_flag = False

    # Determine canopy presence: require some vegetation cover to classify drought or waterlogging
    canopy_ok = False
    if pd.notna(evi) and evi >= evi_cover_thresh:
        canopy_ok = True
    elif pd.notna(ndvi) and ndvi >= ndvi_thresh:
        canopy_ok = True

    # ----- Drought / water stress -----
    # Severe drought if NDMI or MSI crosses strong threshold regardless of rain
    if canopy_ok:
        if pd.notna(ndmi) and ndmi < ndmi_strong_thresh:
            drought_flag = True
            reasons.append(f"NDMI={ndmi:.3f}<strong_thresh{ndmi_strong_thresh}")
        elif pd.notna(msi) and msi > msi_strong_thresh:
            drought_flag = True
            reasons.append(f"MSI={msi:.3f}>strong_thresh{msi_strong_thresh}")
        else:
            # Moderate drought: NDMI < thresh or MSI > thresh AND low precipitation
            if pd.notna(ndmi) and ndmi < ndmi_thresh:
                if pd.notna(precip) and precip < precip_thresh:
                    drought_flag = True
                    reasons.append(
                        f"NDMI={ndmi:.3f}<thresh{ndmi_thresh} & precip_7d={precip:.1f}mm<{precip_thresh}"
                    )
            if not drought_flag and pd.notna(msi) and msi > msi_thresh:
                if pd.notna(precip) and precip < precip_thresh:
                    drought_flag = True
                    reasons.append(
                        f"MSI={msi:.3f}>thresh{msi_thresh} & precip_7d={precip:.1f}mm<{precip_thresh}"
                    )

    # ----- Waterlogging / excess moisture -----
    # Only evaluate waterlogging if drought not already flagged
    if not drought_flag and canopy_ok:
        # Wet canopy plus heavy rain and low NDVI/EVI indicates standing water
        wet_canopy = pd.notna(ndmi) and ndmi > ndmi_wet_thresh
        low_cover = False
        if pd.notna(ndvi) and ndvi < ndvi_thresh:
            low_cover = True
        elif pd.notna(evi) and evi < evi_thresh:
            low_cover = True
        heavy_rain = pd.notna(precip) and precip > rainlong_thresh
        if wet_canopy and low_cover and heavy_rain:
            waterlog_flag = True
            reasons.append(
                f"NDMI={ndmi:.3f}>wet_thresh{ndmi_wet_thresh} & precip_7d={precip:.1f}mm>{rainlong_thresh} & low cover"
            )

    # ----- Heat stress -----
    # High temperature combined with canopy stress and low humidity
    if pd.notna(tmean) and tmean > tmean_hot_thresh:
        # Consider humidity only if provided; convert thresholds to percent
        rh_ok = pd.isna(rh) or (rh < rh_low_thresh * 100)
        if rh_ok:
            # Require canopy stress (EVI below threshold) to avoid misclassifying short heat spikes
            if pd.notna(evi) and evi < evi_thresh:
                heat_flag = True
                reasons.append(
                    f"tmean_7d={tmean:.1f}>hot_thresh{tmean_hot_thresh} & EVI={evi:.3f}<evi_thresh{evi_thresh}"
                )

    # ----- Cold stress -----
    if pd.notna(tmean) and tmean < tmean_cold_thresh:
        if pd.notna(rh) and rh > rh_high_thresh * 100:
            cold_flag = True
            reasons.append(
                f"tmean_7d={tmean:.1f}<cold_thresh{tmean_cold_thresh} & RH={rh:.0f}%>high_RH{rh_high_thresh*100:.0f}%"
            )

    # ----- Nutrient deficiency or pest/disease -----
    # Evaluate only if no other primary stress is present
    if not (drought_flag or waterlog_flag or heat_flag or cold_flag):
        indicators: List[str] = []
        if pd.notna(ndre) and ndre < ndre_strong_thresh:
            indicators.append(f"NDRE={ndre:.3f}<strong_thresh{ndre_strong_thresh}")
        elif pd.notna(ndre) and ndre < ndre_thresh:
            indicators.append(f"NDRE={ndre:.3f}<thresh{ndre_thresh}")
        if pd.notna(gndvi) and gndvi < gndvi_thresh:
            indicators.append(f"GNDVI={gndvi:.3f}<thresh{gndvi_thresh}")
        if pd.notna(evi) and evi < evi_thresh:
            indicators.append(f"EVI={evi:.3f}<evi_thresh{evi_thresh}")
        # Ensure moisture conditions are adequate so that low indices are not due to drought
        moisture_ok = True
        if pd.notna(ndmi) and ndmi < ndmi_thresh:
            moisture_ok = False
        if pd.notna(msi) and msi > msi_thresh:
            moisture_ok = False
        if indicators and moisture_ok:
            nutrient_flag = True
            reasons.extend(indicators)
            # Add humidity qualifier if high humidity encourages disease
            if pd.notna(rh) and rh > humidity_thresh * 100:
                reasons.append(f"RH={rh:.0f}%>humid_thresh{humidity_thresh*100:.0f}%")

    # ----- Determine final event type -----
    flags = [drought_flag, waterlog_flag, heat_flag, cold_flag, nutrient_flag]
    flag_names = [
        (drought_flag, "drought"),
        (waterlog_flag, "waterlogging"),
        (heat_flag, "heat_stress"),
        (cold_flag, "cold_stress"),
        (nutrient_flag, "nutrient_or_pest"),
    ]
    # Count how many flags are true
    active = [name for flag, name in flag_names if flag]
    if not active:
        return {"event_type": "normal", "reason": ""}
    # Single flag -> assign that event; multiple -> composite
    if len(active) == 1:
        return {"event_type": active[0], "reason": "; ".join(reasons)}
    return {"event_type": "composite", "reason": "; ".join(reasons)}


def detect_composite_alerts(
    df: pd.DataFrame,
    train_years: Optional[List[int]] = None,
    # NDVI threshold: lower to 0.35 to permit detection of partial canopy cover
    # for waterlogging and drought checks in mixed cropping phases.
    ndvi_thresh: float = 0.35,
    # NDMI thresholds: moderate drought below 0.20; severe drought below 0.10.
    ndmi_thresh: float = 0.20,
    ndmi_strong_thresh: float = 0.10,
    # MSI thresholds: moderate drought above 1.0; severe drought above 1.5.
    msi_thresh: float = 1.0,
    msi_strong_thresh: float = 1.5,
    # NDRE thresholds: nutrient/pest stress below 0.28; severe below 0.20【675122315655608†L140-L156】.
    ndre_thresh: float = 0.28,
    ndre_strong_thresh: float = 0.20,
    # EVI stress threshold remains 0.2
    evi_thresh: float = 0.2,
    # GNDVI stress threshold lowered to 0.5 (from 0.5 default) to detect early stress.
    gndvi_thresh: float = 0.5,
    # Precipitation thresholds: moderate drought if 7‑day rainfall < 15 mm;
    precip_thresh: float = 15.0,
    # Waterlogging threshold: heavy rain > 60 mm in 7 days triggers waterlogging
    rainlong_thresh: float = 60.0,
    humidity_thresh: float = 0.75,
    # NDMI wet threshold for waterlogging: values above 0.40 indicate a wet canopy【73289060569815†L120-L137】.
    ndmi_wet_thresh: float = 0.40,
    tmean_hot_thresh: float = 30.0,
    tmean_cold_thresh: float = 5.0,
    rh_high_thresh: float = 0.75,
    rh_low_thresh: float = 0.30,
    # EVI cover threshold: lowered to 0.20 to include early growth phases.
    evi_cover_thresh: float = 0.20,
    drop_normal: bool = True,
) -> pd.DataFrame:
    """Detect composite crop stress events from a merged data frame.

    Parameters
    ----------
    df : pandas.DataFrame
        The merged data frame with date index and index/weather columns.  If
        the ``date`` column is not already a datetime index, it will be
        converted.
    train_years : list[int], optional
        Not used in the current rule‑based implementation, but retained for
        API compatibility.  In future, threshold training could be derived
        from historical data.
    ndvi_thresh : float
        NDVI threshold below which plants are considered stressed.
    ndmi_thresh : float
        NDMI threshold indicating moderate water stress when precipitation is
        simultaneously low.
    ndmi_strong_thresh : float
        NDMI threshold indicating severe drought regardless of rainfall.
    msi_thresh : float
        MSI threshold indicating moderate moisture stress when rainfall is low.
    msi_strong_thresh : float
        MSI threshold indicating severe drought regardless of rainfall.
    ndre_thresh : float
        NDRE threshold signalling potential nutrient deficiency or pest/disease.
    ndre_strong_thresh : float
        Stricter NDRE threshold for pronounced chlorophyll stress.
    evi_thresh : float
        EVI threshold for stressed canopy conditions.
    gndvi_thresh : float
        GNDVI threshold for early stress detection.
    precip_thresh : float
        7‑day precipitation threshold below which drought conditions are
        inferred when NDMI/MSI indicate moisture stress.
    rainlong_thresh : float
        7‑day precipitation threshold above which waterlogging is inferred
        when NDVI is low and NDMI high.
    humidity_thresh : float
        Relative humidity fraction (0–1) above which nutrient/pest anomalies
        are more likely to be disease/pest (wet environment).  Values are
        compared against the ``relative_humidity_2m_mean`` column expressed in
        percent (0–100).
    drop_normal : bool
        Whether to exclude rows classified as ``normal`` from the output.

    Returns
    -------
    pandas.DataFrame
        A dataframe with columns ``date``, ``event_type``, and ``reason``.
    """
    df = df.copy()
    if "date" not in df.columns:
        raise ValueError("input dataframe must contain a 'date' column")
    if not pd.api.types.is_datetime64_any_dtype(df["date"]):
        df["date"] = pd.to_datetime(df["date"])

    results = []
    for _, row in df.iterrows():
        res = _classify_row(
            row,
            ndvi_thresh,
            ndmi_thresh,
            ndmi_strong_thresh,
            msi_thresh,
            msi_strong_thresh,
            ndre_thresh,
            ndre_strong_thresh,
            evi_thresh,
            gndvi_thresh,
            precip_thresh,
            rainlong_thresh,
            humidity_thresh,
            ndmi_wet_thresh,
            tmean_hot_thresh,
            tmean_cold_thresh,
            rh_high_thresh,
            rh_low_thresh,
            evi_cover_thresh,
        )
        results.append(res)

    out = pd.DataFrame({"date": df["date"], "event_type": [r["event_type"] for r in results], "reason": [r["reason"] for r in results]})
    if drop_normal:
        out = out[out["event_type"] != "normal"].reset_index(drop=True)
    return out
