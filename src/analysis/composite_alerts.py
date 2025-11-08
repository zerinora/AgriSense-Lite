"""
Composite alert detection module.

This module implements a rule‑based approach for classifying crop stress
events using multiple vegetation indices and basic weather features.  The
classification scheme draws on published agronomic thresholds for each
index (for example, NDMI values above ~0.4 indicate a wet canopy while
values below ~0.2 signal water stress【844326301735958†L142-L158】; NDRE values
below ~0.3 reflect low chlorophyll or nutrient stress【675122315655608†L140-L156】).
By combining these indices with short‑term precipitation and temperature
aggregates we infer whether a day is subject to drought (water stress),
nutrient deficiency/pest/disease stress, waterlogging, heat stress,
cold stress or normal conditions.  Users can customise the thresholds via
keyword arguments.

The core function, :func:`detect_composite_alerts`, expects a pandas
``DataFrame`` with at least the following columns:

* ``date`` (datetime): daily timestamp
* ``ndvi_mean_daily`` (float): day‑interpolated NDVI
* ``ndmi_mean`` (float): Normalised Difference Moisture Index
* ``msi_mean`` (float): Moisture Stress Index
* ``ndre_mean`` (float): Normalised Difference Red Edge Index
* ``evi_mean`` (float): Enhanced Vegetation Index
* ``gndvi_mean`` (float): Green NDVI
* ``precip_7d`` (float): 7‑day cumulative precipitation (mm)
* ``tmean_7d`` (float): 7‑day mean temperature (°C)
* ``relative_humidity_2m_mean`` (float): daily mean relative humidity (%), optional

    It returns a new DataFrame with one row per input date and the following
    columns:

* ``date``: the original date
    * ``event_type``: one of ``['drought', 'nutrient_or_pest', 'waterlogging', 'heat_stress', 'cold_stress', 'normal']``
    * ``reason``: textual description of which rule(s) triggered the classification

.. note::
    This rule set is deliberately simple and interpretable.  It does not
    replace expert agronomic advice nor exhaustive in‑field scouting.  It
    merely combines remotely sensed indices and basic weather aggregates to
    triage potential areas of concern.
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
    evi_cover_thresh: float = 0.25,
) -> Dict[str, str]:
    """Classify a single day's conditions.

    This routine evaluates each index and simple weather metrics to assign
    one of several event types.  It distinguishes drought/water stress,
    waterlogging (excess moisture), heat stress, cold stress and
    nutrient/pest stresses.  Thresholds are informed by agronomic
    literature – for example, NDMI below ~0.2 signals moisture
    deficiency while values above ~0.4 indicate a wet canopy【844326301735958†L142-L158】.  NDRE
    below ~0.3 or GNDVI below ~0.5 can indicate nutrient deficiency or
    disease【675122315655608†L140-L156】.  Users may adjust these parameters to
    suit local conditions.

    Parameters
    ----------
    row : pd.Series
        A single row of the merged data containing index and weather values.
    ndvi_thresh : float
        NDVI threshold below which canopy cover is considered sparse (for
        waterlogging detection).  Typical values ~0.4.
    ndmi_thresh : float
        NDMI value below which mild water stress is suspected when rainfall
        is low.  Typical values ~0.2【844326301735958†L142-L158】.
    ndmi_strong_thresh : float
        NDMI value below which severe drought is flagged regardless of
        rainfall.  Typical values ~0.1.
    msi_thresh, msi_strong_thresh : float
        MSI thresholds signalling mild and severe drought (higher MSI
        indicates greater water stress).
    ndre_thresh, ndre_strong_thresh : float
        NDRE thresholds for mild and severe chlorophyll stress.  Lower
        values indicate nitrogen deficiency or disease【675122315655608†L140-L156】.
    evi_thresh : float
        EVI threshold for general canopy stress.
    gndvi_thresh : float
        GNDVI threshold for early stress detection.
    precip_thresh : float
        7‑day precipitation threshold below which drought conditions are
        inferred when NDMI/MSI indicate moisture stress.  Typical value
        ~15 mm.
    rainlong_thresh : float
        7‑day precipitation threshold above which waterlogging is inferred
        when NDMI is high and NDVI is low.  Typical value ~60 mm.
    humidity_thresh : float
        Relative humidity fraction (0–1) above which nutrient/pest events
        are more likely disease/pest rather than nutrient deficiency.  The
        value is compared against RH expressed in percent (0–100).
    ndmi_wet_thresh : float
        NDMI threshold above which canopy is considered wet; used in
        waterlogging detection.  Typical value ~0.4【844326301735958†L142-L158】.
    tmean_hot_thresh : float
        7‑day mean temperature threshold for heat stress (°C).  Defaults
        to 30 °C.
    tmean_cold_thresh : float
        7‑day mean temperature threshold for cold stress (°C).  Defaults
        to 5 °C.
    rh_high_thresh : float
        Relative humidity fraction above which high humidity stresses may
        exacerbate diseases.  Defaults to 0.75 (75 %).
    rh_low_thresh : float
        Relative humidity fraction below which dry air may intensify
        drought impacts.  Defaults to 0.30 (30 %).

    Returns
    -------
    Dict[str, str]
        A mapping with keys ``event_type`` and ``reason``.
    """
    reasons: List[str] = []

    # Extract values; fall back to NaN if column missing
    ndvi = row.get("ndvi_mean_daily")
    ndmi = row.get("ndmi_mean")
    msi = row.get("msi_mean")
    ndre = row.get("ndre_mean")
    evi = row.get("evi_mean")
    gndvi = row.get("gndvi_mean")
    precip = row.get("precip_7d")
    tmean = row.get("tmean_7d")
    rh = row.get("relative_humidity_2m_mean")

    # Flags initialised to False
    drought_flag = False
    waterlog_flag = False
    heat_flag = False
    cold_flag = False
    nutrient_flag = False

    # Drought/water stress detection
    # Only evaluate drought if there is reasonable canopy cover (EVI or NDVI above threshold)
    canopy_ok = False
    if pd.notna(evi) and evi >= evi_cover_thresh:
        canopy_ok = True
    elif pd.notna(ndvi) and ndvi >= ndvi_thresh:
        canopy_ok = True

    # Strong drought: NDMI far below normal (< strong thresh) or MSI very high
    if canopy_ok:
        if pd.notna(ndmi) and ndmi < ndmi_strong_thresh:
            drought_flag = True
            reasons.append(f"NDMI={ndmi:.3f}<strong drought thresh {ndmi_strong_thresh}")
        elif pd.notna(msi) and msi > msi_strong_thresh:
            drought_flag = True
            reasons.append(f"MSI={msi:.3f}>strong drought thresh {msi_strong_thresh}")
        else:
            # Mild drought: NDMI moderately low or MSI moderately high AND low rainfall
            if pd.notna(ndmi) and ndmi < ndmi_thresh:
                if pd.notna(precip) and precip < precip_thresh:
                    drought_flag = True
                    reasons.append(
                        f"NDMI={ndmi:.3f}<drought thresh {ndmi_thresh} with precip_7d={precip:.1f}mm< {precip_thresh}"
                    )
            if not drought_flag and pd.notna(msi) and msi > msi_thresh:
                if pd.notna(precip) and precip < precip_thresh:
                    drought_flag = True
                    reasons.append(
                        f"MSI={msi:.3f}>drought thresh {msi_thresh} with precip_7d={precip:.1f}mm< {precip_thresh}"
                    )

    # Waterlogging: NDMI above wet threshold (wet canopy) AND NDVI below cover threshold AND high rainfall
    if not drought_flag:
        if pd.notna(ndmi) and ndmi > ndmi_wet_thresh and pd.notna(ndvi) and ndvi < ndvi_thresh:
            if pd.notna(precip) and precip > rainlong_thresh:
                waterlog_flag = True
                reasons.append(
                    f"NDMI={ndmi:.3f}>wet thresh {ndmi_wet_thresh} & NDVI={ndvi:.3f}< {ndvi_thresh} with precip_7d={precip:.1f}mm> {rainlong_thresh}"
                )

    # Heat stress: high mean temperature and negative EVI deviation (slowing growth) with low humidity
    if pd.notna(tmean) and tmean > tmean_hot_thresh:
        # Use relative humidity if available; if absent or low (<rh_low_thresh) treat as dry heat
        if (pd.isna(rh) or (rh < rh_low_thresh * 100)):
            # Optionally require EVI drop (<evi_thresh) to avoid spurious flags
            if pd.notna(evi) and evi < evi_thresh:
                heat_flag = True
                reasons.append(
                    f"tmean_7d={tmean:.1f}°C> {tmean_hot_thresh} & EVI={evi:.3f}< {evi_thresh}"
                )

    # Cold stress: low temperature and high humidity causing cold damage (winter crops)
    if pd.notna(tmean) and tmean < tmean_cold_thresh:
        if pd.notna(rh) and rh > rh_high_thresh * 100:
            cold_flag = True
            reasons.append(
                f"tmean_7d={tmean:.1f}°C< {tmean_cold_thresh} & RH={rh:.0f}%> {rh_high_thresh*100:.0f}%"
            )

    # Nutrient deficiency or pest/disease: low chlorophyll indices under adequate moisture/humidity
    # Only check if other stresses not triggered
    if not (drought_flag or waterlog_flag or heat_flag or cold_flag):
        nutrient_indicators: List[str] = []
        if pd.notna(ndre) and ndre < ndre_strong_thresh:
            nutrient_indicators.append(f"NDRE={ndre:.3f}< {ndre_strong_thresh}")
        elif pd.notna(ndre) and ndre < ndre_thresh:
            nutrient_indicators.append(f"NDRE={ndre:.3f}< {ndre_thresh}")
        if pd.notna(gndvi) and gndvi < gndvi_thresh:
            nutrient_indicators.append(f"GNDVI={gndvi:.3f}< {gndvi_thresh}")
        if pd.notna(evi) and evi < evi_thresh:
            nutrient_indicators.append(f"EVI={evi:.3f}< {evi_thresh}")
        # Determine if moisture is sufficient for nutrient/pest flag (avoid drought misclassification)
        moisture_ok = True
        if pd.notna(ndmi) and ndmi < ndmi_thresh:
            moisture_ok = False
        if pd.notna(msi) and msi > msi_thresh:
            moisture_ok = False
        if nutrient_indicators and moisture_ok:
            nutrient_flag = True
            reasons.extend(nutrient_indicators)
            # Optionally add humidity qualifier: high humidity favours disease/pest
            if pd.notna(rh) and rh > humidity_thresh * 100:
                reasons.append(
                    f"RH={rh:.0f}%> {humidity_thresh*100:.0f}%"
                )

    # Determine final event type
    if drought_flag:
        event_type = "drought"
    elif waterlog_flag:
        event_type = "waterlogging"
    elif heat_flag:
        event_type = "heat_stress"
    elif cold_flag:
        event_type = "cold_stress"
    elif nutrient_flag:
        event_type = "nutrient_or_pest"
    else:
        event_type = "normal"
    reason_str = "; ".join(reasons) if reasons else ""
    return {"event_type": event_type, "reason": reason_str}


def detect_composite_alerts(
    df: pd.DataFrame,
    train_years: Optional[List[int]] = None,
    # NDVI threshold: lower to 0.35 to permit detection of partial canopy cover
    # for waterlogging and drought checks in mixed cropping phases.
    ndvi_thresh: float = 0.35,
    # NDMI thresholds: moderate drought below 0.25; severe drought below 0.15
    # according to published NDMI ranges for water stress【73289060569815†L120-L137】.
    ndmi_thresh: float = 0.25,
    ndmi_strong_thresh: float = 0.15,
    # MSI thresholds: moderate drought above 0.8; severe drought above 1.2
    # (values >1.5 denote extreme moisture stress【73289060569815†L120-L137】).
    msi_thresh: float = 0.8,
    msi_strong_thresh: float = 1.2,
    # NDRE thresholds: nutrient/pest stress below 0.28; severe below 0.20【675122315655608†L140-L156】.
    ndre_thresh: float = 0.28,
    ndre_strong_thresh: float = 0.20,
    # EVI stress threshold remains 0.2
    evi_thresh: float = 0.2,
    # GNDVI stress threshold lowered to 0.5 (from 0.5 default) to detect early stress.
    gndvi_thresh: float = 0.5,
    # Precipitation thresholds: moderate drought if 7‑day rainfall < 20 mm;
    # this accounts for high baseline rainfall in Cwa climates.
    precip_thresh: float = 20.0,
    # Waterlogging threshold: heavy rain > 40 mm in 7 days triggers waterlogging
    # when canopy moisture is high and vegetation indices are low.
    rainlong_thresh: float = 40.0,
    humidity_thresh: float = 0.75,
    # NDMI wet threshold for waterlogging: values above 0.60 indicate a saturated canopy【73289060569815†L120-L137】.
    ndmi_wet_thresh: float = 0.60,
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
