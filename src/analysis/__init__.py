"""
Analysis package for composite alerts.

This package provides routines for building seasonal baselines from multi‑year
remote sensing data and classifying multi‑source anomalies using a rule‑based
approach.  The composite alert logic integrates multiple vegetation indices
(NDVI, NDMI, NDRE, EVI, GNDVI, MSI) along with aggregated weather features
(e.g. 7‑day precipitation, temperature and humidity) to infer the most
probable type of stress affecting crops.  It is designed to operate on the
merged dataset produced by ``merge_data.merge_weather_ndvi`` and does not
require any external dependencies beyond pandas.

Usage example::

    from src.analysis.composite_alerts import detect_composite_alerts
    import pandas as pd

    df = pd.read_csv("data/processed/merged.csv", parse_dates=['date'])
    alerts = detect_composite_alerts(df)
    alerts.to_csv("data/processed/alerts_composite.csv", index=False)

The module defines a simple rule base with thresholds drawn from literature
sources (e.g. Alabama Extension ANR‑3180) for each index.  These can be
tuned via function arguments when calling the detection routine.
"""

from ..transform.composite_alerts import detect_composite_alerts  # noqa: F401
