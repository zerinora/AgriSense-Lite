# -*- coding: utf-8 -*-
"""
config_loader.py
=================

This helper centralises reading of the ``config/config.yml`` file and
provides convenient access to commonly used paths.  In addition to the
standard locations (``data/raw``, ``data/processed``, ``assets``, ``logs``),
it also resolves the output file for remote‑sensing indices.  Historically
this project only produced a single NDVI CSV file via the Sentinel‑2
preprocessing step; in later versions the same file may contain multiple
indices (NDVI, NDMI, NDRE, EVI, GNDVI, MSI).  To support both scenarios
without changing downstream code, the loader will prioritise
``gee_s2.indices_outfile`` if present, otherwise fall back to
``gee_s2.ndvi_outfile``.  For backward compatibility we expose both
``INDICES_CSV`` and ``NDVI_CSV`` variables, pointing to the same resolved
location.

Usage::

    from src.utils.config_loader import CFG, INDICES_CSV, NDVI_CSV
    print(INDICES_CSV)  # Path to the indices CSV defined in config

"""

from __future__ import annotations

from pathlib import Path
import yaml

# Determine the project root (two levels above this file)
ROOT = Path(__file__).resolve().parents[2]


def load_config(path: Path | None = None) -> dict:
    """Read and parse the YAML configuration file.

    Parameters
    ----------
    path : pathlib.Path or None, optional
        Path to a YAML configuration file.  If ``None``, defaults to
        ``ROOT / 'config/config.yml'``.

    Returns
    -------
    dict
        Parsed configuration dictionary with selected paths made absolute.
    """
    path = path or (ROOT / "config" / "config.yml")
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    # Convert relative path strings to absolute paths for key sections
    p = cfg.get("paths", {})
    for k in ("data_raw", "data_processed", "assets", "logs"):
        if k in p:
            p[k] = str((ROOT / p[k]).resolve())
    cfg["paths"] = p
    return cfg


# Load the configuration on import
CFG = load_config()

DATA_RAW = Path(CFG["paths"]["data_raw"])
DATA_PROCESSED = Path(CFG["paths"]["data_processed"])
ASSETS = Path(CFG["paths"]["assets"])
LOGS = Path(CFG["paths"]["logs"])

# Always ensure the directories exist
for d in (DATA_RAW, DATA_PROCESSED, ASSETS, LOGS):
    d.mkdir(parents=True, exist_ok=True)

# Resolve the indices CSV
gee_cfg = CFG.get("gee_s2", {})
# Support both ``indices_outfile`` and legacy ``ndvi_outfile`` keys
_indices_outfile = gee_cfg.get("indices_outfile") or gee_cfg.get("ndvi_outfile")
if _indices_outfile is None:
    raise KeyError(
        "No indices or NDVI output file defined in config under 'gee_s2'. "
        "Please specify either 'indices_outfile' or 'ndvi_outfile'."
    )
INDICES_CSV = (ROOT / _indices_outfile).resolve()
# Backward compatibility: NDVI_CSV references the same file
NDVI_CSV = INDICES_CSV

WEATHER_CSV = (ROOT / CFG.get("open_meteo", {}).get("outfile", "data/raw/weather.csv")).resolve()
MERGED_CSV = (ROOT / CFG.get("merge", {}).get("outfile", "data/processed/merged.csv")).resolve()