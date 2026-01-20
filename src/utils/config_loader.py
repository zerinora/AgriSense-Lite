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
import json
import logging
from datetime import date, datetime
import yaml

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
    _apply_period_defaults(cfg)
    _validate_region(cfg)
    p = cfg.get("paths", {})
    for k in ("data_raw", "data_processed", "assets", "logs"):
        if k in p:
            p[k] = str((ROOT / p[k]).resolve())
    cfg["paths"] = p
    return cfg


def _parse_date(value: str, field: str) -> date:
    try:
        return datetime.fromisoformat(str(value)).date()
    except ValueError as exc:
        raise ValueError(f"Invalid date for {field}: {value}") from exc


def _apply_period_defaults(cfg: dict) -> None:
    period = cfg.get("period", {}) if isinstance(cfg, dict) else {}
    data_start = period.get("data_start") or period.get("start_date")
    data_end = period.get("data_end") or period.get("end_date")
    if not data_start or not data_end:
        raise ValueError("period.data_start/data_end (or start_date/end_date) must be set")
    report_start = period.get("report_start") or data_start
    report_end = period.get("report_end") or data_end

    data_start_dt = _parse_date(data_start, "period.data_start")
    data_end_dt = _parse_date(data_end, "period.data_end")
    report_start_dt = _parse_date(report_start, "period.report_start")
    report_end_dt = _parse_date(report_end, "period.report_end")

    if data_start_dt > data_end_dt:
        raise ValueError("period.data_start must be <= period.data_end")
    if report_start_dt > report_end_dt:
        raise ValueError("period.report_start must be <= period.report_end")
    if report_start_dt < data_start_dt or report_end_dt > data_end_dt:
        raise ValueError("period.report_* must be within period.data_* range")

    period["data_start"] = data_start_dt.isoformat()
    period["data_end"] = data_end_dt.isoformat()
    period["report_start"] = report_start_dt.isoformat()
    period["report_end"] = report_end_dt.isoformat()
    period["start_date"] = period["data_start"]
    period["end_date"] = period["data_end"]
    cfg["period"] = period


def _point_in_ring(point: tuple[float, float], ring: list[list[float]]) -> bool:
    x, y = point
    inside = False
    n = len(ring)
    for i in range(n):
        x1, y1 = ring[i]
        x2, y2 = ring[(i + 1) % n]
        if ((y1 > y) != (y2 > y)) and (x < (x2 - x1) * (y - y1) / (y2 - y1 + 1e-12) + x1):
            inside = not inside
    return inside


def _point_in_polygon(point: tuple[float, float], coords: list) -> bool:
    if not coords:
        return False
    outer = coords[0]
    if not _point_in_ring(point, outer):
        return False
    for hole in coords[1:]:
        if _point_in_ring(point, hole):
            return False
    return True


def _validate_region(cfg: dict) -> None:
    region = cfg.get("region", {}) if isinstance(cfg, dict) else {}
    center_lat = region.get("center_lat")
    center_lon = region.get("center_lon")
    if center_lat is None or center_lon is None:
        return
    point = (float(center_lon), float(center_lat))

    polygon_path = region.get("roi_polygon_geojson")
    if polygon_path:
        path = Path(polygon_path)
        if not path.is_absolute():
            path = (ROOT / path).resolve()
        if not path.exists():
            raise FileNotFoundError(f"roi_polygon_geojson not found: {path}")
        with open(path, "r", encoding="utf-8") as f:
            geo = json.load(f)
        geom = geo.get("geometry") or {}
        if geo.get("type") == "Feature":
            geom = geo.get("geometry") or {}
        if geo.get("type") == "FeatureCollection":
            features = geo.get("features") or []
            geom = (features[0] or {}).get("geometry") or {}
        gtype = geom.get("type")
        coords = geom.get("coordinates")
        if gtype == "Polygon":
            inside = _point_in_polygon(point, coords or [])
        elif gtype == "MultiPolygon":
            inside = any(_point_in_polygon(point, poly) for poly in (coords or []))
        else:
            raise ValueError("roi_polygon_geojson must be Polygon or MultiPolygon")
        if not inside:
            logging.getLogger(__name__).warning(
                "center point is outside roi_polygon_geojson"
            )
        return

    bbox = region.get("roi_rectangle")
    if bbox and len(bbox) == 4:
        min_lon, min_lat, max_lon, max_lat = bbox
        inside = min_lon <= point[0] <= max_lon and min_lat <= point[1] <= max_lat
        if not inside:
            logging.getLogger(__name__).warning(
                "center point is outside roi_rectangle"
            )


CFG = load_config()

DATA_RAW = Path(CFG["paths"]["data_raw"])
DATA_PROCESSED = Path(CFG["paths"]["data_processed"])
ASSETS = Path(CFG["paths"]["assets"])
LOGS = Path(CFG["paths"]["logs"])

for d in (DATA_RAW, DATA_PROCESSED, ASSETS, LOGS):
    d.mkdir(parents=True, exist_ok=True)

PERIOD_DATA_START = datetime.fromisoformat(CFG["period"]["data_start"]).date()
PERIOD_DATA_END = datetime.fromisoformat(CFG["period"]["data_end"]).date()
PERIOD_REPORT_START = datetime.fromisoformat(CFG["period"]["report_start"]).date()
PERIOD_REPORT_END = datetime.fromisoformat(CFG["period"]["report_end"]).date()

gee_cfg = CFG.get("gee_s2", {})
_indices_outfile = gee_cfg.get("indices_outfile") or gee_cfg.get("ndvi_outfile")
if _indices_outfile is None:
    raise KeyError(
        "No indices or NDVI output file defined in config under 'gee_s2'. "
        "Please specify either 'indices_outfile' or 'ndvi_outfile'."
    )
INDICES_CSV = (ROOT / _indices_outfile).resolve()
NDVI_CSV = INDICES_CSV

WEATHER_CSV = (ROOT / CFG.get("open_meteo", {}).get("outfile", "data/raw/weather.csv")).resolve()
MERGED_CSV = (ROOT / CFG.get("merge", {}).get("outfile", "data/processed/01_merged.csv")).resolve()
ALERTS_RAW_CSV = (
    ROOT
    / CFG.get("composite_alerts", {}).get(
        "outfile_raw", "data/processed/03_alerts_raw.csv"
    )
).resolve()
ALERTS_GATED_CSV = (
    ROOT
    / CFG.get("composite_alerts", {}).get(
        "outfile", "data/processed/04_alerts_gated.csv"
    )
).resolve()
ALERTS_MERGED_CSV = (
    ROOT
    / CFG.get("composite_alerts", {}).get(
        "outfile_merged", "data/processed/05_events.csv"
    )
).resolve()
RS_DEBUG_CSV = (
    ROOT
    / CFG.get("composite_alerts", {}).get(
        "outfile_debug", "data/processed/02_rs_debug.csv"
    )
).resolve()
