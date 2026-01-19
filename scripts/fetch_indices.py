#!/usr/bin/env python3
"""
fetch_indices.py
=================

This script uses the Google Earth Engine (GEE) Python API to download time-series
spectral indices for the region of interest (ROI) defined in ``config/config.yml``.
The indices include NDVI, NDMI, NDRE, EVI, GNDVI and MSI, computed from the
Sentinel-2 Level-2A (surface reflectance) collection.  The output is a CSV file
written to ``data/raw/indices.csv`` with one row per acquisition date and one
column per index.

To use this script you must install the ``earthengine-api`` and ``geemap``
packages and authenticate with your Earth Engine account (``ee.Authenticate()``
and ``ee.Initialize()``).  See the README for more details on setting up GEE
credentials.

Example usage::

    python fetch_indices.py

You can run this script as part of your data pipeline before merging with
weather data and building baselines.  If you extend or modify the set of
indices, adjust the ``add_indices`` function below.
"""

from __future__ import annotations

import os
from typing import List, Dict, Any, Optional

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd

try:
    import ee  # type: ignore
except ImportError as exc:
    raise ImportError(
        "Google Earth Engine API is required. Install with `pip install earthengine-api`."
    ) from exc

geemap = None

from src.utils.config_loader import CFG


def authenticate_ee(project_id: Optional[str] = None) -> None:
    """Authenticate and initialize the Earth Engine API.

    This helper tries to initialize the Earth Engine client.  If the caller
    specifies a ``project_id`` (either explicitly or via configuration), it
    will be passed to :func:`ee.Initialize`.  If initialization fails
    (for example, because the user has not authenticated or because a
    project ID is required but missing), the function will prompt for
    interactive authentication via :func:`ee.Authenticate` and then retry
    initialization with the provided project ID.

    Args:
        project_id: Optional ID of the Google Cloud project to associate
            with this session.  Starting from 2024, Earth Engine accounts
            must be linked to a Cloud project.  See ``config/config.yml``
            (``gee.project``) or the ``EE_PROJECT`` environment variable.
    """
    project = project_id or os.environ.get('EE_PROJECT') or None
    try:
        if project:
            ee.Initialize(project=project)
        else:
            ee.Initialize()
    except Exception:
        ee.Authenticate()
        if project:
            ee.Initialize(project=project)
        else:
            ee.Initialize()


def mask_s2_sr_clouds(img: ee.Image) -> ee.Image:
    """Mask clouds and cirrus based on the QA60 band of Sentinel-2 SR.

    Args:
        img: A Sentinel-2 surface reflectance image.

    Returns:
        The image with cloudy pixels masked out.
    """
    qa = img.select('QA60')
    cloud_bit_mask = 1 << 10
    cirrus_bit_mask = 1 << 11
    mask = qa.bitwiseAnd(cloud_bit_mask).eq(0).And(
        qa.bitwiseAnd(cirrus_bit_mask).eq(0)
    )
    return img.updateMask(mask)


def add_indices(img: ee.Image) -> ee.Image:
    """Compute vegetation/water indices and add them as bands.

    The indices computed are:

    - NDVI  = (B8 - B4) / (B8 + B4)
    - NDMI  = (B8 - B11) / (B8 + B11)
    - NDRE  = (B8 - B5) / (B8 + B5)
    - EVI   = 2.5 * (B8 - B4) / (B8 + 6 * B4 - 7.5 * B2 + 1)
    - GNDVI = (B8 - B3) / (B8 + B3)
    - MSI   = B11 / B8

    Args:
        img: A Sentinel-2 SR image.

    Returns:
        The input image with new bands attached.
    """
    b2 = img.select('B2')
    b3 = img.select('B3')
    b4 = img.select('B4')
    b5 = img.select('B5')
    b8 = img.select('B8')
    b11 = img.select('B11')

    ndvi = b8.subtract(b4).divide(b8.add(b4)).rename('NDVI')
    ndmi = b8.subtract(b11).divide(b8.add(b11)).rename('NDMI')
    ndre = b8.subtract(b5).divide(b8.add(b5)).rename('NDRE')
    evi = b8.subtract(b4).multiply(2.5).divide(
        b8.add(b4.multiply(6)).subtract(b2.multiply(7.5)).add(1.0)
    ).rename('EVI')
    gndvi = b8.subtract(b3).divide(b8.add(b3)).rename('GNDVI')
    msi = b11.divide(b8).rename('MSI')

    return img.addBands([ndvi, ndmi, ndre, evi, gndvi, msi])


def extract_feature(img: ee.Image, region: ee.Geometry, scale: int = 10) -> ee.Feature:
    """Reduce an image over a region to a feature containing mean indices.

    Args:
        img: A Sentinel-2 image already containing index bands.
        region: An EE geometry defining the ROI.
        scale: Pixel resolution in metres for reduction (default 10 m).

    Returns:
        An EE Feature with properties ``date`` (string) and each index mean.
    """
    date_str = img.date().format('yyyy-MM-dd')
    stats = img.select(['NDVI', 'NDMI', 'NDRE', 'EVI', 'GNDVI', 'MSI']).reduceRegion(
        reducer=ee.Reducer.mean(),
        geometry=region,
        scale=scale,
        maxPixels=1_000_000_000,
    )
    return ee.Feature(None, stats).set('date', date_str)


def fetch_indices() -> pd.DataFrame:
    """Fetch spectral indices time series for the configured ROI and period.

    Reads settings from ``config/config.yml`` via ``src.utils.config_loader.CFG``.
    Returns a pandas DataFrame with columns ``date``, ``ndvi_mean``, ``ndmi_mean``,
    ``ndre_mean``, ``evi_mean``, ``gndvi_mean``, ``msi_mean``.
    """
    gee_cfg = CFG.get('gee', {}) or CFG.get('gee_s2', {}) or {}
    project_id = gee_cfg.get('project') or os.environ.get('EE_PROJECT')
    authenticate_ee(project_id)

    region_cfg = CFG['region']
    period_cfg = CFG['period']
    gee_cfg = CFG.get('gee', {}) or CFG.get('gee_s2', {}) or {}

    start_date = period_cfg['start_date']
    end_date = period_cfg['end_date']
    try:
        import datetime
        if isinstance(start_date, (datetime.date, datetime.datetime)):
            start_date = start_date.isoformat()
        else:
            start_date = str(start_date)
        if isinstance(end_date, (datetime.date, datetime.datetime)):
            end_date = end_date.isoformat()
        else:
            end_date = str(end_date)
    except Exception:
        start_date = str(start_date)
        end_date = str(end_date)
    roi_rect = region_cfg['roi_rectangle']

    collection_name = gee_cfg.get('collection', 'COPERNICUS/S2_SR_HARMONIZED')

    region = ee.Geometry.Rectangle(roi_rect)

    collection = (
        ee.ImageCollection(collection_name)
        .filterBounds(region)
        .filterDate(start_date, end_date)
        .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 80))
        .map(mask_s2_sr_clouds)
        .map(add_indices)
    )

    count = collection.size().getInfo()
    try:
        print(f"[INFO] {count} images found in the collection. Starting processing...")
    except Exception:
        pass
    img_list = collection.toList(count)
    records: List[Dict[str, Any]] = []
    for i in range(count):
        if (i == 0) or ((i + 1) % 10 == 0) or (i + 1 == count):
            try:
                print(f"[INFO] Processing image {i + 1}/{count}...")
            except Exception:
                pass
        img = ee.Image(img_list.get(i))
        date_str: str = img.date().format('yyyy-MM-dd').getInfo()  # type: ignore
        stats: Dict[str, Any] = img.select(['NDVI', 'NDMI', 'NDRE', 'EVI', 'GNDVI', 'MSI']).reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=region,
            scale=10,
            maxPixels=1_000_000_000,
        ).getInfo()
        record: Dict[str, Any] = {'date': date_str}
        for orig_name, out_name in [
            ('NDVI', 'ndvi_mean'),
            ('NDMI', 'ndmi_mean'),
            ('NDRE', 'ndre_mean'),
            ('EVI', 'evi_mean'),
            ('GNDVI', 'gndvi_mean'),
            ('MSI', 'msi_mean'),
        ]:
            value = stats.get(orig_name)
            record[out_name] = value if value is not None else None
        records.append(record)

    df = pd.DataFrame(records)

    rename_map = {
        'NDVI': 'ndvi_mean',
        'NDMI': 'ndmi_mean',
        'NDRE': 'ndre_mean',
        'EVI': 'evi_mean',
        'GNDVI': 'gndvi_mean',
        'MSI': 'msi_mean',
    }
    df = df.rename(columns=rename_map)

    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').reset_index(drop=True)

    for col in rename_map.values():
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    return df


def main() -> None:
    """Entry point for script execution."""
    df = fetch_indices()
    outdir = os.path.join(CFG['paths']['data_raw'])
    os.makedirs(outdir, exist_ok=True)
    outfile = os.path.join(outdir, 'indices.csv')
    df.to_csv(outfile, index=False, encoding='utf-8', float_format='%.4f')
    print(f"[OK] Spectral indices saved to {outfile}")


if __name__ == '__main__':
    main()