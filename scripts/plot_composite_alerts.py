"""
plot_composite_alerts.py
========================

This script visualises the results of the composite alert detection.  It
combines vegetation indices (NDVI, EVI) with short‑term precipitation
information and overlays coloured markers for each detected event type.
The plot is saved to ``assets/composite_alerts.png``.

Usage::

    python scripts/plot_composite_alerts.py

Make sure ``build_composite_alerts.py`` has been run first so that
``data/processed/alerts_composite.csv`` exists.
"""

from __future__ import annotations

import os
from pathlib import Path
import sys
import pandas as pd
import matplotlib.pyplot as plt

# Change working directory to project root
ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)

def load_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load merged data and alerts.

    Returns
    -------
    tuple of (merged DataFrame, alerts DataFrame)
    """
    merged_path = Path("data/processed/merged.csv")
    alerts_path = Path("data/processed/alerts_composite.csv")
    if not merged_path.exists():
        raise FileNotFoundError(f"Merged data not found at {merged_path}")
    if not alerts_path.exists():
        raise FileNotFoundError(f"Alerts file not found at {alerts_path}")
    merged = pd.read_csv(merged_path, parse_dates=["date"])
    alerts = pd.read_csv(alerts_path, parse_dates=["date"])
    return merged, alerts


def main() -> Path:
    merged, alerts = load_data()

    fig, ax1 = plt.subplots(figsize=(12, 6))

    # Plot vegetation indices on the primary axis
    has_ndvi = 'ndvi_mean_daily' in merged.columns
    has_evi = 'evi_mean' in merged.columns
    y_vals = []
    if has_ndvi:
        ax1.plot(merged['date'], merged['ndvi_mean_daily'], label='NDVI', color='darkgreen', linewidth=1.2)
        y_vals.append(merged['ndvi_mean_daily'])
    if has_evi:
        ax1.plot(merged['date'], merged['evi_mean'], label='EVI', color='royalblue', linewidth=1.2)
        y_vals.append(merged['evi_mean'])
    ax1.set_ylabel('Vegetation Index')
    ax1.set_xlabel('Date')

    # Plot 7‑day precipitation on secondary axis
    if 'precip_7d' in merged.columns:
        ax2 = ax1.twinx()
        ax2.plot(merged['date'], merged['precip_7d'], label='7d Precip (mm)', color='lightblue', linewidth=1.0, alpha=0.6)
        ax2.set_ylabel('7‑Day Precip (mm)')
        ax2.tick_params(axis='y', labelcolor='lightblue')
    else:
        ax2 = None

    # Mark events on the NDVI/EVI lines
    event_colors = {
        'drought': 'red',
        'waterlogging': 'cyan',
        'nutrient_or_pest': 'orange',
        'heat_stress': 'purple',
        'cold_stress': 'blue',
    }
    # Prepare baseline y values for markers (we pick NDVI if available, else EVI)
    if y_vals:
        base_series = y_vals[0]
    else:
        base_series = pd.Series([0] * len(merged), index=merged.index)

    for event_type, group in alerts.groupby('event_type'):
        color = event_colors.get(event_type, 'black')
        # Determine y position: use NDVI or EVI values at event dates; if missing, use NaN
        y_positions = []
        for d in group['date']:
            row = merged.loc[merged['date'] == d]
            if not row.empty:
                if has_ndvi:
                    y_positions.append(row.iloc[0]['ndvi_mean_daily'])
                elif has_evi:
                    y_positions.append(row.iloc[0]['evi_mean'])
                else:
                    y_positions.append(float('nan'))
            else:
                y_positions.append(float('nan'))
        ax1.scatter(group['date'], y_positions, marker='o', color=color, label=event_type.capitalize())

    # Combine legends (avoid duplicates)
    handles, labels = ax1.get_legend_handles_labels()
    if ax2 is not None:
        h2, l2 = ax2.get_legend_handles_labels()
        handles += h2
        labels += l2
    # Remove duplicate labels
    unique = dict()
    new_handles = []
    new_labels = []
    for h, l in zip(handles, labels):
        if l not in unique:
            unique[l] = h
            new_handles.append(h)
            new_labels.append(l)
    ax1.legend(new_handles, new_labels, loc='upper left', fontsize=8)
    fig.tight_layout()

    out_path = Path('assets') / 'composite_alerts.png'
    out_path.parent.mkdir(exist_ok=True)
    fig.savefig(out_path, dpi=300)
    print(f"[OK] Plot saved to {out_path}")
    return out_path

if __name__ == '__main__':
    main()