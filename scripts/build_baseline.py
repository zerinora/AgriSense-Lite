# -*- coding: utf-8 -*-
"""
build_baseline.py
===================

This script serves as a thin wrapper around the baseline analysis module.  It
reads configuration values from ``config/config.yml`` via the
``config_loader`` helper and passes them into the baseline functions.  By
externalising these parameters into the configuration, we avoid hard‑coding
year ranges, thresholds and other hyper‑parameters in the code.

Example usage::

    python scripts/build_baseline.py

The resulting baseline and alerts CSV files will be written into
``data/processed`` as defined by the configuration.
"""

import sys
from pathlib import Path

# Ensure the project root is on the path so that src imports work when
# executing this script directly.
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.analysis.baseline import baseline_and_alerts
from src.utils.config_loader import CFG


def main() -> None:
    """Read parameters from the config and run the baseline analysis."""
    # Fetch baseline smoothing window and training years
    smooth_window = CFG.get("baseline", {}).get("smooth_window", 15)
    train_years = CFG.get("baseline", {}).get("train_years")
    # Alerts configuration
    alerts_cfg = CFG.get("alerts", {})
    dev_thresh = alerts_cfg.get("dev_thresh", -0.05)
    min_run = alerts_cfg.get("min_run", 6)
    precip7_max = alerts_cfg.get("precip7_max")
    target_years = alerts_cfg.get("target_years")

    paths = baseline_and_alerts(
        smooth_window=smooth_window,
        dev_thresh=dev_thresh,
        min_run=min_run,
        precip7_max=precip7_max,
        train_years=train_years,
        target_years=target_years,
    )
    print("\n=== 基线 & 告警完成 ===")
    for k, v in paths.items():
        print(f"{k}: {v}")


if __name__ == "__main__":
    main()