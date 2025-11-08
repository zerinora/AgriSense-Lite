"""
build_composite_alerts.py
=========================

This script reads the merged data set (``data/processed/merged.csv``), applies
the composite alert detection rules defined in
``src/analysis/composite_alerts.py``, and writes the resulting event list
to ``data/processed/alerts_composite.csv``.  It is intended as a thin
wrapper so that end users do not need to import any internal modules; run
``python scripts/build_composite_alerts.py`` from the project root and
the script will take care of locating the correct inputs and producing
the output.

If you need to customise thresholds or detection logic, edit
``src/analysis/composite_alerts.py`` and adjust the default arguments in
``detect_composite_alerts``.
"""

from __future__ import annotations

import os
from pathlib import Path
import sys
import pandas as pd

# Determine project root (two directories up from this script)
ROOT = Path(__file__).resolve().parents[1]

def main() -> Path:
    """Run the composite alert detection and write the CSV.

    Returns
    -------
    pathlib.Path
        The path to the written alerts file.
    """
    # Change working directory to the project root so that relative paths resolve
    os.chdir(ROOT)

    # Make sure the src package is on sys.path
    src_path = ROOT / "src"
    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))

    # Import the detection function.  The composite_alerts module does not
    # expose a ``load_merged`` helper, so we import only detect_composite_alerts.
    try:
        from analysis.composite_alerts import detect_composite_alerts
    except ImportError:
        from transform.composite_alerts import detect_composite_alerts  # type: ignore

    # Load the merged data directly from CSV.  The detection function expects
    # columns such as ndvi_mean_daily, ndmi_mean, ndre_mean, etc.  If your
    # merged.csv has different column names, adjust them here.
    merged_path = Path("data/processed/merged.csv")
    if not merged_path.exists():
        raise FileNotFoundError(f"Merged data not found at {merged_path}")
    df = pd.read_csv(merged_path, parse_dates=["date"])

    # Run the detection
    alerts = detect_composite_alerts(df)

    # Write output
    out_path = Path("data/processed/alerts_composite.csv")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    alerts.to_csv(out_path, index=False)
    print(f"[OK] Composite alerts saved to {out_path}")
    return out_path

if __name__ == "__main__":
    main()