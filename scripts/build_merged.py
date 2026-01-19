"""
build_merged.py
================

Script to merge weather data and remote‑sensing indices into a single
processed CSV.  It wraps the ``merge_weather_ndvi`` function from
``src.transform.merge_data`` and writes the resulting file to the location
configured in ``config/config.yml``.

Example usage::

    python scripts/build_merged.py

You can adjust the cloud filtering threshold and whether NDVI is interpolated
by modifying the default arguments in the ``merge_weather_ndvi`` call below
or by passing parameters via your own wrapper.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.transform.merge_data import merge_weather_ndvi
from src.utils.config_loader import MERGED_CSV


def main() -> None:
    out = merge_weather_ndvi(cloud_frac_max=0.6, interpolate_ndvi=True)
    print("\n=== 合并完成 ===")
    print("输出文件：", MERGED_CSV)
    print(
        "若不想插值，把 interpolate_ndvi=False；若想更严格，可把 cloud_frac_max 调小。"
    )


if __name__ == "__main__":
    main()