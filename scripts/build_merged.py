"""
build_merged.py
================

Script to merge weather data and remote-sensing indices into a single
processed CSV. It wraps ``merge_weather_ndvi`` from
``src.transform.merge_data`` and writes the resulting file to the location
configured in ``config/config.yml``.

Example usage::

    python scripts/build_merged.py

You can adjust the cloud filtering threshold and whether indices are
interpolated by modifying the default arguments in the call below.
"""

import sys
import logging
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.transform.merge_data import merge_weather_ndvi
from src.utils.config_loader import CFG, MERGED_CSV
from src.utils.logging_utils import setup_logging_from_cfg


def main() -> None:
    setup_logging_from_cfg(CFG, app_name="build_merged")
    logger = logging.getLogger(__name__)
    try:
        out = merge_weather_ndvi(cloud_frac_max=0.6, interpolate_ndvi=True)
        logger.info("Merge complete")
        logger.info("Output: %s", MERGED_CSV)
        logger.info("Stage 01 done -> %s", MERGED_CSV)
        logger.info("To disable interpolation: interpolate_ndvi=False")
    except Exception as exc:
        logger.error("Stage 01 failed: %s", exc)
        logger.debug("Stage 01 exception detail", exc_info=exc)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
