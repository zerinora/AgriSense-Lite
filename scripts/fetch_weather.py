"""
Fetch daily weather data and save CSV + metadata.
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import List, Optional

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.fetch.open_meteo import fetch_and_save
from src.utils.config_loader import CFG, WEATHER_CSV
from src.utils.logging_utils import setup_logging_from_cfg


def configure_logging() -> str:
    return setup_logging_from_cfg(CFG, app_name="fetch_weather")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch daily weather from Open-Meteo")
    parser.add_argument("--lat", type=float, help="Latitude (override config)")
    parser.add_argument("--lon", type=float, help="Longitude (override config)")
    parser.add_argument("--start", type=str, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", type=str, help="End date YYYY-MM-DD (inclusive)")
    parser.add_argument("--timezone", type=str, help="Timezone, e.g. Asia/Shanghai")
    parser.add_argument(
        "--daily",
        type=str,
        help="Comma-separated daily variables (override config)",
    )
    parser.add_argument(
        "--outfile",
        type=str,
        help="Output CSV path (override config)",
    )
    parser.add_argument("--no-raw-json", action="store_true", help="Skip raw JSON")
    return parser.parse_args()


def main() -> None:
    configure_logging()
    logger = logging.getLogger(__name__)
    args = parse_args()
    daily_vars: Optional[List[str]] = None
    if args.daily:
        daily_vars = [x.strip() for x in args.daily.split(",") if x.strip()]

    meta = fetch_and_save(
        lat=args.lat,
        lon=args.lon,
        start_date=args.start,
        end_date=args.end,
        daily_vars=daily_vars,
        timezone=args.timezone,
        outfile=args.outfile,
        save_raw_json=not args.no_raw_json,
    )

    logger.info("Task complete")
    logger.info("CSV: %s", WEATHER_CSV)
    logger.info("Meta: %s", (WEATHER_CSV.parent / "weather_meta.json"))
    logger.info("Effective vars: %s", meta.get("effective_daily_vars"))
    if meta.get("fallback_used"):
        logger.warning("Fallback to minimal variables was used")


if __name__ == "__main__":
    main()
