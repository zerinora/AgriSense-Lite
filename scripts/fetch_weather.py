"""
scripts/fetch_weather.py
------------------------
命令行入口：读取 config.yml，支持传参覆盖，写出 data/raw/weather.csv + weather_meta.json
"""
import sys
import logging
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.fetch.open_meteo import fetch_and_save
from src.utils.config_loader import CFG, WEATHER_CSV

import argparse
from typing import List, Optional

from src.fetch.open_meteo import fetch_and_save
from src.utils.config_loader import CFG, WEATHER_CSV

def configure_logging():
    log_cfg = CFG.get("logging", {})
    level_name = str(log_cfg.get("level", "INFO")).upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format="[%(asctime)s] %(levelname)s - %(message)s",
    )

def parse_args():
    p = argparse.ArgumentParser(description="从 Open-Meteo ERA5 获取逐日气象数据")
    p.add_argument("--lat", type=float, help="纬度（可覆盖 config.yml）")
    p.add_argument("--lon", type=float, help="经度（可覆盖 config.yml）")
    p.add_argument("--start", type=str, help="开始日期 YYYY-MM-DD")
    p.add_argument("--end", type=str, help="结束日期 YYYY-MM-DD（包含）")
    p.add_argument("--timezone", type=str, help="时区，例如 Asia/Shanghai 或 auto")
    p.add_argument("--daily", type=str, help="逗号分隔的日变量名列表（覆盖 config）")
    p.add_argument("--outfile", type=str, help="输出 CSV 相对路径（默认使用 config 的 data/raw/weather.csv）")
    p.add_argument("--no-raw-json", action="store_true", help="不保存原始响应 JSON")
    return p.parse_args()

def main():
    configure_logging()
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
        save_raw_json=not args.no_raw_json
    )

    print("\n=== 任务完成 ===")
    print("写入 CSV：", WEATHER_CSV)
    print("元数据：  ", (WEATHER_CSV.parent / "weather_meta.json"))
    print("有效变量：", meta["effective_daily_vars"])
    if meta.get("fallback_used"):
        print("注意：已自动降级为最小变量集合（可在 config.yml 调整 daily_vars 后重试）。")

if __name__ == "__main__":
    main()
