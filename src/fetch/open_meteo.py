"""
open_meteo.py
--------------
从 Open-Meteo 的 ERA5 回析接口拉取逐日气象数据，输出 CSV + 元数据 JSON。
支持从 config.yml 读取参数，也支持由脚本传入覆盖值。
内置一次“最小变量集合”的自动降级重试，确保能拉到基础数据。
"""

from __future__ import annotations
import time, json, logging
from datetime import datetime
from typing import List, Dict, Any, Optional

import pandas as pd
import requests

from src.utils.config_loader import CFG, WEATHER_CSV, DATA_RAW, LOGS

ARCHIVE_API = "https://archive-api.open-meteo.com/v1/era5"

MIN_DAILY_VARS = ["temperature_2m_max", "temperature_2m_min", "precipitation_sum"]

def _setup_logger() -> logging.Logger:
    LOGS.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = LOGS / f"weather_fetch_{ts}.log"
    logger = logging.getLogger("weather_fetch")
    logger.handlers.clear()
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler(log_path, encoding="utf-8")
    sh = logging.StreamHandler()
    fmt = logging.Formatter("[%(asctime)s] %(levelname)s - %(message)s")
    fh.setFormatter(fmt); sh.setFormatter(fmt)
    logger.addHandler(fh); logger.addHandler(sh)
    logger.info(f"Log file: {log_path}")
    return logger

def _request_daily(lat: float, lon: float, start_date: str, end_date: str,
                   daily_vars: List[str], timezone: str = "auto",
                   retries: int = 3, pause: float = 1.5) -> Dict[str, Any]:
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,

        "daily": ",".join(daily_vars),
        "timezone": timezone
    }
    last_err = None
    for _ in range(retries):
        try:
            r = requests.get(ARCHIVE_API, params=params, timeout=60)
            if r.status_code == 200:
                return r.json()
            last_err = f"HTTP {r.status_code}: {r.text[:300]}"
        except requests.RequestException as e:
            last_err = str(e)
        time.sleep(pause)
    raise RuntimeError(f"请求失败：{last_err}")

def _json_to_df(payload: Dict[str, Any]) -> pd.DataFrame:
    if "daily" not in payload or "time" not in payload["daily"]:
        raise ValueError("返回结果缺少 daily/time 字段；请检查变量名或日期范围。")
    daily = payload["daily"]
    df = pd.DataFrame(daily)
    df = df.rename(columns={"time": "date"})
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")
    return df

def fetch_and_save(
    lat: Optional[float] = None,
    lon: Optional[float] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    daily_vars: Optional[List[str]] = None,
    timezone: Optional[str] = None,
    outfile: Optional[str] = None,
    save_raw_json: bool = True
) -> Dict[str, Any]:
    """
    读取参数→请求→保存 CSV/JSON→返回元数据字典
    """
    logger = _setup_logger()

    region = CFG["region"]
    period = CFG["period"]
    om = CFG["open_meteo"]

    lat = lat if lat is not None else region["center_lat"]
    lon = lon if lon is not None else region["center_lon"]
    start_date = start_date or period["start_date"]
    end_date = end_date or period["end_date"]
    timezone = timezone or region.get("timezone", "auto")
    daily_vars = daily_vars or list(om["daily_vars"])
    outfile_path = (WEATHER_CSV if outfile is None else (WEATHER_CSV.parent / outfile).resolve())

    logger.info(f"坐标：lat={lat}, lon={lon}")
    logger.info(f"时间：{start_date} → {end_date} @ {timezone}")
    logger.info(f"变量：{daily_vars}")
    logger.info(f"输出：{outfile_path}")

    tried_minimal = False
    try:
        payload = _request_daily(lat, lon, start_date, end_date, daily_vars, timezone)
        effective_daily = daily_vars
    except Exception as e:
        logger.warning(f"首轮请求失败：{e}")
        logger.warning(f"尝试使用最小变量集合：{MIN_DAILY_VARS}")
        payload = _request_daily(lat, lon, start_date, end_date, MIN_DAILY_VARS, timezone)
        effective_daily = MIN_DAILY_VARS
        tried_minimal = True

    df = _json_to_df(payload)

    DATA_RAW.mkdir(parents=True, exist_ok=True)
    df.to_csv(outfile_path, index=False, float_format="%.3f", encoding="utf-8")
    logger.info(f"CSV 已保存：{outfile_path}（{len(df)} 天）")

    meta_json_path = DATA_RAW / "weather_meta.json"
    raw_json_path = DATA_RAW / "weather_raw.json"
    units = payload.get("daily_units", {})
    meta = {
        "source": "open-meteo/era5",
        "api": ARCHIVE_API,
        "latitude": lat,
        "longitude": lon,
        "timezone": payload.get("timezone", timezone),
        "start_date": start_date,
        "end_date": end_date,
        "requested_daily_vars": list(daily_vars),
        "effective_daily_vars": list(effective_daily),
        "daily_units": units,
        "region_id": region.get("id"),
        "region_name": region.get("name"),
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "fallback_used": tried_minimal
    }
    with open(meta_json_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2, default=str)
    logger.info(f"元数据已保存：{meta_json_path}")

    if save_raw_json:
        with open(raw_json_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
        logger.info(f"原始响应已保存：{raw_json_path}")

    return meta
