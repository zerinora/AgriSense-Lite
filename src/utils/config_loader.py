# -*- coding: utf-8 -*-
"""
src/utils/config_loader.py
读取 config/config.yml，统一管理路径与参数。
"""
from pathlib import Path
import yaml

ROOT = Path(__file__).resolve().parents[2]  # 指向项目根目录

def load_config(path: Path = None) -> dict:
    path = path or (ROOT / "config" / "config.yml")
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    # 绝对化路径
    p = cfg.get("paths", {})
    for k in ("data_raw", "data_processed", "assets", "logs"):
        if k in p:
            p[k] = str((ROOT / p[k]).resolve())
    cfg["paths"] = p
    return cfg

# 常用便捷项
CFG = load_config()
DATA_RAW = Path(CFG["paths"]["data_raw"])
DATA_PROCESSED = Path(CFG["paths"]["data_processed"])
ASSETS = Path(CFG["paths"]["assets"])
LOGS = Path(CFG["paths"]["logs"])

WEATHER_CSV = ROOT / CFG["open_meteo"]["outfile"]
NDVI_CSV = ROOT / CFG["gee_s2"]["ndvi_outfile"]
MERGED_CSV = ROOT / CFG["merge"]["outfile"]

# 确保目录存在
for d in (DATA_RAW, DATA_PROCESSED, ASSETS, LOGS):
    d.mkdir(parents=True, exist_ok=True)
