# -*- coding: utf-8 -*-
# --- 把项目根加入 sys.path（手动法） ---
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
# --------------------------------------

from src.transform.merge_data import merge_weather_ndvi
from src.utils.config_loader import MERGED_CSV

if __name__ == "__main__":
    out = merge_weather_ndvi(cloud_frac_max=0.6, interpolate_ndvi=True)
    print("\n=== 合并完成 ===")
    print("输出文件：", MERGED_CSV)
    print("若不想插值，把 interpolate_ndvi=False；若想更严格，可把 cloud_frac_max 调小。")
