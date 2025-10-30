# -*- coding: utf-8 -*-
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]; sys.path.insert(0, str(ROOT))

import pandas as pd
from src.utils.config_loader import MERGED_CSV, DATA_PROCESSED

df = pd.read_csv(MERGED_CSV, parse_dates=["date"]).set_index("date").sort_index()
base = pd.read_csv(DATA_PROCESSED / "ndvi_baseline.csv", index_col="doy")

df = df[df.index.year == 2025]        # 只看 2025
df["doy"] = df.index.dayofyear
df["base_p50"] = df["doy"].map(base["p50"])
df["dev"] = df["ndvi_mean_daily"] - df["base_p50"]

print("2025 dev 统计：")
print(df["dev"].describe())
for th in [-0.06, -0.05, -0.04]:
    m = (df["dev"] <= th)
    run = best = 0
    for v in m:
        run = run + 1 if v else 0
        best = max(best, run)
    print(f"阈值 {th: .2f} 时，最长连续天数 = {best}")
