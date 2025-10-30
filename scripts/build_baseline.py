# -*- coding: utf-8 -*-
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]; sys.path.insert(0, str(ROOT))

from src.analysis.baseline import baseline_and_alerts

if __name__ == "__main__":
    paths = baseline_and_alerts(
        smooth_window=15,   # DOY 平滑
        dev_thresh=-0.05,   # 偏离基线中位线 0.06 视作异常（比 -0.04 稍严）
        min_run=6,          # 连续 >=4 天
        precip7_max=None,   # 如专看“干旱型”，可设为 5
        train_years=[2024], # 基线只用 2024
        target_years=[2025] # 只在 2025 判告警
    )
    print("\n=== 基线 & 告警完成 ===")
    for k, v in paths.items():
        print(f"{k}: {v}")
