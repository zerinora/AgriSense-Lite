# -*- coding: utf-8 -*-
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]; sys.path.insert(0, str(ROOT))
from src.analysis.baseline import detect_slope_alerts, union_alerts

if __name__ == "__main__":
    # 只对 2025 判斜率型；如需屏蔽收割窗口，填 [(230,260)] 之类的 DOY 段
    detect_slope_alerts(drop_days=10, drop_thresh=-0.08, min_run=3, target_years=[2025], harvest_windows=None)
    p = union_alerts()
    print("已合并：", p)
