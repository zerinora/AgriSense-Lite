# -*- coding: utf-8 -*-
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]; sys.path.insert(0, str(ROOT))

from src.analysis.baseline import plot_baseline_with_alerts

if __name__ == "__main__":
    out = plot_baseline_with_alerts(target_years=[2025])
    print("已输出图像：", out)
