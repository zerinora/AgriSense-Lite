# -*- coding: utf-8 -*-
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]; sys.path.insert(0, str(ROOT))

from src.analysis.severity import attach_severity
from src.utils.config_loader import DATA_PROCESSED

if __name__ == "__main__":
    # 优先打分 alerts_all.csv，没有则打分 alerts.csv
    p = DATA_PROCESSED / ("alerts_all.csv" if (DATA_PROCESSED / "alerts_all.csv").exists()
                          else "alerts.csv")
    out = attach_severity(p)
    print("已写入严重度：", out)
