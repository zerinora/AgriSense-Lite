# scripts/build_composite_alerts.py
# -*- coding: utf-8 -*-
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT/"src"))

from transform.composite_alerts import run  # noqa

if __name__ == "__main__":
    out = run()
    print("==== 合并告警完成 ====")
    print("输出文件：", out)
    print("建议下一步：plot_baseline_alerts.py / 仪表盘联调")
