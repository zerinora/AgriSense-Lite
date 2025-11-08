# scripts/build_composite_alerts.py
from pathlib import Path
import sys, os

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

# 兼容 analysis/ 或 transform/
for modname in ("analysis.composite_alerts", "transform.composite_alerts"):
    try:
        comp = __import__(modname, fromlist=["*"])
        break
    except ModuleNotFoundError:
        comp = None
        continue
if comp is None:
    raise ModuleNotFoundError("找不到 composite_alerts 模块，请确认放在 src/analysis/ 或 src/transform/ 下。")

# 优先用 run()；没有就兜底调用 detect_composite_alerts()
if hasattr(comp, "run"):
    out = comp.run()
else:
    df = comp.load_merged()
    alerts = comp.detect_composite_alerts(df)
    out = Path("data/processed/alerts_composite.csv")
    alerts.to_csv(out, index=False)
    print(f"[OK] Composite alerts saved → {out}")

print("==== 合并告警完成 ====")
print("输出文件：", out)
print("建议下一步：plot_baseline_alerts.py / 仪表盘联调")
