from pathlib import Path
import sys
import pandas as pd
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

MERGED = ROOT / "data/processed/merged.csv"
ALERTS = ROOT / "data/processed/alerts_composite.csv"
OUT = ROOT / "assets/composite_alerts.png"

def main():
    df = pd.read_csv(MERGED, parse_dates=["date"]).sort_values("date")
    for c in ("ndvi_mean_daily","evi_mean","ndmi_mean","precip_7d"):
        if c not in df.columns: raise SystemExit(f"missing {c}")

    fig, ax = plt.subplots(figsize=(16,6), dpi=120)
    ax.plot(df["date"], df["ndvi_mean_daily"].clip(-0.2, 1.0), color="forestgreen", lw=1.5, label="NDVI")
    ax.plot(df["date"], df["evi_mean"].clip(-0.2, 1.0), color="tab:blue", lw=1.0, alpha=0.8, label="EVI")
    ax.plot(df["date"], df["ndmi_mean"].clip(-0.2, 1.0), color="tab:gray", lw=1.0, alpha=0.6, label="NDMI")
    ax.set_ylim(-0.2, 1.0)
    ax.set_ylabel("Vegetation Index")

    ax2 = ax.twinx()
    ax2.fill_between(df["date"], 0, df["precip_7d"], color="#cfe9ff", alpha=0.6, label="7d Precip (mm)")
    ax2.set_ylabel("7-Day Precip (mm)")
    ax2.grid(False)

    if ALERTS.exists():
        ev = pd.read_csv(ALERTS, parse_dates=["date"])
        sym = {
            "drought": ("red", "o"),
            "waterlogging": ("#3776ff", "s"),
            "heat_stress": ("#8e44ad", "^"),
            "cold_stress": ("#16a085", "v"),
            "nutrient_or_pest": ("#f39c12", "o"),
            "composite": ("black", "*"),
        }
        for t, (col, m) in sym.items():
            sub = ev[ev["event_type"] == t]
            if not sub.empty:
                ax.scatter(sub["date"], [0.45]*len(sub), c=col, marker=m, s=35, edgecolors="none", label=t.replace("_"," ").title())

    h1, l1 = ax.get_legend_handles_labels()
    h2, l2 = ax2.get_legend_handles_labels()
    ax.legend(h1+h2, l1+l2, loc="upper left", ncol=3, frameon=False)
    ax.set_xlabel("Date")
    fig.tight_layout()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUT)
    print("[OK] Plot saved to", OUT)

if __name__ == "__main__":
    main()
