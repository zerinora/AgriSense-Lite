# -*- coding: utf-8 -*-
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
ALERTS = ROOT / "data/processed/alerts_composite.csv"
OUT = ROOT / "assets/report_composite.md"

def main():
    if not ALERTS.exists():
        raise SystemExit("alerts_composite.csv not found")
    df = pd.read_csv(ALERTS, parse_dates=["date"]).sort_values("date")
    cnt = df["event_type"].value_counts().rename_axis("event_type").reset_index(name="count")
    head = df.head(20)

    md = []
    md += ["# Composite Alert Report", ""]
    md += ["## Event Summary", ""]
    md += [cnt.to_markdown(index=False), ""]
    md += ["## Event Details", ""]
    md += [head.to_markdown(index=False), ""]
    OUT.write_text("\n".join(md), encoding="utf-8")
    print("[OK] Report written to", OUT)

if __name__ == "__main__":
    main()
