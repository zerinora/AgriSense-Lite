# -*- coding: utf-8 -*-
import sys
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.utils.config_loader import ASSETS, DATA_PROCESSED

def df_to_md_table(df: pd.DataFrame) -> str:
    """优先用 pandas.to_markdown；缺少 tabulate 时降级为管道表。"""
    try:
        return df.to_markdown(index=False)
    except Exception:
        cols = list(df.columns)
        lines = []
        lines.append("| " + " | ".join(cols) + " |")
        lines.append("| " + " | ".join(["---"] * len(cols)) + " |")
        for _, row in df.iterrows():
            vals = [("" if pd.isna(row[c]) else row[c]) for c in cols]
            lines.append("| " + " | ".join(map(str, vals)) + " |")
        return "\n".join(lines)

if __name__ == "__main__":
    ASSETS.mkdir(parents=True, exist_ok=True)

    # 先用合并清单，没有就退回季节偏离
    p = DATA_PROCESSED / "alerts_all.csv"
    if not p.exists():
        p = DATA_PROCESSED / "alerts.csv"

    df = pd.read_csv(p)
    year = 2025

    # 日期解析与年份过滤
    for c in ["start_date", "end_date", "min_date"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")
    if "start_date" in df.columns:
        df = df[df["start_date"].dt.year == year]

    n = len(df)
    total_days = int(df.get("duration_days", pd.Series(dtype=int)).sum()) if n > 0 else 0

    # ====== 排序键（动态）：优先 severity_score；否则 duration_days, min_dev ======
    by_cols = []
    if "severity_score" in df.columns:
        by_cols.append("severity_score")
    for c in ["duration_days", "min_dev"]:
        if c in df.columns and c not in by_cols:
            by_cols.append(c)

    if by_cols:
        asc = []
        for c in by_cols:
            if c == "min_dev":
                asc.append(True)   # 越负越严重 → 升序
            else:
                asc.append(False)  # 分数、持续天数越大越靠前
        top_idx = df.sort_values(by=by_cols, ascending=asc).head(5).index
    else:
        top_idx = df.head(5).index

    # ====== 生成展示表（更友好列 + 四舍五入） ======
    show = df.copy()
    # 统一“最深偏离”：season 用 min_dev，slope 用 min_drop
    show["min"] = show.get("min_dev").fillna(show.get("min_drop"))
    # 统一 type 字段
    show["type"] = show.get("type", "season").fillna("season")
    # 日期转 date
    for c in ["start_date", "end_date", "min_date"]:
        if c in show.columns:
            show[c] = pd.to_datetime(show[c], errors="coerce").dt.date

    cols = [c for c in [
        "type", "start_date", "end_date", "duration_days", "min",
        "deficit", "dry_days_share", "high_cloud_share", "precip7_mean"
    ] if c in show.columns]
    show = show[cols].copy()
    for c in ["min", "deficit", "dry_days_share", "high_cloud_share", "precip7_mean"]:
        if c in show.columns:
            show[c] = pd.to_numeric(show[c], errors="coerce").round(3)
    show = show.fillna("")
    top = show.loc[top_idx]

    # ====== 写 Markdown ======
    md = []
    md.append(f"# NDVI 异常简报 · {year}\n")
    md.append(f"- 事件数：**{n}**；累计天数：**{total_days}**\n")
    md.append("## 重点事件（Top 5）\n")
    if n > 0:
        md.append(df_to_md_table(top))
    else:
        md.append("> 本期无事件。")
    md.append("\n## 可视图\n")
    # 注意：report.md 位于 assets/ 中，图片与之同目录，故直接写文件名
    md.append("![baseline](ndvi_baseline_alerts.png)")

    out = ASSETS / "report.md"
    out.write_text("\n".join(md), encoding="utf-8")
    print("已生成：", out)
