from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
ALERTS = ROOT / "data/processed/alerts_composite.csv"
MERGED = ROOT / "data/processed/merged.csv"
OUT = ROOT / "assets/report_composite.md"

EVENT_LABELS = {
    "drought": "干旱",
    "waterlogging": "水涝",
    "heat_stress": "热胁迫",
    "cold_stress": "冷胁迫",
    "nutrient_or_pest": "营养/病虫",
    "composite": "复合事件",
}


def _format_date_range(df: pd.DataFrame) -> str:
    if df.empty or "date" not in df.columns:
        return "无"
    start = df["date"].min().date().isoformat()
    end = df["date"].max().date().isoformat()
    return f"{start} ~ {end}"


def _load_merged() -> pd.DataFrame:
    if not MERGED.exists():
        return pd.DataFrame()
    return pd.read_csv(MERGED, parse_dates=["date"]).sort_values("date")


def _load_alerts() -> pd.DataFrame:
    if not ALERTS.exists():
        raise SystemExit("alerts_composite.csv not found")
    return pd.read_csv(ALERTS, parse_dates=["date"]).sort_values("date")


def _build_summary(merged: pd.DataFrame, alerts: pd.DataFrame) -> list[str]:
    total_events = len(alerts)
    composite_count = int((alerts["event_type"] == "composite").sum()) if total_events else 0
    if total_events:
        top_type = alerts["event_type"].value_counts().idxmax()
        top_label = EVENT_LABELS.get(top_type, top_type)
        top_count = int(alerts["event_type"].value_counts().max())
        summary = f"共识别 {total_events} 次事件，其中复合事件 {composite_count} 次；最频繁类型为 {top_label}（{top_count} 次）。"
    else:
        summary = "本期未识别到复合事件。"

    data_range = _format_date_range(merged) if not merged.empty else "无"
    days = int(len(merged)) if not merged.empty else 0
    rs_rate = ""
    if not merged.empty and "ndvi_mean_daily" in merged.columns:
        rate = merged["ndvi_mean_daily"].notna().mean()
        rs_rate = f"，遥感有效率 {rate:.1%}"
    return [
        f"- 数据范围：{data_range}，共 {days} 天{rs_rate}",
        f"- {summary}",
    ]


def _recommendations(alerts: pd.DataFrame) -> list[str]:
    if alerts.empty:
        return ["- 暂无告警，保持常规巡检与数据更新即可。"]
    counts = alerts["event_type"].value_counts()
    recs = []
    if counts.get("drought", 0) > 0:
        recs.append("- 干旱事件：关注灌溉与土壤墒情，必要时安排补水。")
    if counts.get("waterlogging", 0) > 0:
        recs.append("- 水涝事件：检查排水能力与田间积水，避免根系缺氧。")
    if counts.get("heat_stress", 0) > 0:
        recs.append("- 热胁迫事件：加强高温时段水分保障与遮阴管理。")
    if counts.get("cold_stress", 0) > 0:
        recs.append("- 冷胁迫事件：关注寒潮预警，必要时采取覆盖或保温措施。")
    if counts.get("nutrient_or_pest", 0) > 0:
        recs.append("- 营养/病虫事件：结合田间巡查，评估追肥或病虫防治。")
    if counts.get("composite", 0) > 0:
        recs.append("- 复合事件：优先现场核查，多因素协同处置。")
    if not recs:
        recs.append("- 当前告警类型较少，保持常规巡检与数据更新。")
    return recs


def _counts_table(alerts: pd.DataFrame) -> pd.DataFrame:
    if alerts.empty:
        return pd.DataFrame(columns=["事件类型", "次数"])
    counts = alerts["event_type"].map(lambda x: EVENT_LABELS.get(x, x)).value_counts()
    return counts.rename_axis("事件类型").reset_index(name="次数")


def _monthly_table(alerts: pd.DataFrame) -> pd.DataFrame:
    if alerts.empty:
        return pd.DataFrame(columns=["月份", "事件数"])
    m = alerts.copy()
    m["月份"] = m["date"].dt.month
    out = m.groupby("月份").size().reindex(range(1, 13), fill_value=0).reset_index(name="事件数")
    return out


def _detail_table(alerts: pd.DataFrame, n: int = 20) -> pd.DataFrame:
    if alerts.empty:
        return pd.DataFrame(columns=["日期", "事件类型", "触发原因"])
    out = alerts.copy()
    out["日期"] = out["date"].dt.date
    out["事件类型"] = out["event_type"].map(lambda x: EVENT_LABELS.get(x, x))
    out["触发原因"] = out["reason"].fillna("")
    return out[["日期", "事件类型", "触发原因"]].head(n)


def main():
    merged = _load_merged()
    alerts = _load_alerts()

    md = []
    md += ["# 复合告警业务简报", ""]
    md += ["## 业务要点", ""]
    md += _build_summary(merged, alerts)
    md += ["", "## 行动建议", ""]
    md += _recommendations(alerts)
    md += ["", "## 可视化", ""]
    md += ["![复合告警与时序](composite_alerts.png)", ""]
    md += ["![告警类型统计](composite_alerts_counts.png)", ""]
    md += ["![告警月度分布](composite_alerts_monthly.png)", ""]
    md += ["## 告警概览（中文）", ""]
    md += [_counts_table(alerts).to_markdown(index=False), ""]
    md += ["## 月度分布（中文）", ""]
    md += [_monthly_table(alerts).to_markdown(index=False), ""]
    md += ["## 事件明细（最新20条）", ""]
    md += [_detail_table(alerts, n=20).to_markdown(index=False), ""]
    OUT.write_text("\n".join(md), encoding="utf-8")
    print("[OK] Report written to", OUT)

if __name__ == "__main__":
    main()
