from pathlib import Path
import sys
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

try:
    from utils.config_loader import CFG
except ImportError:
    from src.utils.config_loader import CFG

ALERTS = ROOT / "data/processed/alerts_composite.csv"
ALERTS_RAW = ROOT / "data/processed/alerts_composite_raw.csv"
ALERTS_MERGED = ROOT / "data/processed/alerts_composite_merged.csv"
DEBUG = ROOT / "data/processed/rs_debug.csv"
MERGED = ROOT / "data/processed/merged.csv"
OUT = ROOT / "assets/report_composite.md"

EVENT_LABELS = {
    "drought": "干旱",
    "waterlogging": "水洞",
    "heat_stress": "热胁迫",
    "cold_stress": "冷胁迫",
    "nutrient_or_pest": "营养/病虫疑似信号",
    "composite": "复合事件",
}


def _format_date_range(df: pd.DataFrame) -> str:
    if df.empty or "date" not in df.columns:
        return "无"
    start = df["date"].min().date().isoformat()
    end = df["date"].max().date().isoformat()
    return f"{start} ~ {end}"


def _load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path)
    for c in ("date", "start_date", "end_date", "peak_date"):
        if c in df.columns:
            df[c] = pd.to_datetime(df[c])
    if "date" in df.columns:
        df = df.sort_values("date")
    return df


def _load_alerts() -> pd.DataFrame:
    if not ALERTS.exists():
        raise SystemExit("alerts_composite.csv not found")
    return _load_csv(ALERTS)


def _rs_obs_flag(df: pd.DataFrame) -> pd.Series:
    cols = [
        c
        for c in (
            "ndvi_mean",
            "evi_mean",
            "ndmi_mean",
            "ndre_mean",
            "gndvi_mean",
            "msi_mean",
        )
        if c in df.columns
    ]
    if not cols:
        return pd.Series(False, index=df.index)
    return df[cols].notna().any(axis=1)


def _rs_quality_summary(
    merged: pd.DataFrame, debug: pd.DataFrame
) -> tuple[list[str], pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    data_range = _format_date_range(merged) if not merged.empty else "无"
    days = int(len(merged)) if not merged.empty else 0

    seq_rate = (
        merged["ndvi_mean_daily"].notna().mean()
        if (not merged.empty and "ndvi_mean_daily" in merged.columns)
        else 0.0
    )
    obs_flag = _rs_obs_flag(merged)
    obs_rate = obs_flag.mean() if not merged.empty else 0.0

    obs_dates = (
        merged.loc[obs_flag, "date"]
        if not merged.empty
        else pd.Series([], dtype="datetime64[ns]")
    )
    if len(obs_dates) >= 2:
        max_gap = int(obs_dates.sort_values().diff().dt.days.max())
    else:
        max_gap = 0

    month_counts = (
        obs_dates.dt.month.value_counts()
        .reindex(range(1, 13), fill_value=0)
        .sort_index()
        if len(obs_dates) > 0
        else pd.Series(0, index=range(1, 13))
    )
    month_table = month_counts.rename_axis("月份").reset_index(
        name="真实观测次数"
    )

    if len(obs_dates) > 0:
        quarter = obs_dates.dt.quarter
        quarter_counts = (
            quarter.value_counts().reindex(range(1, 4 + 1), fill_value=0).sort_index()
        )
    else:
        quarter_counts = pd.Series(0, index=range(1, 5))
    quarter_table = quarter_counts.rename_axis("季度").reset_index(
        name="真实观测次数"
    )

    skip_table = pd.DataFrame(columns=["原因", "天数"])
    if not debug.empty and "skip_reason" in debug.columns:
        counts = debug["skip_reason"].value_counts().reindex(
            ["missing_remote", "missing_weather", "rs_max_age"], fill_value=0
        )
        skip_table = counts.rename_axis("原因").reset_index(name="天数")
        reason_map = {
            "missing_remote": "缺遥感",
            "missing_weather": "缺气象",
            "rs_max_age": "超 rs_max_age",
        }
        skip_table["原因"] = skip_table["原因"].map(
            lambda x: reason_map.get(x, x)
        )

    lines = [
        f"- 数据范围：{data_range}，共 {days} 天",
        f"- 序列完整率：{seq_rate:.1%}（基于 ndvi_mean_daily）",
        f"- 真实观测有效率：{obs_rate:.1%}（基于真实遥感观测日）",
        f"- 真实观测最大间隔：{max_gap} 天",
    ]
    return lines, month_table, quarter_table, skip_table


def _build_summary(
    merged: pd.DataFrame, alerts: pd.DataFrame, merged_events: pd.DataFrame
) -> list[str]:
    total_events = len(alerts)
    merged_count = len(merged_events)
    composite_count = int((alerts["event_type"] == "composite").sum()) if total_events else 0
    if total_events:
        top_type = alerts["event_type"].value_counts().idxmax()
        top_label = EVENT_LABELS.get(top_type, top_type)
        top_count = int(alerts["event_type"].value_counts().max())
        summary = (
            f"逐日触发 {total_events} 次，其中复合事件 {composite_count} 次；"
            f"合并后事件 {merged_count} 次；最频繁类型为 {top_label}（{top_count} 次）。"
        )
    else:
        summary = "本期未识别到复合事件。"

    data_range = _format_date_range(merged) if not merged.empty else "无"
    days = int(len(merged)) if not merged.empty else 0
    return [
        f"- 数据范围：{data_range}，共 {days} 天",
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
        recs.append("- 水洞事件：检查排水能力与田间积水，避免根系缺氧。")
    if counts.get("heat_stress", 0) > 0:
        recs.append("- 热胁迫事件：加强高温时段水分保障与遮阴管理。")
    if counts.get("cold_stress", 0) > 0:
        recs.append("- 冷胁迫事件：关注寒潮预警，必要时采取覆盖或保温措施。")
    if counts.get("nutrient_or_pest", 0) > 0:
        recs.append("- 营养/病虫疑似信号：结合田间巡查，评估追肥或病虫防治。")
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
    out = (
        m.groupby("月份")
        .size()
        .reindex(range(1, 13), fill_value=0)
        .reset_index(name="事件数")
    )
    return out


def _monthly_compare_table(raw: pd.DataFrame, gated: pd.DataFrame) -> pd.DataFrame:
    def _empty_counts(name: str) -> pd.Series:
        s = pd.Series(0, index=range(1, 13), name=name)
        s.index.name = "月份"
        return s

    raw_counts = (
        raw.assign(月份=raw["date"].dt.month)
        .groupby("月份")
        .size()
        .rename("gating前")
        if not raw.empty
        else _empty_counts("gating前")
    )
    gated_counts = (
        gated.assign(月份=gated["date"].dt.month)
        .groupby("月份")
        .size()
        .rename("gating后")
        if not gated.empty
        else _empty_counts("gating后")
    )
    df = (
        pd.DataFrame({"月份": range(1, 13)})
        .merge(raw_counts.reset_index(), on="月份", how="left")
        .merge(gated_counts.reset_index(), on="月份", how="left")
        .fillna(0)
    )
    df["gating前"] = df["gating前"].astype(int)
    df["gating后"] = df["gating后"].astype(int)
    return df


def _detail_table(alerts: pd.DataFrame, n: int = 20) -> pd.DataFrame:
    if alerts.empty:
        return pd.DataFrame(columns=["日期", "事件类型", "触发原因"])
    out = alerts.copy()
    out["日期"] = out["date"].dt.date
    out["事件类型"] = out["event_type"].map(lambda x: EVENT_LABELS.get(x, x))
    out["触发原因"] = out["reason"].fillna("")
    return out[["日期", "事件类型", "触发原因"]].head(n)


def _merged_event_table(merged_events: pd.DataFrame) -> pd.DataFrame:
    if merged_events.empty:
        return pd.DataFrame(
            columns=[
                "事件类型",
                "起始日期",
                "结束日期",
                "持续天数",
                "峰值日期",
                "峰值强度",
                "原因摘要",
            ]
        )
    out = merged_events.copy()
    out["事件类型"] = out["event_type"].map(lambda x: EVENT_LABELS.get(x, x))
    out["起始日期"] = (
        out["start_date"].dt.date if "start_date" in out.columns else ""
    )
    out["结束日期"] = (
        out["end_date"].dt.date if "end_date" in out.columns else ""
    )
    out["持续天数"] = out["duration_days"]
    out["峰值日期"] = (
        out["peak_date"].dt.date if "peak_date" in out.columns else ""
    )
    out["峰值强度"] = out["peak_value"].round(3)
    out["原因摘要"] = out["reason_summary"].fillna("")
    return out[
        [
            "事件类型",
            "起始日期",
            "结束日期",
            "持续天数",
            "峰值日期",
            "峰值强度",
            "原因摘要",
        ]
    ]


def _tmean_stats(merged: pd.DataFrame, heat_threshold: float) -> pd.DataFrame:
    if merged.empty or "tmean_7d" not in merged.columns:
        return pd.DataFrame(columns=["统计项", "数值"])
    s = merged["tmean_7d"].dropna()
    if s.empty:
        return pd.DataFrame(columns=["统计项", "数值"])
    qs = s.quantile([0.1, 0.25, 0.5, 0.75, 0.9, 0.95])
    rows = [
        ("P10", qs.loc[0.1]),
        ("P25", qs.loc[0.25]),
        ("P50", qs.loc[0.5]),
        ("P75", qs.loc[0.75]),
        ("P90", qs.loc[0.9]),
        ("P95", qs.loc[0.95]),
        ("阈值(heat_tmean7)", heat_threshold),
    ]
    return pd.DataFrame(rows, columns=["统计项", "数值"]).assign(
        数值=lambda d: d["数值"].round(2)
    )


def main():
    merged = _load_csv(MERGED)
    alerts = _load_alerts()
    alerts_raw = _load_csv(ALERTS_RAW)
    merged_events = _load_csv(ALERTS_MERGED)
    debug = _load_csv(DEBUG)

    alert_cfg = CFG.get("composite_alerts", {}) if isinstance(CFG, dict) else {}
    heat_threshold = float(alert_cfg.get("heat_tmean7", 30.0))
    gating_mode = str(alert_cfg.get("gating_mode", "canopy_obs")).lower()
    canopy_obs_min = int(alert_cfg.get("canopy_obs_min", 2))
    gating_months = alert_cfg.get("gating_months", [4, 5, 6, 7, 8, 9, 10])

    rs_lines, rs_month, rs_quarter, skip_table = _rs_quality_summary(merged, debug)

    md = []
    md += ["# 复合告警业务简报", ""]
    md += ["## 业务要点", ""]
    md += _build_summary(merged, alerts, merged_events)
    md += ["", "## 遥感质量与覆盖", ""]
    md += rs_lines
    md += ["", "### 真实遥感观测次数（按月）", ""]
    md += [rs_month.to_markdown(index=False), ""]
    md += ["### 真实遥感观测次数（按季）", ""]
    md += [rs_quarter.to_markdown(index=False), ""]
    md += ["### 被跳过的天数及原因", ""]
    md += [skip_table.to_markdown(index=False) if not skip_table.empty else "无", ""]

    md += ["## 行动建议", ""]
    md += _recommendations(alerts)
    md += ["", "## 假阳性控制（本期生效）", ""]
    md += [
        "- 生长季 gating + 缺测拦截 + 事件合并后，告警数量显著收敛。",
        "- gating 前后月度分布对比见下图。",
        f"- gating 模式：{gating_mode}，canopy_obs_min={canopy_obs_min}，gating_months={gating_months}",
    ]

    md += ["", "## 可视化", ""]
    md += ["![复合告警与时序](composite_alerts.png)", ""]
    md += ["![告警类型统计](composite_alerts_counts.png)", ""]
    md += ["![告警月度分布](composite_alerts_monthly.png)", ""]
    md += ["![告警月度分布对比](composite_alerts_monthly_compare.png)", ""]
    md += [
        "- 注：事件标记点位置使用对应日期的 NDVI/EVI 值，仅用于表达事件发生日期。",
        "",
    ]

    md += ["## 告警概览（逐日）", ""]
    md += [_counts_table(alerts).to_markdown(index=False), ""]
    md += ["## 月度分布（逐日）", ""]
    md += [_monthly_table(alerts).to_markdown(index=False), ""]
    md += ["## 月度分布对比（gating 前后）", ""]
    md += [_monthly_compare_table(alerts_raw, alerts).to_markdown(index=False), ""]
    md += ["## 事件明细（最新20条）", ""]
    md += [_detail_table(alerts, n=20).to_markdown(index=False), ""]

    md += ["## 合并后事件统计", ""]
    md += [_merged_event_table(merged_events).to_markdown(index=False), ""]

    md += ["## 热胁迫阈值与气温分布", ""]
    md += [_tmean_stats(merged, heat_threshold).to_markdown(index=False), ""]

    OUT.write_text("\n".join(md), encoding="utf-8")
    print("[OK] Report written to", OUT)


if __name__ == "__main__":
    main()
