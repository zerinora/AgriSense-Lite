import json
import sys
import logging
from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

try:
    from utils.config_loader import (
        CFG,
        MERGED_CSV,
        ALERTS_GATED_CSV,
        ALERTS_RAW_CSV,
        ALERTS_MERGED_CSV,
        RS_DEBUG_CSV,
        ASSETS,
        PERIOD_REPORT_START,
        PERIOD_REPORT_END,
    )
    from utils.logging_utils import setup_logging_from_cfg
except ImportError:
    from src.utils.config_loader import (
        CFG,
        MERGED_CSV,
        ALERTS_GATED_CSV,
        ALERTS_RAW_CSV,
        ALERTS_MERGED_CSV,
        RS_DEBUG_CSV,
        ASSETS,
        PERIOD_REPORT_START,
        PERIOD_REPORT_END,
    )
    from src.utils.logging_utils import setup_logging_from_cfg

OUT_FUNNEL = ASSETS / "alert_pipeline_funnel.png"
OUT_EVENTS_MONTHLY = ASSETS / "events_monthly_by_type.png"
OUT_EVENTS_PIE = ASSETS / "events_type_pie.png"

EVENT_META = {
    "drought": ("\u5e72\u65f1", "#d62728"),
    "waterlogging": ("\u6c34\u6d9d", "#3776ff"),
    "heat_stress": ("\u70ed\u80c1\u8feb", "#8e44ad"),
    "cold_stress": ("\u51b7\u80c1\u8feb", "#16a085"),
    "nutrient_or_pest": ("\u8425\u517b/\u75c5\u866b\u7591\u4f3c\u4fe1\u53f7", "#f39c12"),
    "composite": ("\u590d\u5408\u4e8b\u4ef6", "#111111"),
}

plt.rcParams["font.sans-serif"] = [
    "Microsoft YaHei",
    "SimHei",
    "Noto Sans CJK SC",
    "Arial Unicode MS",
    "DejaVu Sans",
]
plt.rcParams["axes.unicode_minus"] = False


def _load_stage_summary() -> dict | None:
    path = MERGED_CSV.parent / "stage_summary.json"
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return None


def _filter_by_report_range(df: pd.DataFrame, date_col: str) -> pd.DataFrame:
    if df.empty or date_col not in df.columns:
        return df
    start = pd.Timestamp(PERIOD_REPORT_START)
    end = pd.Timestamp(PERIOD_REPORT_END)
    return df[(df[date_col] >= start) & (df[date_col] <= end)]


def _fallback_stage_summary() -> dict:
    merged = pd.read_csv(MERGED_CSV, parse_dates=["date"]) if MERGED_CSV.exists() else pd.DataFrame()
    debug = pd.read_csv(RS_DEBUG_CSV, parse_dates=["date"]) if RS_DEBUG_CSV.exists() else pd.DataFrame()
    raw = pd.read_csv(ALERTS_RAW_CSV, parse_dates=["date"]) if ALERTS_RAW_CSV.exists() else pd.DataFrame()
    gated = pd.read_csv(ALERTS_GATED_CSV, parse_dates=["date"]) if ALERTS_GATED_CSV.exists() else pd.DataFrame()
    events = pd.read_csv(ALERTS_MERGED_CSV, parse_dates=["start_date"]) if ALERTS_MERGED_CSV.exists() else pd.DataFrame()

    merged = _filter_by_report_range(merged, "date")
    debug = _filter_by_report_range(debug, "date")
    raw = _filter_by_report_range(raw, "date")
    gated = _filter_by_report_range(gated, "date")
    events = _filter_by_report_range(events, "start_date")

    total_days = int(len(merged))
    qc_ok_days = int(debug.get("qc_ok", pd.Series(False, index=debug.index)).sum()) if not debug.empty else 0
    allow_alert_days = int(debug.get("allow_alert", pd.Series(False, index=debug.index)).sum()) if not debug.empty else 0

    stages = [
        {"stage": "01", "days_count": total_days, "alerts_count": None, "events_count": None},
        {"stage": "02", "days_count": qc_ok_days, "alerts_count": None, "events_count": None},
        {"stage": "03", "days_count": qc_ok_days, "alerts_count": int(len(raw)), "events_count": None},
        {"stage": "04", "days_count": allow_alert_days, "alerts_count": int(len(gated)), "events_count": None},
        {"stage": "05", "days_count": allow_alert_days, "alerts_count": int(len(gated)), "events_count": int(len(events))},
    ]
    return {"stages": stages}


def _plot_pipeline_funnel(summary: dict) -> None:
    stages = summary.get("stages", [])
    if not stages:
        return

    fig, ax = plt.subplots(figsize=(16, 4), dpi=140)
    ax.axis("off")

    titles = [
        "01_merged\n日序列底表",
        "02_rs_debug\nQC 质量判定",
        "03_alerts_raw\n仅 QC 的告警",
        "04_alerts_gated\nQC + 门禁",
        "05_events\n事件合并",
    ]

    width = 0.18
    height = 0.6
    y = 0.2
    x_positions = np.linspace(0.02, 0.98 - width, len(stages))

    for i, stage in enumerate(stages):
        x = x_positions[i]
        box = FancyBboxPatch(
            (x, y),
            width,
            height,
            boxstyle="round,pad=0.02,rounding_size=0.02",
            linewidth=1,
            edgecolor="#4c78a8",
            facecolor="#f2f5f9",
            transform=ax.transAxes,
        )
        ax.add_patch(box)

        days = stage.get("days_count", 0)
        alerts = stage.get("alerts_count", None)
        events = stage.get("events_count", None)
        alerts_text = "-" if alerts is None else str(alerts)
        events_text = "-" if events is None else str(events)

        text = (
            f"{titles[i]}\n"
            f"通过天数: {days}\n"
            f"告警条数: {alerts_text}\n"
            f"事件数: {events_text}"
        )
        ax.text(
            x + width / 2,
            y + height / 2,
            text,
            ha="center",
            va="center",
            fontsize=9,
            transform=ax.transAxes,
        )

        if i < len(stages) - 1:
            ax.annotate(
                "",
                xy=(x + width, y + height / 2),
                xytext=(x_positions[i + 1], y + height / 2),
                xycoords=ax.transAxes,
                textcoords=ax.transAxes,
                arrowprops=dict(arrowstyle="->", lw=1.2, color="#4c78a8"),
            )

    fig.tight_layout()
    ASSETS.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUT_FUNNEL)
    plt.close(fig)


def _plot_events_monthly_by_type() -> None:
    if not ALERTS_MERGED_CSV.exists():
        return
    events = pd.read_csv(ALERTS_MERGED_CSV, parse_dates=["start_date"]) if ALERTS_MERGED_CSV.exists() else pd.DataFrame()
    if events.empty or "event_type" not in events.columns:
        return
    events = _filter_by_report_range(events, "start_date")
    if events.empty:
        return
    events = events.assign(month=events["start_date"].dt.month)
    counts = (
        events.groupby(["month", "event_type"])
        .size()
        .unstack(fill_value=0)
        .reindex(index=range(1, 13), fill_value=0)
    )

    fig, ax = plt.subplots(figsize=(12, 5), dpi=140)
    bottom = np.zeros(len(counts))
    for event_type in counts.columns:
        label, color = EVENT_META.get(event_type, (str(event_type), "#999999"))
        values = counts[event_type].values
        ax.bar(counts.index, values, bottom=bottom, label=label, color=color)
        bottom += values

    ax.set_xticks(range(1, 13))
    ax.set_xlabel("\u6708\u4efd")
    ax.set_ylabel("\u4e8b\u4ef6\u6570")
    ax.set_title("\u4e8b\u4ef6\u7c7b\u578b\u6708\u5ea6\u5206\u5e03")
    ax.legend(frameon=False, ncol=2)
    ax.grid(axis="y", alpha=0.2)

    fig.tight_layout()
    ASSETS.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUT_EVENTS_MONTHLY)
    plt.close(fig)


def _plot_events_type_pie() -> None:
    if not ALERTS_MERGED_CSV.exists():
        return
    events = pd.read_csv(ALERTS_MERGED_CSV, parse_dates=["start_date"]) if ALERTS_MERGED_CSV.exists() else pd.DataFrame()
    if events.empty or "event_type" not in events.columns:
        return
    events = _filter_by_report_range(events, "start_date")
    if events.empty:
        return
    counts = events["event_type"].value_counts()
    labels = []
    colors = []
    for event_type in counts.index:
        label, color = EVENT_META.get(event_type, (str(event_type), "#999999"))
        labels.append(label)
        colors.append(color)

    fig, ax = plt.subplots(figsize=(6, 6), dpi=140)
    ax.pie(counts.values, labels=labels, colors=colors, autopct="%1.1f%%", startangle=90)
    ax.set_title("\u4e8b\u4ef6\u7c7b\u578b\u5360\u6bd4")
    ax.axis("equal")
    fig.tight_layout()
    ASSETS.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUT_EVENTS_PIE)
    plt.close(fig)


def main() -> None:
    setup_logging_from_cfg(CFG, app_name="plot_composite_alerts")
    logger = logging.getLogger(__name__)
    summary = _load_stage_summary() or _fallback_stage_summary()
    _plot_pipeline_funnel(summary)
    _plot_events_monthly_by_type()
    _plot_events_type_pie()
    logger.info("Plots saved to %s", ASSETS)


if __name__ == "__main__":
    main()
