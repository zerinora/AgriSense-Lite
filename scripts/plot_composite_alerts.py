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
ALERTS_RAW = ROOT / "data/processed/alerts_composite_raw.csv"
ASSETS = ROOT / "assets"
OUT_MAIN = ASSETS / "composite_alerts.png"
OUT_COUNTS = ASSETS / "composite_alerts_counts.png"
OUT_MONTHLY = ASSETS / "composite_alerts_monthly.png"
OUT_MONTHLY_COMPARE = ASSETS / "composite_alerts_monthly_compare.png"

EVENT_META = {
    "drought": ("干旱", "#d62728", "o"),
    "waterlogging": ("水涝", "#3776ff", "s"),
    "heat_stress": ("热胁迫", "#8e44ad", "^"),
    "cold_stress": ("冷胁迫", "#16a085", "v"),
    "nutrient_or_pest": ("营养/病虫疑似信号", "#f39c12", "o"),
    "composite": ("复合事件", "#111111", "*"),
}

plt.rcParams["font.sans-serif"] = [
    "Microsoft YaHei",
    "SimHei",
    "Noto Sans CJK SC",
    "Arial Unicode MS",
    "DejaVu Sans",
]
plt.rcParams["axes.unicode_minus"] = False


def _load_merged() -> pd.DataFrame:
    df = pd.read_csv(MERGED, parse_dates=["date"]).sort_values("date")
    for c in ("ndvi_mean_daily", "evi_mean", "ndmi_mean", "precip_7d"):
        if c not in df.columns:
            raise SystemExit(f"missing {c}")
    return df


def _load_alerts() -> pd.DataFrame:
    if ALERTS.exists():
        return pd.read_csv(ALERTS, parse_dates=["date"]).sort_values("date")
    return pd.DataFrame(columns=["date", "event_type", "reason"])

def _load_alerts_raw() -> pd.DataFrame:
    if ALERTS_RAW.exists():
        return pd.read_csv(ALERTS_RAW, parse_dates=["date"]).sort_values("date")
    return pd.DataFrame(columns=["date", "event_type", "reason"])


def _plot_timeseries(df: pd.DataFrame, ev: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(16, 6), dpi=140)
    ax.plot(
        df["date"],
        df["ndvi_mean_daily"].clip(-0.2, 1.0),
        color="forestgreen",
        lw=1.5,
        label="NDVI",
    )
    ax.plot(
        df["date"],
        df["evi_mean"].clip(-0.2, 1.0),
        color="tab:blue",
        lw=1.0,
        alpha=0.8,
        label="EVI",
    )
    ax.plot(
        df["date"],
        df["ndmi_mean"].clip(-0.2, 1.0),
        color="tab:gray",
        lw=1.0,
        alpha=0.6,
        label="NDMI",
    )
    ax.set_ylim(-0.2, 1.0)
    ax.set_ylabel("植被指数")

    ax2 = ax.twinx()
    ax2.fill_between(
        df["date"],
        0,
        df["precip_7d"],
        color="#cfe9ff",
        alpha=0.6,
        label="7日累计降水",
    )
    ax2.set_ylabel("7日累计降水 (mm)")
    ax2.grid(False)

    if not ev.empty:
        ev_plot = ev.merge(
            df[["date", "ndvi_mean_daily", "evi_mean"]], on="date", how="left"
        )
        ev_plot["marker_y"] = ev_plot["ndvi_mean_daily"].fillna(ev_plot["evi_mean"])
        for t, (label, col, marker) in EVENT_META.items():
            sub = ev_plot[(ev_plot["event_type"] == t) & (ev_plot["marker_y"].notna())]
            if not sub.empty:
                ax.scatter(
                    sub["date"],
                    sub["marker_y"],
                    c=col,
                    marker=marker,
                    s=40,
                    edgecolors="none",
                    label=label,
                )

    h1, l1 = ax.get_legend_handles_labels()
    h2, l2 = ax2.get_legend_handles_labels()
    ax.legend(h1 + h2, l1 + l2, loc="upper left", ncol=3, frameon=False)
    ax.set_xlabel("日期")
    ax.set_title("复合告警与植被/降水时序")
    fig.tight_layout()
    ASSETS.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUT_MAIN)
    plt.close(fig)


def _plot_counts(ev: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(10, 5), dpi=140)
    if ev.empty:
        ax.text(0.5, 0.5, "无告警记录", ha="center", va="center", fontsize=12)
        ax.axis("off")
    else:
        counts = (
            ev["event_type"]
            .map(lambda x: EVENT_META.get(x, (x, "#666", "o"))[0])
            .value_counts()
            .rename_axis("事件类型")
            .reset_index(name="次数")
        )
        ax.bar(counts["事件类型"], counts["次数"], color="#4c78a8")
        ax.set_ylabel("次数")
        ax.set_title("告警类型统计")
        ax.grid(axis="y", alpha=0.2)
    fig.tight_layout()
    ASSETS.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUT_COUNTS)
    plt.close(fig)


def _plot_monthly(ev: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(12, 5), dpi=140)
    if ev.empty:
        ax.text(0.5, 0.5, "无告警记录", ha="center", va="center", fontsize=12)
        ax.axis("off")
    else:
        ev = ev.copy()
        ev["month"] = ev["date"].dt.month
        pivot = (
            ev.pivot_table(
                index="month",
                columns="event_type",
                values="reason",
                aggfunc="count",
                fill_value=0,
            )
            .reindex(range(1, 13), fill_value=0)
        )
        bottom = None
        for t, (label, color, _) in EVENT_META.items():
            if t not in pivot.columns:
                continue
            values = pivot[t].values
            ax.bar(
                pivot.index,
                values,
                bottom=bottom,
                label=label,
                color=color,
                alpha=0.9,
            )
            bottom = values if bottom is None else bottom + values
        ax.set_xticks(range(1, 13))
        ax.set_xlabel("月份")
        ax.set_ylabel("次数")
        ax.set_title("告警月度分布（堆叠）")
        ax.legend(ncol=3, frameon=False)
        ax.grid(axis="y", alpha=0.2)
    fig.tight_layout()
    ASSETS.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUT_MONTHLY)
    plt.close(fig)

def _plot_monthly_compare(ev_raw: pd.DataFrame, ev_gated: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(12, 5), dpi=140)
    if ev_raw.empty and ev_gated.empty:
        ax.text(0.5, 0.5, "无告警记录", ha="center", va="center", fontsize=12)
        ax.axis("off")
    else:
        def _counts(ev: pd.DataFrame) -> pd.Series:
            if ev.empty:
                return pd.Series(0, index=range(1, 13))
            return (
                ev.assign(month=ev["date"].dt.month)
                .groupby("month")
                .size()
                .reindex(range(1, 13), fill_value=0)
            )

        raw_counts = _counts(ev_raw)
        gated_counts = _counts(ev_gated)
        x = range(1, 13)
        ax.bar([i - 0.18 for i in x], raw_counts.values, width=0.35, label="gating 前", color="#b0c4de")
        ax.bar([i + 0.18 for i in x], gated_counts.values, width=0.35, label="gating 后", color="#4c78a8")
        ax.set_xticks(range(1, 13))
        ax.set_xlabel("月份")
        ax.set_ylabel("次数")
        ax.set_title("告警月度分布（gating 前后对比）")
        ax.legend(frameon=False)
        ax.grid(axis="y", alpha=0.2)
    fig.tight_layout()
    ASSETS.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUT_MONTHLY_COMPARE)
    plt.close(fig)

def main():
    df = _load_merged()
    ev = _load_alerts()
    ev_raw = _load_alerts_raw()
    _plot_timeseries(df, ev)
    _plot_counts(ev)
    _plot_monthly(ev)
    _plot_monthly_compare(ev_raw, ev)
    print("[OK] Plots saved to", ASSETS)

if __name__ == "__main__":
    main()
