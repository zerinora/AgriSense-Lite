from __future__ import annotations

import json
from pathlib import Path
import sys
import logging
import pandas as pd

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

OUT = ASSETS / "report_composite.md"
STAGE_SUMMARY = MERGED_CSV.parent / "stage_summary.json"

EVENT_NAME_MAP = {
    "drought": "\u5e72\u65f1",
    "waterlogging": "\u6c34\u6d9d",
    "heat_stress": "\u70ed\u80c1\u8feb",
    "cold_stress": "\u51b7\u80c1\u8feb",
    "nutrient_or_pest": "\u8425\u517b/\u75c5\u866b\u7591\u4f3c\u4fe1\u53f7",
    "composite": "\u590d\u5408\u4e8b\u4ef6",
}


def _rel(path: Path) -> str:
    try:
        return path.relative_to(ROOT).as_posix()
    except ValueError:
        return str(path)


def _load_stage_summary() -> dict:
    if STAGE_SUMMARY.exists():
        return json.loads(STAGE_SUMMARY.read_text(encoding="utf-8"))
    return {
        "totals": {},
        "stages": [],
    }


def _load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def _alert_counts(df: pd.DataFrame) -> list[tuple[str, int]]:
    if df.empty or "event_type" not in df.columns:
        return []
    counts = df["event_type"].value_counts().to_dict()
    rows: list[tuple[str, int]] = []
    for key, value in counts.items():
        name = EVENT_NAME_MAP.get(str(key), str(key))
        rows.append((name, int(value)))
    rows.sort(key=lambda item: item[1], reverse=True)
    return rows


def _event_counts(df: pd.DataFrame) -> list[tuple[str, int]]:
    if df.empty or "event_type" not in df.columns:
        return []
    counts = df["event_type"].value_counts().to_dict()
    rows: list[tuple[str, int]] = []
    for key, value in counts.items():
        name = EVENT_NAME_MAP.get(str(key), str(key))
        rows.append((name, int(value)))
    rows.sort(key=lambda item: item[1], reverse=True)
    return rows


def _filter_by_report_range(df: pd.DataFrame, date_col: str) -> pd.DataFrame:
    if df.empty or date_col not in df.columns:
        return df
    if not pd.api.types.is_datetime64_any_dtype(df[date_col]):
        df = df.copy()
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    start = pd.Timestamp(PERIOD_REPORT_START)
    end = pd.Timestamp(PERIOD_REPORT_END)
    return df[(df[date_col] >= start) & (df[date_col] <= end)]


def _stage_table() -> list[list[str]]:
    rows = [
        [
            "01",
            "01_merged.csv",
            "\u65e5",
            "\u65e5\u5e8f\u5217\u5e95\u8868 (\u6c14\u8c61+\u9065\u611f+\u6eda\u52a8\u7edf\u8ba1)",
            "\u5bf9\u9f50\u65e5\u671f, \u65e0\u7b5b\u9009",
            "\u5b8c\u6574\u65e5\u5e8f\u5217\u7ed9 QC",
        ],
        [
            "02",
            "02_rs_debug.csv",
            "\u65e5",
            "QC (\u8d28\u91cf\u63a7\u5236) \u5224\u5b9a\u8868",
            "\u771f\u5b9e\u89c2\u6d4b/\u7a97\u53e3\u53ef\u7528/\u6c14\u8c61\u5b8c\u6574/\u6307\u6807\u6709\u9650",
            "\u4ea7\u51fa qc_ok / allow_alert \u7ed9\u544a\u8b66",
        ],
        [
            "03",
            "03_alerts_raw.csv",
            "\u544a\u8b66",
            "raw (\u53ea\u770b QC) \u89e6\u53d1\u544a\u8b66",
            "qc_ok & \u544a\u8b66\u89c4\u5219\u89e6\u53d1",
            "\u63d0\u4f9b raw \u544a\u8b66\u6761\u6570",
        ],
        [
            "04",
            "04_alerts_gated.csv",
            "\u544a\u8b66",
            "gated (\u95e8\u7981/gating) \u540e\u4fdd\u7559\u544a\u8b66",
            "allow_alert & \u544a\u8b66\u89c4\u5219\u89e6\u53d1",
            "\u4f5c\u4e3a\u4e3b\u8f93\u51fa\u544a\u8b66\u6e05\u5355",
        ],
        [
            "05",
            "05_events.csv",
            "\u4e8b\u4ef6",
            "event merge (\u4e8b\u4ef6\u5408\u5e76)",
            "\u540c\u7c7b\u544a\u8b66 gap<=X \u5929\u5408\u5e76",
            "\u8d77\u6b62/\u5cf0\u503c/\u6458\u8981 \u4e8b\u4ef6\u6e05\u5355",
        ],
    ]
    return rows


def main() -> None:
    setup_logging_from_cfg(CFG, app_name="make_report")
    logger = logging.getLogger(__name__)
    summary = _load_stage_summary()
    totals = summary.get("totals", {})

    total_days = totals.get("total_days", "-")
    qc_ok_days = totals.get("qc_ok_days", "-")
    raw_alerts = totals.get("raw_alerts", "-")
    gated_alerts = totals.get("gated_alerts", "-")
    events = totals.get("events", "-")
    allow_alert_days = totals.get("allow_alert_days", "-")

    md: list[str] = []
    md += ["# " + "\u590d\u5408\u544a\u8b66\u7b5b\u9009\u94fe\u6761\u7b80\u62a5", ""]

    md += ["## " + "\u7b5b\u9009\u5c42\u7ea7\u89e3\u91ca\u8868", ""]
    header = [
        "Stage",
        "\u6587\u4ef6\u540d",
        "\u7c92\u5ea6",
        "\u8fd9\u4e00\u6b65\u505a\u4ec0\u4e48",
        "\u901a\u8fc7\u6761\u4ef6 (\u4e2d\u6587)",
        "\u8f93\u51fa\u7ed9\u4e0b\u4e00\u6b65\u4ec0\u4e48",
    ]
    table_rows = _stage_table()
    md += ["|" + "|".join(header) + "|"]
    md += ["|" + "|".join(["---"] * len(header)) + "|"]
    for row in table_rows:
        md += ["|" + "|".join(row) + "|"]
    md += [""]

    md += ["## " + "\u7b5b\u9009\u6f0f\u6597 (\u65e5\u6570 + \u544a\u8b66\u6761\u6570 + \u4e8b\u4ef6\u6570)", ""]
    md += ["![alert pipeline funnel](alert_pipeline_funnel.png)", ""]
    md += [
        "\u4e00\u53e5\u8bdd\u56de\u7b54 \u201c\u7b5b\u4e86\u51e0\u6b21\u201d:",
        f"QC (\u8d28\u91cf\u63a7\u5236) \u7b5b\u65e5\u5b50 {total_days} -> {qc_ok_days},",
        f"gating (\u95e8\u7981) \u7b5b\u65e5\u5b50 {qc_ok_days} -> {allow_alert_days},",
        f"\u4e8b\u4ef6\u5408\u5e76\u628a\u544a\u8b66 {gated_alerts} \u6761\u538b\u6210 {events} \u4ef6.",
        "",
    ]
    md += [
        "\u63d0\u793a: QC \u4f1a\u533a\u5206 \u771f\u5b9e\u89c2\u6d4b\u65e5 \u4e0e window (\u7a97\u53e3) \u652f\u6491\u65e5.",
        "",
    ]
    md += [
        "\u903b\u8f91\u6e05\u695a\u4e00\u53e5\u8bdd:",
        "\u544a\u8b66\u4ea7\u751f\u6761\u4ef6\u662f QC\u901a\u8fc7 (\u6570\u636e\u53ef\u7528) -> gating\u901a\u8fc7 (\u5141\u8bb8\u544a\u8b66) -> \u89c4\u5219\u89e6\u53d1.",
        "",
    ]

    md += ["## " + "\u6700\u7ec8\u8f93\u51fa\u6982\u89c8", ""]
    md += [
        f"- \u7edf\u8ba1\u8303\u56f4: {PERIOD_REPORT_START.isoformat()} \u81f3 {PERIOD_REPORT_END.isoformat()}",
    ]
    md += [
        f"- \u7b5b\u5b8c\u5269\u4f59\u5929\u6570: {allow_alert_days}",
        f"- raw \u544a\u8b66 (QC\u540e) : {raw_alerts} \u6761",
        f"- gated \u544a\u8b66 (\u95e8\u7981\u540e) : {gated_alerts} \u6761",
        f"- \u5408\u5e76\u4e8b\u4ef6: {events} \u4ef6",
        "",
    ]

    gated_df = _load_csv(ALERTS_GATED_CSV)
    gated_df = _filter_by_report_range(gated_df, "date")
    gated_counts = _alert_counts(gated_df)
    md += ["## " + "\u544a\u8b66\u7c7b\u578b\u6570\u91cf (gated)", ""]
    md += ["|\u544a\u8b66\u7c7b\u578b|\u6570\u91cf|"]
    md += ["|---|---|"]
    if gated_counts:
        for name, count in gated_counts:
            md += [f"|{name}|{count}|"]
    else:
        md += ["|\u65e0\u6570\u636e|0|"]
    md += [""]

    events_df = _load_csv(ALERTS_MERGED_CSV)
    events_df = _filter_by_report_range(events_df, "start_date")
    event_counts = _event_counts(events_df)
    md += ["## " + "\u4e8b\u4ef6\u7c7b\u578b\u6570\u91cf (merged)", ""]
    md += ["|\u4e8b\u4ef6\u7c7b\u578b|\u6570\u91cf|"]
    md += ["|---|---|"]
    if event_counts:
        for name, count in event_counts:
            md += [f"|{name}|{count}|"]
    else:
        md += ["|\u65e0\u6570\u636e|0|"]
    md += [""]

    md += ["## " + "\u4e8b\u4ef6\u7c7b\u578b\u6708\u5ea6\u5206\u5e03", ""]
    md += ["![events monthly by type](events_monthly_by_type.png)", ""]
    md += ["## " + "\u4e8b\u4ef6\u7c7b\u578b\u5360\u6bd4", ""]
    md += ["![events type pie](events_type_pie.png)", ""]

    md += ["## " + "\u672f\u8bed\u8868 (\u7b80\u77ed\u5b9a\u4e49)", ""]
    md += [
        "- QC (\u8d28\u91cf\u63a7\u5236): \u5224\u65ad\u67d0\u5929\u9065\u611f\u6307\u6570\u662f\u5426\u53ef\u7528",
        "- window (\u7a97\u53e3): \u524d\u540e\u4e24\u5929\u542b\u5f53\u5929\u7684\u652f\u6491\u7a97\u53e3 (\u00b12)",
        "- \u771f\u5b9e\u89c2\u6d4b\u65e5: \u5f53\u5929\u5b58\u5728\u9065\u611f\u539f\u59cb\u89c2\u6d4b\u503c (*_obs \u975e\u7a7a)",
        "- rs_age: \u5f53\u5929\u8ddd\u79bb\u6700\u8fd1\u9065\u611f\u89c2\u6d4b\u65e5\u7684\u5929\u6570",
        "- gating (\u95e8\u7981): \u751f\u957f\u5b63/\u51a0\u5c42\u6761\u4ef6\u8fc7\u6ee4, \u4ec5\u51b3\u5b9a\u65e5\u671f\u8d44\u683c",
        "- allow_alert (\u5141\u8bb8\u544a\u8b66): QC + gating \u5408\u683c\u7684\u65e5\u5b50",
        "- raw vs gated: raw=\u53ea\u770b QC, gated=\u518d\u52a0 gating",
        "- event merge (\u4e8b\u4ef6\u5408\u5e76): \u8fde\u7eed\u544a\u8b66\u5929\u5408\u5e76\u4e3a\u4e8b\u4ef6",
    ]

    cleaned = [line.rstrip() for line in md]
    OUT.write_text("\n".join(cleaned).rstrip() + "\n", encoding="utf-8")
    logger.info("Report written to %s", OUT)


if __name__ == "__main__":
    main()
