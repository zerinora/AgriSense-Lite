from __future__ import annotations

import json
import sys
import logging
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

try:
    from utils.config_loader import (
        CFG,
        WEATHER_CSV,
        INDICES_CSV,
        MERGED_CSV,
        RS_DEBUG_CSV,
        ALERTS_RAW_CSV,
        ALERTS_GATED_CSV,
        ALERTS_MERGED_CSV,
        PERIOD_DATA_START,
        PERIOD_DATA_END,
        PERIOD_REPORT_START,
        PERIOD_REPORT_END,
    )
    from utils.logging_utils import setup_logging_from_cfg
except ImportError:
    from src.utils.config_loader import (
        CFG,
        WEATHER_CSV,
        INDICES_CSV,
        MERGED_CSV,
        RS_DEBUG_CSV,
        ALERTS_RAW_CSV,
        ALERTS_GATED_CSV,
        ALERTS_MERGED_CSV,
        PERIOD_DATA_START,
        PERIOD_DATA_END,
        PERIOD_REPORT_START,
        PERIOD_REPORT_END,
    )
    from src.utils.logging_utils import setup_logging_from_cfg


def _load_csv(path: Path, parse_dates: list[str] | None = None) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, parse_dates=parse_dates or [])


def _rel(path: Path) -> str:
    try:
        return path.relative_to(ROOT).as_posix()
    except ValueError:
        return str(path)


def _summary_path(csv_path: Path) -> Path:
    return csv_path.with_suffix(".summary.json")


def _stage_summary_path() -> Path:
    return MERGED_CSV.parent / "stage_summary.json"


def _thresholds(cfg: dict, rs_cfg: dict, gating_cfg: dict) -> dict:
    keys = [
        "ndvi_crop",
        "evi_crop",
        "rs_max_age",
        "ndmi_dry",
        "msi_dry",
        "precip_low7",
        "ndmi_wet",
        "precip_high7",
        "heat_tmean7",
        "heat_rh7",
        "cold_tmin7",
        "ndre_low",
        "gndvi_low",
        "slope7_drop",
        "merge_gap_days",
    ]
    out = {}
    for k in keys:
        if k in cfg:
            v = cfg[k]
            if isinstance(v, tuple):
                v = list(v)
            out[k] = v

    out.update(
        {
            "remote_sensing.window_half_days": rs_cfg.get("window_half_days"),
            "remote_sensing.window_mode": rs_cfg.get("window_mode"),
            "remote_sensing.support_pick": rs_cfg.get("support_pick"),
            "gating.mode": gating_cfg.get("mode"),
            "gating.months": gating_cfg.get("months"),
            "gating.canopy_obs_min": gating_cfg.get("canopy_obs_min"),
            "gating.canopy_ndvi_min": gating_cfg.get("canopy_ndvi_min"),
            "gating.canopy_evi_min": gating_cfg.get("canopy_evi_min"),
        }
    )
    return out


def _pass_rates(debug: pd.DataFrame) -> dict:
    if debug.empty:
        return {
            "qc_pass_rate": None,
            "gating_pass_rate": None,
            "allow_alert_rate": None,
        }
    total = len(debug)
    qc_ok = debug["qc_ok"] if "qc_ok" in debug.columns else (debug["skip_reason"] == "ok")
    gating_ok = debug["gating_ok"] if "gating_ok" in debug.columns else pd.Series(False, index=debug.index)
    allow_alert = debug["allow_alert"] if "allow_alert" in debug.columns else (qc_ok & gating_ok)
    qc_ok_count = int(qc_ok.sum())
    gating_pass_rate = float((qc_ok & gating_ok).sum()) / qc_ok_count if qc_ok_count else 0.0
    return {
        "qc_pass_rate": float(qc_ok.mean()) if total else 0.0,
        "gating_pass_rate": float(gating_pass_rate),
        "allow_alert_rate": float(allow_alert.mean()) if total else 0.0,
    }


def _skip_reason_stats(debug: pd.DataFrame) -> dict:
    if debug.empty or "skip_reason" not in debug.columns:
        return {}
    total = len(debug)
    order = ["missing_remote", "missing_weather", "nonfinite", "ok"]
    counts = debug["skip_reason"].value_counts(dropna=False).reindex(order, fill_value=0)
    out = {}
    for key in order:
        if key in counts:
            count = int(counts[key])
            out[key] = {"count": count, "ratio": round(count / total, 4)}
    for key, count in debug["skip_reason"].value_counts(dropna=False).items():
        if key not in out:
            out[str(key)] = {"count": count, "ratio": round(count / total, 4)}
    return out


def _qc_counts(debug: pd.DataFrame) -> dict:
    if debug.empty:
        return {}
    return {
        "total_days": int(len(debug)),
        "real_obs_days": int(debug.get("real_obs_day", pd.Series(False, index=debug.index)).sum()),
        "rs_window_ok_days": int(debug.get("rs_window_ok", pd.Series(False, index=debug.index)).sum()),
        "qc_ok_days": int(debug.get("qc_ok", pd.Series(False, index=debug.index)).sum()),
        "allow_alert_days": int(debug.get("allow_alert", pd.Series(False, index=debug.index)).sum()),
    }


def _write_summary(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _filter_by_report_range(df: pd.DataFrame, date_col: str) -> pd.DataFrame:
    if df.empty or date_col not in df.columns:
        return df
    start = pd.Timestamp(PERIOD_REPORT_START)
    end = pd.Timestamp(PERIOD_REPORT_END)
    return df[(df[date_col] >= start) & (df[date_col] <= end)]


def _build_stage_summary(
    merged: pd.DataFrame,
    debug: pd.DataFrame,
    alerts_raw: pd.DataFrame,
    alerts_gated: pd.DataFrame,
    merged_events: pd.DataFrame,
) -> dict:
    total_days = int(len(merged))
    qc_ok_days = int(debug.get("qc_ok", pd.Series(False, index=debug.index)).sum()) if not debug.empty else 0
    allow_alert_days = int(debug.get("allow_alert", pd.Series(False, index=debug.index)).sum()) if not debug.empty else 0
    raw_alerts = int(len(alerts_raw))
    gated_alerts = int(len(alerts_gated))
    events_count = int(len(merged_events))

    stages = [
        {
            "stage": "01",
            "file": _rel(MERGED_CSV),
            "granularity": "days",
            "days_count": total_days,
            "alerts_count": None,
            "events_count": None,
            "removed_count": None,
        },
        {
            "stage": "02",
            "file": _rel(RS_DEBUG_CSV),
            "granularity": "days",
            "days_count": qc_ok_days,
            "alerts_count": None,
            "events_count": None,
            "removed_count": max(total_days - qc_ok_days, 0),
        },
        {
            "stage": "03",
            "file": _rel(ALERTS_RAW_CSV),
            "granularity": "alerts",
            "days_count": qc_ok_days,
            "alerts_count": raw_alerts,
            "events_count": None,
            "removed_count": None,
        },
        {
            "stage": "04",
            "file": _rel(ALERTS_GATED_CSV),
            "granularity": "alerts",
            "days_count": allow_alert_days,
            "alerts_count": gated_alerts,
            "events_count": None,
            "removed_count": max(qc_ok_days - allow_alert_days, 0),
        },
        {
            "stage": "05",
            "file": _rel(ALERTS_MERGED_CSV),
            "granularity": "events",
            "days_count": allow_alert_days,
            "alerts_count": gated_alerts,
            "events_count": events_count,
            "removed_count": None,
        },
    ]

    return {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "totals": {
            "total_days": total_days,
            "qc_ok_days": qc_ok_days,
            "allow_alert_days": allow_alert_days,
            "raw_alerts": raw_alerts,
            "gated_alerts": gated_alerts,
            "events": events_count,
        },
        "stages": stages,
    }


def main() -> None:
    setup_logging_from_cfg(CFG, app_name="build_stage_summaries")
    logger = logging.getLogger(__name__)
    try:
        weather = _load_csv(WEATHER_CSV)
        indices = _load_csv(INDICES_CSV)
        merged = _load_csv(MERGED_CSV, parse_dates=["date"])
        debug = _load_csv(RS_DEBUG_CSV, parse_dates=["date", "rs_support_date"])
        alerts_raw = _load_csv(ALERTS_RAW_CSV, parse_dates=["date"])
        alerts_gated = _load_csv(ALERTS_GATED_CSV, parse_dates=["date"])
        merged_events = _load_csv(
            ALERTS_MERGED_CSV, parse_dates=["start_date", "end_date", "peak_date"]
        )

        merged_report = _filter_by_report_range(merged, "date")
        debug_report = _filter_by_report_range(debug, "date")
        alerts_raw_report = _filter_by_report_range(alerts_raw, "date")
        alerts_gated_report = _filter_by_report_range(alerts_gated, "date")
        events_report = _filter_by_report_range(merged_events, "start_date")

        alert_cfg = CFG.get("composite_alerts", {}) if isinstance(CFG, dict) else {}
        rs_cfg = CFG.get("remote_sensing", {}) if isinstance(CFG, dict) else {}
        gating_cfg = CFG.get("gating", {}) if isinstance(CFG, dict) else {}

        thresholds = _thresholds(alert_cfg, rs_cfg, gating_cfg)
        pass_rates = _pass_rates(debug_report)
        skip_reason = _skip_reason_stats(debug_report)
        qc_counts = _qc_counts(debug_report)
        timestamp = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

        ranges = {
            "data_range": {
                "start": PERIOD_DATA_START.isoformat(),
                "end": PERIOD_DATA_END.isoformat(),
            },
            "report_range": {
                "start": PERIOD_REPORT_START.isoformat(),
                "end": PERIOD_REPORT_END.isoformat(),
            },
        }

        summaries = [
            {
                "stage": {"id": "stage_1", "name": "merged"},
                "paths": {"inputs": [_rel(WEATHER_CSV), _rel(INDICES_CSV)], "output": _rel(MERGED_CSV)},
                "rows": {
                    "inputs": {"weather": int(len(weather)), "indices": int(len(indices))},
                    "output": int(len(merged_report)),
                },
                "ranges": ranges,
                "pass_rates": pass_rates,
                "skip_reason": skip_reason,
                "thresholds": thresholds,
                "generated_at": timestamp,
            },
            {
                "stage": {"id": "stage_2", "name": "rs_debug"},
                "paths": {"inputs": [_rel(MERGED_CSV)], "output": _rel(RS_DEBUG_CSV)},
                "rows": {"inputs": int(len(merged_report)), "output": int(len(debug_report))},
                "qc_counts": qc_counts,
                "ranges": ranges,
                "pass_rates": pass_rates,
                "skip_reason": skip_reason,
                "thresholds": thresholds,
                "generated_at": timestamp,
            },
            {
                "stage": {"id": "stage_3", "name": "alerts_raw", "gating_applied": False},
                "paths": {"inputs": [_rel(MERGED_CSV)], "output": _rel(ALERTS_RAW_CSV)},
                "rows": {"inputs": int(len(merged_report)), "output": int(len(alerts_raw_report))},
                "ranges": ranges,
                "pass_rates": pass_rates,
                "skip_reason": skip_reason,
                "thresholds": thresholds,
                "generated_at": timestamp,
            },
            {
                "stage": {"id": "stage_4", "name": "alerts_gated", "gating_applied": True},
                "paths": {"inputs": [_rel(MERGED_CSV)], "output": _rel(ALERTS_GATED_CSV)},
                "rows": {"inputs": int(len(merged_report)), "output": int(len(alerts_gated_report))},
                "ranges": ranges,
                "pass_rates": pass_rates,
                "skip_reason": skip_reason,
                "thresholds": thresholds,
                "generated_at": timestamp,
            },
            {
                "stage": {"id": "stage_5", "name": "events_merged"},
                "paths": {"inputs": [_rel(ALERTS_GATED_CSV)], "output": _rel(ALERTS_MERGED_CSV)},
                "rows": {"inputs": int(len(alerts_gated_report)), "output": int(len(events_report))},
                "ranges": ranges,
                "pass_rates": pass_rates,
                "skip_reason": skip_reason,
                "thresholds": thresholds,
                "generated_at": timestamp,
            },
        ]

        outputs = [
            (MERGED_CSV, summaries[0]),
            (RS_DEBUG_CSV, summaries[1]),
            (ALERTS_RAW_CSV, summaries[2]),
            (ALERTS_GATED_CSV, summaries[3]),
            (ALERTS_MERGED_CSV, summaries[4]),
        ]
        for csv_path, payload in outputs:
            _write_summary(_summary_path(csv_path), payload)

        stage_summary = _build_stage_summary(
            merged_report, debug_report, alerts_raw_report, alerts_gated_report, events_report
        )
        stage_summary["ranges"] = ranges
        _write_summary(_stage_summary_path(), stage_summary)

        logger.info("Stage summaries written to %s", _rel(ALERTS_MERGED_CSV.parent))
    except Exception as exc:
        logger.error("Stage summaries failed: %s", exc)
        logger.debug("Stage summaries exception detail", exc_info=exc)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
