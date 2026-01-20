# AgriSense-Lite

## 简介（CN）

AgriSense-Lite 是一个轻量级农业告警管线，融合逐日气象与 Sentinel-2 指数。
它按 QC（质量控制）→ gating（门禁/生长季门控）→ 规则触发 → 事件合并 的顺序
输出告警与报告，便于快速解释“筛选了几次、每次剩多少”。

## Overview (EN)

AgriSense-Lite is a lightweight alert pipeline that fuses daily weather and Sentinel-2 indices.
It follows QC (quality control) → gating (season eligibility) → rules → event merge
and produces alerts and a concise report.

## 功能 / Features

- 逐日气象与遥感指数融合 / Daily weather + remote-sensing fusion
- QC（质量控制）判定与可追溯理由 / QC decisions with traceable reasons
- gating（门禁/生长季门控）过滤 / Season eligibility gating
- 多类告警规则与事件合并 / Multiple alert rules and event merge
- 一键输出报告与图表 / One-click report and charts

## 快速开始 / Quick Start

1. 安装依赖 / Install dependencies.

```bash
pip install -r requirements.txt
```

1. 编辑配置 / Edit configuration.

```bash
notepad config/config.yml
```

1. 拉取并合并基础数据 / Fetch and merge inputs.

```bash
python scripts/pipeline_fetch_merge.py
```

1. 生成告警与报告 / Generate alerts and report.

```bash
python scripts/pipeline_composite_report.py
```

## 运行流程 / Pipeline Stages

管线核心顺序如下：
01_merged（日序列底表） → 02_rs_debug（QC 判定） → 03_alerts_raw（仅 QC）
→ 04_alerts_gated（QC + gating） → 05_events（事件合并） → 报告与图表。

## 输入输出 / Inputs & Outputs

- 配置 / Config: `config/config.yml`
- 气象 / Weather: `data/raw/weather.csv`
- 遥感指数 / Indices: `data/raw/indices.csv`
- Stage 01: `data/processed/01_merged.csv`
- Stage 02: `data/processed/02_rs_debug.csv`
- Stage 03: `data/processed/03_alerts_raw.csv`
- Stage 04: `data/processed/04_alerts_gated.csv`
- Stage 05: `data/processed/05_events.csv`
- 汇总统计 / Summaries: `data/processed/*.summary.json`, `data/processed/stage_summary.json`
- 报告与图 / Report & charts: `assets/report_composite.md`, `assets/alert_pipeline_funnel.png`,
  `assets/events_monthly_by_type.png`, `assets/events_type_pie.png`

## 配置说明 / Configuration

### 区域与时间段 / Region & Period

区域和时间段是项目最主要的个性化入口。
只改 `region` 和 `period` 就能在新区域/新时间段运行。

Region and period are the primary customization knobs.
Editing `region` and `period` is enough to run on new areas/time ranges.

```yaml
region:
  id: "SCXJ_Baodun_6x6km"
  name: "Sichuan Xinjin Baodun 6x6km"
  center_lat: 30.433
  center_lon: 103.75
  roi_polygon_geojson: ""
  roi_rectangle: [103.72, 30.403, 103.78, 30.463]
  timezone: "Asia/Shanghai"

period:
  data_start: "2019-01-01"
  data_end: "2025-10-01"
  report_start: "2019-01-01"
  report_end: "2025-10-01"
```

常见坑 / Common pitfalls:

- bbox 顺序固定为 `[minLon, minLat, maxLon, maxLat]`。
- 时间范围是闭区间（含起止日）。
- `report_*` 必须在 `data_*` 范围内，否则会报错。

- BBox order is `[minLon, minLat, maxLon, maxLat]`.
- Date ranges are inclusive.
- `report_*` must be within `data_*`, otherwise an error is raised.

### 其他配置 / Other Settings

- `remote_sensing.window_half_days`: 遥感支撑窗口半径（默认 ±2 天，含当天）。
- `remote_sensing.window_mode`: `symmetric` 或 `past_only`，控制是否允许未来观测。
- `gating.mode`: `off` / `month_window` / `canopy_obs` / `both`。
- `gating.months`: 生长季月份窗口。
- `gating.canopy_obs_min`: 冠层就绪最少观测次数。
- 告警阈值：在 `config.yml` 的 `composite_alerts` 中配置。

## 结果解读 / How to Read Outputs

QC（质量控制）用于判断“这一天数据是否可用”，gating（门禁）用于判断
“这一天是否允许产生告警”。raw / gated / events 分别表示
QC 后触发的告警、门禁后保留的告警、合并后的事件。

注意区分“天数”与“告警条数”：
天数用于 QC/gating 的筛选，告警条数用于规则触发与事件合并。

![告警计数漏斗 / Alert pipeline funnel](assets/alert_pipeline_funnel.png)

![事件类型月度分布 / Events by month](assets/events_monthly_by_type.png)

![事件类型占比 / Events pie](assets/events_type_pie.png)

一句话逻辑：QC 通过（数据可用）→ gating 通过（允许告警）→ 规则触发 → 事件合并。

## Logging & Rotation（日志与轮转）

日志使用 Python 标准库 logging，默认控制台 + 文件双输出，日志文件统一在 `logs/`。
每次运行会生成 `run_id`，并用于日志文件名与行内字段，便于对齐同一次运行。

Logging uses Python standard logging with console + file output. Logs are under `logs/`.
Each run has a `run_id` that appears in filenames and log lines for traceability.

- level：`config.yml` 的 `logging.level` 控制日志级别。
- console + file：`logging.to_console` 与 `logging.to_file` 控制双输出。
- dir：`logging.dir` 控制日志目录（默认 `logs/`）。
- daily/size：`logging.rotate` 支持 `daily`（默认）或 `size`（按大小）。
- backup_count：控制保留的历史文件数。
- run_id：日志文件命名为 `logs/<script>_<run_id>.log`，行内包含 `run=<id>`。

Windows 注意：使用 size 轮转时若报 `PermissionError`，通常是日志文件被占用。
请关闭 VS Code 的日志预览、文件资源管理器预览窗格或记事本等占用程序后重试。

## FAQ

Q: 为什么 gating 后仍然有告警？
A: gating 只是过滤日期资格，不改告警规则，所以合格日期仍可能触发告警。

Q: “天数”和“告警条数”为什么不同？
A: 天数统计的是日级筛选，告警条数统计的是触发次数，两者口径不同。
