# AgriSense-Lite

## 简介（CN）

AgriSense-Lite 是一个**强可配置**的轻量智慧农业监测/告警管线，核心只依赖两类信息源：

- **逐日气象（Open-Meteo）**：温度、降水、辐射、风速、湿度等

- **遥感指数（Sentinel-2 / GEE）**：NDVI/NDMI/NDRE/EVI/GNDVI/MSI 等

你可以通过 `config/config.yml` 进行“强自定义”：

- **地点与区域**：中心点 + ROI（矩形或 GeoJSON 多边形）、区域大小

- **时间口径**：数据拉取范围（data_range）与报告统计范围（report_range）分离

- **遥感有效支撑**：窗口半径/取样策略（用于决定某天是否有“遥感支撑”）

- **生长季门禁（gating）**：月份窗口、冠层就绪（观测次数 + 指数阈值）

- **病虫害/胁迫规则阈值**：干旱/水涝/冷热/营养病虫疑似等可调参数

输出包括：日级告警（raw / gated）、合并后的事件统计（events）、以及**可解释的报告与图表**。

一句话：这是一个“用气象 + Sentinel-2 指数做农业风险告警”的可复用原型，重点在**配置驱动**与**可解释产物**。

## Overview (EN)

AgriSense-Lite is a **highly configurable** lightweight smart-ag monitoring/alert pipeline built on only two inputs:

- **Daily weather (Open-Meteo)**: temperature, precipitation, radiation, wind, humidity, etc.

- **Remote-sensing indices (Sentinel-2 via GEE)**: NDVI/NDMI/NDRE/EVI/GNDVI/MSI, etc.

Everything is driven by `config/config.yml`, including:

- **Location & ROI**: rectangle or GeoJSON polygon, adjustable region size

- **Time scopes**: `data_range` (fetch/merge) separated from `report_range` (stats/plots/report)

- **RS support window**: window radius & selection strategy (whether a day is “RS-supported”)

- **Season gating**: month window and canopy readiness (min obs + index thresholds)

- **Rule thresholds**: tunable parameters for drought/waterlogging/heat/cold/nutrient-or-pest signals

Outputs include daily alerts (raw/gated), merged events, and an **explainable report with charts**.

In short: a config-driven, explainable prototype for agricultural risk alerts from weather + Sentinel-2 indices.

## 功能 / Features

- **强自定义配置入口**：ROI、时间范围、窗口口径、门禁策略、规则阈值均可配置

- **两源融合**：逐日气象 + Sentinel-2 指数合并为日序列底表

- **可解释筛选链条**：QC → gating → rules → event merge，每一步都有理由与统计

- **多类告警与事件合并**：日级触发 + 连续触发合并为事件（起止/持续/峰值/摘要）

- **一键交付产物**：自动生成报告（md）与核心图表（png）

- **Config-first customization**: ROI, time ranges, window policy, gating strategy, thresholds

- **Two-source fusion**: daily weather + Sentinel-2 indices into a daily base table

- **Explainable pipeline**: QC → gating → rules → event merge with reasons & counts

- **Alerts + events**: daily triggers merged into events (start/end/duration/peak/summary)

- **One-click deliverables**: report (md) + key charts (png)

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
