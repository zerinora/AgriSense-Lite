# AgriSense-Lite · 智慧农业轻量化监测流水线

AgriSense-Lite 是一个轻量化的智慧农业监测原型：融合 ERA5 日尺度气象与 Sentinel-2 遥感指数，识别干旱、水涝、热/冷胁迫、营养/病虫等异常。

## 功能概览

- 获取 ERA5 气象日表（Open-Meteo）
- 通过 GEE 拉取 Sentinel-2 指数（NDVI/NDMI/NDRE/EVI/GNDVI/MSI）
- 合并气象与遥感为日尺度总表
- 复合事件判定与可视化/报告输出

## 目录结构

- **src/** 可复用的程序（库函数与模块）
- **scripts/** 可执行入口（命令行外壳），解析参数后调用 src
- **config/** 参数中心（研究区、时间段、路径、变量、阈值），**只改这里**
- **data/raw/** 原始层（外部接口/首次导出的第一落地），**禁止手改**
- **data/processed/** 处理层（清洗/合并/派生后的分析结果，可由代码重建）
- **notebooks/** 探索、可视化与出图（尽量调用 src）
- **docs/** 报告、计划书、PPT 等文档
- **assets/** 图片、底图、Logo 等静态资源
- **logs/** 运行日志与调试输出

## 关键输入/输出（默认）

- 配置文件：`config/config.yml`
- 气象原始表：`data/raw/weather.csv`
- 遥感指数表：`data/raw/indices.csv`
- 融合总表：`data/processed/merged.csv`
- 复合告警：`data/processed/alerts_composite.csv`
- 图表/报告：`assets/composite_alerts.png`, `assets/report_composite.md`

## 快速开始

1. 安装依赖：`pip install -r requirements.txt`
2. 配置研究区与时间段：`config/config.yml`
3. 获取气象：`python scripts/fetch_weather.py`
4. 获取遥感指数（需 GEE 账号与项目）：`python scripts/fetch_indices.py`
5. 合并数据：`python scripts/build_merged.py`
6. 生成复合告警：`python scripts/build_composite_alerts.py`
7. 可视化与报告：
   - `python scripts/plot_composite_alerts.py`
   - `python scripts/make_report.py`

## 告警规则（简版）

- 仅当冠层可靠（NDVI ≥ 0.45 或 EVI ≥ 0.35）且遥感更新时间 ≤ 5 天时，才判定冷/热/营养/病虫、水涝等事件
- 冷胁迫使用 7 日最低气温阈值（`tmin_7d < 3℃`），并要求指数低/下跌
- 休耕/冬闲：11–3 月若无可靠冠层，视为正常
- 遥感缺测或“陈旧（>5 天）”当天不做冠层相关异常判定
