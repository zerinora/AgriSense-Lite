# 路径与文件映射（Path Map）

| 模块 | 输入 | 输出 | 说明 |
|---|---|---|---|
| 气象获取 (Open-Meteo) | config.yml: open_meteo.* | data/raw/weather.csv | 逐日表，含日期与各气象变量 |
| 指数获取 (GEE) | config.yml: gee_s2.* | data/raw/indices.csv | ROI 面均值 NDVI/EVI/NDMI/NDRE/GNDVI/MSI |
| 数据融合 (merge) | weather.csv + indices.csv | data/processed/01_merged.csv | 以 `date` 为键，左连接 |
| QC 判卷/门禁基础 | 01_merged.csv | data/processed/02_rs_debug.csv | 记录 rs_age、skip_reason、gating_ok |
| 逐日告警（raw） | 01_merged.csv | data/processed/03_alerts_raw.csv | QC 后，不做 gating |
| 逐日告警（gated） | 01_merged.csv + 02_rs_debug.csv | data/processed/04_alerts_gated.csv | QC + gating 后主输出 |
| 事件合并 | 04_alerts_gated.csv | data/processed/05_events.csv | gap<=merge_gap_days 合并 |
