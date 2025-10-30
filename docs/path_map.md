# 路径与文件映射（Path Map）

| 模块 | 输入 | 输出 | 说明 |
|---|---|---|---|
| 气象获取 (Open-Meteo) | config.yml: open_meteo.* | data/raw/weather.csv | 逐日表，含日期与各气象变量 |
| NDVI 获取 (GEE 导出) | config.yml: gee_s2.* | data/raw/ndvi.csv | ROI 面均值 NDVI 时序 |
| 数据融合 (merge) | weather.csv + ndvi.csv | data/processed/merged.csv | 以 `date` 为键，左连接或内连接 |
