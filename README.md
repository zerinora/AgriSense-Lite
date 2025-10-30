# AgriSense-Lite · 项目骨架说明

这是标准化的智慧农业原型系统目录。规则很简单：
- **src/** 放“可复用的程序”（库函数与模块），不要把业务逻辑散落在 Notebook；
- **scripts/** 放“可执行入口”（命令行外壳），解析参数后调用 src 里的函数；
- **config/** 集中写参数（研究区、时间段、路径、变量、阈值），**只改这里**；
- **data/raw/** 是原始层，外部接口/首次导出的第一落地；**禁止手改**；
- **data/processed/** 是处理层，清洗/合并/派生后的分析基线，可由代码重建；
- **notebooks/** 做探索、可视化与出图，但尽量调用 src 中的函数；
- **docs/** 放报告、计划书、PPT 等文档；
- **assets/** 放图片、底图、Logo 等静态资源；
- **logs/** 放运行日志与调试输出。

## 关键路径（默认）
- 气象原始表：`data/raw/weather.csv`
- NDVI 原始表：`data/raw/ndvi.csv`
- 融合总表：`data/processed/merged.csv`
- 配置文件：`config/config.yml`
- 日志目录：`logs/`
