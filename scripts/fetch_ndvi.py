# scripts/fetch_ndvi.py
# 作用：示范脚本外壳（仅打印占位信息）
from src.utils.config_loader import CFG, NDVI_CSV

if __name__ == "__main__":
    print("[INFO] 将把 NDVI 原始表写入：", NDVI_CSV)
    print("[INFO] ROI 矩形：", CFG["region"]["roi_rectangle"])
    print("[OK] 此脚本仅为占位示例。")
