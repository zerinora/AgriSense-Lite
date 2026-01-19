from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from analysis.composite_alerts import OUT_DEBUG, OUT_MERGED, OUT_RAW, run

if __name__ == "__main__":
    out = run()
    print("[OK] Composite alerts saved to", out)
    print("[OK] Raw alerts saved to", OUT_RAW)
    print("[OK] Merged events saved to", OUT_MERGED)
    print("[OK] RS debug saved to", OUT_DEBUG)
