# -*- coding: utf-8 -*-
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from analysis.composite_alerts import run  # noqa

if __name__ == "__main__":
    out = run()
    print("[OK] Composite alerts saved to", out)
