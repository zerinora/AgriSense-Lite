from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"


def _run_script(name: str) -> None:
    script = SCRIPTS / name
    if not script.exists():
        raise SystemExit(f"Missing script: {script}")
    subprocess.run([sys.executable, str(script)], check=True)


def main() -> None:
    for name in ("fetch_weather.py", "fetch_indices.py", "build_merged.py"):
        _run_script(name)
    print("[OK] Pipeline fetch/merge finished.")


if __name__ == "__main__":
    main()
