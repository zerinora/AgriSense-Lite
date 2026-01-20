from __future__ import annotations

import subprocess
import sys
import os
import logging
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.utils.config_loader import CFG, WEATHER_CSV, INDICES_CSV, MERGED_CSV
from src.utils.logging_utils import setup_logging_from_cfg


def _run_script(name: str, env: dict) -> None:
    script = SCRIPTS / name
    if not script.exists():
        raise SystemExit(f"Missing script: {script}")
    subprocess.run([sys.executable, str(script)], check=True, env=env)


def main() -> None:
    run_id = setup_logging_from_cfg(CFG, app_name="pipeline_fetch_merge")
    logger = logging.getLogger(__name__)
    env = os.environ.copy()
    env["AGRISENSE_RUN_ID"] = run_id
    status = "ok"
    failed_stage = None
    for name in ("fetch_weather.py", "fetch_indices.py", "build_merged.py"):
        logger.info("Running %s", name)
        try:
            _run_script(name, env)
        except subprocess.CalledProcessError as exc:
            status = "failed"
            failed_stage = name
            logger.error("Pipeline failed at %s (exit=%s)", name, exc.returncode)
            break

    outputs = [
        f"weather={WEATHER_CSV}",
        f"indices={INDICES_CSV}",
        f"stage01={MERGED_CSV}",
    ]
    summary = "status={0} run_id={1} outputs={2}".format(
        status, run_id, "; ".join(outputs)
    )
    if failed_stage:
        summary += f" failed_at={failed_stage}"
    logger.info(summary)
    if status != "ok":
        raise SystemExit(1)

    logger.info("Pipeline fetch/merge finished.")


if __name__ == "__main__":
    main()
