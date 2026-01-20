from pathlib import Path
import sys
import logging

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from analysis.composite_alerts import OUT_DEBUG, OUT_MERGED, OUT_RAW, run
from utils.config_loader import CFG
from utils.logging_utils import setup_logging_from_cfg

if __name__ == "__main__":
    setup_logging_from_cfg(CFG, app_name="build_composite_alerts")
    logger = logging.getLogger(__name__)
    try:
        out = run()
        logger.info("Composite alerts saved to %s", out)
        logger.info("Raw alerts saved to %s", OUT_RAW)
        logger.info("Merged events saved to %s", OUT_MERGED)
        logger.info("RS debug saved to %s", OUT_DEBUG)
        logger.info("Stage 02 done -> %s", OUT_DEBUG)
        logger.info("Stage 03 done -> %s", OUT_RAW)
        logger.info("Stage 04 done -> %s", out)
        logger.info("Stage 05 done -> %s", OUT_MERGED)
    except Exception as exc:
        logger.error("Composite alerts failed: %s", exc)
        logger.debug("Composite alerts exception detail", exc_info=exc)
        raise SystemExit(1) from None
