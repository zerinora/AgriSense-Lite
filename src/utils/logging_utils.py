from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
from pathlib import Path
from uuid import uuid4


def _default_run_id() -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{stamp}-{uuid4().hex[:6]}"


def get_run_id() -> str:
    run_id = os.environ.get("AGRISENSE_RUN_ID") or os.environ.get("RUN_ID")
    if not run_id:
        run_id = _default_run_id()
        os.environ["AGRISENSE_RUN_ID"] = run_id
    return run_id


class RunIdFilter(logging.Filter):
    def __init__(self, run_id: str) -> None:
        super().__init__()
        self.run_id = run_id

    def filter(self, record: logging.LogRecord) -> bool:  # type: ignore[override]
        record.run_id = self.run_id
        return True


class SafeRotatingFileHandler(RotatingFileHandler):
    def doRollover(self) -> None:  # type: ignore[override]
        try:
            super().doRollover()
        except PermissionError as exc:
            _notify_log_permission_error(self.baseFilename, exc)


class SafeTimedRotatingFileHandler(TimedRotatingFileHandler):
    def doRollover(self) -> None:  # type: ignore[override]
        try:
            super().doRollover()
        except PermissionError as exc:
            _notify_log_permission_error(self.baseFilename, exc)


def _notify_log_permission_error(filename: str, exc: Exception) -> None:
    message = (
        f"[LOGGING] Permission denied while rotating log: {filename}. "
        "Please close any program that is viewing the log file."
    )
    try:
        import sys

        sys.stderr.write(message + "\n")
    except Exception:
        pass


def setup_logging(
    *,
    level: str = "INFO",
    log_dir: str | Path = "logs",
    app_name: str = "app",
    rotate: str = "daily",
    when: str = "midnight",
    interval: int = 1,
    backup_count: int = 14,
    max_bytes: int = 5_000_000,
    to_console: bool = True,
    to_file: bool = True,
    reset: bool = True,
) -> str:
    logger = logging.getLogger()
    if reset and logger.handlers:
        for handler in list(logger.handlers):
            logger.removeHandler(handler)

    level_name = str(level).upper()
    level_value = getattr(logging, level_name, logging.INFO)
    logger.setLevel(level_value)

    run_id = get_run_id()
    run_filter = RunIdFilter(run_id)
    formatter = logging.Formatter(
        "[%(asctime)s] %(levelname)s %(name)s run=%(run_id)s - %(message)s"
    )

    if to_console:
        console = logging.StreamHandler()
        console.setLevel(level_value)
        console.setFormatter(formatter)
        console.addFilter(run_filter)
        logger.addHandler(console)

    if to_file:
        log_path = Path(log_dir)
        log_path.mkdir(parents=True, exist_ok=True)
        filename = log_path / f"{app_name}_{run_id}.log"
        rotate_mode = str(rotate).lower()
        try:
            if rotate_mode == "size":
                file_handler: logging.Handler = SafeRotatingFileHandler(
                    filename,
                    maxBytes=max_bytes,
                    backupCount=backup_count,
                    encoding="utf-8",
                )
            else:
                file_handler = SafeTimedRotatingFileHandler(
                    filename,
                    when=when,
                    interval=interval,
                    backupCount=backup_count,
                    encoding="utf-8",
                )
                file_handler.suffix = "%Y%m%d"
            file_handler.setLevel(level_value)
            file_handler.setFormatter(formatter)
            file_handler.addFilter(run_filter)
            logger.addHandler(file_handler)
        except PermissionError as exc:
            logger.error("Log file access denied: %s", filename)
            logger.debug("Log file handler error", exc_info=exc)
        except OSError as exc:
            logger.error("Log file handler error: %s", exc)
            logger.debug("Log file handler error detail", exc_info=exc)

    return run_id


def setup_logging_from_cfg(cfg: dict, app_name: str) -> str:
    log_cfg = cfg.get("logging", {}) if isinstance(cfg, dict) else {}
    rotate = str(log_cfg.get("rotate", "daily")).lower()
    if rotate not in {"daily", "size"}:
        rotate = "daily"

    return setup_logging(
        level=str(log_cfg.get("level", "INFO")).upper(),
        log_dir=log_cfg.get("dir", "logs"),
        app_name=app_name,
        rotate=rotate,
        when=str(log_cfg.get("when", "midnight")),
        interval=int(log_cfg.get("interval", 1)),
        backup_count=int(log_cfg.get("backup_count", 14)),
        max_bytes=int(log_cfg.get("max_bytes", 5_000_000)),
        to_console=bool(log_cfg.get("to_console", True)),
        to_file=bool(log_cfg.get("to_file", True)),
        reset=True,
    )
