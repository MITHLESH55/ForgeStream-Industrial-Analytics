"""
Structured JSON logging module for ForgeStream.
Provides machine-readable single-line JSON logs with contextual correlation IDs.
"""

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any, Optional


class JSONFormatter(logging.Formatter):
    """Formats log records as structured JSON strings."""

    def format(self, record: logging.LogRecord) -> str:
        log_obj = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "component": getattr(record, "component", record.name),
            "message": record.getMessage(),
        }

        # Optional correlation fields
        for field in ("event_id", "asset_id", "scenario_id", "run_id", "rule_code", "duration_ms"):
            if hasattr(record, field):
                log_obj[field] = getattr(record, field)

        if record.exc_info:
            log_obj["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_obj)


def setup_logging(level: str = "INFO", log_format: str = "json") -> None:
    """Configures the root logging subsystem."""
    root_logger = logging.getLogger("forgestream")
    root_logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    root_logger.handlers.clear()

    handler = logging.StreamHandler(sys.stdout)
    if log_format.lower() == "json":
        handler.setFormatter(JSONFormatter())
    else:
        handler.setFormatter(
            logging.Formatter("[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s")
        )

    root_logger.addHandler(handler)
    root_logger.propagate = False


def get_logger(component_name: str) -> logging.Logger:
    """Creates a contextual logger under the forgestream namespace."""
    return logging.getLogger(f"forgestream.{component_name}")
