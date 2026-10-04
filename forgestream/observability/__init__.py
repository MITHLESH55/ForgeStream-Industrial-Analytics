"""
Observability package for ForgeStream (structured logging and metrics).
"""

from forgestream.observability.logging import get_logger, setup_logging
from forgestream.observability.metrics import MetricsCollector, metrics

__all__ = ["get_logger", "setup_logging", "MetricsCollector", "metrics"]
