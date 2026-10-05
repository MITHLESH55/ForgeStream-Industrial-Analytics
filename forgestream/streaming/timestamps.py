"""Event-time timestamp extraction protocol for ForgeStream Phase 2."""

from typing import Any, Dict, Union
from datetime import datetime
from forgestream.schemas.telemetry_schema import TelemetryEvent


class EventTimeExtractor:
    """Extracts monotonic epoch-second event timestamps from incoming telemetry payloads."""

    @staticmethod
    def extract_timestamp(event: Union[TelemetryEvent, Dict[str, Any]]) -> float:
        """Extract epoch timestamp in seconds from a TelemetryEvent or dictionary.

        Args:
            event: Either a Pydantic TelemetryEvent model or parsed dictionary.

        Returns:
            float: Epoch timestamp in seconds.

        Raises:
            ValueError: If timestamp cannot be extracted or parsed.
        """
        if isinstance(event, TelemetryEvent):
            return float(event.timestamp)

        if isinstance(event, dict):
            if "timestamp" in event and event["timestamp"] is not None:
                try:
                    return float(event["timestamp"])
                except (ValueError, TypeError):
                    pass

            if "event_time" in event and event["event_time"] is not None:
                raw_time = event["event_time"]
                if isinstance(raw_time, datetime):
                    return raw_time.timestamp()
                if isinstance(raw_time, str):
                    try:
                        # Parse ISO-8601 string
                        dt = datetime.fromisoformat(raw_time.replace("Z", "+00:00"))
                        return dt.timestamp()
                    except Exception as e:
                        raise ValueError(f"Cannot parse ISO event_time '{raw_time}': {e}") from e

        raise ValueError(f"Missing or unparseable event timestamp in payload: {event}")
