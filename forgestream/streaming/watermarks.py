"""Bounded out-of-orderness watermark generator and lateness policy for event-time stream processing."""

from enum import Enum
from typing import Tuple


class LatenessClassification(str, Enum):
    """Classification of event arrival timing relative to the current watermark."""
    ON_TIME = "ON_TIME"
    LATE_ACCEPTED = "LATE_ACCEPTED"
    EXCESSIVELY_LATE = "EXCESSIVELY_LATE"


class LatenessPolicy:
    """Encapsulates watermark delay bounds and allowed lateness thresholds."""

    def __init__(self, max_out_of_orderness_sec: float = 5.0, allowed_lateness_sec: float = 30.0):
        """Initialize lateness policy.

        Args:
            max_out_of_orderness_sec: Maximum expected delay for out-of-order events (default 5.0s).
            allowed_lateness_sec: Maximum window lateness window before dropping events (default 30.0s).
        """
        self.max_out_of_orderness_sec = max_out_of_orderness_sec
        self.allowed_lateness_sec = allowed_lateness_sec


class BoundedOutOfOrdernessWatermarkGenerator:
    """Generates deterministic event-time watermarks with bounded out-of-orderness lag."""

    def __init__(self, lateness_policy: LatenessPolicy = None):
        self.policy = lateness_policy or LatenessPolicy()
        self.max_timestamp: float = float("-inf")
        self.current_watermark: float = float("-inf")
        self.events_evaluated: int = 0
        self.on_time_count: int = 0
        self.late_accepted_count: int = 0
        self.excessively_late_count: int = 0

    def observe_timestamp(self, event_timestamp: float) -> Tuple[LatenessClassification, float]:
        """Ingests an event timestamp, advances the watermark, and classifies timing.

        Args:
            event_timestamp: Event timestamp in epoch seconds.

        Returns:
            Tuple[LatenessClassification, float]: (Timing classification, current watermark).
        """
        self.events_evaluated += 1

        # First event observed
        if self.max_timestamp == float("-inf"):
            self.max_timestamp = event_timestamp
            self.current_watermark = event_timestamp - self.policy.max_out_of_orderness_sec
            self.on_time_count += 1
            return LatenessClassification.ON_TIME, self.current_watermark

        # Classify arrival relative to current watermark
        if event_timestamp >= self.current_watermark:
            classification = LatenessClassification.ON_TIME
            self.on_time_count += 1
        elif event_timestamp >= (self.current_watermark - self.policy.allowed_lateness_sec):
            classification = LatenessClassification.LATE_ACCEPTED
            self.late_accepted_count += 1
        else:
            classification = LatenessClassification.EXCESSIVELY_LATE
            self.excessively_late_count += 1

        # Advance max timestamp if strictly newer
        if event_timestamp > self.max_timestamp:
            self.max_timestamp = event_timestamp
            # Watermark monotonically advances: W(t) = max(T_seen) - delay
            new_watermark = self.max_timestamp - self.policy.max_out_of_orderness_sec
            if new_watermark > self.current_watermark:
                self.current_watermark = new_watermark

        return classification, self.current_watermark

    @property
    def watermark_lag_sec(self) -> float:
        """Difference between highest observed timestamp and current watermark."""
        if self.max_timestamp == float("-inf"):
            return 0.0
        return max(0.0, self.max_timestamp - self.current_watermark)
