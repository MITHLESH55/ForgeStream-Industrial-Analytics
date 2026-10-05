"""Windowed analytics and aggregation accumulators for ForgeStream Phase 2."""

import math
import uuid
from typing import Dict, List, Optional, Tuple, Any
from pydantic import BaseModel, Field
from forgestream.streaming.schemas import WindowStateEvent


class WindowResult(BaseModel):
    """Aggregate metric results emitted when an analytical window closes."""

    window_id: str
    asset_id: str
    window_type: str  # TUMBLING or SLIDING
    window_start: float
    window_end: float
    sample_count: int
    mean_temperature: float = 0.0
    std_temperature: float = 0.0
    thermal_rise_rate: float = 0.0
    mean_vibration: float = 0.0
    max_vibration: float = 0.0
    vibration_slope: float = 0.0
    mean_pressure: float = 0.0
    std_pressure: float = 0.0
    anomalies_in_window: int = 0

    def to_event(self) -> WindowStateEvent:
        """Convert to standardized WindowStateEvent."""
        return WindowStateEvent(
            window_id=self.window_id,
            asset_id=self.asset_id,
            window_type=self.window_type,
            window_start=self.window_start,
            window_end=self.window_end,
            sample_count=self.sample_count,
            mean_temperature=self.mean_temperature,
            std_temperature=self.std_temperature,
            thermal_rise_rate=self.thermal_rise_rate,
            mean_vibration=self.mean_vibration,
            max_vibration=self.max_vibration,
            vibration_slope=self.vibration_slope,
            mean_pressure=self.mean_pressure,
            std_pressure=self.std_pressure,
            anomalies_in_window=self.anomalies_in_window,
        )


class WindowAccumulator:
    """Manages event-time tumbling and sliding windows per asset."""

    def __init__(
        self,
        tumbling_size_sec: float = 5.0,
        sliding_size_sec: float = 30.0,
        sliding_step_sec: float = 5.0,
    ):
        self.tumbling_size_sec = tumbling_size_sec
        self.sliding_size_sec = sliding_size_sec
        self.sliding_step_sec = sliding_step_sec

        # Asset -> List of event dicts: {"timestamp": float, "temperature": float, ...}
        self._asset_events: Dict[str, List[Dict[str, Any]]] = {}
        self._last_tumbling_end: Dict[str, float] = {}
        self._last_sliding_end: Dict[str, float] = {}

    @staticmethod
    def _compute_stats(values: List[float]) -> Tuple[float, float]:
        """Compute mean and sample standard deviation."""
        if not values:
            return 0.0, 0.0
        n = len(values)
        if n == 1:
            return values[0], 0.0
        mean = sum(values) / n
        var = sum((x - mean) ** 2 for x in values) / (n - 1)
        return mean, math.sqrt(max(0.0, var))

    @staticmethod
    def _compute_slope(events: List[Dict[str, Any]], field: str) -> float:
        """Compute rate of change per minute."""
        valid_pairs = [(e["timestamp"], e[field]) for e in events if field in e and e[field] is not None]
        if len(valid_pairs) < 2:
            return 0.0

        t0 = valid_pairs[0][0]
        dt = valid_pairs[-1][0] - t0
        if dt <= 0.001:
            return 0.0

        dy = valid_pairs[-1][1] - valid_pairs[0][1]
        return (dy / dt) * 60.0

    def add_event(
        self,
        asset_id: str,
        timestamp: float,
        metrics: Dict[str, float],
        is_anomaly: bool = False,
    ) -> List[WindowResult]:
        """Add event and evaluate if any tumbling or sliding windows have completed."""
        if asset_id not in self._asset_events:
            self._asset_events[asset_id] = []

        event_record = {"timestamp": timestamp, "is_anomaly": is_anomaly, **metrics}
        self._asset_events[asset_id].append(event_record)

        # Retain only events within sliding_size_sec * 2 of latest timestamp
        cutoff = timestamp - (self.sliding_size_sec * 2.0)
        self._asset_events[asset_id] = [e for e in self._asset_events[asset_id] if e["timestamp"] >= cutoff]

        completed_windows: List[WindowResult] = []

        # 1. Evaluate Tumbling Window (e.g. 5.0s aligned)
        current_tumb_end = math.floor(timestamp / self.tumbling_size_sec) * self.tumbling_size_sec
        last_tumb = self._last_tumbling_end.get(asset_id, current_tumb_end - self.tumbling_size_sec)

        if current_tumb_end > last_tumb:
            window_start = last_tumb
            window_end = current_tumb_end
            w_events = [
                e for e in self._asset_events[asset_id]
                if window_start <= e["timestamp"] < window_end
            ]
            if w_events:
                completed_windows.append(
                    self._create_window_result(asset_id, "TUMBLING", window_start, window_end, w_events)
                )
            self._last_tumbling_end[asset_id] = current_tumb_end

        # 2. Evaluate Sliding Window (e.g. 30.0s window, slide step 5.0s)
        current_slide_end = math.floor(timestamp / self.sliding_step_sec) * self.sliding_step_sec
        last_slide = self._last_sliding_end.get(asset_id, current_slide_end - self.sliding_step_sec)

        if current_slide_end > last_slide:
            window_end = current_slide_end
            window_start = window_end - self.sliding_size_sec
            w_events = [
                e for e in self._asset_events[asset_id]
                if window_start <= e["timestamp"] < window_end
            ]
            if len(w_events) >= 3:  # Only emit if sufficient history
                completed_windows.append(
                    self._create_window_result(asset_id, "SLIDING", window_start, window_end, w_events)
                )
            self._last_sliding_end[asset_id] = current_slide_end

        return completed_windows

    def _create_window_result(
        self,
        asset_id: str,
        window_type: str,
        start_ts: float,
        end_ts: float,
        events: List[Dict[str, Any]],
    ) -> WindowResult:
        """Helper to construct WindowResult from list of events."""
        temps = [e["temperature"] for e in events if "temperature" in e and e["temperature"] is not None]
        vibs = [e["vibration"] for e in events if "vibration" in e and e["vibration"] is not None]
        press = [e["pressure"] for e in events if "pressure" in e and e["pressure"] is not None]
        anomalies = sum(1 for e in events if e.get("is_anomaly", False))

        m_temp, s_temp = self._compute_stats(temps)
        m_vib, s_vib = self._compute_stats(vibs)
        m_press, s_press = self._compute_stats(press)

        thermal_slope = self._compute_slope(events, "temperature")
        vib_slope = self._compute_slope(events, "vibration")

        return WindowResult(
            window_id=f"win-{asset_id[:4]}-{int(end_ts)}-{uuid.uuid4().hex[:6]}",
            asset_id=asset_id,
            window_type=window_type,
            window_start=start_ts,
            window_end=end_ts,
            sample_count=len(events),
            mean_temperature=round(m_temp, 2),
            std_temperature=round(s_temp, 3),
            thermal_rise_rate=round(thermal_slope, 4),
            mean_vibration=round(m_vib, 2),
            max_vibration=round(max(vibs) if vibs else 0.0, 2),
            vibration_slope=round(vib_slope, 4),
            mean_pressure=round(m_press, 2),
            std_pressure=round(s_press, 3),
            anomalies_in_window=anomalies,
        )
