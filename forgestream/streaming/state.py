"""Keyed bounded state management and TTL lifecycle for ForgeStream Phase 2 streaming."""

import time
from collections import deque
from typing import Dict, List, Optional, Tuple, Any
from forgestream.streaming.schemas import HealthState, AnomalySeverity


class CircularSensorBuffer:
    """Fixed-capacity circular buffer storing (timestamp, value) pairs with O(1) appending."""

    __slots__ = ("capacity", "buffer")

    def __init__(self, capacity: int = 120):
        self.capacity = capacity
        self.buffer: deque = deque(maxlen=capacity)

    def append(self, timestamp: float, value: float) -> None:
        """Add a sensor reading to the circular buffer."""
        self.buffer.append((timestamp, float(value)))

    def get_values(self) -> List[float]:
        """Return list of all buffered sensor values."""
        return [val for _, val in self.buffer]

    def get_time_series(self) -> List[Tuple[float, float]]:
        """Return list of (timestamp, value) tuples."""
        return list(self.buffer)

    def get_window(self, window_sec: float, current_timestamp: float) -> List[Tuple[float, float]]:
        """Return samples falling within [current_timestamp - window_sec, current_timestamp]."""
        cutoff = current_timestamp - window_sec
        return [(ts, val) for ts, val in self.buffer if ts >= cutoff]

    def __len__(self) -> int:
        return len(self.buffer)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "capacity": self.capacity,
            "samples": list(self.buffer),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CircularSensorBuffer":
        buf = cls.__new__(cls)
        buf.capacity = data.get("capacity", 120)
        buf.buffer = deque(data.get("samples", ()), maxlen=buf.capacity)
        return buf


class AssetStreamingState:
    """Keyed state encapsulation for an individual industrial asset."""

    def __init__(self, asset_id: str, asset_type: str = "GENERIC", capacity: int = 120):
        self.asset_id = asset_id
        self.asset_type = asset_type
        self.capacity = capacity

        # Bounded Circular Buffers
        self.temperatures = CircularSensorBuffer(capacity)
        self.vibrations = CircularSensorBuffer(capacity)
        self.pressures = CircularSensorBuffer(capacity)
        self.currents = CircularSensorBuffer(capacity)
        self.voltages = CircularSensorBuffer(capacity)
        self.powers = CircularSensorBuffer(capacity)
        self.speeds = CircularSensorBuffer(capacity)

        # Baseline & Operating Mode
        self.operating_mode: str = "NORMAL"
        self.baseline_temperature: Optional[float] = None
        self.baseline_vibration: Optional[float] = None
        self.baseline_pressure: Optional[float] = None
        self.baseline_current: Optional[float] = None

        # Fault Tracking & Counters
        self.consecutive_fault_count: int = 0
        self.total_anomalies_detected: int = 0
        self.consecutive_healthy_samples: int = 0

        # Health & Alert State
        self.current_health_state: HealthState = HealthState.HEALTHY
        self.previous_health_state: Optional[HealthState] = None
        self.current_health_score: float = 1.0
        self.last_alert_timestamp: float = 0.0
        self.last_alert_severity: Optional[AnomalySeverity] = None
        self.active_alert_id: Optional[str] = None

        # Temporal Metadata
        self.last_event_timestamp: float = 0.0
        self.last_updated_wall_clock: float = time.time()
        self.total_events_processed: int = 0

    def update_telemetry(self, timestamp: float, sensor_data: Dict[str, float], operating_mode: str = "NORMAL") -> None:
        """Update state buffers with incoming telemetry."""
        self.last_event_timestamp = timestamp
        self.last_updated_wall_clock = time.time()
        self.total_events_processed += 1
        self.operating_mode = operating_mode

        if "temperature" in sensor_data and sensor_data["temperature"] is not None:
            self.temperatures.append(timestamp, sensor_data["temperature"])
        if "vibration" in sensor_data and sensor_data["vibration"] is not None:
            self.vibrations.append(timestamp, sensor_data["vibration"])
        if "pressure" in sensor_data and sensor_data["pressure"] is not None:
            self.pressures.append(timestamp, sensor_data["pressure"])
        if "current" in sensor_data and sensor_data["current"] is not None:
            self.currents.append(timestamp, sensor_data["current"])
        if "voltage" in sensor_data and sensor_data["voltage"] is not None:
            self.voltages.append(timestamp, sensor_data["voltage"])
        if "power" in sensor_data and sensor_data["power"] is not None:
            self.powers.append(timestamp, sensor_data["power"])
        if "speed" in sensor_data and sensor_data["speed"] is not None:
            self.speeds.append(timestamp, sensor_data["speed"])

    def to_dict(self) -> Dict[str, Any]:
        """Serialize state for checkpointing."""
        return {
            "asset_id": self.asset_id,
            "asset_type": self.asset_type,
            "capacity": self.capacity,
            "temperatures": self.temperatures.to_dict(),
            "vibrations": self.vibrations.to_dict(),
            "pressures": self.pressures.to_dict(),
            "currents": self.currents.to_dict(),
            "voltages": self.voltages.to_dict(),
            "powers": self.powers.to_dict(),
            "speeds": self.speeds.to_dict(),
            "operating_mode": self.operating_mode,
            "baseline_temperature": self.baseline_temperature,
            "baseline_vibration": self.baseline_vibration,
            "baseline_pressure": self.baseline_pressure,
            "baseline_current": self.baseline_current,
            "consecutive_fault_count": self.consecutive_fault_count,
            "total_anomalies_detected": self.total_anomalies_detected,
            "consecutive_healthy_samples": self.consecutive_healthy_samples,
            "current_health_state": self.current_health_state.value,
            "previous_health_state": self.previous_health_state.value if self.previous_health_state else None,
            "current_health_score": self.current_health_score,
            "last_alert_timestamp": self.last_alert_timestamp,
            "last_alert_severity": self.last_alert_severity.value if self.last_alert_severity else None,
            "active_alert_id": self.active_alert_id,
            "last_event_timestamp": self.last_event_timestamp,
            "last_updated_wall_clock": self.last_updated_wall_clock,
            "total_events_processed": self.total_events_processed,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "AssetStreamingState":
        """Deserialize state from checkpoint snapshot."""
        state = cls(
            asset_id=data["asset_id"],
            asset_type=data.get("asset_type", "GENERIC"),
            capacity=data.get("capacity", 120),
        )
        if "temperatures" in data:
            state.temperatures = CircularSensorBuffer.from_dict(data["temperatures"])
        if "vibrations" in data:
            state.vibrations = CircularSensorBuffer.from_dict(data["vibrations"])
        if "pressures" in data:
            state.pressures = CircularSensorBuffer.from_dict(data["pressures"])
        if "currents" in data:
            state.currents = CircularSensorBuffer.from_dict(data["currents"])
        if "voltages" in data:
            state.voltages = CircularSensorBuffer.from_dict(data["voltages"])
        if "powers" in data:
            state.powers = CircularSensorBuffer.from_dict(data["powers"])
        if "speeds" in data:
            state.speeds = CircularSensorBuffer.from_dict(data["speeds"])

        state.operating_mode = data.get("operating_mode", "NORMAL")
        state.baseline_temperature = data.get("baseline_temperature")
        state.baseline_vibration = data.get("baseline_vibration")
        state.baseline_pressure = data.get("baseline_pressure")
        state.baseline_current = data.get("baseline_current")
        state.consecutive_fault_count = data.get("consecutive_fault_count", 0)
        state.total_anomalies_detected = data.get("total_anomalies_detected", 0)
        state.consecutive_healthy_samples = data.get("consecutive_healthy_samples", 0)

        if "current_health_state" in data and data["current_health_state"]:
            state.current_health_state = HealthState(data["current_health_state"])
        if "previous_health_state" in data and data["previous_health_state"]:
            state.previous_health_state = HealthState(data["previous_health_state"])

        state.current_health_score = data.get("current_health_score", 1.0)
        state.last_alert_timestamp = data.get("last_alert_timestamp", 0.0)
        if "last_alert_severity" in data and data["last_alert_severity"]:
            state.last_alert_severity = AnomalySeverity(data["last_alert_severity"])
        state.active_alert_id = data.get("active_alert_id")
        state.last_event_timestamp = data.get("last_event_timestamp", 0.0)
        state.last_updated_wall_clock = data.get("last_updated_wall_clock", time.time())
        state.total_events_processed = data.get("total_events_processed", 0)
        return state


class AssetStateStore:
    """Thread-safe, keyed in-memory state store managing all active asset streaming states."""

    def __init__(self, buffer_capacity: int = 120, state_ttl_hours: float = 24.0):
        self.buffer_capacity = buffer_capacity
        self.state_ttl_sec = state_ttl_hours * 3600.0
        self._states: Dict[str, AssetStreamingState] = {}

    def get_or_create(self, asset_id: str, asset_type: str = "GENERIC") -> AssetStreamingState:
        """Retrieve existing state or initialize a new keyed state for asset."""
        if asset_id not in self._states:
            self._states[asset_id] = AssetStreamingState(
                asset_id=asset_id,
                asset_type=asset_type,
                capacity=self.buffer_capacity,
            )
        return self._states[asset_id]

    def get(self, asset_id: str) -> Optional[AssetStreamingState]:
        """Get asset state if present."""
        return self._states.get(asset_id)

    def evict_expired_states(self, current_time: Optional[float] = None) -> List[str]:
        """Evict states that have not received events within the configured TTL."""
        now = current_time if current_time is not None else time.time()
        expired_keys = []
        for asset_id, state in list(self._states.items()):
            if (now - state.last_updated_wall_clock) > self.state_ttl_sec:
                expired_keys.append(asset_id)
                del self._states[asset_id]
        return expired_keys

    @property
    def active_asset_count(self) -> int:
        """Count of currently active keyed asset states."""
        return len(self._states)

    def snapshot(self) -> Dict[str, Any]:
        """Generate a complete snapshot of all keyed states for checkpointing."""
        return {
            "snapshot_timestamp": time.time(),
            "asset_count": len(self._states),
            "states": {asset_id: state.to_dict() for asset_id, state in self._states.items()},
        }

    def restore(self, snapshot_data: Dict[str, Any]) -> None:
        """Restore all keyed states from a checkpoint snapshot."""
        self._states.clear()
        for asset_id, state_dict in snapshot_data.get("states", {}).items():
            self._states[asset_id] = AssetStreamingState.from_dict(state_dict)
