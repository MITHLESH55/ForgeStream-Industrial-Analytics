"""Unit tests for circular sensor buffers, keyed streaming state, and TTL eviction."""

import time
from forgestream.streaming.state import (
    CircularSensorBuffer,
    AssetStreamingState,
    AssetStateStore,
    HealthState,
)


def test_circular_sensor_buffer_capacity_and_window():
    """Verify circular buffer maintains strict capacity limit and window retrieval."""
    buf = CircularSensorBuffer(capacity=5)

    for i in range(10):
        buf.append(timestamp=100.0 + i, value=float(i * 10))

    # Buffer length should not exceed capacity 5
    assert len(buf) == 5
    assert buf.get_values() == [50.0, 60.0, 70.0, 80.0, 90.0]

    # Test window slicing (last 3 seconds from t=109.0)
    window = buf.get_window(window_sec=2.0, current_timestamp=109.0)
    assert len(window) == 3  # timestamps 107.0, 108.0, 109.0
    assert window[-1] == (109.0, 90.0)


def test_asset_streaming_state_serialization_roundtrip():
    """Verify state serialization to dict and restoration preserves all data."""
    state = AssetStreamingState(asset_id="PUMP-001", asset_type="PUMP", capacity=10)
    state.update_telemetry(
        timestamp=1700000000.0,
        sensor_data={"temperature": 72.5, "vibration": 2.1, "pressure": 5.4, "current": 45.0},
        operating_mode="HIGH_LOAD",
    )
    state.current_health_state = HealthState.WATCH
    state.current_health_score = 0.78
    state.consecutive_fault_count = 3

    state_dict = state.to_dict()
    restored = AssetStreamingState.from_dict(state_dict)

    assert restored.asset_id == "PUMP-001"
    assert restored.operating_mode == "HIGH_LOAD"
    assert restored.current_health_state == HealthState.WATCH
    assert restored.current_health_score == 0.78
    assert restored.consecutive_fault_count == 3
    assert len(restored.temperatures) == 1
    assert restored.temperatures.get_values() == [72.5]


def test_asset_state_store_get_or_create_and_ttl_eviction():
    """Verify state store manages keyed assets and evicts inactive ones upon TTL."""
    store = AssetStateStore(buffer_capacity=50, state_ttl_hours=0.001)  # ~3.6s TTL

    s1 = store.get_or_create("MOTOR-001", "MOTOR")
    s2 = store.get_or_create("PUMP-001", "PUMP")
    assert store.active_asset_count == 2

    # Manually backdate s1's last update
    s1.last_updated_wall_clock = time.time() - 10.0  # 10s old, exceeds 3.6s TTL

    expired = store.evict_expired_states()
    assert "MOTOR-001" in expired
    assert store.active_asset_count == 1
    assert store.get("PUMP-001") is not None
    assert store.get("MOTOR-001") is None
