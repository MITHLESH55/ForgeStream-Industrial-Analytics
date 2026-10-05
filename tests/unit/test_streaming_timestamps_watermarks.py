"""Unit tests for event-time timestamp extraction and bounded out-of-orderness watermarks."""

import pytest
from datetime import datetime, timezone
from forgestream.schemas.telemetry_schema import TelemetryEvent, AssetType, OperatingMode
from forgestream.streaming.timestamps import EventTimeExtractor
from forgestream.streaming.watermarks import (
    BoundedOutOfOrdernessWatermarkGenerator,
    LatenessPolicy,
    LatenessClassification,
)


def test_event_time_extractor_from_pydantic_event():
    """Verify timestamp extraction from a valid TelemetryEvent model."""
    event = TelemetryEvent(
        event_id="evt-001",
        asset_id="MOTOR-001",
        asset_type=AssetType.MOTOR,
        timestamp=1700000000.5,
        event_time="2023-11-14T22:13:20.500000Z",
        operating_mode=OperatingMode.NORMAL,
        temperature=65.0,
        vibration=1.5,
        pressure=4.0,
        rpm=1750.0,
        current=50.0,
        voltage=400.0,
        power=25.0,
        load=80.0,
        sequence_number=1,
    )
    extracted = EventTimeExtractor.extract_timestamp(event)
    assert extracted == 1700000000.5


def test_event_time_extractor_from_dict_and_iso_string():
    """Verify timestamp extraction from dictionary with numeric timestamp or ISO event_time."""
    dict_with_ts = {"timestamp": 1700000010.0, "asset_id": "PUMP-001"}
    assert EventTimeExtractor.extract_timestamp(dict_with_ts) == 1700000010.0

    dict_with_iso = {"event_time": "2026-10-05T12:00:00Z", "asset_id": "PUMP-001"}
    extracted_iso = EventTimeExtractor.extract_timestamp(dict_with_iso)
    assert isinstance(extracted_iso, float)
    assert extracted_iso > 1700000000.0


def test_event_time_extractor_invalid_payload():
    """Verify ValueError is raised on missing timestamp."""
    with pytest.raises(ValueError):
        EventTimeExtractor.extract_timestamp({"asset_id": "TURBINE-001"})


def test_watermark_generator_monotonic_progression():
    """Verify watermark lags strictly by max_out_of_orderness_sec and advances monotonically."""
    policy = LatenessPolicy(max_out_of_orderness_sec=5.0, allowed_lateness_sec=30.0)
    gen = BoundedOutOfOrdernessWatermarkGenerator(policy)

    # First event at t=100.0 -> Watermark = 95.0
    status, wm = gen.observe_timestamp(100.0)
    assert status == LatenessClassification.ON_TIME
    assert wm == 95.0
    assert gen.watermark_lag_sec == 5.0

    # Newer event at t=102.0 -> Watermark = 97.0
    status, wm = gen.observe_timestamp(102.0)
    assert status == LatenessClassification.ON_TIME
    assert wm == 97.0

    # In-order event at t=105.0 -> Watermark = 100.0
    status, wm = gen.observe_timestamp(105.0)
    assert status == LatenessClassification.ON_TIME
    assert wm == 100.0


def test_watermark_generator_out_of_order_within_lateness():
    """Verify out-of-order events behind watermark but within allowed lateness are accepted."""
    policy = LatenessPolicy(max_out_of_orderness_sec=5.0, allowed_lateness_sec=30.0)
    gen = BoundedOutOfOrdernessWatermarkGenerator(policy)

    # Advance to t=100.0 -> WM = 95.0
    gen.observe_timestamp(100.0)

    # Event arrives with timestamp t=96.0 (>= 95.0 -> ON_TIME)
    status, wm = gen.observe_timestamp(96.0)
    assert status == LatenessClassification.ON_TIME
    assert wm == 95.0  # Watermark does not regress

    # Event arrives with timestamp t=80.0 (< 95.0, but >= 95.0 - 30.0 = 65.0 -> LATE_ACCEPTED)
    status, wm = gen.observe_timestamp(80.0)
    assert status == LatenessClassification.LATE_ACCEPTED
    assert wm == 95.0


def test_watermark_generator_excessively_late_dropped():
    """Verify events arriving past allowed lateness are flagged as EXCESSIVELY_LATE."""
    policy = LatenessPolicy(max_out_of_orderness_sec=5.0, allowed_lateness_sec=30.0)
    gen = BoundedOutOfOrdernessWatermarkGenerator(policy)

    # Advance to t=100.0 -> WM = 95.0, cut-off = 65.0
    gen.observe_timestamp(100.0)

    # Event arrives with timestamp t=50.0 (< 65.0 -> EXCESSIVELY_LATE)
    status, wm = gen.observe_timestamp(50.0)
    assert status == LatenessClassification.EXCESSIVELY_LATE
    assert wm == 95.0
    assert gen.excessively_late_count == 1
