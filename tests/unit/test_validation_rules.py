"""
Unit tests for the 13 Data Quality Rules and Quarantine Engine.
"""

import math
import pytest
from forgestream.validation.validator import DataQualityValidator
from forgestream.validation.quality_rules import DQRuleCode
from forgestream.validation.quarantine import QuarantineManager


@pytest.fixture
def validator():
    return DataQualityValidator()


@pytest.fixture
def valid_payload():
    return {
        "event_id": "evt-motor-00000001",
        "asset_id": "MOTOR-001",
        "asset_type": "MOTOR",
        "timestamp": 1775347200.0,
        "event_time": "2026-10-05T12:00:00Z",
        "temperature": 68.5,
        "vibration": 1.25,
        "pressure": 1.0,
        "rpm": 1750.0,
        "current": 48.2,
        "voltage": 400.0,
        "power": 27.4,
        "load": 85.0,
        "operating_mode": "NORMAL",
        "sequence_number": 1,
        "schema_version": "1.0.0",
    }


def test_valid_payload_passes(validator, valid_payload):
    """Clean payload must be marked valid with no violations."""
    res, parsed = validator.validate_event(valid_payload)
    assert res.is_valid is True
    assert len(res.violated_rules) == 0
    assert parsed["event_id"] == valid_payload["event_id"]


def test_rule_001_missing_required_fields(validator, valid_payload):
    """Omitting required fields triggers RULE_001."""
    del valid_payload["temperature"]
    del valid_payload["vibration"]

    res, _ = validator.validate_event(valid_payload)
    assert res.is_valid is False
    assert DQRuleCode.RULE_001_REQUIRED_FIELDS in res.violated_rules


def test_rule_002_invalid_data_types(validator, valid_payload):
    """String in numeric field triggers RULE_002."""
    valid_payload["rpm"] = "FAST_SPEED"

    res, _ = validator.validate_event(valid_payload)
    assert res.is_valid is False
    assert DQRuleCode.RULE_002_DATA_TYPES in res.violated_rules


def test_rule_003_invalid_timestamp(validator, valid_payload):
    """Negative or future timestamp triggers RULE_003."""
    valid_payload["timestamp"] = -100.0

    res, _ = validator.validate_event(valid_payload)
    assert res.is_valid is False
    assert DQRuleCode.RULE_003_INVALID_TIMESTAMP in res.violated_rules


def test_rule_004_unsupported_mode(validator, valid_payload):
    """Unknown operating mode triggers RULE_004."""
    valid_payload["operating_mode"] = "HYPER_DRIVE"

    res, _ = validator.validate_event(valid_payload)
    assert res.is_valid is False
    assert DQRuleCode.RULE_004_UNSUPPORTED_OPERATING_MODE in res.violated_rules


def test_rule_005_impossible_temperature_and_nan(validator, valid_payload):
    """Temperature 2500°C and NaN values trigger RULE_005."""
    valid_payload["temperature"] = 2500.0
    res, _ = validator.validate_event(valid_payload)
    assert res.is_valid is False
    assert DQRuleCode.RULE_005_IMPOSSIBLE_NUMERIC_VALUE in res.violated_rules

    valid_payload["temperature"] = float("nan")
    res, _ = validator.validate_event(valid_payload)
    assert res.is_valid is False
    assert DQRuleCode.RULE_005_IMPOSSIBLE_NUMERIC_VALUE in res.violated_rules


def test_rule_006_out_of_range_rpm(validator, valid_payload):
    """RPM > 50000 triggers RULE_006."""
    valid_payload["rpm"] = 80000.0

    res, _ = validator.validate_event(valid_payload)
    assert res.is_valid is False
    assert DQRuleCode.RULE_006_OUT_OF_RANGE_RPM in res.violated_rules


def test_rule_007_negative_vibration(validator, valid_payload):
    """Negative vibration RMS triggers RULE_007."""
    valid_payload["vibration"] = -2.5

    res, _ = validator.validate_event(valid_payload)
    assert res.is_valid is False
    assert DQRuleCode.RULE_007_NEGATIVE_VIBRATION in res.violated_rules


def test_rule_008_negative_voltage(validator, valid_payload):
    """Negative voltage triggers RULE_008."""
    valid_payload["voltage"] = -400.0

    res, _ = validator.validate_event(valid_payload)
    assert res.is_valid is False
    assert DQRuleCode.RULE_008_INVALID_ELECTRICAL in res.violated_rules


def test_rule_009_duplicate_event_id(validator, valid_payload):
    """Same event_id sent twice triggers RULE_009."""
    res1, _ = validator.validate_event(valid_payload)
    assert res1.is_valid is True

    # Mutate timestamp so it doesn't collide on logical check, but same event_id
    payload_dup = dict(valid_payload)
    payload_dup["timestamp"] += 1.0

    res2, _ = validator.validate_event(payload_dup)
    assert res2.is_valid is False
    assert DQRuleCode.RULE_009_DUPLICATE_EVENT_ID in res2.violated_rules


def test_rule_010_duplicate_logical_event(validator, valid_payload):
    """Different event_id but identical asset_id + timestamp triggers RULE_010."""
    res1, _ = validator.validate_event(valid_payload)
    assert res1.is_valid is True

    payload_logical_dup = dict(valid_payload)
    payload_logical_dup["event_id"] = "evt-motor-00000002"  # distinct event_id

    res2, _ = validator.validate_event(payload_logical_dup)
    assert res2.is_valid is False
    assert DQRuleCode.RULE_010_DUPLICATE_LOGICAL_EVENT in res2.violated_rules


def test_rule_011_malformed_json_string(validator):
    """Broken JSON string triggers RULE_011."""
    raw_str = '{"event_id": "bad-json", "temperature": 45.0, broken...'

    res, _ = validator.validate_event(raw_str)
    assert res.is_valid is False
    assert DQRuleCode.RULE_011_MALFORMED_JSON in res.violated_rules


def test_rule_012_delayed_event_flag(validator, valid_payload):
    """Event generated in the past is flagged as delayed."""
    now_ts = 1775347200.0 + 120.0  # current clock is 120s ahead
    res, _ = validator.validate_event(valid_payload, current_time_ts=now_ts)

    assert res.is_delayed is True


def test_rule_013_out_of_order_flag(validator, valid_payload):
    """Inverted sequence number is flagged as out of order."""
    valid_payload["sequence_number"] = 10
    res1, _ = validator.validate_event(valid_payload)
    assert res1.is_valid is True

    valid_payload["event_id"] = "evt-motor-00000002"
    valid_payload["timestamp"] += 0.1
    valid_payload["sequence_number"] = 5  # Arrived with lower sequence number

    res2, _ = validator.validate_event(valid_payload)
    assert res2.is_out_of_order is True


def test_quarantine_serialization(validator, valid_payload):
    """Quarantine record encapsulates error metadata and raw payload."""
    valid_payload["temperature"] = -100.0  # physically impossible
    res, parsed = validator.validate_event(valid_payload)

    quarantine_rec = QuarantineManager.create_quarantine_record(parsed, res)
    assert quarantine_rec.quarantine_id is not None
    assert quarantine_rec.asset_id == "MOTOR-001"
    assert DQRuleCode.RULE_005_IMPOSSIBLE_NUMERIC_VALUE.value in quarantine_rec.violated_rules
    assert len(quarantine_rec.reasons) > 0
