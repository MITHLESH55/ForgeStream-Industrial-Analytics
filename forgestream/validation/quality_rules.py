"""
Discrete Data Quality Rules for Industrial Telemetry Validation.
Implements the 13 standard ForgeStream quality rules with explicit diagnostic error codes.
"""

import math
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Set
from pydantic import BaseModel


class DQRuleCode(str, Enum):
    RULE_001_REQUIRED_FIELDS = "DQ-001_REQUIRED_FIELDS_MISSING"
    RULE_002_DATA_TYPES = "DQ-002_INVALID_DATA_TYPES"
    RULE_003_INVALID_TIMESTAMP = "DQ-003_INVALID_TIMESTAMP"
    RULE_004_UNSUPPORTED_OPERATING_MODE = "DQ-004_UNSUPPORTED_OPERATING_MODE"
    RULE_005_IMPOSSIBLE_NUMERIC_VALUE = "DQ-005_IMPOSSIBLE_NUMERIC_VALUE"
    RULE_006_OUT_OF_RANGE_RPM = "DQ-006_OUT_OF_RANGE_RPM"
    RULE_007_NEGATIVE_VIBRATION = "DQ-007_NEGATIVE_VIBRATION"
    RULE_008_INVALID_ELECTRICAL = "DQ-008_INVALID_ELECTRICAL_VALUES"
    RULE_009_DUPLICATE_EVENT_ID = "DQ-009_DUPLICATE_EVENT_ID"
    RULE_010_DUPLICATE_LOGICAL_EVENT = "DQ-010_DUPLICATE_LOGICAL_EVENT"
    RULE_011_MALFORMED_JSON = "DQ-011_MALFORMED_JSON"
    RULE_012_DELAYED_EVENT = "DQ-012_DELAYED_EVENT"
    RULE_013_OUT_OF_ORDER = "DQ-013_OUT_OF_ORDER_SEQUENCE"


class DQValidationResult(BaseModel):
    """Result of a data quality evaluation for a single event."""
    is_valid: bool
    violated_rules: List[DQRuleCode] = []
    error_messages: List[str] = []
    is_delayed: bool = False
    is_out_of_order: bool = False


class QualityRules:
    """Evaluates all 13 DQ rules against raw or parsed telemetry payloads."""

    REQUIRED_FIELDS = {
        "event_id", "asset_id", "asset_type", "timestamp", "temperature",
        "vibration", "pressure", "rpm", "current", "voltage", "power",
        "load", "operating_mode", "sequence_number"
    }

    VALID_MODES = {"IDLE", "STARTUP", "NORMAL", "HIGH_LOAD", "MAINTENANCE", "DEGRADED"}

    @classmethod
    def check_required_fields(cls, record: Dict[str, Any]) -> Optional[str]:
        """Rule 1: Checks that all required fields are present."""
        missing = cls.REQUIRED_FIELDS - set(record.keys())
        if missing:
            return f"Missing required fields: {', '.join(sorted(missing))}"
        return None

    @classmethod
    def check_data_types(cls, record: Dict[str, Any]) -> Optional[str]:
        """Rule 2: Checks that fields have appropriate types."""
        numeric_fields = ["timestamp", "temperature", "vibration", "pressure", "rpm", "current", "voltage", "power", "load"]
        for f in numeric_fields:
            if f in record:
                val = record[f]
                if not isinstance(val, (int, float)) or isinstance(val, bool):
                    return f"Field '{f}' must be numeric, got {type(val).__name__}"
        if "event_id" in record and not isinstance(record["event_id"], str):
            return f"Field 'event_id' must be string, got {type(record['event_id']).__name__}"
        if "sequence_number" in record and not isinstance(record["sequence_number"], int):
            return f"Field 'sequence_number' must be integer, got {type(record['sequence_number']).__name__}"
        return None

    @classmethod
    def check_timestamp(cls, record: Dict[str, Any]) -> Optional[str]:
        """Rule 3: Checks that timestamp is a positive finite epoch and not in distant future/past."""
        ts = record.get("timestamp")
        if ts is None or not isinstance(ts, (int, float)) or math.isnan(ts) or math.isinf(ts):
            return "Timestamp is missing, non-numeric, or NaN/Inf"
        if ts <= 0:
            return f"Timestamp must be positive epoch, got {ts}"
        # Bounds check: between 2020-01-01 and 2035-01-01
        if ts < 1577836800.0 or ts > 2051222400.0:
            return f"Timestamp {ts} is outside plausible operational range (2020-2035)"
        return None

    @classmethod
    def check_operating_mode(cls, record: Dict[str, Any]) -> Optional[str]:
        """Rule 4: Checks that operating_mode is a valid enum member."""
        mode = record.get("operating_mode")
        if mode not in cls.VALID_MODES:
            return f"Unsupported operating mode: '{mode}'. Must be one of {cls.VALID_MODES}"
        return None

    @classmethod
    def check_impossible_values(cls, record: Dict[str, Any]) -> Optional[str]:
        """Rule 5: Checks for NaN, Inf, and physical impossibilities (e.g. temperature bounds)."""
        for k, v in record.items():
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                return f"Metric '{k}' contains non-finite float value ({v})"

        temp = record.get("temperature")
        if temp is not None and isinstance(temp, (int, float)):
            if temp < -50.0 or temp > 1500.0:
                return f"Temperature {temp}°C is physically impossible (allowed: -50 to 1500°C)"

        press = record.get("pressure")
        if press is not None and isinstance(press, (int, float)):
            if press < 0.0 or press > 2000.0:
                return f"Pressure {press} bar is physically impossible (allowed: 0 to 2000 bar)"

        load = record.get("load")
        if load is not None and isinstance(load, (int, float)):
            if load < 0.0 or load > 200.0:
                return f"Load {load}% is out of bounds (allowed: 0 to 200%)"

        return None

    @classmethod
    def check_rpm_range(cls, record: Dict[str, Any]) -> Optional[str]:
        """Rule 6: Checks that RPM is non-negative and <= 50000."""
        rpm = record.get("rpm")
        if rpm is not None and isinstance(rpm, (int, float)):
            if rpm < 0.0 or rpm > 50000.0:
                return f"RPM {rpm} is out of allowable range [0, 50000]"
        return None

    @classmethod
    def check_vibration_non_negative(cls, record: Dict[str, Any]) -> Optional[str]:
        """Rule 7: Checks that vibration RMS velocity is non-negative."""
        vibe = record.get("vibration")
        if vibe is not None and isinstance(vibe, (int, float)):
            if vibe < 0.0:
                return f"Negative vibration {vibe} mm/s is physically invalid"
        return None

    @classmethod
    def check_electrical_values(cls, record: Dict[str, Any]) -> Optional[str]:
        """Rule 8: Checks voltage, current, and power non-negativity."""
        voltage = record.get("voltage")
        current = record.get("current")
        power = record.get("power")

        if voltage is not None and isinstance(voltage, (int, float)) and voltage < 0.0:
            return f"Negative voltage {voltage} V is invalid"
        if current is not None and isinstance(current, (int, float)) and current < 0.0:
            return f"Negative current {current} A is invalid"
        if power is not None and isinstance(power, (int, float)) and power < 0.0:
            return f"Negative power {power} kW is invalid"
        return None

    @classmethod
    def check_duplicate_event_id(cls, event_id: str, seen_ids: Set[str]) -> Optional[str]:
        """Rule 9: Checks if event_id has already been seen in the deduplication cache."""
        if event_id in seen_ids:
            return f"Duplicate event ID detected: '{event_id}'"
        return None

    @classmethod
    def check_duplicate_logical(cls, asset_id: str, ts: float, seen_logicals: Set[str]) -> Optional[str]:
        """Rule 10: Checks for duplicate (asset_id, timestamp) pair."""
        key = f"{asset_id}:{ts:.3f}"
        if key in seen_logicals:
            return f"Duplicate logical event for asset '{asset_id}' at timestamp {ts}"
        return None

    @classmethod
    def check_delayed_event(cls, record: Dict[str, Any], current_time_ts: float, threshold_sec: float = 30.0) -> bool:
        """Rule 12: Flags if event arrived significantly later than its generation time."""
        event_ts = record.get("timestamp")
        if event_ts and isinstance(event_ts, (int, float)):
            return (current_time_ts - event_ts) > threshold_sec
        return False

    @classmethod
    def check_out_of_order(cls, asset_id: str, seq_num: int, last_seen_seqs: Dict[str, int]) -> bool:
        """Rule 13: Flags if sequence number is lower than previously processed sequence for this asset."""
        last_seq = last_seen_seqs.get(asset_id, -1)
        return seq_num <= last_seq
