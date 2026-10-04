"""
Data Quality Validation Engine for ForgeStream.
Coordinates multi-rule verification, deduplication caching, and metrics emission.
"""

import json
import time
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from forgestream.schemas.telemetry_schema import TelemetryEvent
from forgestream.validation.quality_rules import DQRuleCode, DQValidationResult, QualityRules
from forgestream.observability.metrics import metrics
from forgestream.observability.logging import get_logger

logger = get_logger("validator")


class DataQualityValidator:
    """
    High-performance validation engine for streaming telemetry events.
    """

    def __init__(self, deduplication_window_size: int = 50000):
        self.dedup_window_size = deduplication_window_size
        self._seen_event_ids: Set[str] = set()
        self._seen_logical_events: Set[str] = set()
        self._last_seen_sequences: Dict[str, int] = {}

    def validate(self, raw_event: Union[str, Dict[str, Any], TelemetryEvent]) -> DQValidationResult:
        """Evaluates an event against all 13 DQ rules and returns DQValidationResult."""
        result, _ = self.validate_event(raw_event)
        return result

    def validate_event(
        self,
        raw_event: Union[str, Dict[str, Any], TelemetryEvent],
        current_time_ts: Optional[float] = None,
    ) -> Tuple[DQValidationResult, Optional[Dict[str, Any]]]:
        """
        Evaluates an incoming event against all 13 DQ rules.
        Returns (ValidationResult, parsed_payload_dict).
        """
        t0 = time.perf_counter()
        now_ts = current_time_ts or time.time()
        violated_rules: List[DQRuleCode] = []
        error_msgs: List[str] = []

        # 1. Parsing and Malformed JSON check (Rule 11)
        if isinstance(raw_event, str):
            try:
                record = json.loads(raw_event)
                if not isinstance(record, dict):
                    violated_rules.append(DQRuleCode.RULE_011_MALFORMED_JSON)
                    error_msgs.append("Parsed JSON root must be an object/dict")
                    return DQValidationResult(is_valid=False, violated_rules=violated_rules, error_messages=error_msgs), None
            except json.JSONDecodeError as exc:
                violated_rules.append(DQRuleCode.RULE_011_MALFORMED_JSON)
                error_msgs.append(f"JSON decode failure: {str(exc)}")
                return DQValidationResult(is_valid=False, violated_rules=violated_rules, error_messages=error_msgs), None
        elif isinstance(raw_event, TelemetryEvent):
            record = raw_event.model_dump(mode="json")
        elif isinstance(raw_event, dict):
            record = dict(raw_event)
        else:
            violated_rules.append(DQRuleCode.RULE_002_DATA_TYPES)
            error_msgs.append(f"Unsupported event type: {type(raw_event).__name__}")
            return DQValidationResult(is_valid=False, violated_rules=violated_rules, error_messages=error_msgs), None

        # 2. Required Fields Check (Rule 1)
        err = QualityRules.check_required_fields(record)
        if err:
            violated_rules.append(DQRuleCode.RULE_001_REQUIRED_FIELDS)
            error_msgs.append(err)

        # 3. Data Types Check (Rule 2)
        err = QualityRules.check_data_types(record)
        if err:
            violated_rules.append(DQRuleCode.RULE_002_DATA_TYPES)
            error_msgs.append(err)

        # 4. Timestamp Check (Rule 3)
        err = QualityRules.check_timestamp(record)
        if err:
            violated_rules.append(DQRuleCode.RULE_003_INVALID_TIMESTAMP)
            error_msgs.append(err)

        # 5. Operating Mode Check (Rule 4)
        err = QualityRules.check_operating_mode(record)
        if err:
            violated_rules.append(DQRuleCode.RULE_004_UNSUPPORTED_OPERATING_MODE)
            error_msgs.append(err)

        # 6. Impossible Numeric Values / NaN (Rule 5)
        err = QualityRules.check_impossible_values(record)
        if err:
            violated_rules.append(DQRuleCode.RULE_005_IMPOSSIBLE_NUMERIC_VALUE)
            error_msgs.append(err)

        # 7. Out of Range RPM (Rule 6)
        err = QualityRules.check_rpm_range(record)
        if err:
            violated_rules.append(DQRuleCode.RULE_006_OUT_OF_RANGE_RPM)
            error_msgs.append(err)

        # 8. Negative Vibration (Rule 7)
        err = QualityRules.check_vibration_non_negative(record)
        if err:
            violated_rules.append(DQRuleCode.RULE_007_NEGATIVE_VIBRATION)
            error_msgs.append(err)

        # 9. Invalid Electrical Values (Rule 8)
        err = QualityRules.check_electrical_values(record)
        if err:
            violated_rules.append(DQRuleCode.RULE_008_INVALID_ELECTRICAL)
            error_msgs.append(err)

        # 10. Duplicate Event ID Check (Rule 9)
        event_id = record.get("event_id")
        if event_id and isinstance(event_id, str):
            err = QualityRules.check_duplicate_event_id(event_id, self._seen_event_ids)
            if err:
                violated_rules.append(DQRuleCode.RULE_009_DUPLICATE_EVENT_ID)
                error_msgs.append(err)
                metrics.increment("duplicate_events_detected")

        # 11. Duplicate Logical Event Check (Rule 10)
        asset_id = record.get("asset_id")
        ts = record.get("timestamp")
        if asset_id and ts and isinstance(ts, (int, float)):
            err = QualityRules.check_duplicate_logical(asset_id, ts, self._seen_logical_events)
            if err:
                violated_rules.append(DQRuleCode.RULE_010_DUPLICATE_LOGICAL_EVENT)
                error_msgs.append(err)

        # 12. Delayed Event Flag (Rule 12)
        is_delayed = QualityRules.check_delayed_event(record, now_ts)

        # 13. Out of Order Sequence Flag (Rule 13)
        seq = record.get("sequence_number")
        is_ooo = False
        if asset_id and isinstance(seq, int):
            is_ooo = QualityRules.check_out_of_order(asset_id, seq, self._last_seen_sequences)

        # Update state if valid
        is_valid = (len(violated_rules) == 0)
        if is_valid:
            if event_id:
                self._add_to_seen_ids(event_id)
            if asset_id and ts:
                self._seen_logical_events.add(f"{asset_id}:{ts:.3f}")
            if asset_id and isinstance(seq, int):
                self._last_seen_sequences[asset_id] = max(seq, self._last_seen_sequences.get(asset_id, -1))
            metrics.increment("events_validated_valid")
        else:
            for r in violated_rules:
                metrics.record_violation(r.value)

        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        metrics.observe("validation_latency_ms", elapsed_ms)

        result = DQValidationResult(
            is_valid=is_valid,
            violated_rules=violated_rules,
            error_messages=error_msgs,
            is_delayed=is_delayed,
            is_out_of_order=is_ooo,
        )
        return result, record

    def _add_to_seen_ids(self, event_id: str) -> None:
        """Maintains bounded set of seen event IDs."""
        if len(self._seen_event_ids) >= self.dedup_window_size:
            # Drop oldest elements by re-instantiating subset
            self._seen_event_ids.clear()
            self._seen_logical_events.clear()
        self._seen_event_ids.add(event_id)

    def reset(self) -> None:
        """Clears deduplication and sequence state."""
        self._seen_event_ids.clear()
        self._seen_logical_events.clear()
        self._last_seen_sequences.clear()
