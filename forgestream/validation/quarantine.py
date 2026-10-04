"""
Quarantine Handler and Serialization Module for Defective Telemetry.
"""

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field
from forgestream.validation.quality_rules import DQRuleCode, DQValidationResult


class QuarantinedRecord(BaseModel):
    """Structured record representing a quarantined defective telemetry event."""
    quarantine_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_id: Optional[str] = None
    asset_id: Optional[str] = None
    violated_rules: List[str]
    reasons: List[str]
    raw_payload: Dict[str, Any]
    quarantined_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class QuarantineManager:
    """Manages creation, routing, and serialization of quarantined events."""

    @staticmethod
    def create_quarantine_record(
        raw_payload: Dict[str, Any],
        validation_result: DQValidationResult,
    ) -> QuarantinedRecord:
        """Packages a failed validation event into a structured QuarantinedRecord."""
        return QuarantinedRecord(
            event_id=raw_payload.get("event_id") if isinstance(raw_payload, dict) else None,
            asset_id=raw_payload.get("asset_id") if isinstance(raw_payload, dict) else None,
            violated_rules=[r.value for r in validation_result.violated_rules],
            reasons=validation_result.error_messages,
            raw_payload=raw_payload if isinstance(raw_payload, dict) else {"raw": str(raw_payload)},
        )
