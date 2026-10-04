"""
Pydantic schemas and models for ForgeStream telemetry events.
Includes strict type validation, constraints, and ground-truth metadata isolation.
"""

from datetime import datetime, timezone
from enum import Enum
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field, field_validator


class AssetType(str, Enum):
    MOTOR = "MOTOR"
    PUMP = "PUMP"
    COMPRESSOR = "COMPRESSOR"
    CONVEYOR = "CONVEYOR"
    TURBINE = "TURBINE"


class OperatingMode(str, Enum):
    IDLE = "IDLE"
    STARTUP = "STARTUP"
    NORMAL = "NORMAL"
    HIGH_LOAD = "HIGH_LOAD"
    MAINTENANCE = "MAINTENANCE"
    DEGRADED = "DEGRADED"


class MaintenanceState(str, Enum):
    HEALTHY = "HEALTHY"
    NEEDS_INSPECTION = "NEEDS_INSPECTION"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


class ScenarioID(str, Enum):
    SCENARIO_001 = "SCENARIO_001_NORMAL_OPERATION"
    SCENARIO_002 = "SCENARIO_002_BEARING_DEGRADATION"
    SCENARIO_003 = "SCENARIO_003_OVERHEATING"
    SCENARIO_004 = "SCENARIO_004_PRESSURE_ANOMALY"
    SCENARIO_005 = "SCENARIO_005_ELECTRICAL_INSTABILITY"
    SCENARIO_006 = "SCENARIO_006_SENSOR_MISSING_DATA"
    SCENARIO_007 = "SCENARIO_007_DELAYED_EVENTS"
    SCENARIO_008 = "SCENARIO_008_OUT_OF_ORDER_EVENTS"


class TelemetryMetadata(BaseModel):
    """
    Evaluation ground truth & audit metadata.
    IMPORTANT: These fields are for evaluation/audit ONLY and must NOT be used
    as predictive features in downstream ML models (leakage prevention).
    """
    scenario_id: ScenarioID = Field(default=ScenarioID.SCENARIO_001, description="Ground truth scenario label")
    maintenance_state: MaintenanceState = Field(default=MaintenanceState.HEALTHY, description="True asset health state")
    is_synthetic: bool = Field(default=True, description="Flag indicating synthetic origin")
    ground_truth_rul_hours: Optional[float] = Field(default=None, description="Actual remaining useful life if degraded")


class TelemetryEvent(BaseModel):
    """Primary telemetry event model for industrial assets."""
    event_id: str = Field(..., description="Unique event identifier (UUID or deterministic ID)")
    asset_id: str = Field(..., description="Unique asset identifier (e.g. MOTOR-001)")
    asset_type: AssetType = Field(..., description="Category of the industrial asset")
    timestamp: float = Field(..., description="Unix epoch timestamp in seconds")
    event_time: datetime = Field(..., description="ISO-8601 UTC timestamp of event generation")
    ingestion_time: Optional[datetime] = Field(default=None, description="ISO-8601 UTC timestamp at ingestion point")

    # Core telemetry metrics
    temperature: float = Field(..., description="Operating temperature in degrees Celsius")
    vibration: float = Field(..., description="RMS vibration velocity in mm/s")
    pressure: float = Field(..., description="Process pressure in bar")
    rpm: float = Field(..., description="Rotational speed in RPM")
    current: float = Field(..., description="Electrical current in Amperes")
    voltage: float = Field(..., description="Electrical voltage in Volts")
    power: float = Field(..., description="Active electrical power in kiloWatts")
    load: float = Field(..., description="Operating mechanical load percentage (0 - 100%)")

    # State and protocol fields
    operating_mode: OperatingMode = Field(..., description="Current operational state")
    sequence_number: int = Field(..., ge=0, description="Monotonically increasing sequence number per asset")
    schema_version: str = Field(default="1.0.0", description="Contract schema version")

    # Segregated ground truth metadata
    metadata: TelemetryMetadata = Field(default_factory=TelemetryMetadata, description="Ground-truth audit payload")

    @field_validator("event_time", mode="before")
    def ensure_utc(cls, v):
        if isinstance(v, str):
            dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        if isinstance(v, datetime):
            return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
        return v

    def to_feature_dict(self) -> Dict[str, Any]:
        """
        Returns a dictionary strictly containing feature columns for ML/analytics.
        Explicitly excludes metadata / scenario labels to prevent target leakage.
        """
        return {
            "event_id": self.event_id,
            "asset_id": self.asset_id,
            "asset_type": self.asset_type.value,
            "event_time": self.event_time.isoformat(),
            "temperature": self.temperature,
            "vibration": self.vibration,
            "pressure": self.pressure,
            "rpm": self.rpm,
            "current": self.current,
            "voltage": self.voltage,
            "power": self.power,
            "load": self.load,
            "operating_mode": self.operating_mode.value,
            "sequence_number": self.sequence_number,
        }
