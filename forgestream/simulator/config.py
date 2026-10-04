"""
Simulator Configuration Dataclasses and Defaults.
"""

from dataclasses import dataclass, field
from typing import List, Optional
from forgestream.schemas.telemetry_schema import AssetType, ScenarioID


@dataclass
class SimulatorOptions:
    """Runtime options for the telemetry generator."""
    seed: int = 42
    duration_sec: int = 30
    event_rate_hz: float = 10.0
    asset_ids: Optional[List[str]] = None
    default_scenario: ScenarioID = ScenarioID.SCENARIO_001
    inject_anomalies: bool = True
    anomaly_probability: float = 0.10
    start_time_iso: Optional[str] = None
