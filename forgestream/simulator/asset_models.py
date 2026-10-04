"""
Industrial Asset Models and Operational Profiles.
Defines baseline physical properties, operational bounds, and ratings for each asset type.
"""

from dataclasses import dataclass, field
from typing import Dict, List
from forgestream.schemas.telemetry_schema import AssetType


@dataclass
class AssetProfile:
    """Detailed operational and physical baseline profile for an industrial machine."""
    asset_id: str
    asset_type: AssetType
    model_name: str
    rated_rpm: float
    rated_load: float
    rated_voltage: float
    rated_current: float
    power_factor: float
    efficiency: float
    baseline_temperature: float
    baseline_vibration: float
    baseline_pressure: float
    ambient_temperature: float = 25.0
    thermal_time_constant_sec: float = 300.0
    criticality: str = "MEDIUM"


DEFAULT_ASSETS: Dict[str, AssetProfile] = {
    "MOTOR-001": AssetProfile(
        asset_id="MOTOR-001",
        asset_type=AssetType.MOTOR,
        model_name="Siemens 1LE1501-1DB4 Induction Motor",
        rated_rpm=1750.0,
        rated_load=100.0,
        rated_voltage=400.0,
        rated_current=50.0,
        power_factor=0.88,
        efficiency=0.94,
        baseline_temperature=68.0,
        baseline_vibration=1.2,
        baseline_pressure=1.0,
        criticality="HIGH",
    ),
    "PUMP-001": AssetProfile(
        asset_id="PUMP-001",
        asset_type=AssetType.PUMP,
        model_name="Grundfos CR 45 Centrifugal Pump",
        rated_rpm=2900.0,
        rated_load=100.0,
        rated_voltage=400.0,
        rated_current=35.0,
        power_factor=0.86,
        efficiency=0.82,
        baseline_temperature=62.0,
        baseline_vibration=1.6,
        baseline_pressure=6.5,
        criticality="CRITICAL",
    ),
    "COMPRESSOR-001": AssetProfile(
        asset_id="COMPRESSOR-001",
        asset_type=AssetType.COMPRESSOR,
        model_name="Atlas Copco GA 37 Rotary Screw Compressor",
        rated_rpm=3600.0,
        rated_load=100.0,
        rated_voltage=400.0,
        rated_current=92.0,
        power_factor=0.89,
        efficiency=0.91,
        baseline_temperature=84.0,
        baseline_vibration=2.2,
        baseline_pressure=8.5,
        criticality="HIGH",
    ),
    "CONVEYOR-001": AssetProfile(
        asset_id="CONVEYOR-001",
        asset_type=AssetType.CONVEYOR,
        model_name="Continental Heavy Duty Troughed Conveyor",
        rated_rpm=120.0,
        rated_load=100.0,
        rated_voltage=400.0,
        rated_current=22.0,
        power_factor=0.82,
        efficiency=0.88,
        baseline_temperature=45.0,
        baseline_vibration=0.8,
        baseline_pressure=1.0,
        criticality="MEDIUM",
    ),
    "TURBINE-001": AssetProfile(
        asset_id="TURBINE-001",
        asset_type=AssetType.TURBINE,
        model_name="GE Vernova LM2500 Industrial Gas Turbine",
        rated_rpm=5400.0,
        rated_load=100.0,
        rated_voltage=6600.0,
        rated_current=210.0,
        power_factor=0.92,
        efficiency=0.96,
        baseline_temperature=135.0,
        baseline_vibration=3.1,
        baseline_pressure=24.0,
        criticality="CRITICAL",
    ),
}


def get_default_asset_list() -> List[AssetProfile]:
    """Returns a list of standard preconfigured industrial assets."""
    return list(DEFAULT_ASSETS.values())


get_all_default_profiles = get_default_asset_list
