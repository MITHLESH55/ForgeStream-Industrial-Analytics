"""
Industrial Degradation and Fault Scenarios.
Implements the 8 standard operational scenarios for deterministic fault injection.
"""

from dataclasses import dataclass
from typing import Dict, Any, Tuple
from forgestream.schemas.telemetry_schema import ScenarioID, MaintenanceState, OperatingMode


@dataclass
class ScenarioModifiers:
    """Multipliers and fault injection parameters applied by the active scenario."""
    load_multiplier: float = 1.0
    degradation_factor: float = 0.0
    cooling_loss_factor: float = 0.0
    cavitation_factor: float = 0.0
    blockage_factor: float = 0.0
    voltage_sag_factor: float = 0.0
    imbalance_factor: float = 0.0
    is_missing_data: bool = False
    delay_seconds: float = 0.0
    is_out_of_order: bool = False
    maintenance_state: MaintenanceState = MaintenanceState.HEALTHY
    suggested_mode: OperatingMode = OperatingMode.NORMAL


class ScenarioManager:
    """Orchestrates scenario progression and fault modifier calculation over time."""

    @staticmethod
    def get_modifiers(
        scenario_id: ScenarioID,
        progress_ratio: float,  # 0.0 (start of run) to 1.0 (end of run)
    ) -> ScenarioModifiers:
        """
        Calculates time-dependent physical modifiers for a given scenario.
        """
        p = max(0.0, min(progress_ratio, 1.0))

        if scenario_id == ScenarioID.SCENARIO_001:
            # Normal operation with mild cyclical load fluctuations
            return ScenarioModifiers(
                load_multiplier=1.0 + 0.05 * (p - 0.5),
                maintenance_state=MaintenanceState.HEALTHY,
                suggested_mode=OperatingMode.NORMAL,
            )

        elif scenario_id == ScenarioID.SCENARIO_002:
            # Progressive Bearing Degradation: vibration rises exponentially, friction increases
            deg = p ** 1.8
            maint = MaintenanceState.HEALTHY
            if deg > 0.4:
                maint = MaintenanceState.NEEDS_INSPECTION
            if deg > 0.75:
                maint = MaintenanceState.WARNING
            if deg > 0.95:
                maint = MaintenanceState.CRITICAL

            return ScenarioModifiers(
                degradation_factor=deg,
                imbalance_factor=deg * 0.5,
                maintenance_state=maint,
                suggested_mode=OperatingMode.DEGRADED if deg > 0.6 else OperatingMode.NORMAL,
            )

        elif scenario_id == ScenarioID.SCENARIO_003:
            # Overheating / Loss of Cooling: rapid thermal buildup
            loss = min(1.0, p * 1.5)
            maint = MaintenanceState.WARNING if loss > 0.5 else MaintenanceState.HEALTHY
            if loss > 0.85:
                maint = MaintenanceState.CRITICAL

            return ScenarioModifiers(
                cooling_loss_factor=loss,
                maintenance_state=maint,
                suggested_mode=OperatingMode.DEGRADED if loss > 0.7 else OperatingMode.NORMAL,
            )

        elif scenario_id == ScenarioID.SCENARIO_004:
            # Pressure Anomaly / Cavitation / Blockage
            cav = 0.8 * (p ** 2) if p > 0.3 else 0.0
            return ScenarioModifiers(
                cavitation_factor=cav,
                maintenance_state=MaintenanceState.WARNING if cav > 0.4 else MaintenanceState.HEALTHY,
                suggested_mode=OperatingMode.DEGRADED if cav > 0.5 else OperatingMode.NORMAL,
            )

        elif scenario_id == ScenarioID.SCENARIO_005:
            # Electrical Instability: Grid voltage sags and surges
            sag = 0.25 * ((1.0 + p) / 2.0)
            return ScenarioModifiers(
                voltage_sag_factor=sag,
                maintenance_state=MaintenanceState.WARNING if sag > 0.15 else MaintenanceState.HEALTHY,
                suggested_mode=OperatingMode.DEGRADED if sag > 0.2 else OperatingMode.NORMAL,
            )

        elif scenario_id == ScenarioID.SCENARIO_006:
            # Sensor Missing Data: Random or systematic channel dropouts
            return ScenarioModifiers(
                is_missing_data=(p > 0.4 and p < 0.7),
                maintenance_state=MaintenanceState.NEEDS_INSPECTION if (p > 0.4 and p < 0.7) else MaintenanceState.HEALTHY,
            )

        elif scenario_id == ScenarioID.SCENARIO_007:
            # Delayed Events: Emitted with timestamps in the past
            delay = 45.0 if p > 0.5 else 0.0
            return ScenarioModifiers(
                delay_seconds=delay,
                maintenance_state=MaintenanceState.HEALTHY,
            )

        elif scenario_id == ScenarioID.SCENARIO_008:
            # Out of Order Events: Flagged for sequence shuffling
            return ScenarioModifiers(
                is_out_of_order=(p > 0.3),
                maintenance_state=MaintenanceState.HEALTHY,
            )

        # Default fallback
        return ScenarioModifiers()
