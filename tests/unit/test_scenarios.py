"""
Unit tests for the 8 degradation scenarios.
"""

import pytest
from forgestream.schemas.telemetry_schema import ScenarioID, MaintenanceState, OperatingMode
from forgestream.simulator.scenarios import ScenarioManager


def test_scenario_001_normal_modifiers():
    """SCENARIO_001 should remain healthy throughout."""
    m_start = ScenarioManager.get_modifiers(ScenarioID.SCENARIO_001, 0.0)
    m_end = ScenarioManager.get_modifiers(ScenarioID.SCENARIO_001, 1.0)

    assert m_start.maintenance_state == MaintenanceState.HEALTHY
    assert m_end.maintenance_state == MaintenanceState.HEALTHY
    assert m_start.degradation_factor == 0.0


def test_scenario_002_bearing_progression():
    """SCENARIO_002 should transition through health states as degradation increases."""
    m_early = ScenarioManager.get_modifiers(ScenarioID.SCENARIO_002, 0.2)
    m_mid = ScenarioManager.get_modifiers(ScenarioID.SCENARIO_002, 0.6)
    m_late = ScenarioManager.get_modifiers(ScenarioID.SCENARIO_002, 0.98)

    assert m_early.degradation_factor < m_mid.degradation_factor < m_late.degradation_factor
    assert m_early.maintenance_state == MaintenanceState.HEALTHY
    assert m_late.maintenance_state in (MaintenanceState.WARNING, MaintenanceState.CRITICAL)


def test_scenario_003_overheating_loss():
    """SCENARIO_003 should induce high cooling loss."""
    m = ScenarioManager.get_modifiers(ScenarioID.SCENARIO_003, 0.9)
    assert m.cooling_loss_factor >= 0.8
    assert m.maintenance_state in (MaintenanceState.WARNING, MaintenanceState.CRITICAL)


def test_scenario_005_voltage_sag():
    """SCENARIO_005 should produce voltage sag factor."""
    m = ScenarioManager.get_modifiers(ScenarioID.SCENARIO_005, 0.8)
    assert m.voltage_sag_factor > 0.15


def test_scenario_007_delayed_events():
    """SCENARIO_007 should apply non-zero delay seconds."""
    m = ScenarioManager.get_modifiers(ScenarioID.SCENARIO_007, 0.8)
    assert m.delay_seconds > 0.0


def test_scenario_008_out_of_order():
    """SCENARIO_008 should set out-of-order flag."""
    m = ScenarioManager.get_modifiers(ScenarioID.SCENARIO_008, 0.8)
    assert m.is_out_of_order is True
