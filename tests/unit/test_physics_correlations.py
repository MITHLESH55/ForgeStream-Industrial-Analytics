"""
Unit tests for physical signal correlation equations.
Verifies electrical, thermal, mechanical, and fluid mechanics behavior.
"""

import math
import pytest
from forgestream.simulator.physics import PhysicsEngine
from forgestream.simulator.asset_models import DEFAULT_ASSETS


def test_current_increases_with_load():
    """Verifies that higher mechanical load strictly increases electrical current."""
    motor = DEFAULT_ASSETS["MOTOR-001"]

    i_low = PhysicsEngine.calculate_current(motor, load_pct=25.0, rpm=1750.0)
    i_mid = PhysicsEngine.calculate_current(motor, load_pct=50.0, rpm=1750.0)
    i_high = PhysicsEngine.calculate_current(motor, load_pct=100.0, rpm=1750.0)

    assert i_low < i_mid < i_high
    assert i_high > motor.rated_current * 0.8  # Near rated current at 100% load


def test_power_calculation_equation():
    """Verifies 3-phase active power formula accuracy."""
    motor = DEFAULT_ASSETS["MOTOR-001"]
    voltage = 400.0
    current = 50.0

    p_kw = PhysicsEngine.calculate_power(voltage, current, motor)
    expected_p = (math.sqrt(3.0) * voltage * current * motor.power_factor * motor.efficiency) / 1000.0

    assert pytest.approx(p_kw, rel=1e-4) == expected_p


def test_vibration_quadratic_with_speed():
    """Verifies that vibration increases quadratically with RPM ratio."""
    motor = DEFAULT_ASSETS["MOTOR-001"]

    v_half = PhysicsEngine.calculate_vibration(motor, rpm=875.0, load_pct=100.0)
    v_full = PhysicsEngine.calculate_vibration(motor, rpm=1750.0, load_pct=100.0)

    assert v_full > v_half * 3.0  # (1.0 / 0.5)^2 = 4x theoretical rotational scaling


def test_bearing_fault_vibration_growth():
    """Verifies that bearing degradation increases vibration significantly above baseline."""
    motor = DEFAULT_ASSETS["MOTOR-001"]

    v_healthy = PhysicsEngine.calculate_vibration(motor, rpm=1750.0, load_pct=100.0, degradation_factor=0.0)
    v_degraded = PhysicsEngine.calculate_vibration(motor, rpm=1750.0, load_pct=100.0, degradation_factor=0.8)

    assert v_degraded > v_healthy * 2.5


def test_thermal_heating_and_cooling_loss():
    """Verifies temperature rise under loss-of-cooling fault."""
    pump = DEFAULT_ASSETS["PUMP-001"]

    t_healthy = PhysicsEngine.calculate_temperature(
        pump, current_temp=62.0, load_pct=100.0, ambient_temp=25.0,
        elapsed_sec=60.0, cooling_loss_factor=0.0
    )
    t_fault = PhysicsEngine.calculate_temperature(
        pump, current_temp=62.0, load_pct=100.0, ambient_temp=25.0,
        elapsed_sec=60.0, cooling_loss_factor=0.9
    )

    assert t_fault > t_healthy
