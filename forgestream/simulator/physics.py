"""
Physics Correlation Engine for Industrial Telemetry Simulation.
Implements governing physical equations linking electrical, mechanical, thermal, and fluid signals.
"""

import math
import numpy as np
from forgestream.simulator.asset_models import AssetProfile
from forgestream.schemas.telemetry_schema import OperatingMode


class PhysicsEngine:
    """
    Computes physically consistent multi-parameter industrial telemetry signals.
    """

    @staticmethod
    def calculate_current(
        profile: AssetProfile,
        load_pct: float,
        rpm: float,
        degradation_factor: float = 0.0,
        noise: float = 0.0,
    ) -> float:
        """
        Calculates electrical current (Amperes) proportional to load, speed ratio, and mechanical drag.
        """
        speed_ratio = max(0.0, rpm / max(profile.rated_rpm, 1.0))
        load_ratio = max(0.0, min(load_pct / 100.0, 1.5))

        # Base magnetizing current + torque-producing active current
        i_no_load = profile.rated_current * 0.28
        i_active = profile.rated_current * 0.72 * load_ratio * speed_ratio

        # Degradation causes mechanical friction and loss of efficiency (surging current)
        i_degrade = profile.rated_current * 0.15 * degradation_factor

        current = i_no_load + i_active + i_degrade + noise
        return max(0.0, float(current))

    @staticmethod
    def calculate_voltage(
        profile: AssetProfile,
        sag_factor: float = 0.0,
        noise: float = 0.0,
    ) -> float:
        """
        Calculates line-to-line 3-phase voltage (Volts).
        """
        # Nominal rated voltage adjusted for grid sags and random fluctuations
        v = profile.rated_voltage * (1.0 - sag_factor) + noise
        return max(0.0, float(v))

    @staticmethod
    def calculate_power(
        voltage: float,
        current: float,
        profile: AssetProfile,
        noise: float = 0.0,
    ) -> float:
        """
        Calculates active 3-phase electrical power (kiloWatts).
        P = sqrt(3) * V * I * PF * efficiency / 1000
        """
        p_kw = (math.sqrt(3.0) * voltage * current * profile.power_factor * profile.efficiency) / 1000.0
        p_kw += noise
        return max(0.0, float(p_kw))

    @staticmethod
    def calculate_temperature(
        profile: AssetProfile,
        current_temp: float,
        load_pct: float,
        ambient_temp: float,
        elapsed_sec: float,
        degradation_factor: float = 0.0,
        cooling_loss_factor: float = 0.0,
        noise: float = 0.0,
    ) -> float:
        """
        Calculates operating temperature using Newton's law of cooling and Joule/mechanical heating.
        T_steady = T_ambient + Delta_T_rated * (Load/100)^1.2 + T_fault
        """
        load_ratio = max(0.0, load_pct / 100.0)
        delta_t_rated = profile.baseline_temperature - profile.ambient_temperature

        # Steady state temperature target for this load
        steady_state_t = ambient_temp + delta_t_rated * (load_ratio ** 1.2)

        # Fault additions (friction heating / cooling failure)
        fault_heating = 45.0 * degradation_factor + 60.0 * cooling_loss_factor
        steady_state_t += fault_heating

        # Exponential approach to steady state: dT/dt = (T_ss - T) / tau
        tau = profile.thermal_time_constant_sec / max(1.0 - cooling_loss_factor * 0.8, 0.2)
        alpha = 1.0 - math.exp(-elapsed_sec / tau)

        next_temp = current_temp + alpha * (steady_state_t - current_temp) + noise
        return float(next_temp)

    @staticmethod
    def calculate_vibration(
        profile: AssetProfile,
        rpm: float,
        load_pct: float,
        degradation_factor: float = 0.0,
        imbalance_factor: float = 0.0,
        noise: float = 0.0,
    ) -> float:
        """
        Calculates RMS vibration velocity (mm/s).
        V_rms = V_baseline * (RPM/RPM_rated)^2 * (1 + beta * fault) + noise
        """
        speed_ratio = max(0.0, rpm / max(profile.rated_rpm, 1.0))
        load_ratio = max(0.0, load_pct / 100.0)

        # Quadratic rotational vibration component
        v_rotational = profile.baseline_vibration * (speed_ratio ** 2) * (0.8 + 0.2 * load_ratio)

        # Non-linear exponential fault growth (bearing / imbalance)
        v_fault = profile.baseline_vibration * (3.5 * (degradation_factor ** 1.5) + 4.0 * imbalance_factor)

        vibration = v_rotational + v_fault + noise
        return max(0.0, float(vibration))

    @staticmethod
    def calculate_pressure(
        profile: AssetProfile,
        rpm: float,
        load_pct: float,
        cavitation_or_leak_factor: float = 0.0,
        blockage_factor: float = 0.0,
        noise: float = 0.0,
    ) -> float:
        """
        Calculates process pressure (bar) based on pump/compressor affinity laws and flow resistances.
        """
        speed_ratio = max(0.0, rpm / max(profile.rated_rpm, 1.0))
        load_ratio = max(0.0, load_pct / 100.0)

        # Affinity law: Pressure varies with RPM^1.8 to RPM^2.0
        p_nominal = profile.baseline_pressure * (speed_ratio ** 1.8) * (0.5 + 0.5 * (load_ratio ** 0.5))

        # Blockage increases pressure; cavitation/leaks drop discharge pressure
        p_anomaly = p_nominal * (1.8 * blockage_factor - 0.7 * cavitation_or_leak_factor)

        pressure = p_nominal + p_anomaly + noise
        return max(0.0, float(pressure))
