"""
Deterministic Industrial Telemetry Generator.
Orchestrates seeded PRNG, physics engine, operational state transitions, and fault injection.
"""

import time
import uuid
from datetime import datetime, timezone, timedelta
from typing import Dict, Generator, List, Optional
import numpy as np

from forgestream.schemas.telemetry_schema import (
    TelemetryEvent,
    TelemetryMetadata,
    AssetType,
    OperatingMode,
    MaintenanceState,
    ScenarioID,
)
from forgestream.simulator.asset_models import AssetProfile, DEFAULT_ASSETS, get_default_asset_list
from forgestream.simulator.physics import PhysicsEngine
from forgestream.simulator.scenarios import ScenarioManager, ScenarioModifiers
from forgestream.observability.metrics import metrics


class AssetState:
    """Maintains continuous physical state for a single simulated asset."""

    def __init__(self, profile: AssetProfile, rng: np.random.Generator):
        self.profile = profile
        self.rng = rng
        self.sequence_number = 0
        self.current_temp = profile.baseline_temperature
        self.current_load = profile.rated_load
        self.current_mode = OperatingMode.NORMAL
        self.active_scenario = ScenarioID.SCENARIO_001
        self.ambient_temp = profile.ambient_temperature


class TelemetryGenerator:
    """
    High-throughput deterministic generator for industrial equipment telemetry streams.
    """

    def __init__(
        self,
        seed: int = 42,
        assets: Optional[List[AssetProfile]] = None,
        event_rate_hz: Optional[float] = None,
        sampling_interval_sec: Optional[float] = None,
        active_scenarios: Optional[Dict[str, ScenarioID]] = None,
        start_time_iso: Optional[str] = None,
    ):
        self.seed = seed
        self.rng = np.random.default_rng(seed)
        self.asset_profiles = assets or get_default_asset_list()

        if sampling_interval_sec is not None and sampling_interval_sec > 0:
            self.event_rate_hz = 1.0 / sampling_interval_sec
        elif event_rate_hz is not None:
            self.event_rate_hz = max(0.1, event_rate_hz)
        else:
            self.event_rate_hz = 10.0

        self.dt = 1.0 / self.event_rate_hz

        # Anchor base simulation timestamp
        if start_time_iso:
            self.base_time = datetime.fromisoformat(start_time_iso.replace("Z", "+00:00"))
        else:
            self.base_time = datetime(2026, 10, 5, 12, 0, 0, tzinfo=timezone.utc)

        # Initialize internal asset states with independent deterministic RNG streams
        self.states: Dict[str, AssetState] = {}
        for idx, profile in enumerate(self.asset_profiles):
            asset_seed = (seed * 10007 + idx * 37) & 0xFFFFFFFF
            asset_rng = np.random.default_rng(asset_seed)
            st = AssetState(profile, asset_rng)
            if active_scenarios and profile.asset_id in active_scenarios:
                st.active_scenario = active_scenarios[profile.asset_id]
            self.states[profile.asset_id] = st

    def generate_stream(
        self,
        duration_sec: float = 30.0,
        asset_id_filter: Optional[List[str]] = None,
    ) -> Generator[TelemetryEvent, None, None]:
        """Alias for generate_events."""
        return self.generate_events(duration_sec=duration_sec, asset_id_filter=asset_id_filter)

    def set_scenario(self, asset_id: str, scenario_id: ScenarioID) -> None:
        """Assigns an operational or fault scenario to a specific asset."""
        if asset_id in self.states:
            self.states[asset_id].active_scenario = scenario_id

    def generate_events(
        self,
        duration_sec: float = 30.0,
        asset_id_filter: Optional[List[str]] = None,
    ) -> Generator[TelemetryEvent, None, None]:
        """
        Yields a time-ordered sequence of deterministic TelemetryEvents across all active assets.
        """
        total_steps = int(duration_sec * self.event_rate_hz)
        active_assets = [
            p for p in self.asset_profiles
            if not asset_id_filter or p.asset_id in asset_id_filter
        ]

        # Buffer for out-of-order scenario handling
        ooo_buffer: List[TelemetryEvent] = []

        for step in range(total_steps):
            simulated_elapsed = step * self.dt
            progress_ratio = step / max(total_steps - 1, 1)

            for profile in active_assets:
                state = self.states[profile.asset_id]
                modifiers = ScenarioManager.get_modifiers(state.active_scenario, progress_ratio)

                # Update operational load
                effective_load = profile.rated_load * modifiers.load_multiplier
                # Minor load fluctuation noise
                effective_load += state.rng.normal(0.0, 0.5)
                effective_load = max(5.0, min(effective_load, 120.0))
                state.current_load = effective_load

                # Speed calculation (RPM)
                if modifiers.suggested_mode == OperatingMode.IDLE:
                    effective_rpm = 0.0
                elif modifiers.suggested_mode == OperatingMode.STARTUP:
                    effective_rpm = profile.rated_rpm * (step % 20) / 20.0
                else:
                    speed_fluctuation = state.rng.normal(0.0, 1.5)
                    effective_rpm = profile.rated_rpm * (0.95 + 0.05 * (effective_load / 100.0)) + speed_fluctuation
                effective_rpm = max(0.0, effective_rpm)

                # Correlated physics calculations
                current_noise = state.rng.normal(0.0, 0.2)
                current_val = PhysicsEngine.calculate_current(
                    profile=profile,
                    load_pct=effective_load,
                    rpm=effective_rpm,
                    degradation_factor=modifiers.degradation_factor,
                    noise=current_noise,
                )

                voltage_noise = state.rng.normal(0.0, 1.0)
                voltage_val = PhysicsEngine.calculate_voltage(
                    profile=profile,
                    sag_factor=modifiers.voltage_sag_factor,
                    noise=voltage_noise,
                )

                power_noise = state.rng.normal(0.0, 0.1)
                power_val = PhysicsEngine.calculate_power(
                    voltage=voltage_val,
                    current=current_val,
                    profile=profile,
                    noise=power_noise,
                )

                temp_noise = state.rng.normal(0.0, 0.1)
                temp_val = PhysicsEngine.calculate_temperature(
                    profile=profile,
                    current_temp=state.current_temp,
                    load_pct=effective_load,
                    ambient_temp=state.ambient_temp,
                    elapsed_sec=self.dt,
                    degradation_factor=modifiers.degradation_factor,
                    cooling_loss_factor=modifiers.cooling_loss_factor,
                    noise=temp_noise,
                )
                state.current_temp = temp_val

                vibe_noise = state.rng.normal(0.0, 0.05)
                vibe_val = PhysicsEngine.calculate_vibration(
                    profile=profile,
                    rpm=effective_rpm,
                    load_pct=effective_load,
                    degradation_factor=modifiers.degradation_factor,
                    imbalance_factor=modifiers.imbalance_factor,
                    noise=vibe_noise,
                )

                press_noise = state.rng.normal(0.0, 0.08)
                press_val = PhysicsEngine.calculate_pressure(
                    profile=profile,
                    rpm=effective_rpm,
                    load_pct=effective_load,
                    cavitation_or_leak_factor=modifiers.cavitation_factor,
                    blockage_factor=modifiers.blockage_factor,
                    noise=press_noise,
                )

                # Compute event timestamp
                raw_event_time = self.base_time + timedelta(seconds=simulated_elapsed)
                if modifiers.delay_seconds > 0:
                    effective_event_time = raw_event_time - timedelta(seconds=modifiers.delay_seconds)
                else:
                    effective_event_time = raw_event_time

                # Deterministic Event ID: includes asset_id and sequence
                event_id = f"evt-{profile.asset_id.lower()}-{state.sequence_number:08d}"

                metadata = TelemetryMetadata(
                    scenario_id=state.active_scenario,
                    maintenance_state=modifiers.maintenance_state,
                    is_synthetic=True,
                    ground_truth_rul_hours=120.0 * (1.0 - modifiers.degradation_factor) if modifiers.degradation_factor > 0 else None,
                )

                event = TelemetryEvent(
                    event_id=event_id,
                    asset_id=profile.asset_id,
                    asset_type=profile.asset_type,
                    timestamp=effective_event_time.timestamp(),
                    event_time=effective_event_time,
                    ingestion_time=None,  # Populated at Kafka/Worker intake
                    temperature=round(temp_val, 2),
                    vibration=round(vibe_val, 3),
                    pressure=round(press_val, 2),
                    rpm=round(effective_rpm, 1),
                    current=round(current_val, 2),
                    voltage=round(voltage_val, 1),
                    power=round(power_val, 2),
                    load=round(effective_load, 1),
                    operating_mode=modifiers.suggested_mode,
                    sequence_number=state.sequence_number,
                    schema_version="1.0.0",
                    metadata=metadata,
                )

                state.sequence_number += 1
                metrics.increment("events_generated")

                if modifiers.is_out_of_order:
                    ooo_buffer.append(event)
                    if len(ooo_buffer) >= 3:
                        # Yield buffer in reversed order to create authentic out-of-order sequence
                        for item in reversed(ooo_buffer):
                            yield item
                        ooo_buffer.clear()
                else:
                    # Flush any remaining buffer first
                    while ooo_buffer:
                        yield ooo_buffer.pop(0)
                    yield event

        # Flush any remaining items in out-of-order buffer
        while ooo_buffer:
            yield ooo_buffer.pop(0)
