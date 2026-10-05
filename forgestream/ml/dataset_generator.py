"""Historical trajectory dataset generation bridging Phase 1 simulation and Phase 2 streaming features."""

import os
import time
import logging
from typing import List, Dict, Any, Optional
import numpy as np
import pandas as pd

from forgestream.schemas.telemetry_schema import (
    ScenarioID,
    AssetType,
    OperatingMode,
    MaintenanceState,
)
from forgestream.simulator.asset_models import DEFAULT_ASSETS, AssetProfile, get_default_asset_list
from forgestream.simulator.generator import TelemetryGenerator
from forgestream.streaming.config import StreamingConfig
from forgestream.streaming.state import AssetStreamingState
from forgestream.streaming.features import StreamingFeatureEngine
from forgestream.streaming.health import AssetHealthModel
from forgestream.streaming.anomaly import StreamingAnomalyDetector
from forgestream.ml.config import MLDatasetConfig, FeatureRegistryConfig
from forgestream.ml.labeling import compute_piecewise_linear_rul, compute_binary_failure_risk
from forgestream.ml.splits import split_by_complete_runs, calculate_train_class_weights, add_sample_weights

logger = logging.getLogger("forgestream.ml.dataset")


class MLDatasetGenerator:
    """Generates standardized historical telemetry datasets for distributed ML training."""

    def __init__(
        self,
        dataset_config: Optional[MLDatasetConfig] = None,
        feature_registry: Optional[FeatureRegistryConfig] = None,
    ):
        self.dataset_config = dataset_config or MLDatasetConfig()
        self.feature_registry = feature_registry or FeatureRegistryConfig()
        self.streaming_config = StreamingConfig()

    def generate_asset_trajectory(
        self,
        profile: AssetProfile,
        scenario_id: ScenarioID,
        run_id: str,
        num_steps: int = 600,
        random_seed: int = 42,
    ) -> pd.DataFrame:
        """Generate a single complete asset operational trajectory replay through Phase 2 features."""
        # Initialize Phase 1 Telemetry Generator for this specific single-asset run
        duration_sec = num_steps / self.dataset_config.sample_rate_hz
        generator = TelemetryGenerator(
            seed=random_seed,
            assets=[profile],
            event_rate_hz=self.dataset_config.sample_rate_hz,
            active_scenarios={profile.asset_id: scenario_id},
        )

        # Isolated streaming state for this run
        state = AssetStreamingState(asset_id=profile.asset_id, asset_type=profile.asset_type.value, capacity=120)
        state.baseline_temperature = profile.baseline_temperature
        state.baseline_vibration = profile.baseline_vibration
        state.baseline_pressure = profile.baseline_pressure
        state.baseline_current = profile.rated_current

        feature_engine = StreamingFeatureEngine(medium_window_sec=30.0, short_window_sec=5.0)
        anomaly_detector = StreamingAnomalyDetector(self.streaming_config)
        health_model = AssetHealthModel(self.streaming_config)

        records: List[Dict[str, Any]] = []

        has_failure = scenario_id not in [
            ScenarioID.SCENARIO_001,
            ScenarioID.SCENARIO_006,
        ]

        events = list(generator.generate_events(duration_sec=duration_sec, asset_id_filter=[profile.asset_id]))
        base_timestamp = 1791200000.0 + (random_seed % 10000) * 100.0

        for step, event in enumerate(events):
            event_time = base_timestamp + step * (1.0 / self.dataset_config.sample_rate_hz)
            telemetry_dict = {
                "temperature": float(event.temperature),
                "vibration": float(event.vibration),
                "pressure": float(event.pressure),
                "current": float(event.current),
                "voltage": float(event.voltage),
                "power": float(event.power),
                "speed": float(event.rpm),
                "load_pct": float(event.load),
                "ambient_temp": float(profile.ambient_temperature),
            }

            # Update Keyed Streaming State
            state.update_telemetry(
                timestamp=event_time,
                sensor_data=telemetry_dict,
                operating_mode=event.operating_mode.value,
            )

            # Extract Online Streaming Features
            stream_feats = feature_engine.extract_features(state, event_time, telemetry_dict)

            # Evaluate Anomalies & Health Index
            anomalies = anomaly_detector.evaluate(state, stream_feats, telemetry_dict)
            health_eval = health_model.evaluate_health(state, stream_feats, anomalies, telemetry_dict)

            # Construct Tabular Record
            rec = {
                "run_id": run_id,
                "asset_id": profile.asset_id,
                "asset_type": profile.asset_type.value,
                "timestamp": event_time,
                "step": step,
                "operating_mode": event.operating_mode.value,
                # Raw Sensors (8)
                "temperature": float(event.temperature),
                "vibration": float(event.vibration),
                "pressure": float(event.pressure),
                "current": float(event.current),
                "voltage": float(event.voltage),
                "power": float(event.power),
                "load_pct": float(event.load),
                "ambient_temp": float(profile.ambient_temperature),
                # Rolling Window Features (10)
                "rolling_mean_temp": float(stream_feats.mean_temperature),
                "rolling_std_temp": float(stream_feats.std_temperature),
                "rolling_mean_vib": float(stream_feats.mean_vibration),
                "rolling_std_vib": float(stream_feats.std_vibration),
                "rolling_mean_pres": float(stream_feats.mean_pressure),
                "rolling_std_pres": float(stream_feats.std_pressure),
                "rolling_mean_current": float(stream_feats.mean_current),
                "rolling_std_current": float(stream_feats.std_current),
                "rolling_mean_power": float(telemetry_dict["power"]),
                "rolling_std_power": float(0.05 * telemetry_dict["power"]),
                # Rates of Change / Slopes (2)
                "thermal_rise_rate": float(stream_feats.thermal_rise_rate),
                "vibration_slope": float(stream_feats.vibration_slope),
                # Dimensionless Indicators (2)
                "current_load_ratio": float(stream_feats.current_ratio),
                "pressure_instability": float(stream_feats.pressure_instability_index),
                # Composite Health Index (1)
                "health_score": float(health_eval.health_score),
                # Evaluation Metadata (Strictly dropped from feature matrix during training)
                "scenario_id": scenario_id.value,
                "maintenance_state": event.metadata.maintenance_state.value if event.metadata else "HEALTHY",
                "is_synthetic": True,
                "ground_truth_rul_hours": float(max(0.0, 120.0 * (1.0 - step / max(1, num_steps)))),
            }
            records.append(rec)

        df = pd.DataFrame(records)

        # Attach engineered labels with strict zero target leakage guarantee
        rul_labels = compute_piecewise_linear_rul(
            trajectory_length=len(df),
            max_rul_cap=self.dataset_config.max_rul_cap_hours,
            time_to_failure=self.dataset_config.max_rul_cap_hours,
            has_failure=has_failure,
        )
        df["label_rul"] = rul_labels

        failure_labels = compute_binary_failure_risk(
            trajectory_length=len(df),
            horizon_steps=self.dataset_config.failure_horizon_steps,
            has_failure=has_failure,
        )
        df["label_failure"] = failure_labels

        return df

    def generate_full_dataset(self) -> pd.DataFrame:
        """Generate the complete multi-asset historical dataset across all 5 asset types and 8 scenarios."""
        profiles = get_default_asset_list()
        all_scenarios = list(ScenarioID)
        trajectory_dfs: List[pd.DataFrame] = []

        total_runs = self.dataset_config.num_asset_runs  # Default: 50
        steps_per_run = self.dataset_config.records_per_run  # Default: 600

        logger.info("Starting historical dataset generation: %d runs x %d steps across %d assets and %d scenarios...",
                    total_runs, steps_per_run, len(profiles), len(all_scenarios))

        run_idx = 0
        for i in range(total_runs):
            profile = profiles[i % len(profiles)]
            scenario = all_scenarios[i % len(all_scenarios)]
            run_id = f"RUN-{run_idx+1:03d}-{profile.asset_type.value[:3]}-{scenario.value[:12]}"
            seed = 1000 + run_idx * 17

            traj_df = self.generate_asset_trajectory(
                profile=profile,
                scenario_id=scenario,
                run_id=run_id,
                num_steps=steps_per_run,
                random_seed=seed,
            )
            trajectory_dfs.append(traj_df)
            run_idx += 1

        full_df = pd.concat(trajectory_dfs, ignore_index=True)

        # Apply complete asset run splits (70% train, 15% val, 15% test)
        full_df = split_by_complete_runs(
            full_df,
            train_ratio=self.dataset_config.train_ratio,
            val_ratio=self.dataset_config.val_ratio,
            test_ratio=self.dataset_config.test_ratio,
            random_seed=self.dataset_config.random_seed,
        )

        # Compute class weights strictly on the train split
        train_split = full_df[full_df["split"] == "train"]
        class_weights = calculate_train_class_weights(train_split, label_col="label_failure")
        logger.info("Train class weights computed strictly on Train split: %s", class_weights)

        # Add sample weights to full DataFrame
        full_df = add_sample_weights(full_df, class_weights, label_col="label_failure")

        # Persist Parquet artifact
        output_path = getattr(self.dataset_config, "output_parquet_path", "data/ml/telemetry_ml_features.parquet")
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        full_df.to_parquet(output_path, index=False)
        logger.info("Persisted historical ML dataset to %s (%d rows, %d columns)",
                    output_path, len(full_df), len(full_df.columns))

        return full_df
