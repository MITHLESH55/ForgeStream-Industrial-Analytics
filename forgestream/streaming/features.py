"""Real-time streaming feature extraction engine for ForgeStream Phase 2."""

import math
from typing import Dict, List, Optional, Tuple, Any
from pydantic import BaseModel, Field
from forgestream.streaming.state import AssetStreamingState


class StreamFeatures(BaseModel):
    """Encapsulates extracted real-time statistical and physical derivatives for an asset."""

    asset_id: str
    timestamp: float
    operating_mode: str = "NORMAL"

    # Thermal Features
    mean_temperature: float = 0.0
    std_temperature: float = 0.0
    thermal_rise_rate: float = Field(default=0.0, description="Rate of temperature change in °C/minute")
    temperature_zscore: float = 0.0

    # Vibration Features
    mean_vibration: float = 0.0
    std_vibration: float = 0.0
    max_vibration: float = 0.0
    vibration_slope: float = Field(default=0.0, description="Vibration trend slope in mm/s/minute")
    vibration_zscore: float = 0.0

    # Pressure / Hydraulic Features
    mean_pressure: float = 0.0
    std_pressure: float = 0.0
    pressure_instability_index: float = Field(default=0.0, description="Pressure coefficient of variation (sigma/mu)")
    pressure_zscore: float = 0.0

    # Electrical Features
    current_ratio: float = Field(default=1.0, description="Ratio of current to rated/baseline current")
    mean_current: float = 0.0
    std_current: float = 0.0
    power_factor_deviation: float = 0.0

    # Window Metadata
    sample_count: int = 0
    window_duration_sec: float = 30.0


class StreamingFeatureEngine:
    """Computes online statistical aggregations and physical derivatives from keyed asset state."""

    def __init__(self, medium_window_sec: float = 30.0, short_window_sec: float = 5.0):
        self.medium_window_sec = medium_window_sec
        self.short_window_sec = short_window_sec

    @staticmethod
    def _compute_mean_std(values: List[float]) -> Tuple[float, float]:
        """Compute sample mean and standard deviation."""
        if not values:
            return 0.0, 0.0
        n = len(values)
        if n == 1:
            return values[0], 0.0
        mean = sum(values) / n
        variance = sum((x - mean) ** 2 for x in values) / (n - 1)
        return mean, math.sqrt(max(0.0, variance))

    @staticmethod
    def _compute_linear_slope(time_series: List[Tuple[float, float]]) -> float:
        """Compute rate of change per minute (dy/dt * 60) via ordinary least squares."""
        if len(time_series) < 2:
            return 0.0

        t0 = time_series[0][0]
        n = len(time_series)
        dt_total = time_series[-1][0] - t0
        if dt_total <= 0.001:
            return 0.0

        sum_t = 0.0
        sum_y = 0.0
        sum_ty = 0.0
        sum_t2 = 0.0

        for t, y in time_series:
            rel_t = t - t0  # in seconds
            sum_t += rel_t
            sum_y += y
            sum_ty += rel_t * y
            sum_t2 += rel_t * rel_t

        denominator = (n * sum_t2) - (sum_t * sum_t)
        if abs(denominator) < 1e-9:
            # Fallback to simple endpoint difference
            dy = time_series[-1][1] - time_series[0][1]
            return (dy / dt_total) * 60.0

        slope_per_sec = ((n * sum_ty) - (sum_t * sum_y)) / denominator
        return slope_per_sec * 60.0  # Convert to per minute

    def extract_features(
        self,
        state: AssetStreamingState,
        current_timestamp: float,
        current_telemetry: Optional[Dict[str, float]] = None,
    ) -> StreamFeatures:
        """Extract multi-signal rolling features from asset circular buffers.

        Args:
            state: Keyed asset streaming state.
            current_timestamp: Event timestamp.
            current_telemetry: Current raw sensor reading map.

        Returns:
            StreamFeatures: Computed feature set.
        """
        # Extract medium window time series
        temp_ts = state.temperatures.get_window(self.medium_window_sec, current_timestamp)
        vib_ts = state.vibrations.get_window(self.medium_window_sec, current_timestamp)
        press_ts = state.pressures.get_window(self.medium_window_sec, current_timestamp)
        curr_ts = state.currents.get_window(self.medium_window_sec, current_timestamp)

        temp_vals = [v for _, v in temp_ts]
        vib_vals = [v for _, v in vib_ts]
        press_vals = [v for _, v in press_ts]
        curr_vals = [v for _, v in curr_ts]

        # Thermal Features
        mean_temp, std_temp = self._compute_mean_std(temp_vals)
        thermal_rise = self._compute_linear_slope(temp_ts)
        curr_temp = current_telemetry.get("temperature", mean_temp) if current_telemetry else mean_temp
        temp_z = (curr_temp - mean_temp) / (std_temp + 1e-5) if std_temp > 1e-3 else 0.0

        # Vibration Features
        mean_vib, std_vib = self._compute_mean_std(vib_vals)
        max_vib = max(vib_vals) if vib_vals else 0.0
        vib_slope = self._compute_linear_slope(vib_ts)
        curr_vib = current_telemetry.get("vibration", mean_vib) if current_telemetry else mean_vib
        vib_z = (curr_vib - mean_vib) / (std_vib + 1e-5) if std_vib > 1e-3 else 0.0

        # Pressure Features
        mean_press, std_press = self._compute_mean_std(press_vals)
        pressure_instability = (std_press / (mean_press + 1e-5)) if mean_press > 0.01 else 0.0
        curr_press = current_telemetry.get("pressure", mean_press) if current_telemetry else mean_press
        press_z = (curr_press - mean_press) / (std_press + 1e-5) if std_press > 1e-3 else 0.0

        # Electrical Features
        mean_curr, std_curr = self._compute_mean_std(curr_vals)
        baseline_curr = state.baseline_current or mean_curr or 1.0
        curr_curr = current_telemetry.get("current", mean_curr) if current_telemetry else mean_curr
        current_ratio = curr_curr / baseline_curr if baseline_curr > 0.01 else 1.0

        # Calculate power factor deviation if power and voltage are available
        power_factor_dev = 0.0
        if current_telemetry:
            p_meas = current_telemetry.get("power", 0.0)
            v_meas = current_telemetry.get("voltage", 0.0)
            i_meas = current_telemetry.get("current", 0.0)
            if v_meas > 10.0 and i_meas > 0.1 and p_meas > 0.1:
                # Apparent power S = sqrt(3) * V * I / 1000 for 3-phase, or V*I/1000 for single
                apparent_power = (math.sqrt(3) * v_meas * i_meas) / 1000.0
                if apparent_power > 0.01:
                    estimated_pf = min(1.0, max(0.0, p_meas / apparent_power))
                    power_factor_dev = abs(estimated_pf - 0.88)

        return StreamFeatures(
            asset_id=state.asset_id,
            timestamp=current_timestamp,
            operating_mode=state.operating_mode,
            mean_temperature=round(mean_temp, 3),
            std_temperature=round(std_temp, 3),
            thermal_rise_rate=round(thermal_rise, 4),
            temperature_zscore=round(temp_z, 3),
            mean_vibration=round(mean_vib, 3),
            std_vibration=round(std_vib, 3),
            max_vibration=round(max_vib, 3),
            vibration_slope=round(vib_slope, 4),
            vibration_zscore=round(vib_z, 3),
            mean_pressure=round(mean_press, 3),
            std_pressure=round(std_press, 3),
            pressure_instability_index=round(pressure_instability, 4),
            pressure_zscore=round(press_z, 3),
            current_ratio=round(current_ratio, 3),
            mean_current=round(mean_curr, 3),
            std_current=round(std_curr, 3),
            power_factor_deviation=round(power_factor_dev, 4),
            sample_count=len(temp_vals),
            window_duration_sec=self.medium_window_sec,
        )
