# ForgeStream Phase 3: Known Operational Limitations & Future Mitigations

## Overview
While the Phase 3 Spark MLlib modeling pipeline delivers robust predictive accuracy on historical multi-asset trajectories, real-world deployment across manufacturing plants involves physical edge cases, sensor noise degradation, and non-stationary operational dynamics.

---

## 1. Simulation vs. Real-World Physical Gaps
- **Linear Degradation Simplification**: While piecewise linear RUL caps capture early healthy vs. late degradation stages, actual machine degradation frequently follows non-linear exponential damage accumulation (e.g. Paris-Erdogan fatigue crack law).
  - *Mitigation*: Incorporate physics-informed neural network (PINN) loss penalties or non-linear state-space filters in future iterations.
- **Constant Operating Regimes**: The synthetic simulator alternates between discrete modes (`STEADY_LOW`, `STEADY_HIGH`). Real industrial machinery experiences continuous variable-frequency drives (VFD) and stochastic ambient temperature fluctuations.
  - *Mitigation*: Dynamically normalize sensor inputs relative to instantaneous load and ambient temperature indices (`current_load_ratio`, `thermal_rise_rate`).

---

## 2. Sensor Noise & Network Transport Vulnerabilities
- **Missing or Dropped Sensor Bursts**: During network outages or packet drop spikes, rolling window aggregations may encounter sparse buffers.
  - *Mitigation*: The Phase 2 circular buffer maintains bounded sliding windows with fallback zero-order hold (ZOH) interpolation.
- **Out-of-Order Sensor Arrivals**: Extreme out-of-order latency ($>30\text{s}$) can affect instantaneous slope calculations (`vibration_slope`, `thermal_rise_rate`).
  - *Mitigation*: Bounded out-of-orderness watermarks with lateness quarantine routing ensure out-of-bounds events do not distort rolling statistical features.

---

## 3. RUL Prediction Horizon Boundary Effects
- **Plateau Inaccuracy at Healthy States**: For assets with $T_{\text{fail}} - t \gg 120\text{h}$, the RUL model predicts $120.0\text{h}$ identically across all healthy time steps. While operationally sound, this limits long-term maintenance planning beyond the 120-hour window.
  - *Mitigation*: Maintain dual-horizon models (short-term operational warning $H = 24\text{h}$ + long-term strategic prognostic $H_{\text{long}} = 720\text{h}$).
