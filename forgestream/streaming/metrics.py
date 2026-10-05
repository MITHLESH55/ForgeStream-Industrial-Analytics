"""Operational health, throughput, and latency metrics collector for ForgeStream Phase 2."""

import time
import uuid
from collections import deque
from datetime import datetime, timezone
from typing import List, Optional
import numpy as np
from forgestream.streaming.schemas import SystemMetricEvent


class StreamingMetricsCollector:
    """Collects real-time streaming performance metrics, latencies, and watermark progress."""

    def __init__(self, window_duration_sec: float = 1.0, max_latencies: int = 10000):
        self.window_duration_sec = window_duration_sec
        self.latencies_ms: deque[float] = deque(maxlen=max_latencies)

        self.events_ingested: int = 0
        self.events_processed: int = 0
        self.events_late: int = 0
        self.events_excessively_late: int = 0
        self.events_quarantined: int = 0
        self.anomalies_detected: int = 0
        self.alerts_emitted: int = 0

        self.start_time_wall_clock: float = time.time()
        self.last_metric_emit_time: float = time.time()

    def record_event_processed(
        self,
        latency_ms: float,
        is_late: bool = False,
        is_excessively_late: bool = False,
        is_quarantined: bool = False,
        anomalies_count: int = 0,
        alert_emitted: bool = False,
    ) -> None:
        """Record telemetry processing execution metrics."""
        self.events_ingested += 1
        if not is_excessively_late and not is_quarantined:
            self.events_processed += 1
            self.latencies_ms.append(max(0.01, latency_ms))

        if is_late:
            self.events_late += 1
        if is_excessively_late:
            self.events_excessively_late += 1
        if is_quarantined:
            self.events_quarantined += 1

        self.anomalies_detected += anomalies_count
        if alert_emitted:
            self.alerts_emitted += 1

    def calculate_metrics(
        self,
        current_watermark: float,
        watermark_lag_sec: float,
        active_assets_count: int,
    ) -> SystemMetricEvent:
        """Compute performance percentiles and generate SystemMetricEvent."""
        now = time.time()
        elapsed = max(0.001, now - self.start_time_wall_clock)
        throughput = self.events_processed / elapsed

        if self.latencies_ms:
            lat_arr = np.array(list(self.latencies_ms))
            p50 = float(np.percentile(lat_arr, 50))
            p95 = float(np.percentile(lat_arr, 95))
            p99 = float(np.percentile(lat_arr, 99))
        else:
            p50, p95, p99 = 0.0, 0.0, 0.0

        dt = datetime.now(timezone.utc)

        return SystemMetricEvent(
            metric_id=f"met-{int(now)}-{uuid.uuid4().hex[:6]}",
            timestamp=now,
            event_time=dt.isoformat(),
            window_duration_sec=self.window_duration_sec,
            events_ingested=self.events_ingested,
            events_processed=self.events_processed,
            events_late=self.events_late,
            events_excessively_late=self.events_excessively_late,
            events_quarantined=self.events_quarantined,
            anomalies_detected=self.anomalies_detected,
            alerts_emitted=self.alerts_emitted,
            active_assets_count=active_assets_count,
            current_watermark=current_watermark,
            watermark_lag_sec=watermark_lag_sec,
            processing_latency_p50_ms=round(p50, 3),
            processing_latency_p95_ms=round(p95, 3),
            processing_latency_p99_ms=round(p99, 3),
            throughput_events_per_sec=round(throughput, 2),
            schema_version="2.0.0",
        )
