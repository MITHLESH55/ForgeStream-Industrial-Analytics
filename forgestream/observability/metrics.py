"""
Metrics collection module for ForgeStream.
Tracks event counts, processing latencies, data quality violations, and storage metrics.
"""

import threading
import time
from typing import Dict, List, Any


class MetricsCollector:
    """Thread-safe in-memory metrics registry for tracking telemetry pipeline operations."""

    def __init__(self):
        self._lock = threading.Lock()
        self.counters: Dict[str, int] = {
            "events_generated": 0,
            "events_sent_kafka": 0,
            "events_failed_kafka": 0,
            "events_consumed_kafka": 0,
            "events_validated_valid": 0,
            "events_validated_quarantined": 0,
            "events_persisted_iceberg": 0,
            "events_persisted_postgres": 0,
            "duplicate_events_detected": 0,
        }
        self.rule_violations: Dict[str, int] = {}
        self.gauges: Dict[str, float] = {}
        self.histograms: Dict[str, List[float]] = {
            "validation_latency_ms": [],
            "iceberg_commit_latency_ms": [],
            "kafka_produce_latency_ms": [],
        }

    def increment(self, counter_name: str, value: int = 1) -> None:
        """Increments a named counter."""
        with self._lock:
            self.counters[counter_name] = self.counters.get(counter_name, 0) + value

    def record_violation(self, rule_code: str) -> None:
        """Records a specific data quality rule failure."""
        with self._lock:
            self.rule_violations[rule_code] = self.rule_violations.get(rule_code, 0) + 1
            self.counters["events_validated_quarantined"] += 1

    def set_gauge(self, gauge_name: str, value: float) -> None:
        """Sets a gauge value."""
        with self._lock:
            self.gauges[gauge_name] = value

    def observe(self, histogram_name: str, value: float) -> None:
        """Records an observation in a histogram."""
        with self._lock:
            if histogram_name not in self.histograms:
                self.histograms[histogram_name] = []
            self.histograms[histogram_name].append(value)
            # Cap histogram size to prevent unbounded memory growth
            if len(self.histograms[histogram_name]) > 10000:
                self.histograms[histogram_name] = self.histograms[histogram_name][-5000:]

    def record_latency(self, histogram_name: str, value: float) -> None:
        """Alias for observe to record latency observations."""
        self.observe(histogram_name, value)

    def reset(self) -> None:
        """Resets all metrics to baseline zero."""
        with self._lock:
            for k in self.counters:
                self.counters[k] = 0
            self.rule_violations.clear()
            self.gauges.clear()
            for k in self.histograms:
                self.histograms[k].clear()

    def snapshot(self) -> Dict[str, Any]:
        """Returns a point-in-time dictionary snapshot of all collected metrics."""
        with self._lock:
            snap = {
                "counters": dict(self.counters),
                "rule_violations": dict(self.rule_violations),
                "gauges": dict(self.gauges),
                "summary": {
                    "total_events_generated": self.counters.get("events_generated", 0),
                    "total_valid_events": self.counters.get("events_validated_valid", 0),
                    "total_quarantined_events": self.counters.get("events_validated_quarantined", 0),
                    "total_iceberg_rows": self.counters.get("events_persisted_iceberg", 0),
                    "total_postgres_records": self.counters.get("events_persisted_postgres", 0),
                }
            }
            # Compute latency percentiles where observations exist
            for hist_name, vals in self.histograms.items():
                if vals:
                    sorted_vals = sorted(vals)
                    snap[f"{hist_name}_p50"] = sorted_vals[int(len(vals) * 0.50)]
                    snap[f"{hist_name}_p95"] = sorted_vals[int(len(vals) * 0.95)]
                    snap[f"{hist_name}_p99"] = sorted_vals[min(int(len(vals) * 0.99), len(vals) - 1)]
                    snap[f"{hist_name}_avg"] = sum(vals) / len(vals)
            return snap


# Global singleton instance
metrics = MetricsCollector()
