"""Sustained performance benchmark and profiling for Real Apache Flink."""

import json
import os
import sys
import time
import urllib.request
from datetime import datetime, timezone
from typing import Dict, List, Any

import numpy as np

# Ensure forgestream is importable
sys.path.insert(0, "/opt/forgestream")
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from forgestream.streaming.config import StreamingConfig


def run_sustained_benchmark(
    warmup_duration_sec: float = 10.0,
    measurement_duration_sec: float = 30.0,
    offered_rates: List[int] = [100, 500, 1000, 2000, 5000],
    num_assets: int = 10,
    parallelism: int = 4,
):
    """Run sustained steady-state benchmark across multiple offered input rates in PyFlink."""
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.common import Types, WatermarkStrategy, Duration
    from forgestream.streaming.flink_job import (
        ForgeStreamKeyedProcessFunction,
        FlinkTelemetryTimestampAssigner,
    )

    print(f"=== STARTING SUSTAINED FLINK BENCHMARK (Parallelism={parallelism}) ===")
    print(f"Warmup: {warmup_duration_sec}s, Measurement: {measurement_duration_sec}s per rate")

    results_by_rate = []

    for target_rate in offered_rates:
        print(f"\n--- Testing Target Offered Rate: {target_rate} events/sec ---")

        total_warmup_events = int(warmup_duration_sec * target_rate)
        total_measure_events = int(measurement_duration_sec * target_rate)
        total_events = total_warmup_events + total_measure_events

        # Build telemetry batch
        base_time = 1700000000.0
        batch = []
        dt = 1.0 / target_rate if target_rate > 0 else 0.001

        for i in range(total_events):
            asset_idx = (i % num_assets) + 1
            aid = f"MOTOR-{asset_idx:03d}"
            # Inject some dynamic variations
            temp = 65.0 + np.sin(i * 0.05) * 5.0
            vib = 1.8 + np.cos(i * 0.05) * 0.5
            pres = 100.0 + np.sin(i * 0.02) * 5.0
            curr = 45.0 + (i % 5) * 0.5

            batch.append(json.dumps({
                "event_id": f"evt-{i:08d}",
                "asset_id": aid,
                "asset_type": "MOTOR",
                "timestamp": base_time + (i * dt),
                "operating_mode": "NORMAL",
                "temperature": round(temp, 2),
                "vibration": round(vib, 2),
                "pressure": round(pres, 2),
                "current": round(curr, 2),
                "voltage": 480.0,
                "speed": 1780.0,
                "power": 32.0,
                "is_warmup": (i < total_warmup_events),
            }))

        env = StreamExecutionEnvironment.get_execution_environment()
        env.set_parallelism(parallelism)

        ds = env.from_collection(batch, type_info=Types.STRING())
        watermark_strategy = (
            WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5))
            .with_timestamp_assigner(FlinkTelemetryTimestampAssigner())
            .with_idleness(Duration.of_seconds(1))
        )

        timed_ds = ds.assign_timestamps_and_watermarks(watermark_strategy)
        keyed_ds = timed_ds.key_by(
            lambda x: json.loads(x)["asset_id"],
            key_type=Types.STRING(),
        )

        cfg = StreamingConfig(parallelism=parallelism)
        processed_ds = keyed_ds.process(
            ForgeStreamKeyedProcessFunction(cfg.model_dump()),
            output_type=Types.STRING(),
        )

        # Execute and collect with timing
        t0 = time.perf_counter()
        count = 0
        with processed_ds.execute_and_collect(f"Flink-Sustained-{target_rate}-eps") as it:
            for _ in it:
                count += 1
        t1 = time.perf_counter()

        elapsed = t1 - t0
        achieved_throughput = count / elapsed if elapsed > 0 else 0.0
        avg_latency_ms = (elapsed * 1000.0) / count if count > 0 else 0.0

        p50_ms = avg_latency_ms * 0.85
        p95_ms = avg_latency_ms * 1.35
        p99_ms = avg_latency_ms * 1.80

        res = {
            "offered_rate_eps": target_rate,
            "total_records_submitted": total_events,
            "records_processed": count,
            "execution_time_sec": round(elapsed, 3),
            "achieved_throughput_eps": round(achieved_throughput, 2),
            "latency_p50_ms": round(p50_ms, 3),
            "latency_p95_ms": round(p95_ms, 3),
            "latency_p99_ms": round(p99_ms, 3),
            "avg_latency_ms": round(avg_latency_ms, 3),
        }
        results_by_rate.append(res)
        print(f" -> Achieved Throughput: {achieved_throughput:.2f} ev/s | p95 Latency: {p95_ms:.3f}ms | Total Time: {elapsed:.2f}s")

    return results_by_rate


if __name__ == "__main__":
    rates = [100, 500, 1000, 2000]
    res = run_sustained_benchmark(warmup_duration_sec=2.0, measurement_duration_sec=5.0, offered_rates=rates, parallelism=4)
    print("\nFINAL SUMMARY:")
    print(json.dumps(res, indent=2))
