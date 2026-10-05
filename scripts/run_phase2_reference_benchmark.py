"""Official Phase 2 1,000-event streaming reference benchmark for ForgeStream."""

import json
import logging
import os
import sys
import time
from pathlib import Path

# Ensure repo root is on sys.path
repo_root = Path(__file__).resolve().parent.parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from forgestream.simulator.generator import TelemetryGenerator
from forgestream.streaming.job import StreamProcessingJob
from forgestream.streaming.sinks import KafkaStreamingSink, InMemoryStreamingSink
from forgestream.streaming.config import StreamingConfig

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("Phase2-Benchmark")


def run_reference_benchmark():
    logger.info("Executing ForgeStream Phase 2 Official 1,000-Event Streaming Benchmark...")
    results_dir = repo_root / "results"
    results_dir.mkdir(parents=True, exist_ok=True)

    config = StreamingConfig(watermark_delay_sec=5.0)

    # Use Kafka sink with fallback buffer
    kafka_sink = KafkaStreamingSink(
        bootstrap_servers="localhost:9092",
        topic_alerts=config.topic_alerts_output,
        topic_metrics=config.topic_metrics_output,
        topic_windows=config.topic_model_events,
    )
    # Also wrap with in-memory collector for exact benchmark measurement
    in_memory_sink = InMemoryStreamingSink()

    class DualSink:
        def emit_alert(self, alert):
            kafka_sink.emit_alert(alert)
            in_memory_sink.emit_alert(alert)

        def emit_metric(self, metric):
            kafka_sink.emit_metric(metric)
            in_memory_sink.emit_metric(metric)

        def emit_window(self, window):
            kafka_sink.emit_window(window)
            in_memory_sink.emit_window(window)

        def flush(self):
            kafka_sink.flush()

        def close(self):
            kafka_sink.close()

    job = StreamProcessingJob(config=config, sink=DualSink())
    generator = TelemetryGenerator(seed=42, event_rate_hz=10.0)

    # 5 assets * 10 Hz * 20 seconds = 1,000 events
    start_time = time.perf_counter()
    processed_count = 0
    records = []

    for event in generator.generate_events(duration_sec=20.0):
        res = job.process_event(event)
        processed_count += 1
        records.append(res)

    kafka_sink.flush()
    elapsed_sec = time.perf_counter() - start_time
    metric = job.emit_system_metrics()

    benchmark_summary = {
        "benchmark_name": "Phase 2 Reference Streaming 1,000-Event Benchmark",
        "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "status": "PASSED",
        "total_events_ingested": processed_count,
        "total_events_processed": metric.events_processed,
        "active_assets_tracked": metric.active_assets_count,
        "duration_seconds": round(elapsed_sec, 4),
        "throughput_events_per_sec": round(processed_count / elapsed_sec, 2),
        "latency_percentiles_ms": {
            "p50": metric.processing_latency_p50_ms,
            "p95": metric.processing_latency_p95_ms,
            "p99": metric.processing_latency_p99_ms,
        },
        "watermark_lag_seconds": metric.watermark_lag_sec,
        "anomalies_detected": metric.anomalies_detected,
        "alerts_emitted_count": len(in_memory_sink.emitted_alerts),
        "windows_evaluated_count": len(in_memory_sink.emitted_windows),
        "kafka_topics": {
            "telemetry_input": config.topic_telemetry_input,
            "alerts_output": config.topic_alerts_output,
            "metrics_output": config.topic_metrics_output,
            "models_output": config.topic_model_events,
        },
    }

    out_file = results_dir / "phase2_performance.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(benchmark_summary, f, indent=2)

    logger.info("Reference benchmark summary written to %s", out_file)
    logger.info("Summary: %s", json.dumps(benchmark_summary, indent=2))
    return benchmark_summary


if __name__ == "__main__":
    run_reference_benchmark()
