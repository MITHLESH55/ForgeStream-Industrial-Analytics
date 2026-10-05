"""CLI and Runner for ForgeStream Phase 2 Stream Processing.

Supports executing either:
- Real Apache Flink / PyFlink distributed stream processing engine (--engine flink)
- Local deterministic reference stream engine (--engine reference)
"""

import argparse
import json
import logging
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

# Ensure repo root is on sys.path
repo_root = Path(__file__).resolve().parent.parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from forgestream.simulator.generator import TelemetryGenerator
from forgestream.streaming.job import StreamProcessingJob
from forgestream.streaming.sinks import KafkaStreamingSink, InMemoryStreamingSink
from forgestream.streaming.config import StreamingConfig

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("ForgeStream-Phase2")


def run_flink_engine():
    """Execute PyFlink streaming job inside the Apache Flink cluster container."""
    logger.info("Executing Real Apache Flink / PyFlink streaming verification suite...")
    cmd = [
        "docker",
        "exec",
        "forgestream-jobmanager",
        "python3",
        "/opt/forgestream/scripts/verify_real_flink_pipeline.py",
    ]
    env = os.environ.copy()
    env["MSYS_NO_PATHCONV"] = "1"
    res = subprocess.run(cmd, env=env)
    if res.returncode != 0:
        logger.error("Flink execution exited with error code %d", res.returncode)
        sys.exit(res.returncode)
    logger.info("Apache Flink execution completed successfully.")


def run_reference_streaming(
    duration_sec: float = 30.0,
    event_rate_hz: float = 10.0,
    use_live_kafka: bool = True,
    bootstrap_servers: str = "localhost:9092",
):
    """Run local deterministic reference engine."""
    logger.info("Initializing ForgeStream Phase 2 Local Reference Engine...")
    config = StreamingConfig()

    if use_live_kafka:
        sink = KafkaStreamingSink(
            bootstrap_servers=bootstrap_servers,
            topic_alerts=config.topic_alerts_output,
            topic_metrics=config.topic_metrics_output,
            topic_windows=config.topic_model_events,
        )
    else:
        sink = InMemoryStreamingSink()

    job = StreamProcessingJob(config=config, sink=sink)
    generator = TelemetryGenerator(seed=42, event_rate_hz=event_rate_hz)

    logger.info(
        "Starting reference stream processing: duration=%.1fs, rate=%.1f Hz per asset (5 assets)",
        duration_sec,
        event_rate_hz,
    )

    start_wall = time.time()
    count = 0
    last_metric_time = time.time()

    for event in generator.generate_events(duration_sec=duration_sec):
        res = job.process_event(event)
        count += 1

        if res.get("alert_emitted"):
            alert = res["alert_emitted"]
            logger.warning(
                "🚨 [ALERT] Asset: %s | State: %s -> %s | Score: %.2f | Anomaly: %s",
                alert["asset_id"],
                alert.get("previous_health_state") or "HEALTHY",
                alert["current_health_state"],
                alert["health_score"],
                alert["anomaly_types"],
            )

        if time.time() - last_metric_time >= 5.0:
            metric = job.emit_system_metrics()
            logger.info(
                "📊 [METRICS] Processed: %d | Latency p95: %.2fms | Throughput: %.1f ev/s | Active: %d",
                metric.events_processed,
                metric.processing_latency_p95_ms,
                metric.throughput_events_per_sec,
                metric.active_assets_count,
            )
            last_metric_time = time.time()

    sink.flush()
    final_metric = job.emit_system_metrics()
    elapsed = time.time() - start_wall

    logger.info(
        "✅ Reference Stream Processing Completed: %d events processed in %.2fs (%.1f ev/s)",
        count,
        elapsed,
        count / elapsed if elapsed > 0 else 0,
    )
    logger.info(
        "Summary: Anomalies Detected: %d | Alerts Emitted: %d | p95 Latency: %.2fms",
        final_metric.anomalies_detected,
        final_metric.alerts_emitted,
        final_metric.processing_latency_p95_ms,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ForgeStream Phase 2 Stream Runner")
    parser.add_argument(
        "--engine",
        choices=["flink", "reference"],
        default="flink",
        help="Engine runtime: 'flink' for Real Apache Flink, 'reference' for deterministic local reference engine",
    )
    parser.add_argument("--duration", type=float, default=20.0, help="Stream duration in seconds")
    parser.add_argument("--rate", type=float, default=10.0, help="Event rate in Hz per asset")
    parser.add_argument("--no-kafka", action="store_true", help="Run with in-memory sink instead of Kafka")
    parser.add_argument("--bootstrap-servers", default="localhost:9092", help="Kafka bootstrap servers")
    args = parser.parse_args()

    if args.engine == "flink":
        run_flink_engine()
    else:
        run_reference_streaming(
            duration_sec=args.duration,
            event_rate_hz=args.rate,
            use_live_kafka=not args.no_kafka,
            bootstrap_servers=args.bootstrap_servers,
        )
