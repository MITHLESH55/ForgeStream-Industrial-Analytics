"""Integration tests for end-to-end streaming execution and Kafka sinks."""

import time
from forgestream.schemas.telemetry_schema import TelemetryEvent, AssetType, OperatingMode
from forgestream.streaming.job import StreamProcessingJob
from forgestream.streaming.sinks import InMemoryStreamingSink
from forgestream.streaming.config import StreamingConfig


def test_streaming_job_full_pipeline_multi_asset():
    """Verify StreamProcessingJob handles multi-asset streams, updates state, and emits alerts/metrics."""
    sink = InMemoryStreamingSink()
    config = StreamingConfig(watermark_delay_sec=2.0)
    job = StreamProcessingJob(config=config, sink=sink)

    assets = [
        ("MOTOR-001", AssetType.MOTOR),
        ("PUMP-001", AssetType.PUMP),
        ("COMPRESSOR-001", AssetType.COMPRESSOR),
        ("CONVEYOR-001", AssetType.CONVEYOR),
        ("TURBINE-001", AssetType.TURBINE),
    ]

    # Ingest 100 events across all 5 assets (20 events per asset)
    base_ts = 1770000000.0
    for step in range(20):
        for idx, (asset_id, asset_type) in enumerate(assets):
            ts = base_ts + (step * 1.0) + (idx * 0.1)

            # Inject degradation on PUMP-001 at step >= 10
            is_fault = (asset_id == "PUMP-001" and step >= 10)
            vib = 6.5 if is_fault else 1.5
            temp = 85.0 if is_fault else 60.0
            press = 2.5 if is_fault else 5.0

            event = TelemetryEvent(
                event_id=f"evt-{asset_id}-{step:04d}",
                asset_id=asset_id,
                asset_type=asset_type,
                timestamp=ts,
                event_time="2026-10-05T12:00:00Z",
                operating_mode=OperatingMode.NORMAL,
                temperature=temp,
                vibration=vib,
                pressure=press,
                current=50.0,
                voltage=400.0,
                power=25.0,
                rpm=1800.0,
                load=80.0,
                sequence_number=step,
            )

            res = job.process_event(event)
            assert res["status"] == "PROCESSED"

    # Emit final metrics
    metric = job.emit_system_metrics()
    assert metric.events_processed == 100
    assert metric.active_assets_count == 5
    assert len(sink.emitted_alerts) >= 1

    # Verify PUMP-001 triggered an alert
    pump_alerts = [a for a in sink.emitted_alerts if a.asset_id == "PUMP-001"]
    assert len(pump_alerts) >= 1
    assert pump_alerts[0].current_health_state.value in ["WATCH", "DEGRADED", "CRITICAL"]
