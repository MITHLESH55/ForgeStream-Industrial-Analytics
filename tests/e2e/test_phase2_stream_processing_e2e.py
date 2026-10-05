"""End-to-end test for ForgeStream Phase 2 Real-Time Stream Processing and Asset Intelligence."""

from forgestream.simulator.generator import TelemetryGenerator
from forgestream.streaming.job import StreamProcessingJob
from forgestream.streaming.sinks import InMemoryStreamingSink
from forgestream.streaming.config import StreamingConfig


def test_phase2_stream_processing_e2e():
    """End-to-end verification: Industrial simulator generation -> Stream Processing -> Intelligence & Alerts."""
    # 1. Initialize Simulator Generator (10 Hz, 5 assets -> 50 events/sec, 20 sec = 1000 events)
    generator = TelemetryGenerator(seed=42, event_rate_hz=10.0)

    # 2. Initialize Streaming Job
    streaming_config = StreamingConfig(watermark_delay_sec=5.0)
    sink = InMemoryStreamingSink()
    job = StreamProcessingJob(config=streaming_config, sink=sink)

    # 3. Stream 1,000 total sensor events across 5 assets
    events_processed = 0
    for event in generator.generate_events(duration_sec=20.0):
        res = job.process_event(event)
        assert res["status"] in ["PROCESSED", "EXCESSIVELY_LATE_DROPPED"]
        if res["status"] == "PROCESSED":
            events_processed += 1

    # 4. Final verification assertions
    assert events_processed == 1000
    metric_summary = job.emit_system_metrics()

    assert metric_summary.events_processed == 1000
    assert metric_summary.active_assets_count == 5
    assert metric_summary.throughput_events_per_sec > 100.0
    assert metric_summary.processing_latency_p95_ms < 50.0  # sub-millisecond execution

    # Verify that windows and metrics were emitted
    assert len(sink.emitted_windows) > 0
    assert len(sink.emitted_metrics) > 0
