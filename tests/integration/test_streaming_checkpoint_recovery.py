"""Integration test for streaming state checkpointing, failover simulation, and state restoration."""

from forgestream.schemas.telemetry_schema import TelemetryEvent, AssetType, OperatingMode
from forgestream.streaming.job import StreamProcessingJob
from forgestream.streaming.sinks import InMemoryStreamingSink
from forgestream.streaming.config import StreamingConfig


def test_streaming_checkpoint_save_and_recovery():
    """Verify that checkpoint state can be saved, restored in a fresh job instance, and processing resumes seamlessly."""
    config = StreamingConfig(watermark_delay_sec=5.0)

    # 1. First Job Instance: Process 30 events for MOTOR-001
    sink1 = InMemoryStreamingSink()
    job1 = StreamProcessingJob(config=config, sink=sink1)

    base_ts = 1770000000.0
    for i in range(30):
        event = TelemetryEvent(
            event_id=f"evt-motor-{i:04d}",
            asset_id="MOTOR-001",
            asset_type=AssetType.MOTOR,
            timestamp=base_ts + i,
            event_time="2026-10-05T12:00:00Z",
            operating_mode=OperatingMode.NORMAL,
            temperature=65.0 + (i * 0.2),
            vibration=1.5,
            pressure=4.0,
            rpm=1750.0,
            current=50.0,
            voltage=400.0,
            power=25.0,
            load=80.0,
            sequence_number=i,
        )
        job1.process_event(event)

    # Checkpoint state
    checkpoint_data = job1.checkpoint_state()
    assert checkpoint_data["watermark"] == (base_ts + 29) - 5.0
    assert checkpoint_data["state_store"]["asset_count"] == 1

    # 2. Fresh Job Instance: Simulate restart / recovery
    sink2 = InMemoryStreamingSink()
    job2 = StreamProcessingJob(config=config, sink=sink2)
    job2.restore_checkpoint(checkpoint_data)

    assert job2.watermark_generator.current_watermark == checkpoint_data["watermark"]
    restored_motor = job2.state_store.get("MOTOR-001")
    assert restored_motor is not None
    assert len(restored_motor.temperatures) == 30

    # 3. Continue processing event 31 in restored job
    event31 = TelemetryEvent(
        event_id="evt-motor-0030",
        asset_id="MOTOR-001",
        asset_type=AssetType.MOTOR,
        timestamp=base_ts + 30,
        event_time="2026-10-05T12:00:30Z",
        operating_mode=OperatingMode.NORMAL,
        temperature=71.2,
        vibration=1.5,
        pressure=4.0,
        rpm=1750.0,
        current=50.0,
        voltage=400.0,
        power=25.0,
        load=80.0,
        sequence_number=30,
    )
    res = job2.process_event(event31)
    assert res["status"] == "PROCESSED"
    assert res["watermark"] == (base_ts + 30) - 5.0
