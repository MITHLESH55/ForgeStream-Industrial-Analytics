"""
Integration and component tests for Kafka streaming layer.
"""

import pytest
from forgestream.kafka.config import KafkaClientConfig
from forgestream.kafka.topics import TOPIC_DEFINITIONS
from forgestream.kafka.producer import ResilientKafkaProducer
from forgestream.kafka.consumer import TelemetryConsumer
from forgestream.schemas.telemetry_schema import TelemetryEvent, AssetType, OperatingMode
from datetime import datetime, timezone


def test_topic_definitions_integrity():
    """Verifies that all required Phase 1 topics are configured."""
    topic_names = {t.name for t in TOPIC_DEFINITIONS}
    expected = {
        "industrial-telemetry",
        "maintenance-events",
        "asset-alerts",
        "model-events",
        "system-metrics",
    }
    assert expected.issubset(topic_names)


def test_producer_and_consumer_fallback_flow():
    """Verifies end-to-end event produce and consume lifecycle in memory-buffered mode."""
    producer = ResilientKafkaProducer(force_fallback=True)
    consumer = TelemetryConsumer()

    event = TelemetryEvent(
        event_id="evt-motor-00000099",
        asset_id="MOTOR-001",
        asset_type=AssetType.MOTOR,
        timestamp=1775347200.0,
        event_time=datetime(2026, 10, 5, 12, 0, 0, tzinfo=timezone.utc),
        temperature=70.0,
        vibration=1.2,
        pressure=1.0,
        rpm=1750.0,
        current=49.0,
        voltage=400.0,
        power=27.5,
        load=80.0,
        operating_mode=OperatingMode.NORMAL,
        sequence_number=99,
    )

    # Produce to telemetry topic
    success = producer.produce_telemetry(event, topic="industrial-telemetry")
    assert success is True

    # Retrieve from queue / consumer
    queue = producer.get_fallback_queue()
    assert len(queue) == 1

    consumed = consumer.consume_batch(max_messages=10, fallback_source=queue)
    assert len(consumed) == 1
    assert consumed[0]["event_id"] == "evt-motor-00000099"
    assert consumed[0]["_kafka_metadata"]["topic"] == "industrial-telemetry"
    assert consumed[0]["_kafka_metadata"]["key"] == "MOTOR-001"


def test_live_kafka_broker_produce_consume_and_partitioning():
    """
    Verifies live Kafka broker connectivity, topic partitions, key routing,
    and message consumption with real offset advancement.
    """
    import time
    from forgestream.kafka.topics import TopicManager
    from forgestream.config import settings

    producer = ResilientKafkaProducer(force_fallback=False)
    if not producer.is_live:
        pytest.skip("Live Kafka broker offline; tested fallback mode.")

    # 1. Verify Topic Partitions
    tm = TopicManager()
    metadata = tm.list_topic_metadata()
    assert settings.kafka.topic_telemetry in metadata
    assert metadata[settings.kafka.topic_telemetry]["partitions"] == 5
    assert metadata[settings.kafka.topic_maintenance]["partitions"] == 3
    assert metadata[settings.kafka.topic_alerts]["partitions"] == 3

    # 2. Subscribe consumer first with latest offset to capture real-time live events
    group_id = f"test-group-{int(time.time())}"
    client_cfg = KafkaClientConfig(auto_offset_reset="latest")
    consumer = TelemetryConsumer(config=client_cfg, group_id=group_id, force_fallback=False)
    assert consumer.is_live is True
    consumer.subscribe([settings.kafka.topic_telemetry])

    # Allow group coordinator partition assignment
    for _ in range(5):
        consumer.consume_batch(max_messages=10, timeout_sec=0.2)

    # 3. Produce live messages across distinct assets to verify partition keying
    assets = ["MOTOR-001", "PUMP-001", "COMPRESSOR-001", "CONVEYOR-001", "TURBINE-001"]
    test_run_tag = f"LIVE-TEST-{int(time.time())}"

    for i, asset_id in enumerate(assets, 1):
        evt = {
            "event_id": f"{test_run_tag}-{i:03d}",
            "asset_id": asset_id,
            "asset_type": asset_id.split("-")[0],
            "timestamp": 1710000000.0 + i,
            "temperature": 55.0 + i,
            "vibration": 1.0 + (i * 0.1),
            "pressure": 5.0,
            "rpm": 1750.0,
            "current": 25.0,
            "voltage": 400.0,
            "power": 15.0,
            "load": 100.0,
            "operating_mode": "NORMAL",
            "sequence_number": i,
        }
        producer.produce_telemetry(evt)

    producer.flush(timeout_sec=5.0)

    # 4. Consume from live topic and verify keying & metadata
    consumed_test_events = []
    for _ in range(15):
        batch = consumer.consume_batch(max_messages=10, timeout_sec=0.5)
        for msg in batch:
            if msg.get("event_id", "").startswith(test_run_tag):
                consumed_test_events.append(msg)
        if len(consumed_test_events) >= 5:
            break
        time.sleep(0.1)

    consumer.close()

    assert len(consumed_test_events) == 5
    for msg in consumed_test_events:
        meta = msg.get("_kafka_metadata", {})
        assert meta.get("topic") == settings.kafka.topic_telemetry
        assert meta.get("key") in assets
        assert meta.get("partition") in [0, 1, 2, 3, 4]
        assert meta.get("offset") >= 0
