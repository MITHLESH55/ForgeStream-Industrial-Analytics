"""Measure real Kafka broker and Kafka -> PyFlink -> Kafka throughput."""

import json
import time
import uuid
from typing import Dict, Any
from confluent_kafka import Producer, Consumer, TopicPartition, OFFSET_BEGINNING
from confluent_kafka.admin import AdminClient, NewTopic


def measure_kafka_raw_throughput(
    bootstrap_servers: str = "kafka:29092",
    num_records: int = 10000,
    topic: str = "forgestream.benchmark.raw",
) -> Dict[str, Any]:
    """Measure raw Kafka producer and consumer throughput independently."""
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    # Create test topic if not exists
    try:
        new_topics = [NewTopic(topic, num_partitions=4, replication_factor=1)]
        futures = admin.create_topics(new_topics)
        for t, f in futures.items():
            f.result(timeout=5)
    except Exception:
        pass

    # 1. Producer Benchmark
    producer = Producer({
        "bootstrap.servers": bootstrap_servers,
        "queue.buffering.max.messages": 100000,
        "linger.ms": 5,
        "batch.num.messages": 1000,
    })

    base_time = time.time()
    t0 = time.perf_counter()
    for i in range(num_records):
        aid = f"MOTOR-{(i % 10) + 1:03d}"
        payload = json.dumps({
            "event_id": f"evt-{i:06d}",
            "asset_id": aid,
            "timestamp": base_time + (i * 0.001),
            "temperature": 65.0,
            "vibration": 1.5,
            "pressure": 100.0,
            "current": 45.0,
        })
        producer.produce(topic, key=aid.encode("utf-8"), value=payload.encode("utf-8"))
        if i % 2000 == 0:
            producer.poll(0)

    producer.flush(timeout=15)
    t1 = time.perf_counter()

    prod_elapsed = t1 - t0
    prod_tps = num_records / prod_elapsed if prod_elapsed > 0 else 0.0

    # 2. Consumer Benchmark
    group_id = f"bench-consumer-{uuid.uuid4().hex[:6]}"
    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "fetch.min.bytes": 1024,
    })
    consumer.subscribe([topic])

    consumed_count = 0
    t0_cons = time.perf_counter()
    while consumed_count < num_records:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            if time.perf_counter() - t0_cons > 10.0:
                break
            continue
        if msg.error():
            continue
        consumed_count += 1
    t1_cons = time.perf_counter()
    consumer.close()

    cons_elapsed = t1_cons - t0_cons
    cons_tps = consumed_count / cons_elapsed if cons_elapsed > 0 else 0.0

    return {
        "num_records": num_records,
        "producer_throughput_eps": round(prod_tps, 2),
        "producer_time_sec": round(prod_elapsed, 3),
        "consumer_throughput_eps": round(cons_tps, 2),
        "consumer_time_sec": round(cons_elapsed, 3),
        "consumed_records": consumed_count,
    }


if __name__ == "__main__":
    res = measure_kafka_raw_throughput(num_records=10000)
    print("KAFKA RAW THROUGHPUT BENCHMARK:")
    print(json.dumps(res, indent=2))
