"""
Pipeline Orchestration Runner.
Coordinates deterministic telemetry generation, Kafka ingestion bus streaming,
data quality evaluation, Iceberg lakehouse commits, and PostgreSQL metadata registration.
"""

from datetime import datetime, timezone
import time
import uuid
from typing import Any, Dict, List, Optional
from forgestream.config import settings
from forgestream.iceberg.catalog import IcebergCatalogManager
from forgestream.iceberg.reader import IcebergHistoricalReader
from forgestream.iceberg.tables import IcebergTableManager
from forgestream.iceberg.writer import IcebergHistoricalWriter
from forgestream.kafka.consumer import TelemetryConsumer
from forgestream.kafka.producer import ResilientKafkaProducer
from forgestream.kafka.topics import KafkaTopicManager
from forgestream.observability.logging import get_logger
from forgestream.observability.metrics import metrics
from forgestream.pipeline.ingestion_worker import IngestionWorker
from forgestream.postgres.connection import DatabaseManager
from forgestream.postgres.repository import PostgresRepository
from forgestream.schemas.telemetry_schema import AssetType, ScenarioID
from forgestream.simulator.asset_models import get_all_default_profiles
from forgestream.simulator.generator import TelemetryGenerator
from forgestream.validation.validator import DataQualityValidator

logger = get_logger("pipeline.runner")


class PipelineRunner:
    """End-to-end execution runner for ForgeStream Phase 1 streaming pipeline."""

    def __init__(
        self,
        db_manager: Optional[DatabaseManager] = None,
        catalog_manager: Optional[IcebergCatalogManager] = None,
        force_fallback: bool = False,
    ):
        self.db_mgr = db_manager or DatabaseManager()
        self.postgres_repo = PostgresRepository(db_manager=self.db_mgr)

        self.catalog_mgr = catalog_manager or IcebergCatalogManager()
        self.table_mgr = IcebergTableManager(catalog_manager=self.catalog_mgr)
        self.iceberg_table = self.table_mgr.get_or_create_telemetry_table()
        self.iceberg_writer = IcebergHistoricalWriter(table_manager=self.table_mgr, table=self.iceberg_table)
        self.iceberg_reader = IcebergHistoricalReader(table_manager=self.table_mgr, table=self.iceberg_table)

        self.force_fallback = force_fallback
        self.topic_mgr = KafkaTopicManager(force_fallback=force_fallback)
        self.producer = ResilientKafkaProducer(force_fallback=force_fallback)
        self.consumer_group_id = f"forgestream-runner-group-{uuid.uuid4().hex[:6]}"
        from forgestream.kafka.config import KafkaClientConfig
        consumer_cfg = KafkaClientConfig(auto_offset_reset="latest")
        self.consumer = TelemetryConsumer(config=consumer_cfg, group_id=self.consumer_group_id, force_fallback=force_fallback)
        if self.consumer.is_live:
            self.consumer.subscribe([settings.kafka.topic_telemetry])

        self.validator = DataQualityValidator()
        self.worker = IngestionWorker(
            consumer=self.consumer,
            iceberg_writer=self.iceberg_writer,
            postgres_repo=self.postgres_repo,
            validator=self.validator,
            dlq_producer=self.producer,
        )

        self._init_asset_registry()

    def _init_asset_registry(self) -> None:
        """Registers the 5 default industrial assets into PostgreSQL operational metadata."""
        profiles = get_all_default_profiles()
        for p in profiles:
            self.postgres_repo.upsert_asset(
                asset_id=p.asset_id,
                asset_type=p.asset_type.value,
                model_name=p.model_name,
                rated_rpm=p.rated_rpm,
                rated_load=p.rated_load,
                rated_voltage=p.rated_voltage,
                rated_current=p.rated_current,
                baseline_temperature=p.baseline_temperature,
                baseline_vibration=p.baseline_vibration,
                baseline_pressure=p.baseline_pressure,
                criticality=p.criticality,
            )
        logger.info(f"Initialized {len(profiles)} assets in operational registry")

    def run_simulation(
        self,
        duration_sec: int = 10,
        seed: int = 42,
        sampling_interval_sec: float = 1.0,
        active_scenarios: Optional[Dict[str, ScenarioID]] = None,
        run_id: Optional[str] = None,
        batch_size: int = 50,
    ) -> Dict[str, Any]:
        """
        Executes a complete deterministic streaming simulation run.
        """
        run_id = run_id or f"RUN-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:6]}"
        start_wall_time = time.perf_counter()

        # Provision Kafka topics
        self.topic_mgr.provision_topics()

        # Register run start in Postgres
        asset_count = len(get_all_default_profiles())
        self.postgres_repo.start_ingestion_run(
            run_id=run_id,
            seed=seed,
            asset_count=asset_count,
            duration_sec=duration_sec,
        )

        # Initialize simulator generator
        generator = TelemetryGenerator(
            seed=seed,
            sampling_interval_sec=sampling_interval_sec,
            active_scenarios=active_scenarios,
        )

        # Warm up partition assignment for live consumer
        if self.consumer.is_live:
            for _ in range(5):
                self.consumer.consume_batch(max_messages=10, timeout_sec=0.1)

        # Start generating and publishing to Kafka
        total_generated = 0
        gen_stream = generator.generate_stream(duration_sec=duration_sec)

        event_buffer: List[Dict[str, Any]] = []

        for event in gen_stream:
            payload = event.model_dump(mode="json")
            total_generated += 1

            # Produce to Kafka telemetry topic
            self.producer.send(
                topic=settings.kafka.topic_telemetry,
                payload=payload,
                key=event.asset_id,
            )

        self.producer.flush(timeout_sec=10.0)

        # Ingestion Worker drains and validates messages
        total_valid = 0
        total_quarantined = 0

        # Drain all produced messages
        fallback_queue = self.producer.get_fallback_queue() if not self.producer.is_live else None

        empty_polls = 0
        max_empty_polls = 10 if self.producer.is_live else 1

        while (total_valid + total_quarantined) < total_generated:
            raw_batch = self.consumer.consume_batch(
                max_messages=batch_size,
                timeout_sec=1.0 if self.producer.is_live else 0.1,
                fallback_source=fallback_queue,
            )
            if not raw_batch:
                empty_polls += 1
                if empty_polls >= max_empty_polls:
                    break
                time.sleep(0.3)
                continue
            empty_polls = 0
            v_cnt, q_cnt = self.worker.process_batch(raw_batch, run_id=run_id)
            total_valid += v_cnt
            total_quarantined += q_cnt

        elapsed_sec = time.perf_counter() - start_wall_time
        events_per_sec = total_generated / elapsed_sec if elapsed_sec > 0 else 0.0

        # Finalize Postgres metadata
        self.postgres_repo.complete_ingestion_run(
            run_id=run_id,
            events_generated=total_generated,
            events_persisted_iceberg=total_valid,
            events_quarantined=total_quarantined,
            status="COMPLETED",
        )

        # Lakehouse inspection metrics
        iceberg_total_rows = self.iceberg_reader.get_total_row_count()
        asset_dist = self.iceberg_reader.get_asset_distribution()
        snapshots = self.iceberg_reader.get_snapshots_summary()

        report = {
            "run_id": run_id,
            "seed": seed,
            "duration_sec": duration_sec,
            "elapsed_wall_sec": round(elapsed_sec, 4),
            "throughput_events_per_sec": round(events_per_sec, 2),
            "events_generated": total_generated,
            "events_persisted_iceberg": total_valid,
            "events_quarantined": total_quarantined,
            "iceberg_total_table_rows": iceberg_total_rows,
            "iceberg_snapshots_count": len(snapshots),
            "asset_distribution": asset_dist,
            "postgres_is_live": self.db_mgr.is_postgres,
            "kafka_is_live": self.producer.is_live,
            "completed_at": datetime.now(timezone.utc).isoformat(),
        }

        logger.info(f"Pipeline run {run_id} completed successfully: {report}")
        return report
