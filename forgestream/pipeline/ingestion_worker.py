"""
Ingestion Worker Module.
Consumes telemetry events from Kafka, applies 13-rule data quality validation,
persists valid records to Apache Iceberg, routes defective payloads to PostgreSQL quarantine,
and logs operational audit metadata.
"""

from datetime import datetime, timezone
import time
from typing import Any, Dict, List, Optional, Tuple
from forgestream.config import settings
from forgestream.iceberg.writer import IcebergHistoricalWriter
from forgestream.kafka.consumer import TelemetryConsumer
from forgestream.kafka.producer import ResilientKafkaProducer
from forgestream.observability.logging import get_logger
from forgestream.observability.metrics import metrics
from forgestream.postgres.repository import PostgresRepository
from forgestream.validation.quarantine import QuarantineManager, QuarantinedRecord
from forgestream.validation.validator import DataQualityValidator

logger = get_logger("pipeline.worker")


class IngestionWorker:
    """
    Core streaming ingestion worker processing events across the Kafka -> Validator -> (Iceberg | Postgres Quarantine) pipeline.
    """

    def __init__(
        self,
        consumer: Optional[TelemetryConsumer] = None,
        iceberg_writer: Optional[IcebergHistoricalWriter] = None,
        postgres_repo: Optional[PostgresRepository] = None,
        validator: Optional[DataQualityValidator] = None,
        dlq_producer: Optional[ResilientKafkaProducer] = None,
    ):
        self.consumer = consumer or TelemetryConsumer()
        self.iceberg_writer = iceberg_writer or IcebergHistoricalWriter()
        self.postgres_repo = postgres_repo or PostgresRepository()
        self.validator = validator or DataQualityValidator()
        self.dlq_producer = dlq_producer

    def process_batch(
        self,
        raw_events: List[Dict[str, Any]],
        run_id: Optional[str] = None,
    ) -> Tuple[int, int]:
        """
        Processes a batch of raw telemetry events:
        1. Validates each record against 13 DQ rules.
        2. Valid records -> Iceberg append batch.
        3. Quarantined records -> Postgres quarantine table + DQ event logs + optional Kafka DLQ.
        Returns: (valid_count, quarantined_count)
        """
        if not raw_events:
            return 0, 0

        valid_events: List[Dict[str, Any]] = []
        quarantined_records: List[QuarantinedRecord] = []

        for raw in raw_events:
            val_result = self.validator.validate(raw)

            if val_result.is_valid:
                # Add ingestion timestamp
                if "ingestion_time" not in raw or raw["ingestion_time"] is None:
                    raw["ingestion_time"] = datetime.now(timezone.utc).isoformat()
                valid_events.append(raw)
            else:
                # Package quarantined defective event
                q_rec = QuarantineManager.create_quarantine_record(raw, val_result)
                quarantined_records.append(q_rec)

                # Persist to PostgreSQL quarantine store
                try:
                    self.postgres_repo.record_quarantine(q_rec, run_id=run_id)
                    for rule in val_result.violated_rules:
                        self.postgres_repo.record_dq_event(
                            rule_code=rule.value,
                            severity="ERROR",
                            run_id=run_id,
                            event_id=q_rec.event_id,
                            asset_id=q_rec.asset_id,
                            details="; ".join(val_result.error_messages),
                        )
                except Exception as e:
                    logger.error(f"Failed to persist quarantine record in Postgres: {e}")

                # Forward to DLQ Kafka topic if available
                if self.dlq_producer:
                    try:
                        self.dlq_producer.send(
                            topic=settings.kafka.topic_quarantine,
                            payload=q_rec.model_dump(mode="json"),
                            key=q_rec.asset_id or "UNKNOWN",
                        )
                    except Exception as e:
                        logger.warning(f"Failed to forward quarantine to DLQ: {e}")

        if self.dlq_producer and quarantined_records:
            self.dlq_producer.flush(timeout_sec=2.0)

        # Persist valid events to Apache Iceberg lakehouse
        if valid_events:
            try:
                self.iceberg_writer.write_events(valid_events)
            except Exception as e:
                logger.error(f"Failed to write valid batch to Iceberg: {e}")
                raise

        logger.info(
            f"Batch processed (run_id={run_id}): {len(valid_events)} valid persisted to Iceberg, {len(quarantined_records)} quarantined"
        )
        return len(valid_events), len(quarantined_records)

    def drain_and_process(
        self,
        max_messages: int = 1000,
        run_id: Optional[str] = None,
        fallback_source: Optional[List] = None,
    ) -> Tuple[int, int]:
        """
        Polls consumer for available messages and processes them through the pipeline.
        """
        raw_events = self.consumer.consume_batch(
            max_messages=max_messages,
            fallback_source=fallback_source,
        )
        return self.process_batch(raw_events, run_id=run_id)
