# ForgeStream Schema Versioning Policy

## 1. Version Format
All ForgeStream schemas adhere to Semantic Versioning (`MAJOR.MINOR.PATCH`):
- **MAJOR (`x.0.0`)**: Incompatible changes (e.g. removing required fields, changing data types). Requires migration scripts and new Kafka topic / Iceberg table branching.
- **MINOR (`1.x.0`)**: Backward-compatible additions (e.g. adding new optional sensor fields). Supported natively by Apache Iceberg schema evolution.
- **PATCH (`1.0.x`)**: Bug fixes or documentation corrections to schema metadata.

## 2. Current Active Versions
- `telemetry.schema.json`: `1.0.0`
- `maintenance.schema.json`: `1.0.0`

## 3. Data Quality & Quarantine Compatibility
- Any event received with an unknown `MAJOR` version is routed immediately to the Quarantine topic with code `RULE_001_SCHEMA_INVALID`.
