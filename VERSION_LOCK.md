# VERSION_LOCK.md — ForgeStream Phase 1 Environment Specification

This document records the exact, tested hardware, operating system, runtime, library, and container versions for ForgeStream Phase 1: Data Foundation & Streaming Infrastructure.

---

## 1. Host Hardware & Operating System

| Component | Specification / Version |
| :--- | :--- |
| **Operating System** | Windows 11 Home Single Language (64-bit) |
| **OS Build** | 10.0.26300 (win32 AMD64) |
| **CPU** | 12th Gen Intel(R) Core(TM) i5-12500H (12 Cores, 16 Logical Processors) |
| **Physical RAM** | 16.0 GB (15.7 GB usable) |
| **Primary Storage (C:)** | NTFS, ~13 GB Free Space |
| **Secondary Storage (M:)** | NTFS, ~22.4 GB Free Space |
| **Shell Environment** | Git Bash (POSIX sh) / PowerShell 7+ |

---

## 2. Runtime & Core Toolchains

| Tool / Runtime | Version | Notes |
| :--- | :--- | :--- |
| **Python** | 3.14.7 (`C:\Python314\python.exe`) | Core language runtime |
| **pip** | 26.2.1 | Package installer |
| **Java JDK** | OpenJDK 21.0.11 LTS (Temurin build 21.0.11+10-LTS) | Installed and verified |
| **Docker Engine** | 29.6.2 (build dfc4efb) | Local container runtime |
| **Docker Compose** | v5.3.1 | Container orchestrator |
| **RTK Proxy** | 0.44.2 | Token-optimized CLI harness |

---

## 3. Streaming & Storage Infrastructure (Containers / Engines)

| Component | Image / Distribution | Tested Version | Port / Protocol |
| :--- | :--- | :--- | :--- |
| **Apache Kafka** | `apache/kafka:latest` | 3.7+ (KRaft Mode, ZooKeeper-free) | `9092:9092` (PLAINTEXT) |
| **PostgreSQL** | `postgres:16-alpine` | 16.x | `5433:5432` |
| **Apache Iceberg Engine** | PyIceberg with `SqlCatalog` | 0.12.0 | Local Filesystem Parquet Storage |
| **Local SQLite Fallback** | SQLite 3 (C-extension built-in) | 3.45+ | SQLite WAL mode |

---

## 4. Python Dependencies & Drivers

| Package | Version Range / Pinned | Purpose |
| :--- | :--- | :--- |
| `pydantic` | `>=2.7.0,<3.0.0` | Strict data validation & schema generation |
| `pyiceberg` | `0.12.0` (installed) | Official Python Apache Iceberg catalog & table reader/writer |
| `pyarrow` | `25.0.1` (installed) | High-performance Parquet serialization & vectorized in-memory Arrow tables |
| `duckdb` | `>=1.0.0` | Embedded analytical query engine for local Iceberg tables |
| `confluent-kafka` | `2.15.1` (installed) | High-throughput C-extension Kafka producer/consumer |
| `psycopg` | `3.3.6` (installed) | Modern PostgreSQL relational client driver (C-extensions) |
| `psycopg2-binary` | `2.9.13` (installed) | Fallback PostgreSQL relational client driver |
| `sqlalchemy` | `2.1.3` (installed) | Database connection pooling & ORM-agnostic SQL execution |
| `numpy` | `>=1.26.0` | Vectorized mathematical calculations for physics correlations |
| `pytest` | `>=8.0.0` | Unit, integration, and E2E test runner |
| `pytest-asyncio` | `>=0.23.0` | Async testing harness |
| `rich` | `>=13.7.0` | Terminal formatting, tables, and progress display |

---

## 5. Verification & Freeze Date

- **Date Recorded:** 2026-10-05
- **Verified by:** ForgeStream Data Engineering Lead
- **Environment Status:** FROZEN for Phase 1 Data Foundation
