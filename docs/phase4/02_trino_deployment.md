# ForgeStream Phase 4: Apache Trino Distributed SQL Deployment

## Architecture Overview
ForgeStream employs **Apache Trino (v438)** as a distributed, massively parallel processing (MPP) query engine to federate analytical queries across the operational relational store (`PostgreSQL 16`), lakehouse Parquet datasets, and benchmark catalogs (`TPCH`).

```
+-----------------------------------------------------------------------------+
|                          APACHE TRINO COORDINATOR                           |
|                      Container: forgestream-trino-phase4                    |
|                      Port: 8085 (HTTP REST / JDBC API)                      |
+-----------------------------------------------------------------------------+
                                     |
         +---------------------------+---------------------------+
         |                                                       |
         v                                                       v
+-----------------------------+                         +-----------------------------+
|    POSTGRESQL CONNECTOR     |                         |       TPCH CONNECTOR        |
|  Catalog: `postgres`        |                         |  Catalog: `tpch`            |
|  Target: postgres:5432      |                         |  Schema: `sf1`              |
|  Schemas: `public`          |                         |  Benchmark: Synthetic Data  |
+-----------------------------+                         +-----------------------------+
```

---

## Configuration Architecture

### 1. `docker-compose.yml` Configuration
```yaml
  trino:
    image: trinodb/trino:438
    container_name: forgestream-trino-phase4
    ports:
      - "8085:8080"
    volumes:
      - ./trino/etc:/etc/trino
    environment:
      - node.environment=production
    healthcheck:
      test: ["CMD-SHELL", "curl -f http://localhost:8080/v1/info || exit 1"]
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 20s
    depends_on:
      postgres:
        condition: service_healthy
```

### 2. Node Configuration (`trino/etc/node.properties`)
```properties
node.environment=production
node.id=forgestream-trino-node-1
node.data-dir=/data/trino
```

### 3. JVM Tuning (`trino/etc/jvm.config`)
```properties
-server
-Xmx2G
-XX:+UseG1GC
-XX:G1ReservePercent=15
-XX:InitiatingHeapOccupancyPercent=40
-XX:ConcGCThreads=2
-Djdk.attach.allowAttachSelf=true
-Dsun.reflect.inflationThreshold=0
```

### 4. Coordinator Configuration (`trino/etc/config.properties`)
```properties
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
query.max-memory=1GB
query.max-memory-per-node=512MB
query.max-total-memory=1GB
discovery.uri=http://localhost:8080
```

### 5. PostgreSQL Connector Catalog (`trino/etc/catalog/postgres.properties`)
```properties
connector.name=postgresql
connection-url=jdbc:postgresql://postgres:5432/forgestream
connection-user=forgestream
connection-password=forgestream
```

### 6. TPCH Benchmark Catalog (`trino/etc/catalog/tpch.properties`)
```properties
connector.name=tpch
tpch.splits-per-node=4
```

---

## REST API Client Architecture (`TrinoClient`)

ForgeStream interacts with the Trino coordinator using its native REST protocol (`/v1/statement` and `/v1/info`):

- **Query Execution**: Submits SQL query strings with custom headers (`X-Trino-User`, `X-Trino-Catalog`, `X-Trino-Schema`).
- **Paging / Polling**: Recursively follows the `nextUri` JSON attribute until the query state transitions to `FINISHED` or `FAILED`.
- **Result Marshalling**: Decodes column types and row tuples into standard Python data structures and Pandas DataFrames.
- **Resilience**: Automatically strips trailing semicolons and retries on transient connection timeouts.
