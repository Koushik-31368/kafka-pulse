# Changelog

All notable changes to **kafka-pulse** are documented here.  
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

---

## [1.2.0] – 2026-08-24

### Added
- **`src/utils.py`** – shared utilities module:
  - `get_db_config()` / `get_db_connection()` helpers
  - `retry()` exponential-backoff decorator
  - `fmt_ms()`, `fmt_eps()`, `highlight_latency()` Rich-markup formatters
  - `SCHEMA_VERSION = "1.2.0"` constant propagated to all scripts
- **`src/health_check.py`** – new infrastructure health-check script:
  - Verifies Kafka broker reachability (with latency)
  - Verifies PostgreSQL connectivity and prints server version + uptime
  - Checks all required pipeline tables and prints row counts
  - Exits with code 1 if any check fails (CI-friendly)
- **`sql/add_schema_version.sql`** – new migration:
  - Adds `schema_version TEXT` column to `pipeline_metrics`
  - Adds `event_schema_version TEXT` column to `raw_events`
  - Creates `idx_processed_events_type_created` index for faster monitor queries
  - Creates `idx_raw_events_event_type` index for aggregation queries

### Changed – `producer.py`
- Added `--rate / -r` flag: set events/sec in continuous mode (e.g. `--rate 5`)
- Added `--count / -c` flag: stop continuous mode after N events
- Richer event payload: `country`, `device_type`, `currency`, `discount_pct`, `final_amount`
- `schema_version` field on every generated event
- Rolling 10-second throughput window displayed in continuous mode
- ASCII progress bar in burst mode
- Kafka producer tuned with `linger_ms=5` and `batch_size=32768` for better burst throughput
- `datetime.utcnow()` replaced with timezone-aware `datetime.now(timezone.utc)` (deprecation fix)

### Changed – `consumer.py`
- **Batched DB commits**: groups `CONSUMER_BATCH_SIZE` events (default 20) per transaction
  - Configurable via `CONSUMER_BATCH_SIZE` env var
  - Expected to significantly increase consumer throughput vs. per-event commits
- **Dead-letter queue (DLQ)**: failed events written to `dlq.jsonl` instead of being silently dropped
  - DLQ path configurable via `DLQ_PATH` env var
- **Automatic DB reconnect**: exponential back-off reconnect on `psycopg.OperationalError`
- `ON CONFLICT (event_id) DO NOTHING` guard added to `processed_events` insert
- `HIGH_VALUE_THRESHOLD` now read from env var (default `200.0`)
- Rich exit panel now shows throughput (ev/s), elapsed time, and batch count
- `schema_version` recorded in `pipeline_metrics` rows

### Changed – `monitor.py`
- **Live latency percentiles panel**: p50 / p95 / p99 / min / max / mean from last 1000 events
- **30-second rolling event-rate**: shows live events/sec
- **DLQ indicator**: shows number of failed events sitting in `dlq.jsonl`
- Colour-coded lag indicator (green < 10, yellow < 100, red ≥ 100)
- Events-by-type table now shows **Max $** column alongside Avg $
- Configurable refresh via `MONITOR_REFRESH_SECONDS` env var

### Changed – `benchmark.py`
- Added **p25** (first quartile) to latency report
- Added **throughput ratio** column: consumer eps / producer eps
- Added **`--out FILE`** flag: write full results as JSON for CI/CD integration
- Uses timezone-aware timestamps in JSON export

---

## [1.1.0] – initial burst benchmark release

### Added
- `--burst N` mode on producer: send N messages with no sleep
- `benchmark.py`: p50/p95/p99 latency + consumer throughput reporter
- `add_latency_ms.sql` migration: adds `latency_ms` column to `processed_events`

---

## [1.0.0] – initial release

### Added
- `producer.py`: continuous fake e-commerce event producer
- `consumer.py`: Kafka consumer with PostgreSQL persistence
- `monitor.py`: live terminal dashboard
- `docker-compose.yml`: Kafka + Zookeeper + Kafka UI + PostgreSQL stack
- `sql/init.sql`: database schema (raw_events, processed_events, pipeline_metrics, event_summary view)
