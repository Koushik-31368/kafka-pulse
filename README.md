# Real-Time Kafka Pipeline  [![version](https://img.shields.io/badge/version-1.2.0-blue)](CHANGELOG.md)

A fully local, production-style data pipeline using **Apache Kafka**, **PostgreSQL**, and **Python** — no cloud accounts, no costs.

## Architecture

```
[Python Producer] --> [Kafka Topic: user-events] --> [Python Consumer] --> [PostgreSQL]
       |                                                     |                   |
  --rate / --burst                               Batched commits (20/tx)   [Monitor Dashboard]
  --count / --schema                             DLQ (dlq.jsonl)           [Benchmark Reporter]
                                                 Auto-reconnect            [Health Check]
```

## Project Structure

```
realtime-kafka-pipeline/
├── docker-compose.yml           # Kafka + PostgreSQL stack
├── .env.example                 # Configuration template (copy to .env)
├── requirements.txt             # Python dependencies
├── CHANGELOG.md                 # Version history
├── sql/
│   ├── init.sql                 # DB schema (auto-applied on first run)
│   ├── add_latency_ms.sql       # Migration v1.1: adds latency_ms column
│   └── add_schema_version.sql   # Migration v1.2: schema version + indexes
└── src/
    ├── utils.py                 # Shared: DB helpers, retry decorator, formatters
    ├── producer.py              # Generates fake events -> Kafka (--rate / --burst / --count)
    ├── consumer.py              # Reads Kafka -> PostgreSQL (batched, DLQ, auto-reconnect)
    ├── monitor.py               # Live terminal dashboard (latency + rate panels)
    ├── benchmark.py             # Burst benchmark reporter (p25/p50/p95/p99 + JSON export)
    └── health_check.py          # Infrastructure health check (Kafka + PostgreSQL)
```

## Quick Start

### 1. Clone and configure

```bash
git clone https://github.com/Koushik-31368/kafka-pulse.git
cd kafka-pulse
cp .env.example .env
```

### 2. Start the infrastructure

```bash
docker-compose up -d
```

Wait about 30 seconds for Kafka to be ready.

### 3. Create a Python virtual environment

```bash
python -m venv venv

# Windows
venv\Scripts\activate

# Mac / Linux
source venv/bin/activate
```

### 4. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 5. (Optional) Run a health check

Verify that Kafka and PostgreSQL are reachable before starting the pipeline:

```bash
python src/health_check.py
```

Expected output:

```
*** KAFKA-PULSE HEALTH CHECK ***  2026-08-24 17:28:00 UTC

┌─────────────────┬────────┬────────────┬────────────────────────────────┐
│ Component       │ Status │    Latency │ Detail                         │
├─────────────────┼────────┼────────────┼────────────────────────────────┤
│ Kafka Broker    │ PASS   │    23.4 ms │ Broker reachable. Topic ...    │
│ PostgreSQL      │ PASS   │     4.1 ms │ DB='pipeline_db' | PostgreSQL  │
│ DB Tables       │ PASS   │     1.8 ms │ raw_events=0  processed_...    │
└─────────────────┴────────┴────────────┴────────────────────────────────┘
All systems operational.
```

### 6. Apply migrations (run once)

```bash
# Windows PowerShell
Get-Content sql\add_latency_ms.sql    | docker exec -i postgres psql -U pipeline_user -d pipeline_db
Get-Content sql\add_schema_version.sql | docker exec -i postgres psql -U pipeline_user -d pipeline_db
```

### 7. Run the pipeline (3 separate terminals)

**Terminal 1 – Producer** (default: 1 event/sec):
```bash
venv\Scripts\activate
python src/producer.py

# Or set a custom rate:
python src/producer.py --rate 5        # 5 events/sec
python src/producer.py --count 200     # stop after 200 events
```

**Terminal 2 – Consumer** (batched commits, dead-letter queue):
```bash
venv\Scripts\activate
python src/consumer.py
```

**Terminal 3 – Monitor** (live latency + rate dashboard):
```bash
venv\Scripts\activate
python src/monitor.py
```

### 8. Run a burst throughput benchmark (optional)

```bash
# 1. Start the consumer (Terminal 2 above)

# 2. Fire 5000 messages with no sleep — copy batch_id + eps from output
python src/producer.py --burst 5000

# 3. Once consumer drains, report p25/p50/p95/p99 latency + throughput
python src/benchmark.py \
    --batch-id <batch_id_from_step_2> \
    --expected 5000 \
    --produced-eps <eps_from_step_2>

# 4. Optionally export results as JSON for CI/CD
python src/benchmark.py --batch-id <id> --out results.json
```

## Stop Everything

```bash
docker-compose down        # stop containers (keeps DB data)
docker-compose down -v     # stop containers and delete all data
```

## Port Reference

| Service      | Host Port | Notes                                 |
|--------------|-----------|---------------------------------------|
| Kafka        | 9092      | Used by Python producer and consumer  |
| Kafka UI     | 8081      | Web dashboard — http://localhost:8081 |
| PostgreSQL   | 5433      | Mapped from 5432 inside container     |
| Zookeeper    | 2181      | Internal Kafka coordination           |

## Database Tables

| Table              | Description                                      |
|--------------------|--------------------------------------------------|
| `raw_events`       | Every event received from Kafka                  |
| `processed_events` | Transformed events with business logic + latency |
| `pipeline_metrics` | Periodic counters + schema version tags          |
| `event_summary`    | View: quick aggregation by event type            |

## Configuration

Copy `.env.example` to `.env` and edit as needed:

| Variable                   | Default              | Description                                       |
|----------------------------|----------------------|---------------------------------------------------|
| `KAFKA_BOOTSTRAP_SERVERS`  | `localhost:9092`     | Kafka broker address                              |
| `KAFKA_TOPIC`              | `user-events`        | Topic name                                        |
| `DB_HOST`                  | `localhost`          | PostgreSQL host                                   |
| `DB_PORT`                  | `5433`               | PostgreSQL port (Docker host port)                |
| `DB_NAME`                  | `pipeline_db`        | Database name                                     |
| `DB_USER`                  | `pipeline_user`      | Database user                                     |
| `DB_PASSWORD`              | `pipeline_pass`      | Database password                                 |
| `PRODUCER_INTERVAL_SECONDS`| `1`                  | Default seconds between events (overridden by `--rate`) |
| `CONSUMER_BATCH_SIZE`      | `20`                 | Events per DB transaction in consumer             |
| `HIGH_VALUE_THRESHOLD`     | `200.0`              | Amount (USD) above which an event is "high value" |
| `DLQ_PATH`                 | `dlq.jsonl`          | Path to dead-letter queue file                    |
| `MONITOR_REFRESH_SECONDS`  | `3`                  | Monitor dashboard refresh interval                |

## Tech Stack

- **Apache Kafka** — distributed event streaming platform
- **Apache Zookeeper** — Kafka cluster coordination
- **PostgreSQL** — relational database for storing events
- **Python 3.11+** — producer, consumer, monitor, benchmark, health-check
- **kafka-python-ng** — Python Kafka client
- **psycopg 3** — Python PostgreSQL adapter
- **Rich** — terminal formatting and live dashboard
- **Faker** — realistic fake event data generation
- **Docker Compose** — local container orchestration

## How It Works

1. The **producer** generates realistic e-commerce events (purchases, page views, logins, etc.) using Faker.  
   Events carry a `schema_version` field and a rich payload including `country`, `device_type`, `currency`, `discount_pct`, and `final_amount`.  
   Use `--rate` to control throughput or `--burst N` for a max-speed benchmark.

2. The **consumer** reads from Kafka and groups events into batches of `CONSUMER_BATCH_SIZE` (default 20) before committing to PostgreSQL — dramatically reducing per-event overhead.  
   Failed events are written to a **dead-letter queue** (`dlq.jsonl`) instead of being dropped.  
   The consumer automatically reconnects to PostgreSQL with exponential back-off on transient failures.

3. The **monitor** queries PostgreSQL every 3 seconds and renders three live panels:
   - **Pipeline Status**: raw/processed counts, lag, high-value events, 30-second rolling rate, DLQ size
   - **Latency (last 1k)**: p50/p95/p99/min/max/mean computed over the last 1000 processed events
   - **Events by Type**: count, average amount, and max amount per event type

4. The **health check** verifies Kafka and PostgreSQL connectivity before the pipeline starts,  
   printing a rich table with latency for each component. Returns exit code 1 on failure (CI-friendly).

## Benchmark Results

Measured locally on a single machine with Docker Compose (no cloud, no external network hops).

### v1.1 baseline (single-event commits)

| Metric | Value | Notes |
|---|---|---|
| **Producer throughput** | **2,619 ev/s** | 5000 msgs sent + acked, no sleep |
| **Consumer throughput** | **108 ev/s** | Per-event DB commits |
| **Min end-to-end latency** | **46 ms** | No queue wait |
| **p50 latency under burst** | **22.1 s** | Queue-drain dominant |
| **p99 latency under burst** | **44.1 s** | Queue-drain dominant |

### v1.2 improvement (batched commits, batch=20)

Batching 20 events per transaction is expected to push consumer throughput toward **2,000+ ev/s**, collapsing p50 burst latency from ~22 s to the **~50 ms** range seen at minimum latency. Re-run the benchmark to capture your results.

## Known Limitations / Next Steps

- **Consumer batching** is now implemented (v1.2) with a default batch size of 20. Increase `CONSUMER_BATCH_SIZE` to push throughput further.
- **DLQ replay**: a `dlq_replay.py` script to re-ingest dead-letter events is planned as a next step.
- **ML/anomaly-detection** (real-time fraud scoring) remains future work — the pipeline is intentionally kept free of ML dependencies so the benchmark reflects pure infrastructure performance.
- **Multi-partition topics**: the current setup uses a single-partition topic. Scaling to multiple partitions with multiple consumer instances is a documented next step.

## See Also

- [CHANGELOG.md](CHANGELOG.md) — full version history
- [Kafka UI](http://localhost:8081) — live topic browser (when Docker is running)
