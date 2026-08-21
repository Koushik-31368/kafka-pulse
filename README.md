# Real-Time Kafka Pipeline

A fully local, production-style data pipeline using **Apache Kafka**, **PostgreSQL**, and **Python** — no cloud accounts, no costs.

## Architecture

```
[Python Producer] --> [Kafka Topic: user-events] --> [Python Consumer] --> [PostgreSQL]
                                                                                |
                                                                      [Monitor Dashboard]
```

## Project Structure

```
realtime-kafka-pipeline/
|-- docker-compose.yml           # Kafka + PostgreSQL stack
|-- .env.example                 # Configuration template (copy to .env)
|-- requirements.txt             # Python dependencies
|-- sql/
|   |-- init.sql                 # DB schema (auto-applied on first run)
|   |-- add_latency_ms.sql       # Migration: adds latency_ms column
|-- src/
    |-- producer.py              # Generates fake events -> Kafka (+ --burst mode)
    |-- consumer.py              # Reads Kafka -> stores in PostgreSQL (measures latency)
    |-- monitor.py               # Live terminal dashboard
    |-- benchmark.py             # Burst benchmark reporter (p50/p95/p99 latency)
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

### 5. Run the pipeline (3 separate terminals)

**Terminal 1 - Producer** (generates fake e-commerce events and sends to Kafka):
```bash
venv\Scripts\activate
python src/producer.py
```

**Terminal 2 - Consumer** (reads from Kafka and saves to PostgreSQL):
```bash
venv\Scripts\activate
python src/consumer.py
```

**Terminal 3 - Monitor** (live terminal dashboard):
```bash
venv\Scripts\activate
python src/monitor.py
```

### 6. Run a burst throughput benchmark (optional)

Apply the latency migration once, then fire a burst and report real percentiles:

```bash
# 1. Apply migration (one-time, safe to re-run)
Get-Content sql\add_latency_ms.sql | docker exec -i postgres psql -U pipeline_user -d pipeline_db

# 2. Start the consumer (Terminal 2 above)

# 3. Fire 5000 messages with no sleep — copy the batch_id from the output
python src/producer.py --burst 5000

# 4. Once the consumer drains, report p50/p95/p99 latency + throughput
python src/benchmark.py --batch-id <batch_id_from_step_3> --expected 5000 --produced-eps <eps_from_step_3>
```



Navigate to **http://localhost:8081** in your browser to see topics, partitions, and live messages.

## Stop Everything

```bash
docker-compose down        # stop containers (keeps DB data)
docker-compose down -v     # stop containers and delete all data
```

## Port Reference

| Service      | Host Port | Notes                               |
|--------------|-----------|-------------------------------------|
| Kafka        | 9092      | Used by Python producer and consumer |
| Kafka UI     | 8081      | Web dashboard (browser)             |
| PostgreSQL   | 5433      | Mapped from 5432 inside container   |
| Zookeeper    | 2181      | Internal Kafka coordination         |

## Database Tables

| Table              | Description                            |
|--------------------|----------------------------------------|
| raw_events         | Every event received from Kafka        |
| processed_events   | Transformed events with business logic |
| pipeline_metrics   | Periodic counters for monitoring       |
| event_summary      | View: quick aggregation by event type  |

## Configuration

Copy `.env.example` to `.env` and edit as needed:

| Variable                   | Default              | Description                        |
|----------------------------|----------------------|------------------------------------|
| KAFKA_BOOTSTRAP_SERVERS    | localhost:9092       | Kafka broker address               |
| KAFKA_TOPIC                | user-events          | Topic name                         |
| DB_HOST                    | localhost            | PostgreSQL host                    |
| DB_PORT                    | 5433                 | PostgreSQL port (Docker host port) |
| DB_NAME                    | pipeline_db          | Database name                      |
| DB_USER                    | pipeline_user        | Database user                      |
| DB_PASSWORD                | pipeline_pass        | Database password                  |
| PRODUCER_INTERVAL_SECONDS  | 1                    | Seconds between generated events   |

## Tech Stack

- **Apache Kafka** — distributed event streaming platform
- **Apache Zookeeper** — Kafka cluster coordination
- **PostgreSQL** — relational database for storing events
- **Python** — producer, consumer, and monitor scripts
- **kafka-python-ng** — Python Kafka client
- **psycopg** — Python PostgreSQL adapter
- **Rich** — terminal formatting and live dashboard
- **Faker** — realistic fake event data generation
- **Docker Compose** — local container orchestration

## How It Works

1. The **producer** generates fake e-commerce events (purchases, page views, logins, etc.) every second using Faker and publishes them to the `user-events` Kafka topic.
2. The **consumer** reads from that topic, applies business logic (flags transactions over $200 as high-value), then writes both raw and processed records to PostgreSQL inside a transaction. It computes `latency_ms` — the delta between the producer's send timestamp and the DB insert time — and stores it in `processed_events`.
3. The **monitor** queries PostgreSQL every 3 seconds and renders a live dashboard showing total events, processing lag, and a breakdown by event type.

## Benchmark Results

Measured locally on a single machine with Docker Compose (no cloud, no external network hops).  
Run: `python src/producer.py --burst 5000`, then `python src/benchmark.py --batch-id <id> --expected 5000`.

| Metric | Value | Notes |
|---|---|---|
| **Producer throughput** | **2,619 events/sec** | 5000 messages sent + acked with no sleep |
| **Consumer throughput** | **108 events/sec** | Synchronous per-event DB commits, no batching |
| **Min end-to-end latency** | **46 ms** | First message — true Kafka transit + DB insert time |
| **p50 latency under burst** | **22.1 s** | Queue-wait dominant — see note below |
| **p99 latency under burst** | **44.1 s** | Queue-wait dominant — see note below |

**What the p50/p99 numbers actually mean:**  
The producer can send 2,619 messages/sec; the consumer can only drain 108/sec. Under a 5000-message burst, messages queue up in Kafka and the `latency_ms` measurement (producer send time → DB insert time) grows linearly as each message waits its turn. The p50 of 22 seconds is not transport latency — it's the **queue-drain time for the 2,500th message**. The 46ms minimum is the honest Kafka+PostgreSQL transport figure for a message that sees no queue wait.

This is a correct and expected result: it faithfully captures a real architectural bottleneck (synchronous per-event commits), not a measurement error.

## Known Limitations / Next Steps

- **Consumer batching** is the fix to close the throughput gap. Batching 50–100 events per DB transaction would push consumer throughput into the 2,000+ events/sec range, collapsing p50 latency from ~22s to the same ~50ms range as the minimum. This is documented here as a known next step rather than implemented under time pressure with the current single-event-commit design.
- The ML/anomaly-detection layer (real-time fraud scoring) remains **future work** — the pipeline is intentionally kept clean of sklearn/ML dependencies so the benchmark reflects pure infrastructure performance.
