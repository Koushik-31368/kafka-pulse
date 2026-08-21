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
|-- docker-compose.yml       # Kafka + PostgreSQL stack
|-- .env.example             # Configuration template (copy to .env)
|-- requirements.txt         # Python dependencies
|-- sql/
|   |-- init.sql             # DB schema (auto-applied on first run)
|-- src/
    |-- producer.py          # Generates fake events -> Kafka
    |-- consumer.py          # Reads Kafka -> stores in PostgreSQL
    |-- monitor.py           # Live terminal dashboard
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

### 6. Open Kafka UI

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
2. The **consumer** reads from that topic, applies business logic (flags transactions over $200 as high-value), then writes both raw and processed records to PostgreSQL inside a transaction.
3. The **monitor** queries PostgreSQL every 3 seconds and renders a live dashboard showing total events, processing lag, and a breakdown by event type.
