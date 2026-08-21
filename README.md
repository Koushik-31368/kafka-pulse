# Real-Time Kafka Pipeline

A fully local, production-style data pipeline using **Apache Kafka**, **PostgreSQL**, and **Python** — no cloud accounts, no costs.

## 🏗 Architecture

```
[Python Producer] ──► [Kafka Topic: user-events] ──► [Python Consumer] ──► [PostgreSQL]
                                                                                  │
                                                                        [Monitor Dashboard]
```

## 📁 Project Structure

```
realtime-kafka-pipeline/
├── docker-compose.yml       # Kafka + PostgreSQL stack
├── .env                     # Configuration (edit if needed)
├── requirements.txt         # Python dependencies
├── sql/
│   └── init.sql             # DB schema (auto-applied on first run)
└── src/
    ├── producer.py          # Generates fake events → Kafka
    ├── consumer.py          # Reads Kafka → stores in PostgreSQL
    └── monitor.py           # Live terminal dashboard
```

## 🚀 Quick Start

### 1. Start the Infrastructure
```bash
docker-compose up -d
```
Wait ~30 seconds for Kafka to be ready.

### 2. Install Python Dependencies
```bash
venv\Scripts\activate          # Windows
pip install -r requirements.txt
```

### 3. Run the Pipeline (3 separate terminals)

**Terminal 1 — Producer:**
```bash
venv\Scripts\activate
python src/producer.py
```

**Terminal 2 — Consumer:**
```bash
venv\Scripts\activate
python src/consumer.py
```

**Terminal 3 — Monitor:**
```bash
venv\Scripts\activate
python src/monitor.py
```

### 4. Open Kafka UI
Navigate to **http://localhost:8080** in your browser.

## 🛑 Stop Everything
```bash
docker-compose down          # stop containers (keeps DB data)
docker-compose down -v       # stop + delete all data
```

## 📊 Database Tables

| Table | Description |
|-------|-------------|
| `raw_events` | Every event received from Kafka |
| `processed_events` | Transformed events with business logic |
| `pipeline_metrics` | Periodic counters for monitoring |
| `event_summary` (view) | Quick aggregation by event type |

## 🔧 Configuration

Edit `.env` to change any settings:
- `PRODUCER_INTERVAL_SECONDS` — how fast events are generated (default: 1s)
- `KAFKA_TOPIC` — topic name
- `DB_*` — database credentials
