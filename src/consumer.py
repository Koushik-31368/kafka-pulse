"""
consumer.py - Kafka Event Consumer with PostgreSQL persistence
Reads events from Kafka, processes them, and writes to PostgreSQL.
Run this in a second terminal alongside the producer.
"""

import json
import time
import sys
import os
from datetime import datetime, timezone
import psycopg
from psycopg.types.json import Jsonb
from kafka import KafkaConsumer
from rich.console import Console
from rich.panel import Panel
from dotenv import load_dotenv

# Force UTF-8 output on Windows
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

load_dotenv()

console = Console(highlight=False)

# --- Configuration ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "user-events")
CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "pipeline-consumer-group")

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "dbname": os.getenv("DB_NAME", "pipeline_db"),
    "user": os.getenv("DB_USER", "pipeline_user"),
    "password": os.getenv("DB_PASSWORD", "pipeline_pass"),
}

# --- Database ---
def get_db_connection():
    return psycopg.connect(**DB_CONFIG)


def insert_raw_event(cursor, event):
    cursor.execute(
        """
        INSERT INTO raw_events (event_id, event_type, user_id, payload, received_at)
        VALUES (%s, %s, %s, %s, NOW())
        ON CONFLICT (event_id) DO NOTHING
        """,
        (
            event["event_id"],
            event["event_type"],
            event["user_id"],
            Jsonb(event["payload"]),
        ),
    )


def compute_latency_ms(event_timestamp_iso: str) -> float | None:
    """Compute end-to-end latency in ms between producer send time and now."""
    try:
        # Producer sets timestamp as a naive UTC ISO string (datetime.utcnow().isoformat())
        sent_at = datetime.fromisoformat(event_timestamp_iso).replace(tzinfo=timezone.utc)
        now_utc = datetime.now(timezone.utc)
        return round((now_utc - sent_at).total_seconds() * 1000, 3)
    except Exception:
        return None


def insert_processed_event(cursor, event, latency_ms: float | None):
    amount = event["payload"].get("amount", 0.0)
    cursor.execute(
        """
        INSERT INTO processed_events (event_id, user_id, event_type, amount, status, latency_ms)
        VALUES (%s, %s, %s, %s, 'processed', %s)
        """,
        (
            event["event_id"],
            event["user_id"],
            event["event_type"],
            amount,
            latency_ms,
        ),
    )


def update_raw_event_processed(cursor, event_id):
    cursor.execute(
        "UPDATE raw_events SET processed_at = NOW() WHERE event_id = %s",
        (event_id,),
    )


def record_metric(cursor, name, value):
    cursor.execute(
        "INSERT INTO pipeline_metrics (metric_name, metric_value) VALUES (%s, %s)",
        (name, value),
    )


# --- Event Processing ---
def process_event(event):
    """Apply business logic / transformation to an event."""
    amount = event["payload"].get("amount", 0)
    event["is_high_value"] = amount > 200.0
    event["processed"] = True
    return event


# --- Main ---
def main():
    console.print("\n[bold cyan]*** KAFKA EVENT CONSUMER STARTING ***[/bold cyan]")
    console.print(f"   Topic: [yellow]{KAFKA_TOPIC}[/yellow]  |  Group: [yellow]{CONSUMER_GROUP}[/yellow]")

    # Connect to PostgreSQL
    try:
        db_conn = get_db_connection()
        db_conn.autocommit = False
        console.print("[green][OK] Connected to PostgreSQL[/green]")
    except psycopg.OperationalError as e:
        console.print(f"[red][FAIL] PostgreSQL connection failed: {e}[/red]")
        console.print("[yellow][TIP] Make sure Docker is running: docker-compose up -d[/yellow]")
        return

    # Connect to Kafka
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=CONSUMER_GROUP,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            auto_commit_interval_ms=1000,
        )
        console.print("[green][OK] Connected to Kafka broker[/green]\n")
    except Exception as e:
        console.print(f"[red][FAIL] Kafka connection failed: {e}[/red]")
        db_conn.close()
        return

    stats = {"consumed": 0, "errors": 0, "high_value": 0}
    console.print("[bold]Consuming events... Press Ctrl+C to stop.\n[/bold]")

    try:
        for message in consumer:
            event = message.value
            try:
                # Measure latency BEFORE any processing so the clock is as accurate as possible
                latency_ms = compute_latency_ms(event.get("timestamp", ""))

                event = process_event(event)
                cursor = db_conn.cursor()

                insert_raw_event(cursor, event)
                insert_processed_event(cursor, event, latency_ms)
                update_raw_event_processed(cursor, event["event_id"])

                if stats["consumed"] % 10 == 0:
                    record_metric(cursor, "events_consumed", stats["consumed"])

                db_conn.commit()
                cursor.close()

                stats["consumed"] += 1
                if event["is_high_value"]:
                    stats["high_value"] += 1

                flag = " $$$ HIGH VALUE" if event["is_high_value"] else ""
                lat_str = f" lat=[white]{latency_ms:.1f}ms[/white]" if latency_ms is not None else ""
                console.print(
                    f"[blue]<< RECV[/blue] "
                    f"[cyan]{event['event_type']:12}[/cyan] "
                    f"user=[yellow]{event['user_id']}[/yellow] "
                    f"amount=[magenta]${event['payload'].get('amount', 0):.2f}[/magenta]"
                    f"{lat_str}"
                    f"[bold green]{flag}[/bold green] "
                    f"[dim]p={message.partition} off={message.offset}[/dim]"
                )

            except Exception as e:
                try:
                    db_conn.rollback()
                except Exception:
                    pass
                stats["errors"] += 1
                console.print(f"[red][ERR] {event.get('event_id', '?')}: {e}[/red]")

    except KeyboardInterrupt:
        console.print(f"\n[bold yellow]STOPPED.[/bold yellow]")
        console.print(
            Panel(
                f"[green]Consumed :[/green] {stats['consumed']}\n"
                f"[red]Errors   :[/red] {stats['errors']}\n"
                f"[cyan]High Value:[/cyan] {stats['high_value']}",
                title="Final Stats",
                border_style="cyan",
            )
        )
    finally:
        consumer.close()
        db_conn.close()
        console.print("[dim]Consumer closed cleanly.[/dim]")


if __name__ == "__main__":
    main()
