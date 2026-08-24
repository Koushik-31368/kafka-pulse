"""
consumer.py - Kafka Event Consumer with PostgreSQL persistence
Reads events from Kafka, processes them, and writes to PostgreSQL.

Key improvements over v1:
  - Batched DB commits (CONSUMER_BATCH_SIZE events per transaction)
  - Dead-letter queue (DLQ): failed events written to dlq.jsonl
  - Automatic DB reconnect on OperationalError
  - Schema version tagging on every processed_events row
  - Rich summary panel with throughput stats on exit
  - Configurable high-value threshold via HIGH_VALUE_THRESHOLD env var

Run this in a second terminal alongside the producer:
    python src/consumer.py
"""

import json
import time
import sys
import os
from datetime import datetime, timezone
from pathlib import Path

import psycopg
from psycopg.types.json import Jsonb
from kafka import KafkaConsumer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from dotenv import load_dotenv

# Force UTF-8 output on Windows
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]

load_dotenv()

console = Console(highlight=False)

# --- Configuration ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC",             "user-events")
CONSUMER_GROUP          = os.getenv("KAFKA_CONSUMER_GROUP",    "pipeline-consumer-group")
CONSUMER_BATCH_SIZE     = int(os.getenv("CONSUMER_BATCH_SIZE", "20"))
HIGH_VALUE_THRESHOLD    = float(os.getenv("HIGH_VALUE_THRESHOLD", "200.0"))
DLQ_PATH                = Path(os.getenv("DLQ_PATH", "dlq.jsonl"))

try:
    from utils import SCHEMA_VERSION  # type: ignore[import]
except ImportError:
    SCHEMA_VERSION = "1.2.0"

DB_CONFIG = {
    "host":     os.getenv("DB_HOST",     "localhost"),
    "port":     int(os.getenv("DB_PORT", "5432")),
    "dbname":   os.getenv("DB_NAME",     "pipeline_db"),
    "user":     os.getenv("DB_USER",     "pipeline_user"),
    "password": os.getenv("DB_PASSWORD", "pipeline_pass"),
}


# --- Database ---
def get_db_connection():
    conn = psycopg.connect(**DB_CONFIG)
    conn.autocommit = False
    return conn


def reconnect_db(old_conn, attempt: int = 1) -> psycopg.Connection:
    """Close stale connection and open a fresh one with back-off."""
    try:
        old_conn.close()
    except Exception:
        pass
    delay = min(2 ** attempt, 30)
    console.print(f"[yellow][DB] Reconnecting in {delay}s (attempt {attempt})...[/yellow]")
    time.sleep(delay)
    return get_db_connection()


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
        sent_at = datetime.fromisoformat(event_timestamp_iso).replace(tzinfo=timezone.utc)
        now_utc = datetime.now(timezone.utc)
        return round((now_utc - sent_at).total_seconds() * 1000, 3)
    except Exception:
        return None


def insert_processed_event(cursor, event, latency_ms: float | None):
    amount = event["payload"].get("amount", 0.0)
    cursor.execute(
        """
        INSERT INTO processed_events
            (event_id, user_id, event_type, amount, status, latency_ms)
        VALUES (%s, %s, %s, %s, 'processed', %s)
        ON CONFLICT (event_id) DO NOTHING
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
        """
        INSERT INTO pipeline_metrics (metric_name, metric_value, schema_version)
        VALUES (%s, %s, %s)
        """,
        (name, value, SCHEMA_VERSION),
    )


def write_to_dlq(event: dict, error: str):
    """Append a failed event to the dead-letter queue file (one JSON per line)."""
    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "error": error,
        "event": event,
    }
    with DLQ_PATH.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry) + "\n")


# --- Event Processing ---
def process_event(event: dict) -> dict:
    """Apply business logic / transformation to an event."""
    amount = event["payload"].get("amount", 0)
    event["is_high_value"]      = amount > HIGH_VALUE_THRESHOLD
    event["processed"]          = True
    event["schema_version"]     = SCHEMA_VERSION
    event["consumer_processed_at"] = datetime.now(timezone.utc).isoformat()
    return event


# --- Batch commit helper ---
def flush_batch(db_conn, batch: list[tuple]) -> int:
    """
    Commit a batch of (event, latency_ms) tuples in a single transaction.
    Returns the number of successfully committed events.
    """
    committed = 0
    with db_conn.cursor() as cursor:
        for event, latency_ms in batch:
            insert_raw_event(cursor, event)
            insert_processed_event(cursor, event, latency_ms)
            update_raw_event_processed(cursor, event["event_id"])
            committed += 1
        record_metric(cursor, "batch_committed", committed)
    db_conn.commit()
    return committed


def _build_exit_panel(stats: dict, t_start: float) -> Panel:
    elapsed = time.perf_counter() - t_start
    eps = stats["consumed"] / elapsed if elapsed > 0 else 0

    grid = Table.grid(padding=(0, 2))
    grid.add_column(style="bold")
    grid.add_column()
    grid.add_row("Consumed :",    f"[green]{stats['consumed']:,}[/green]")
    grid.add_row("High Value :",  f"[cyan]{stats['high_value']:,}[/cyan]")
    grid.add_row("DLQ errors :",  f"[red]{stats['errors']:,}[/red]")
    grid.add_row("Elapsed :",     f"[yellow]{elapsed:.1f}s[/yellow]")
    grid.add_row("Throughput :",  f"[magenta]{eps:.1f} ev/s[/magenta]")
    grid.add_row("Batch size :",  f"[dim]{CONSUMER_BATCH_SIZE}[/dim]")
    return Panel(grid, title="[bold]Final Stats[/bold]", border_style="cyan")


# --- Main ---
def main():
    console.print("\n[bold cyan]*** KAFKA EVENT CONSUMER v2 STARTING ***[/bold cyan]")
    console.print(
        f"   Topic: [yellow]{KAFKA_TOPIC}[/yellow]  |  "
        f"Group: [yellow]{CONSUMER_GROUP}[/yellow]  |  "
        f"Batch: [yellow]{CONSUMER_BATCH_SIZE}[/yellow]  |  "
        f"Schema: [dim]{SCHEMA_VERSION}[/dim]"
    )

    # Connect to PostgreSQL
    try:
        db_conn = get_db_connection()
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
            value_deserializer=lambda m: json.loads(m.decode("utf-8")) if m is not None else {},  # type: ignore[union-attr]
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            auto_commit_interval_ms=1000,
        )
        console.print("[green][OK] Connected to Kafka broker[/green]\n")
    except Exception as e:
        console.print(f"[red][FAIL] Kafka connection failed: {e}[/red]")
        db_conn.close()
        return

    stats = {"consumed": 0, "errors": 0, "high_value": 0, "batches": 0}
    pending_batch: list[tuple] = []
    t_start = time.perf_counter()
    db_reconnect_attempts = 0

    console.print(
        f"[bold]Consuming events (batch={CONSUMER_BATCH_SIZE})... "
        f"Press Ctrl+C to stop.\n[/bold]"
    )

    try:
        for message in consumer:
            event = message.value
            try:
                latency_ms = compute_latency_ms(event.get("timestamp", ""))
                event = process_event(event)
                pending_batch.append((event, latency_ms))

                flag = " [bold green]$$$ HIGH VALUE[/bold green]" if event["is_high_value"] else ""
                lat_str = (
                    f" lat=[white]{latency_ms:.1f}ms[/white]"
                    if latency_ms is not None
                    else ""
                )
                console.print(
                    f"[blue]<< RECV[/blue] "
                    f"[cyan]{event['event_type']:12}[/cyan] "
                    f"user=[yellow]{event['user_id']}[/yellow] "
                    f"amount=[magenta]${event['payload'].get('amount', 0):.2f}[/magenta]"
                    f"{lat_str}"
                    f"{flag} "
                    f"[dim]p={message.partition} off={message.offset} "
                    f"buf={len(pending_batch)}/{CONSUMER_BATCH_SIZE}[/dim]"
                )

                if event["is_high_value"]:
                    stats["high_value"] += 1

                # Flush batch when full
                if len(pending_batch) >= CONSUMER_BATCH_SIZE:
                    try:
                        n = flush_batch(db_conn, pending_batch)
                        stats["consumed"] += n
                        stats["batches"] += 1
                        db_reconnect_attempts = 0  # reset on success
                        console.print(
                            f"[dim]   [BATCH] Flushed {n} events "
                            f"(total={stats['consumed']:,})[/dim]"
                        )
                        pending_batch.clear()
                    except psycopg.OperationalError:
                        db_reconnect_attempts += 1
                        db_conn = reconnect_db(db_conn, db_reconnect_attempts)
                        # retry the same batch once after reconnect
                        try:
                            n = flush_batch(db_conn, pending_batch)
                            stats["consumed"] += n
                            stats["batches"] += 1
                            pending_batch.clear()
                        except psycopg.OperationalError as retry_exc:
                            # Second failure: write every event in the batch to the DLQ
                            # so nothing is silently lost.
                            lost = len(pending_batch)
                            err_msg = str(retry_exc)
                            console.print(
                                f"[bold red][DLQ] Retry flush failed — "
                                f"writing {lost} events to DLQ: {err_msg}[/bold red]"
                            )
                            for dlq_event, _ in pending_batch:
                                write_to_dlq(dlq_event, f"batch retry failed: {err_msg}")
                            stats["errors"] += lost
                            pending_batch.clear()
                            try:
                                db_conn.rollback()
                            except Exception:
                                pass

            except Exception as e:
                stats["errors"] += 1
                err_msg = str(e)
                console.print(
                    f"[red][DLQ] {event.get('event_id', '?')}: {err_msg}[/red]"
                )
                write_to_dlq(event, err_msg)
                # Don't let one bad event break the whole batch
                pending_batch.clear()
                try:
                    db_conn.rollback()
                except Exception:
                    pass

    except KeyboardInterrupt:
        console.print("\n[bold yellow]STOPPED.[/bold yellow]")

        # Flush remaining events
        if pending_batch:
            console.print(
                f"[yellow]Flushing {len(pending_batch)} remaining events...[/yellow]"
            )
            try:
                n = flush_batch(db_conn, pending_batch)
                stats["consumed"] += n
                console.print(f"[green]Flushed {n} remaining events.[/green]")
            except Exception as e:
                console.print(f"[red]Final flush failed: {e}[/red]")

        console.print(_build_exit_panel(stats, t_start))

    finally:
        consumer.close()
        db_conn.close()
        console.print("[dim]Consumer closed cleanly.[/dim]")


if __name__ == "__main__":
    main()
