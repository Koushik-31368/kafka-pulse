"""
health_check.py - Infrastructure Health Check for Kafka-Pulse Pipeline
Verifies connectivity to Kafka broker and PostgreSQL, then prints a rich report.
Usage: python src/health_check.py
"""

import sys
import os
import time
from datetime import datetime, timezone

# Force UTF-8 on Windows
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]

from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

load_dotenv()

console = Console(highlight=False)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "user-events")


def _get_db_config() -> dict:
    """Build DB config from environment (local copy avoids circular import issues)."""
    return {
        "host":     os.getenv("DB_HOST",     "localhost"),
        "port":     int(os.getenv("DB_PORT", "5432")),
        "dbname":   os.getenv("DB_NAME",     "pipeline_db"),
        "user":     os.getenv("DB_USER",     "pipeline_user"),
        "password": os.getenv("DB_PASSWORD", "pipeline_pass"),
    }


def check_kafka() -> tuple[bool, str, float]:
    """Try to create a KafkaProducer and ping the broker. Returns (ok, detail, latency_ms)."""
    from kafka import KafkaProducer  # type: ignore[import-untyped]
    import kafka.errors as kerrors   # type: ignore[import-untyped]

    t0 = time.perf_counter()
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            request_timeout_ms=5000,
            api_version_auto_timeout_ms=5000,
        )
        # Attempt metadata fetch as implicit connectivity check
        producer.partitions_for(KAFKA_TOPIC)
        elapsed = (time.perf_counter() - t0) * 1000
        producer.close(timeout=1)
        return True, f"Broker reachable. Topic '{KAFKA_TOPIC}' exists.", round(elapsed, 1)
    except kerrors.NoBrokersAvailable:
        elapsed = (time.perf_counter() - t0) * 1000
        return False, f"No brokers available at {KAFKA_BOOTSTRAP_SERVERS}", round(elapsed, 1)
    except Exception as exc:
        elapsed = (time.perf_counter() - t0) * 1000
        return False, str(exc), round(elapsed, 1)


def check_postgres() -> tuple[bool, str, float]:
    """Try to connect to PostgreSQL and run a ping query. Returns (ok, detail, latency_ms)."""
    import psycopg

    cfg = _get_db_config()
    t0 = time.perf_counter()
    try:
        conn = psycopg.connect(**cfg, connect_timeout=5)
        with conn.cursor() as cur:
            cur.execute("SELECT version(), current_database(), pg_postmaster_start_time()")
            row = cur.fetchone()
        conn.close()
        elapsed = (time.perf_counter() - t0) * 1000
        if row:
            ver, db, started = row
            started_str = started.strftime("%Y-%m-%d %H:%M:%S UTC") if started else "unknown"
            return True, f"DB='{db}' | {str(ver).split(',')[0]} | up since {started_str}", round(elapsed, 1)
        return True, "Connected (no version info)", round(elapsed, 1)
    except psycopg.OperationalError as exc:
        elapsed = (time.perf_counter() - t0) * 1000
        return False, str(exc).strip(), round(elapsed, 1)


def check_db_tables() -> tuple[bool, str, float]:
    """Verify that the expected pipeline tables exist and return row counts."""
    import psycopg

    EXPECTED_TABLES = ["raw_events", "processed_events", "pipeline_metrics"]
    cfg = _get_db_config()
    t0 = time.perf_counter()
    try:
        conn = psycopg.connect(**cfg)
        counts: dict[str, int] = {}
        with conn.cursor() as cur:
            for tbl in EXPECTED_TABLES:
                cur.execute(f"SELECT COUNT(*) FROM {tbl}")  # noqa: S608
                result = cur.fetchone()
                counts[tbl] = result[0] if result else 0
        conn.close()
        elapsed = (time.perf_counter() - t0) * 1000
        summary = "  ".join(f"{t}={c:,}" for t, c in counts.items())
        return True, summary, round(elapsed, 1)
    except Exception as exc:
        elapsed = (time.perf_counter() - t0) * 1000
        return False, str(exc).strip(), round(elapsed, 1)


def main() -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    console.print(f"\n[bold cyan]*** KAFKA-PULSE HEALTH CHECK ***[/bold cyan]  [dim]{now}[/dim]\n")

    checks = [
        ("Kafka Broker",    check_kafka),
        ("PostgreSQL",      check_postgres),
        ("DB Tables",       check_db_tables),
    ]

    table = Table(border_style="cyan", header_style="bold cyan", show_lines=True)
    table.add_column("Component",  style="bold", min_width=16)
    table.add_column("Status",     min_width=8)
    table.add_column("Latency",    justify="right", min_width=10)
    table.add_column("Detail",     style="dim")

    all_ok = True
    for name, fn in checks:
        ok, detail, latency_ms = fn()
        if not ok:
            all_ok = False
        status_str = "[bold green]PASS[/bold green]" if ok else "[bold red]FAIL[/bold red]"
        lat_str = f"{latency_ms:.1f} ms"
        table.add_row(name, status_str, lat_str, detail)

    console.print(table)

    if all_ok:
        console.print(Panel("[bold green]All systems operational.[/bold green]", border_style="green"))
    else:
        console.print(Panel(
            "[bold red]One or more checks failed.[/bold red]\n"
            "[yellow]Make sure Docker is running: docker-compose up -d[/yellow]",
            border_style="red",
        ))
        sys.exit(1)


if __name__ == "__main__":
    main()
