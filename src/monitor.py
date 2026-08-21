"""
monitor.py - Live Pipeline Monitor
Queries PostgreSQL and displays real-time stats about the pipeline.
Run in a third terminal for a live dashboard.
"""

import time
import sys
import os
import psycopg
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.columns import Columns
from rich.live import Live
from dotenv import load_dotenv

# Force UTF-8 output on Windows
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

load_dotenv()

console = Console(highlight=False)

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "dbname": os.getenv("DB_NAME", "pipeline_db"),
    "user": os.getenv("DB_USER", "pipeline_user"),
    "password": os.getenv("DB_PASSWORD", "pipeline_pass"),
}

REFRESH_SECONDS = 3


def get_stats(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM raw_events")
        total_raw = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM processed_events")
        total_processed = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM processed_events WHERE amount > 200")
        high_value = cur.fetchone()[0]

        cur.execute("""
            SELECT event_type, COUNT(*) AS cnt, ROUND(AVG(amount)::numeric, 2) AS avg_amount
            FROM processed_events
            GROUP BY event_type
            ORDER BY cnt DESC
        """)
        by_type = cur.fetchall()

    return total_raw, total_processed, high_value, by_type


def build_display(total_raw, total_processed, high_value, by_type):
    lag = total_raw - total_processed

    overview = Table.grid(padding=1)
    overview.add_column(style="bold")
    overview.add_column()
    overview.add_row("Raw Events :", f"[cyan]{total_raw:,}[/cyan]")
    overview.add_row("Processed  :", f"[green]{total_processed:,}[/green]")
    overview.add_row("Lag        :", f"[yellow]{lag:,}[/yellow]")
    overview.add_row("High Value :", f"[magenta]{high_value:,}[/magenta]")

    panel = Panel(overview, title="[bold]Pipeline Status[/bold]", border_style="cyan", expand=False)

    table = Table(title="Events by Type", border_style="blue", header_style="bold blue")
    table.add_column("Event Type", style="cyan")
    table.add_column("Count", justify="right", style="white")
    table.add_column("Avg Amount", justify="right", style="green")

    for row in by_type:
        amt = f"${row[2]:.2f}" if row[2] else "N/A"
        table.add_row(row[0], str(row[1]), amt)

    return Columns([panel, table], equal=False, expand=False)


def main():
    console.print("\n[bold cyan]*** PIPELINE MONITOR STARTING ***[/bold cyan]")

    try:
        conn = psycopg.connect(**DB_CONFIG)
        console.print("[green][OK] Connected to PostgreSQL[/green]\n")
    except Exception as e:
        console.print(f"[red][FAIL] Cannot connect: {e}[/red]")
        return

    console.print(f"[dim]Refreshing every {REFRESH_SECONDS}s -- Ctrl+C to exit[/dim]\n")

    try:
        with Live(console=console, refresh_per_second=0.5) as live:
            while True:
                try:
                    stats = get_stats(conn)
                    live.update(build_display(*stats))
                except psycopg.OperationalError:
                    conn = psycopg.connect(**DB_CONFIG)
                    continue
                time.sleep(REFRESH_SECONDS)
    except KeyboardInterrupt:
        console.print("\n[bold yellow]Monitor stopped.[/bold yellow]")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
