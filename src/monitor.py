"""
monitor.py - Live Pipeline Monitor
Queries PostgreSQL and displays real-time stats about the pipeline.

Improvements over v1:
  - Live latency percentiles panel (p50 / p95 / p99 from recent 1000 events)
  - Event-rate tracking (events/sec in the last 30 s)
  - DLQ file size indicator
  - Configurable refresh rate via MONITOR_REFRESH_SECONDS env var
  - Colour-coded lag indicator (green/yellow/red)

Run in a third terminal for a live dashboard:
    python src/monitor.py
"""

import time
import sys
import os
from pathlib import Path
from datetime import datetime, timezone

import psycopg
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.columns import Columns
from rich.live import Live
from dotenv import load_dotenv

# Force UTF-8 output on Windows
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]

load_dotenv()

console = Console(highlight=False)

try:
    from utils import get_db_config  # type: ignore[import]
except ImportError:
    import os as _os
    def get_db_config() -> dict:  # type: ignore[misc]
        return {
            "host":     _os.getenv("DB_HOST",     "localhost"),
            "port":     int(_os.getenv("DB_PORT", "5432")),
            "dbname":   _os.getenv("DB_NAME",     "pipeline_db"),
            "user":     _os.getenv("DB_USER",     "pipeline_user"),
            "password": _os.getenv("DB_PASSWORD", "pipeline_pass"),
        }

REFRESH_SECONDS = int(os.getenv("MONITOR_REFRESH_SECONDS", "3"))
DLQ_PATH        = Path(os.getenv("DLQ_PATH", "dlq.jsonl"))
HIGH_VALUE_THRESHOLD = float(os.getenv("HIGH_VALUE_THRESHOLD", "200.0"))


def get_stats(conn):
    with conn.cursor() as cur:
        # Totals
        cur.execute("SELECT COUNT(*) FROM raw_events")
        total_raw = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM processed_events")
        total_processed = cur.fetchone()[0]

        cur.execute(
            "SELECT COUNT(*) FROM processed_events WHERE amount > %s",
            (HIGH_VALUE_THRESHOLD,),
        )
        high_value = cur.fetchone()[0]

        # Per-type breakdown
        cur.execute("""
            SELECT event_type,
                   COUNT(*)                          AS cnt,
                   ROUND(AVG(amount)::numeric, 2)    AS avg_amount,
                   ROUND(MAX(amount)::numeric, 2)    AS max_amount
            FROM processed_events
            GROUP BY event_type
            ORDER BY cnt DESC
        """)
        by_type = cur.fetchall()

        # Latency percentiles (last 1000 events with latency data)
        cur.execute("""
            SELECT
                PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY latency_ms) AS p50,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_ms) AS p95,
                PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY latency_ms) AS p99,
                MIN(latency_ms)                                           AS min_ms,
                MAX(latency_ms)                                           AS max_ms,
                AVG(latency_ms)                                           AS mean_ms,
                COUNT(*)                                                  AS n
            FROM (
                SELECT latency_ms FROM processed_events
                WHERE latency_ms IS NOT NULL
                ORDER BY created_at DESC
                LIMIT 1000
            ) recent
        """)
        lat_row = cur.fetchone()  # p50, p95, p99, min, max, mean, n

        # Event rate: count of processed_events in last 30 seconds
        cur.execute("""
            SELECT COUNT(*) FROM processed_events
            WHERE created_at >= NOW() - INTERVAL '30 seconds'
        """)
        recent_30s = cur.fetchone()[0]

    return total_raw, total_processed, high_value, by_type, lat_row, recent_30s


def _colour_lag(lag: int) -> str:
    if lag == 0:
        return f"[green]{lag:,}[/green]"
    if lag < 100:
        return f"[yellow]{lag:,}[/yellow]"
    return f"[bold red]{lag:,}[/bold red]"


def _fmt_ms(v) -> str:
    if v is None:
        return "[dim]N/A[/dim]"
    v = float(v)
    if v < 100:
        colour = "green"
    elif v < 500:
        colour = "yellow"
    else:
        colour = "red"
    if v >= 1000:
        return f"[{colour}]{v/1000:.2f} s[/{colour}]"
    return f"[{colour}]{v:.1f} ms[/{colour}]"


def _dlq_size() -> str:
    if not DLQ_PATH.exists():
        return "[dim]0 events[/dim]"
    lines = sum(1 for _ in DLQ_PATH.open(encoding="utf-8"))
    colour = "green" if lines == 0 else "red"
    return f"[{colour}]{lines} events[/{colour}]"


def build_display(total_raw, total_processed, high_value, by_type, lat_row, recent_30s):
    lag = total_raw - total_processed
    eps_30s = recent_30s / 30.0
    now_str = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")

    # ── Overview panel ────────────────────────────────────────────────────────
    overview = Table.grid(padding=(0, 1))
    overview.add_column(style="bold", min_width=14)
    overview.add_column()
    overview.add_row("Raw Events :", f"[cyan]{total_raw:,}[/cyan]")
    overview.add_row("Processed  :", f"[green]{total_processed:,}[/green]")
    overview.add_row("Lag        :", _colour_lag(lag))
    overview.add_row("High Value :", f"[magenta]{high_value:,}[/magenta]")
    overview.add_row("Rate (30s) :", f"[bold]{eps_30s:.1f} ev/s[/bold]")
    overview.add_row("DLQ        :", _dlq_size())
    overview.add_row("Refreshed  :", f"[dim]{now_str}[/dim]")

    overview_panel = Panel(
        overview,
        title="[bold]Pipeline Status[/bold]",
        border_style="cyan",
        expand=False,
    )

    # ── Latency panel ─────────────────────────────────────────────────────────
    p50, p95, p99, min_ms, max_ms, mean_ms, lat_n = lat_row or (None,) * 7
    lat_grid = Table.grid(padding=(0, 1))
    lat_grid.add_column(style="bold", min_width=8)
    lat_grid.add_column()
    lat_grid.add_row("p50  :", _fmt_ms(p50))
    lat_grid.add_row("p95  :", _fmt_ms(p95))
    lat_grid.add_row("p99  :", _fmt_ms(p99))
    lat_grid.add_row("min  :", _fmt_ms(min_ms))
    lat_grid.add_row("max  :", _fmt_ms(max_ms))
    lat_grid.add_row("mean :", _fmt_ms(mean_ms))
    lat_grid.add_row("n    :", f"[dim]{lat_n or 0:,}[/dim]")

    lat_panel = Panel(
        lat_grid,
        title="[bold]Latency (last 1k)[/bold]",
        border_style="magenta",
        expand=False,
    )

    # ── Events-by-type table ──────────────────────────────────────────────────
    table = Table(
        title="Events by Type",
        border_style="blue",
        header_style="bold blue",
        show_lines=False,
    )
    table.add_column("Event Type",  style="cyan",    min_width=14)
    table.add_column("Count",       justify="right",  style="white")
    table.add_column("Avg $",       justify="right",  style="green")
    table.add_column("Max $",       justify="right",  style="yellow")

    for row in by_type:
        avg = f"${row[2]:.2f}" if row[2] else "N/A"
        mx  = f"${row[3]:.2f}" if row[3] else "N/A"
        table.add_row(row[0], f"{row[1]:,}", avg, mx)

    return Columns([overview_panel, lat_panel, table], equal=False, expand=False)


def main():
    console.print("\n[bold cyan]*** PIPELINE MONITOR v2 STARTING ***[/bold cyan]")

    try:
        conn = psycopg.connect(**get_db_config())
        console.print("[green][OK] Connected to PostgreSQL[/green]\n")
    except Exception as e:
        console.print(f"[red][FAIL] Cannot connect: {e}[/red]")
        return

    console.print(
        f"[dim]Refreshing every {REFRESH_SECONDS}s -- Ctrl+C to exit[/dim]\n"
    )

    try:
        with Live(console=console, refresh_per_second=0.5) as live:
            while True:
                try:
                    stats = get_stats(conn)
                    live.update(build_display(*stats))
                except psycopg.OperationalError:
                    conn = psycopg.connect(**get_db_config())
                    continue
                time.sleep(REFRESH_SECONDS)
    except KeyboardInterrupt:
        console.print("\n[bold yellow]Monitor stopped.[/bold yellow]")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
