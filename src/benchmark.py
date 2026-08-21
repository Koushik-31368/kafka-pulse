"""
benchmark.py - Burst Benchmark Reporter
Queries processed_events for a specific burst batch and prints:
  - p50 / p95 / p99 end-to-end latency (ms)
  - Producer throughput  (passed in or estimated from DB timestamps)
  - Consumer throughput  (events/sec, derived from created_at spread)

Usage:
    python src/benchmark.py --batch-id <BATCH_ID> [--produced-eps <float>]

  --batch-id     The 8-char batch ID printed by --burst mode.
  --produced-eps Producer events/sec from the burst output (optional but
                 recommended for an accurate side-by-side comparison).
  --wait         Seconds to wait for the consumer to finish before querying
                 (default 5). Increase if consumer is slow.
"""

import sys
import os
import time
import argparse
import psycopg
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from dotenv import load_dotenv

# Force UTF-8 on Windows
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


def wait_for_batch(conn, batch_id: str, expected: int, timeout: int = 120):
    """Poll until expected rows are inserted or timeout is reached."""
    console.print(f"\n[dim]Waiting for {expected} rows with batch_id={batch_id} "
                  f"to land in processed_events...[/dim]")
    deadline = time.time() + timeout
    while time.time() < deadline:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM processed_events pe
                JOIN raw_events re ON re.event_id = pe.event_id
                WHERE re.payload->>'burst_batch_id' = %s
                """,
                (batch_id,),
            )
            count = cur.fetchone()[0]
        if count >= expected:
            console.print(f"[green]  All {count} rows found.[/green]")
            return count
        console.print(f"  [dim]{count}/{expected} rows so far...[/dim]")
        time.sleep(2)
    console.print(f"[yellow][WARN] Timeout – only {count}/{expected} rows found. "
                  f"Results will be partial.[/yellow]")
    return count


def fetch_latency_percentiles(conn, batch_id: str):
    """Return (count, p50, p95, p99, min, max, mean) latency_ms for the batch."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                COUNT(*)                                              AS n,
                PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY pe.latency_ms) AS p50,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY pe.latency_ms) AS p95,
                PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY pe.latency_ms) AS p99,
                MIN(pe.latency_ms)                                    AS min_ms,
                MAX(pe.latency_ms)                                    AS max_ms,
                AVG(pe.latency_ms)                                    AS mean_ms
            FROM processed_events pe
            JOIN raw_events re ON re.event_id = pe.event_id
            WHERE re.payload->>'burst_batch_id' = %s
              AND pe.latency_ms IS NOT NULL
            """,
            (batch_id,),
        )
        row = cur.fetchone()
    return row  # (n, p50, p95, p99, min, max, mean)


def fetch_consumer_throughput(conn, batch_id: str):
    """Derive consumer events/sec from the created_at timestamps of the batch."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                MIN(pe.created_at) AS first_at,
                MAX(pe.created_at) AS last_at,
                COUNT(*)            AS n
            FROM processed_events pe
            JOIN raw_events re ON re.event_id = pe.event_id
            WHERE re.payload->>'burst_batch_id' = %s
            """,
            (batch_id,),
        )
        first_at, last_at, n = cur.fetchone()

    if first_at and last_at and n and n > 1:
        elapsed = (last_at - first_at).total_seconds()
        eps = (n - 1) / elapsed if elapsed > 0 else float("inf")
    else:
        eps = None
    return eps, n


def main():
    parser = argparse.ArgumentParser(description="Burst Benchmark Reporter")
    parser.add_argument("--batch-id", required=True, help="8-char burst batch ID")
    parser.add_argument(
        "--expected",
        type=int,
        default=None,
        help="Number of messages sent (for wait-polling). If omitted, skips wait.",
    )
    parser.add_argument(
        "--produced-eps",
        type=float,
        default=None,
        help="Producer throughput (events/sec) reported by --burst, for comparison.",
    )
    parser.add_argument(
        "--wait",
        type=int,
        default=5,
        help="Extra seconds to sleep before querying (default 5).",
    )
    args = parser.parse_args()

    console.print("\n[bold cyan]*** KAFKA-PULSE BENCHMARK REPORTER ***[/bold cyan]")
    console.print(f"   Batch ID: [yellow]{args.batch_id}[/yellow]\n")

    try:
        conn = psycopg.connect(**DB_CONFIG)
    except Exception as e:
        console.print(f"[red][FAIL] Cannot connect to PostgreSQL: {e}[/red]")
        return

    if args.wait > 0:
        console.print(f"[dim]Sleeping {args.wait}s to let consumer catch up...[/dim]")
        time.sleep(args.wait)

    if args.expected:
        wait_for_batch(conn, args.batch_id, args.expected)

    # --- Latency percentiles ---
    row = fetch_latency_percentiles(conn, args.batch_id)
    n, p50, p95, p99, min_ms, max_ms, mean_ms = row

    # --- Consumer throughput ---
    consumer_eps, total_rows = fetch_consumer_throughput(conn, args.batch_id)

    conn.close()

    # --- Pretty print ---
    lat_table = Table(
        title=f"End-to-End Latency  (batch_id={args.batch_id}, n={n})",
        border_style="cyan",
        header_style="bold cyan",
    )
    lat_table.add_column("Metric", style="bold")
    lat_table.add_column("Value", justify="right", style="magenta")

    def fmt(v):
        return f"{v:.2f} ms" if v is not None else "N/A"

    lat_table.add_row("p50 (median)", fmt(p50))
    lat_table.add_row("p95",         fmt(p95))
    lat_table.add_row("p99",         fmt(p99))
    lat_table.add_row("min",         fmt(min_ms))
    lat_table.add_row("max",         fmt(max_ms))
    lat_table.add_row("mean",        fmt(mean_ms))

    tput_table = Table(
        title="Throughput",
        border_style="green",
        header_style="bold green",
    )
    tput_table.add_column("Side",    style="bold")
    tput_table.add_column("events/sec", justify="right", style="yellow")

    if args.produced_eps is not None:
        tput_table.add_row("Producer (timed)", f"{args.produced_eps:.1f}")
    else:
        tput_table.add_row("Producer (timed)", "[dim]pass --produced-eps[/dim]")

    if consumer_eps is not None:
        tput_table.add_row("Consumer (DB timestamps)", f"{consumer_eps:.1f}")
    else:
        tput_table.add_row("Consumer (DB timestamps)", "N/A (need ≥2 rows)")

    console.print(lat_table)
    console.print(tput_table)

    if n == 0:
        console.print(
            Panel(
                "[yellow]No latency rows found — make sure the consumer has processed the batch "
                "and that add_latency_ms.sql migration was applied.[/yellow]",
                title="[red]No Data[/red]",
                border_style="red",
            )
        )
    else:
        console.print(
            Panel(
                f"[green]Batch complete.[/green]  "
                f"Rows with latency data: [cyan]{n}[/cyan] / {total_rows} total.",
                title="Summary",
                border_style="green",
            )
        )


if __name__ == "__main__":
    main()
