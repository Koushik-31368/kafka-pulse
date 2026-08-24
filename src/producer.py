"""
producer.py - Kafka Event Producer
Generates realistic fake user events and publishes them to a Kafka topic.

Improvements over v1:
  - --rate / -r flag: set events/sec in continuous mode (default 1)
  - --count / -c flag: stop after N events in continuous mode
  - Event schema versioning (schema_version field on every event)
  - Richer payload: country, device_type, currency, discount_pct
  - Producer-side throughput bar printed every 100 events in burst mode
  - Coloured throughput stats in continuous mode (rolling 10s window)

Run:
    python src/producer.py                    # continuous, 1 ev/s
    python src/producer.py --rate 5           # 5 ev/s
    python src/producer.py --burst 5000       # burst 5000 as fast as possible
    python src/producer.py --count 100        # stop after 100 events
"""

import json
import time
import random
import uuid
import sys
import os
import argparse
from collections import deque
from datetime import datetime, timezone

from kafka import KafkaProducer
from faker import Faker
from rich.console import Console
from rich.panel import Panel
from dotenv import load_dotenv

# Force UTF-8 output on Windows
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]

load_dotenv()

console = Console(highlight=False)
fake = Faker()

# --- Configuration ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC",             "user-events")
DEFAULT_INTERVAL        = float(os.getenv("PRODUCER_INTERVAL_SECONDS", "1"))

try:
    from utils import SCHEMA_VERSION  # type: ignore[import]
except ImportError:
    SCHEMA_VERSION = "1.2.0"

EVENT_TYPES = [
    "purchase", "page_view", "add_to_cart",
    "checkout", "login",    "logout",
    "wishlist_add", "search", "review_submit",
]

CATEGORIES   = ["Electronics", "Clothing", "Books", "Sports", "Home", "Beauty", "Toys"]
DEVICE_TYPES = ["mobile", "desktop", "tablet"]
COUNTRIES    = ["US", "IN", "GB", "DE", "FR", "JP", "BR", "CA", "AU", "MX"]
CURRENCIES   = ["USD", "INR", "GBP", "EUR", "JPY", "BRL", "CAD", "AUD"]


# --- Producer Setup ---
def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
        linger_ms=5,           # micro-batching for burst performance
        batch_size=32_768,     # 32 KB batch
    )


def generate_event(burst_batch_id: str | None = None) -> dict:
    """Create a realistic fake e-commerce user event with schema versioning."""
    event_type   = random.choice(EVENT_TYPES)
    user_id      = random.randint(1000, 9999)
    amount       = round(random.uniform(5.0, 500.0), 2)
    discount_pct = round(random.uniform(0, 30), 1) if event_type == "purchase" else 0.0

    payload: dict = {
        "product_id":   random.randint(1, 500),
        "category":     random.choice(CATEGORIES),
        "amount":       amount,
        "currency":     random.choice(CURRENCIES),
        "discount_pct": discount_pct,
        "final_amount": round(amount * (1 - discount_pct / 100), 2),
        "session_id":   str(uuid.uuid4())[:8],
        "ip_address":   fake.ipv4(),
        "user_agent":   fake.user_agent(),
        "device_type":  random.choice(DEVICE_TYPES),
        "country":      random.choice(COUNTRIES),
    }

    if burst_batch_id:
        payload["burst_batch_id"] = burst_batch_id

    return {
        "event_id":      str(uuid.uuid4()),
        "schema_version": SCHEMA_VERSION,
        "event_type":    event_type,
        "user_id":       user_id,
        "timestamp":     datetime.now(timezone.utc).replace(tzinfo=None).isoformat(),
        "payload":       payload,
    }


def _make_error_cb(counter: list[int]):
    """Return a Kafka send-errback that logs and increments *counter[0]*."""
    def _cb(excp):
        counter[0] += 1
        console.print(f"[red]SEND ERROR: {excp}[/red]")
    return _cb


# --- Burst Mode ---
def burst_mode(producer: KafkaProducer, n: int) -> tuple[str, int, float]:
    """Send N messages as fast as possible and report throughput."""
    batch_id = str(uuid.uuid4())[:8]
    console.print(
        f"\n[bold yellow]*** BURST MODE: {n} messages "
        f"(batch_id={batch_id}) ***[/bold yellow]"
    )
    console.print("[dim]No sleep between sends. Timing starts now...[/dim]\n")

    error_count: list[int] = [0]
    on_err = _make_error_cb(error_count)
    t_start = time.perf_counter()
    report_every = max(1, n // 10)

    for i in range(n):
        event = generate_event(burst_batch_id=batch_id)
        producer.send(
            KAFKA_TOPIC,
            key=str(event["user_id"]),
            value=event,
        ).add_errback(on_err)

        if (i + 1) % report_every == 0:
            pct = (i + 1) / n * 100
            bar_filled = int(pct / 5)
            bar = "[" + "#" * bar_filled + "-" * (20 - bar_filled) + "]"
            console.print(
                f"  [dim]{bar} {i+1:>6}/{n}  ({pct:.0f}%)[/dim]"
            )

    producer.flush()
    t_end = time.perf_counter()

    elapsed = t_end - t_start
    eps = n / elapsed if elapsed > 0 else float("inf")

    console.print(f"""
[bold cyan]━━━ BURST SEND COMPLETE ━━━[/bold cyan]
  Messages sent : [green]{n:,}[/green]
  Elapsed       : [yellow]{elapsed:.3f}s[/yellow]
  Throughput    : [bold magenta]{eps:.1f} events/sec[/bold magenta] (producer side)
  Errors        : [red]{error_count[0]}[/red]
  Batch ID      : [dim]{batch_id}[/dim]

[dim]Now wait for the consumer to drain, then run:[/dim]
  [bold]python src/benchmark.py --batch-id {batch_id} --expected {n} --produced-eps {eps:.1f}[/bold]
""")
    return batch_id, n, eps


# --- Continuous Mode ---
def continuous_mode(producer: KafkaProducer, interval: float, max_count: int | None):
    """Normal continuous produce loop with rolling throughput display."""
    error_count: list[int] = [0]
    on_err = _make_error_cb(error_count)
    stats = {"sent": 0}
    # Rolling window: timestamps over 10 s
    rolling: deque[float] = deque()
    t_start = time.perf_counter()

    console.print(
        f"[bold]Producing events at [yellow]{1/interval:.1f} ev/s[/yellow]"
        f"{f' (max {max_count})' if max_count else ''}... "
        f"Press Ctrl+C to stop.\n[/bold]"
    )

    try:
        while True:
            if max_count and stats["sent"] >= max_count:
                console.print(
                    f"[bold green]Reached --count {max_count}. Done.[/bold green]"
                )
                break

            event = generate_event()
            t_sent = time.perf_counter()

            producer.send(
                KAFKA_TOPIC,
                key=str(event["user_id"]),
                value=event,
            ).add_errback(on_err)

            stats["sent"] += 1
            rolling.append(t_sent)

            # Keep only last 10 s in the rolling window
            cutoff = t_sent - 10.0
            while rolling and rolling[0] < cutoff:
                rolling.popleft()
            rolling_eps = len(rolling) / 10.0 if len(rolling) > 1 else 0.0

            console.print(
                f"[green]>> SENT[/green] "
                f"[cyan]{event['event_type']:14}[/cyan] "
                f"user=[yellow]{event['user_id']}[/yellow] "
                f"amount=[magenta]${event['payload']['amount']:.2f}[/magenta] "
                f"[dim]#{stats['sent']}  ~{rolling_eps:.1f} ev/s[/dim]"
            )

            time.sleep(interval)

    except KeyboardInterrupt:
        elapsed = time.perf_counter() - t_start
        avg_eps = stats["sent"] / elapsed if elapsed > 0 else 0
        console.print(
            Panel(
                f"[green]Sent    :[/green] {stats['sent']:,}\n"
                f"[red]Errors  :[/red] {error_count[0]}\n"
                f"[yellow]Elapsed :[/yellow] {elapsed:.1f}s\n"
                f"[cyan]Avg EPS :[/cyan] {avg_eps:.1f} ev/s",
                title="[bold]Producer Stopped[/bold]",
                border_style="yellow",
            )
        )


# --- Main ---
def main():
    parser = argparse.ArgumentParser(description="Kafka Event Producer")
    parser.add_argument(
        "--burst", type=int, default=None, metavar="N",
        help="Send N messages with no sleep (throughput benchmark mode).",
    )
    parser.add_argument(
        "--rate", "-r", type=float, default=None, metavar="EPS",
        help="Events per second in continuous mode (default: 1/PRODUCER_INTERVAL_SECONDS).",
    )
    parser.add_argument(
        "--count", "-c", type=int, default=None, metavar="N",
        help="Stop continuous mode after N events.",
    )
    args = parser.parse_args()

    interval = (1.0 / args.rate) if args.rate else DEFAULT_INTERVAL

    console.print("\n[bold cyan]*** KAFKA EVENT PRODUCER v2 STARTING ***[/bold cyan]")
    console.print(f"   Schema  : [dim]{SCHEMA_VERSION}[/dim]")
    if args.burst:
        console.print(f"   Mode    : [bold yellow]BURST ({args.burst:,} messages)[/bold yellow]")
    else:
        console.print(
            f"   Topic   : [yellow]{KAFKA_TOPIC}[/yellow]  |  "
            f"Rate: [yellow]{1/interval:.1f} ev/s[/yellow]"
        )
    console.print(f"   Broker  : [yellow]{KAFKA_BOOTSTRAP_SERVERS}[/yellow]\n")

    try:
        producer = create_producer()
        console.print("[green][OK] Connected to Kafka broker[/green]\n")
    except Exception as e:
        console.print(f"[red][FAIL] Could not connect to Kafka: {e}[/red]")
        console.print("[yellow][TIP] Make sure Docker is running: docker-compose up -d[/yellow]")
        return

    try:
        if args.burst:
            burst_mode(producer, args.burst)
        else:
            continuous_mode(producer, interval, args.count)
    finally:
        producer.flush()
        producer.close()
        console.print("[dim]Producer closed cleanly.[/dim]")


if __name__ == "__main__":
    main()
