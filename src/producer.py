"""
producer.py - Kafka Event Producer
Generates realistic fake user events and publishes them to a Kafka topic.
Run this in one terminal while the consumer runs in another.
"""

import json
import time
import random
import uuid
import sys
import os
import argparse
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker
from rich.console import Console
from dotenv import load_dotenv

# Force UTF-8 output on Windows
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

load_dotenv()

console = Console(highlight=False)
fake = Faker()

# --- Configuration ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "user-events")
INTERVAL = float(os.getenv("PRODUCER_INTERVAL_SECONDS", "1"))

EVENT_TYPES = ["purchase", "page_view", "add_to_cart", "checkout", "login", "logout"]

# --- Producer Setup ---
def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
    )


def generate_event():
    """Create a realistic fake e-commerce user event."""
    event_type = random.choice(EVENT_TYPES)
    user_id = random.randint(1000, 9999)

    payload = {
        "product_id": random.randint(1, 500),
        "category": random.choice(["Electronics", "Clothing", "Books", "Sports", "Home"]),
        "amount": round(random.uniform(5.0, 500.0), 2),
        "session_id": str(uuid.uuid4())[:8],
        "ip_address": fake.ipv4(),
        "user_agent": fake.user_agent(),
    }

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "user_id": user_id,
        "timestamp": datetime.utcnow().isoformat(),
        "payload": payload,
    }


def on_send_error(excp):
    console.print(f"[red]SEND ERROR: {excp}[/red]")


# --- Burst Mode ---
def burst_mode(producer, n: int):
    """Send N messages as fast as possible and report throughput."""
    batch_id = str(uuid.uuid4())[:8]  # tag so we can query just this batch
    console.print(f"\n[bold yellow]*** BURST MODE: {n} messages (batch_id={batch_id}) ***[/bold yellow]")
    console.print("[dim]No sleep between sends. Timing starts now...[/dim]\n")

    errors = 0
    t_start = time.perf_counter()

    for i in range(n):
        event = generate_event()
        # Embed the batch_id inside the payload so we can filter later
        event["payload"]["burst_batch_id"] = batch_id

        producer.send(
            KAFKA_TOPIC,
            key=str(event["user_id"]),
            value=event,
        ).add_errback(lambda excp: (on_send_error(excp), globals().update({"errors": errors + 1})))

        if (i + 1) % max(1, n // 10) == 0:
            pct = (i + 1) / n * 100
            console.print(f"  [dim]sent {i+1:>6}/{n}  ({pct:.0f}%)[/dim]")

    # flush blocks until all sends are acked
    producer.flush()
    t_end = time.perf_counter()

    elapsed = t_end - t_start
    eps = n / elapsed if elapsed > 0 else float("inf")

    console.print(f"""
[bold cyan]━━━ BURST SEND COMPLETE ━━━[/bold cyan]
  Messages sent : [green]{n}[/green]
  Elapsed       : [yellow]{elapsed:.3f}s[/yellow]
  Throughput    : [bold magenta]{eps:.1f} events/sec[/bold magenta] (producer side)
  Errors        : [red]{errors}[/red]
  Batch ID      : [dim]{batch_id}[/dim]

[dim]Now wait for the consumer to drain, then run:[/dim]
  [bold]python src/benchmark.py --batch-id {batch_id}[/bold]
""")
    return batch_id, n, eps


# --- Continuous Mode ---
def continuous_mode(producer):
    """Normal continuous produce loop (original behaviour)."""
    stats = {"sent": 0, "errors": 0}
    console.print("[bold]Producing events... Press Ctrl+C to stop.\n[/bold]")

    try:
        while True:
            event = generate_event()

            producer.send(
                KAFKA_TOPIC,
                key=str(event["user_id"]),
                value=event,
            ).add_errback(on_send_error)

            stats["sent"] += 1
            console.print(
                f"[green]>> SENT[/green] "
                f"[cyan]{event['event_type']:12}[/cyan] "
                f"user=[yellow]{event['user_id']}[/yellow] "
                f"amount=[magenta]${event['payload']['amount']:.2f}[/magenta] "
                f"[dim]#{stats['sent']}[/dim]"
            )

            time.sleep(INTERVAL)

    except KeyboardInterrupt:
        console.print(f"\n[bold yellow]STOPPED. Total events sent: {stats['sent']}[/bold yellow]")


# --- Main ---
def main():
    parser = argparse.ArgumentParser(description="Kafka Event Producer")
    parser.add_argument(
        "--burst",
        type=int,
        default=None,
        metavar="N",
        help="Send N messages with no sleep (throughput benchmark mode).",
    )
    args = parser.parse_args()

    console.print("\n[bold cyan]*** KAFKA EVENT PRODUCER STARTING ***[/bold cyan]")
    if args.burst:
        console.print(f"   Mode  : [bold yellow]BURST ({args.burst} messages)[/bold yellow]")
    else:
        console.print(f"   Topic : [yellow]{KAFKA_TOPIC}[/yellow]  |  Interval: [yellow]{INTERVAL}s[/yellow]")
    console.print(f"   Broker: [yellow]{KAFKA_BOOTSTRAP_SERVERS}[/yellow]\n")

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
            continuous_mode(producer)
    finally:
        producer.flush()
        producer.close()
        console.print("[dim]Producer closed cleanly.[/dim]")


if __name__ == "__main__":
    main()
