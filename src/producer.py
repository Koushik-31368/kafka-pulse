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


# --- Main ---
def main():
    console.print("\n[bold cyan]*** KAFKA EVENT PRODUCER STARTING ***[/bold cyan]")
    console.print(f"   Topic : [yellow]{KAFKA_TOPIC}[/yellow]  |  Interval: [yellow]{INTERVAL}s[/yellow]")
    console.print(f"   Broker: [yellow]{KAFKA_BOOTSTRAP_SERVERS}[/yellow]\n")

    try:
        producer = create_producer()
        console.print("[green][OK] Connected to Kafka broker[/green]\n")
    except Exception as e:
        console.print(f"[red][FAIL] Could not connect to Kafka: {e}[/red]")
        console.print("[yellow][TIP] Make sure Docker is running: docker-compose up -d[/yellow]")
        return

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
    finally:
        producer.flush()
        producer.close()
        console.print("[dim]Producer closed cleanly.[/dim]")


if __name__ == "__main__":
    main()
