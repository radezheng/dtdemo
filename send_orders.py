#!/usr/bin/env python3
import argparse
import json
import os
import random
import sys
import time
import uuid
from datetime import datetime, timezone

from azure.eventhub import EventData, EventHubProducerClient
from azure.eventhub.exceptions import EventHubError


# Connection string is intentionally NOT embedded in source.
# Preferred: set it in .env (ignored by git) or export as an environment variable.
DEFAULT_CONNECTION_STR = None
PRODUCT_CATALOG = [
    {"sku": "SKU-1000", "name": "Wireless Mouse"},
    {"sku": "SKU-1001", "name": "Mechanical Keyboard"},
    {"sku": "SKU-1002", "name": "27in Monitor"},
    {"sku": "SKU-1003", "name": "USB-C Hub"},
    {"sku": "SKU-1004", "name": "Noise Cancelling Headphones"},
]

STORE_LOCATIONS = [
    {"store_id": "STORE-NY-001", "city": "New York", "region": "US-East"},
    {"store_id": "STORE-CHI-002", "city": "Chicago", "region": "US-Central"},
    {"store_id": "STORE-SF-003", "city": "San Francisco", "region": "US-West"},
    {"store_id": "STORE-LON-004", "city": "London", "region": "EU-West"},
]


def _load_dotenv_if_present(dotenv_path: str) -> None:
    """Best-effort .env loader (no external dependency).

    Supports simple KEY=VALUE lines and ignores comments/blank lines.
    Does not override variables already present in the environment.
    """

    try:
        with open(dotenv_path, "r", encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line or line.startswith("#"):
                    continue
                if line.lower().startswith("export "):
                    line = line[7:].lstrip()
                if "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'").strip("\u201c\u201d\u2018\u2019")
                if not key:
                    continue
                os.environ.setdefault(key, value)
    except FileNotFoundError:
        return


def resolve_connection_string(cli_value: str | None) -> str:
    """Resolve connection string from CLI, .env, or environment variables."""

    if cli_value and cli_value.strip():
        return cli_value.strip()

    # Load .env next to this script (repo root).
    _load_dotenv_if_present(os.path.join(os.path.dirname(__file__), ".env"))

    for name in ("EVENTHUB_SEND_CONNECTION_STRING", "EVENTHUB_CONNECTION_STRING"):
        value = os.getenv(name)
        if value and value.strip():
            return value.strip()

    raise ValueError(
        "Missing Event Hub connection string. Provide --connection-string, "
        "or set EVENTHUB_SEND_CONNECTION_STRING / EVENTHUB_CONNECTION_STRING in .env."
    )


def build_order_event() -> dict:
    product = random.choice(PRODUCT_CATALOG)
    location = random.choice(STORE_LOCATIONS)
    unit_price = round(random.uniform(10.0, 250.0), 2)
    quantity = random.randint(1, 5)
    order_total = round(unit_price * quantity, 2)

    return {
        "order_id": str(uuid.uuid4()),
        "order_timestamp": datetime.now(tz=timezone.utc).isoformat(),
        "store_id": location["store_id"],
        "store_region": location["region"],
        "store_city": location["city"],
        "items": [
            {
                "sku": product["sku"],
                "name": product["name"],
                "quantity": quantity,
                "unit_price": unit_price,
                "total_price": order_total,
            }
        ],
        "payment_method": random.choice(["credit_card", "debit_card", "cash", "mobile_wallet"]),
        "order_total": order_total,
        "loyalty_member": random.choice([True, False]),
    }


def extract_entity_path(connection_str: str) -> str:
    for part in connection_str.split(";"):
        if part.strip().lower().startswith("entitypath="):
            _, entity = part.split("=", 1)
            entity = entity.strip()
            if entity:
                return entity
    raise ValueError("Connection string does not contain an EntityPath segment")


def send_orders(connection_str: str, event_hub: str | None, min_delay: float, max_delay: float, count: int) -> None:
    if min_delay > max_delay:
        raise ValueError("min_delay cannot be greater than max_delay")

    if not event_hub:
        event_hub = extract_entity_path(connection_str)

    producer = EventHubProducerClient.from_connection_string(connection_str, eventhub_name=event_hub)

    try:
        sent = 0
        while count <= 0 or sent < count:
            order = build_order_event()
            event_data = EventData(json.dumps(order))
            batch = producer.create_batch()
            batch.add(event_data)
            print(f"[DEBUG] Sending order payload: {json.dumps(order)}")
            producer.send_batch(batch)

            sent += 1
            print(f"[{sent}] Sent order {order['order_id']} from {order['store_id']}")

            sleep_time = random.uniform(min_delay, max_delay)
            time.sleep(sleep_time)
    finally:
        producer.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Send simulated retail orders to a Microsoft Fabric Eventstream custom endpoint.")
    parser.add_argument(
        "--connection-string",
        default=DEFAULT_CONNECTION_STR,
        help=(
            "Event Hub connection string. If omitted, reads EVENTHUB_SEND_CONNECTION_STRING "
            "(or EVENTHUB_CONNECTION_STRING) from .env/environment."
        ),
    )
    parser.add_argument(
        "--event-hub",
        default=None,
        help=(
            "Event Hub (entity path) name. Defaults to the entity path from the connection string if omitted."
        ),
    )
    parser.add_argument("--min-delay", type=float, default=0.5, help="Minimum delay between events in seconds.")
    parser.add_argument("--max-delay", type=float, default=2.5, help="Maximum delay between events in seconds.")
    parser.add_argument(
        "--count",
        type=int,
        default=0,
        help="Number of events to send. Omit or set to 0 to run indefinitely.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    try:
        connection_str = resolve_connection_string(args.connection_string)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    try:
        send_orders(
            connection_str=connection_str,
            event_hub=args.event_hub,
            min_delay=args.min_delay,
            max_delay=args.max_delay,
            count=args.count,
        )
    except KeyboardInterrupt:
        print("\nStopped by user.")
    except (EventHubError, ValueError) as exc:
        print(f"Failed to send events: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
