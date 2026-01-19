#!/usr/bin/env python3
"""Consume order events from an Event Hub and print the payloads."""

import argparse
import json
import os
import sys
import threading
from typing import Optional, Set

from azure.eventhub import EventData, EventHubConsumerClient, PartitionContext
from azure.eventhub.exceptions import EventHubError


# Connection string is intentionally NOT embedded in source.
# Preferred: set it in .env (ignored by git) or export as an environment variable.
DEFAULT_CONNECTION_STR = None
DEFAULT_CONSUMER_GROUP = "$Default"


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

    _load_dotenv_if_present(os.path.join(os.path.dirname(__file__), ".env"))

    for name in ("EVENTHUB_RECEIVE_CONNECTION_STRING", "EVENTHUB_CONNECTION_STRING"):
        value = os.getenv(name)
        if value and value.strip():
            return value.strip()

    raise ValueError(
        "Missing Event Hub connection string. Provide --connection-string, "
        "or set EVENTHUB_RECEIVE_CONNECTION_STRING / EVENTHUB_CONNECTION_STRING in .env."
    )


def extract_entity_path(connection_str: str) -> str:
    for segment in connection_str.split(";"):
        if segment.strip().lower().startswith("entitypath="):
            _, entity = segment.split("=", 1)
            entity = entity.strip()
            if entity:
                return entity
    raise ValueError("Connection string does not contain an EntityPath segment")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Receive and print retail order events from a Microsoft Fabric Eventstream custom endpoint."
    )
    parser.add_argument(
        "--connection-string",
        default=DEFAULT_CONNECTION_STR,
        help=(
            "Event Hub connection string. If omitted, reads EVENTHUB_RECEIVE_CONNECTION_STRING "
            "(or EVENTHUB_CONNECTION_STRING) from .env/environment."
        ),
    )
    parser.add_argument(
        "--event-hub",
        default=None,
        help="Event Hub (entity path) name. Defaults to the entity path in the connection string if omitted.",
    )
    parser.add_argument(
        "--consumer-group",
        default=DEFAULT_CONSUMER_GROUP,
        help="Consumer group to use when receiving events.",
    )
    parser.add_argument(
        "--store-id",
        dest="store_ids",
        action="append",
        help="Filter to events whose payload store_id matches any provided value. Repeat to supply multiple IDs.",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=0,
        help="Maximum events to receive before exiting. Set to 0 to run indefinitely.",
    )
    return parser.parse_args()


def on_event(
    partition_context: PartitionContext,
    event: EventData,
    store_filter: Optional[Set[str]],
) -> bool:
    """Print a single received event that matches the optional store filter."""
    data = event.body_as_str(encoding="UTF-8")
    try:
        payload = json.loads(data)
    except json.JSONDecodeError:
        print(f"[WARN] Received non-JSON payload: {data}")
        payload = data

    if store_filter:
        store_id = payload.get("store_id") if isinstance(payload, dict) else None
        if store_id not in store_filter:
            return False

    print(f"[DEBUG] Received raw event: {data}")
    print(
        f"[PARTITION {partition_context.partition_id}] Sequence {event.sequence_number} "
        f"enqueued at {event.enqueued_time.isoformat()}"
    )
    print(json.dumps(payload, indent=2, ensure_ascii=False))

    print("-")
    return True


def receive_events(
    connection_str: str,
    event_hub: Optional[str],
    consumer_group: str,
    max_events: int,
    store_ids: Optional[Set[str]],
) -> int:
    if not event_hub:
        event_hub = extract_entity_path(connection_str)

    store_filter = {store_id.strip() for store_id in store_ids if store_id and store_id.strip()} if store_ids else None

    client = EventHubConsumerClient.from_connection_string(
        conn_str=connection_str,
        consumer_group=consumer_group,
        eventhub_name=event_hub,
    )

    remaining = max_events if max_events > 0 else None
    stop_event = threading.Event()

    def _on_event(partition_context: PartitionContext, event: EventData) -> None:
        nonlocal remaining
        processed = on_event(partition_context, event, store_filter)
        if processed and remaining is not None:
            remaining -= 1
            if remaining <= 0:
                stop_event.set()
        partition_context.update_checkpoint(event)

    def _on_error(partition_context: Optional[PartitionContext], error: Exception) -> None:
        location = partition_context.partition_id if partition_context else "(no partition)"
        print(f"[ERROR] Partition {location}: {error}", file=sys.stderr)

    try:
        with client:
            client.receive(
                on_event=_on_event,
                on_error=_on_error,
                starting_position="-1",  # receive from the earliest available event
                stop_event=stop_event,
            )
    except (EventHubError, ValueError) as exc:
        print(f"Failed to receive events: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("Stopped by user.")
    return 0


def main() -> int:
    args = parse_args()

    try:
        connection_str = resolve_connection_string(args.connection_string)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    return receive_events(
        connection_str=connection_str,
        event_hub=args.event_hub,
        consumer_group=args.consumer_group,
        max_events=args.max_events,
        store_ids={store_id.strip() for store_id in args.store_ids if store_id and store_id.strip()} if args.store_ids else None,
    )


if __name__ == "__main__":
    raise SystemExit(main())
