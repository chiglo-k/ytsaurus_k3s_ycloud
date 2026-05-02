import os
import time
import uuid
from typing import Any
import requests
from kafka_event_schema import KafkaEvent
from kafka_producer import KafkaEventProducer

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "raw-events")
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "30"))

SOURCES = [
    {
        "url": "https://jsonplaceholder.typicode.com/posts",
        "source": "jsonplaceholder_posts",
        "stream_id": "s0",
        "event_type": "post_event",
        "entity_field": "id",
    },
    {
        "url": "https://jsonplaceholder.typicode.com/comments",
        "source": "jsonplaceholder_comments",
        "stream_id": "s1",
        "event_type": "comment_event",
        "entity_field": "id",
    },
    {
        "url": "https://jsonplaceholder.typicode.com/todos",
        "source": "jsonplaceholder_todos",
        "stream_id": "s2",
        "event_type": "todo_event",
        "entity_field": "id",
    },
]


def fetch_json(url: str) -> list[dict[str, Any]]:
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, list):
        raise ValueError(f"Expected list from {url}")
    return data


def build_events(items: list[dict[str, Any]], cfg: dict[str, str]) -> list[KafkaEvent]:
    batch_id = str(uuid.uuid4())
    entity_field = cfg["entity_field"]
    return [
        KafkaEvent.from_payload(
            source=cfg["source"],
            stream_id=cfg["stream_id"],
            event_type=cfg["event_type"],
            entity_id=item[entity_field],
            payload=item,
            ingest_batch_id=batch_id,
        )
        for item in items
    ]


def run_cycle(producer: KafkaEventProducer) -> int:
    total = 0
    for cfg in SOURCES:
        try:
            items = fetch_json(cfg["url"])
            events = build_events(items, cfg)
            producer.send_many(
                events,
                key_fn=lambda e: f"{e.stream_id}:{e.entity_id}",
            )
            total += len(events)
            print(
                f"source={cfg['source']} stream={cfg['stream_id']} "
                f"rows={len(events)}"
            )
        except Exception as e:
            print(f"source={cfg['source']} error: {e}")
    return total


def main() -> None:
    producer = KafkaEventProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        topic=TOPIC,
    )

    print("api_to_kafka started")
    print(f"bootstrap_servers={BOOTSTRAP_SERVERS}")
    print(f"topic={TOPIC}")
    print(f"poll_seconds={POLL_SECONDS}")

    cycle_n = 0
    grand_total = 0
    try:
        while True:
            cycle_n += 1
            total = run_cycle(producer)
            grand_total += total
            print(
                f"cycle={cycle_n} rows={total} "
                f"grand_total={grand_total} sleeping={POLL_SECONDS}s"
            )
            time.sleep(POLL_SECONDS)
    except KeyboardInterrupt:
        print(f"stopped by user, cycles={cycle_n} grand_total={grand_total}")


if __name__ == "__main__":
    main()
