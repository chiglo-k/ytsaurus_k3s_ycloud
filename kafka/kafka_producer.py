import json
from typing import Callable, Iterable
from confluent_kafka import Producer
from kafka_event_schema import KafkaEvent


def _delivery_report(err, msg):
    if err is not None:
        print(f"delivery failed: {err}")
    else:
        print(
            f"delivered topic={msg.topic()} "
            f"partition={msg.partition()} "
            f"offset={msg.offset()} "
            f"key={msg.key().decode('utf-8') if msg.key() else None}"
        )


class KafkaEventProducer:
    def __init__(self, bootstrap_servers: str, topic: str):
        self.topic = topic
        self.producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "client.id": "diplom-python-producer",
            "enable.idempotence": True,
            "acks": "all",
            "retries": 5,
            "linger.ms": 50,
            "batch.num.messages": 1000,
        })

    def send_many(
        self,
        events: Iterable[KafkaEvent],
        key_fn: Callable[[KafkaEvent], str] | None = None,
    ) -> int:
        count = 0
        for event in events:
            key = key_fn(event).encode("utf-8") if key_fn else None
            value = json.dumps(event.to_dict(), ensure_ascii=False).encode("utf-8")
            self.producer.produce(
                self.topic,
                key=key,
                value=value,
                callback=_delivery_report,
            )
            self.producer.poll(0)
            count += 1

        self.producer.flush()
        print(f"flushed total={count}")
        return count
