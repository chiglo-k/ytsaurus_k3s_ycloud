from confluent_kafka import Consumer
import json


class KafkaRawConsumer:
    def __init__(self, bootstrap_servers: str, topic: str, group_id: str):
        self.topic = topic
        self.consumer = Consumer({
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })

    def subscribe(self):
        def on_assign(consumer, partitions):
            print(f"partition_assigned: {partitions}")

        def on_revoke(consumer, partitions):
            print(f"partition_revoked: {partitions}")

        self.consumer.subscribe(
            [self.topic],
            on_assign=on_assign,
            on_revoke=on_revoke,
        )

    def poll_batch(self, batch_size: int = 100, timeout: float = 1.0):
        rows = []
        while len(rows) < batch_size:
            msg = self.consumer.poll(timeout)
            if msg is None:
                if rows:
                    break          # уже что-то собрали — отдаём батч
                else:
                    continue       # ничего нет — ждём первое сообщение
            if msg.error():
                print(f"poll error: {msg.error()}")
                continue
            try:
                payload = json.loads(msg.value().decode("utf-8"))
            except Exception as e:
                print(f"json decode error: {e}")
                continue
            rows.append((msg, payload))
        return rows

    def commit(self):
        self.consumer.commit(asynchronous=False)

    def close(self):
        self.consumer.close()
