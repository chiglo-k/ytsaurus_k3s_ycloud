import os
import uuid
from datetime import datetime, timezone
from kafka_consumer import KafkaRawConsumer
import yt.wrapper as yt

RAW_TABLE = "//home/raw_stage/raw_events"
TOPIC = "raw-events"

# Маппинг stream_id -> tablet_index
STREAM_TO_TABLET = {
    "s0": 0,
    "s1": 1,
    "s2": 2,
}


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def setup_yt_client():
    proxy = os.getenv("YT_PROXY", "http://localhost:31103")
    yt.config["proxy"]["url"] = proxy
    yt.config["proxy"]["enable_proxy_discovery"] = False


def main():
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "10.130.0.27:30092")
    group_id = os.getenv("KAFKA_GROUP_ID", "raw-consumer-v1")

    setup_yt_client()

    consumer = KafkaRawConsumer(
        bootstrap_servers=bootstrap_servers,
        topic=TOPIC,
        group_id=group_id,
    )
    consumer.subscribe()

    print("kafka_to_raw started")
    print(f"bootstrap_servers={bootstrap_servers}")
    print(f"topic={TOPIC}")
    print(f"raw_table={RAW_TABLE}")

    try:
        while True:
            polled = consumer.poll_batch(batch_size=100, timeout=2.0)
            if not polled:
                continue

            batch_id = str(uuid.uuid4())

            # группируем по таблетам
            rows_by_tablet = {0: [], 1: [], 2: []}
            for msg, payload in polled:
                stream_id = payload.get("stream_id", "s0")
                tablet_index = STREAM_TO_TABLET.get(stream_id, 0)

                row = {
                    "source": payload.get("source"),
                    "stream_id": stream_id,
                    "event_type": payload.get("event_type"),
                    "event_ts": payload.get("event_ts"),
                    "entity_id": str(payload.get("entity_id")),
                    "payload_json": payload.get("payload_json"),
                    "kafka_topic": msg.topic(),
                    "kafka_partition": msg.partition(),
                    "kafka_offset": msg.offset(),
                    "ingested_at": utc_now(),
                    "batch_id": batch_id,
                    "$tablet_index": tablet_index,
                }
                rows_by_tablet[tablet_index].append(row)

            all_rows = []
            for tablet, rows in rows_by_tablet.items():
                all_rows.extend(rows)

            if all_rows:
                yt.insert_rows(RAW_TABLE, all_rows, raw=False)
                print(
                    f"inserted_to_raw rows={len(all_rows)} "
                    f"t0={len(rows_by_tablet[0])} "
                    f"t1={len(rows_by_tablet[1])} "
                    f"t2={len(rows_by_tablet[2])} "
                    f"batch_id={batch_id}"
                )

            consumer.commit()

    except KeyboardInterrupt:
        print("stopped by user")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
