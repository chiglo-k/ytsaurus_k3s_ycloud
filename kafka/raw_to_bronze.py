import os
import time
from datetime import datetime, timezone
import yt.wrapper as yt

RAW_TABLE = "//home/raw_stage/raw_events"
CONSUMER_PATH = "//home/raw_stage/raw_events_consumer"

TABLET_TO_BRONZE = {
    0: "//home/bronze_stage/bronze_t0",
    1: "//home/bronze_stage/bronze_t1",
    2: "//home/bronze_stage/bronze_t2",
}

BATCH_SIZE = 100
POLL_SLEEP_SEC = 2.0


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def setup_yt_client():
    proxy = os.getenv("YT_PROXY", "http://localhost:31103")
    yt.config["proxy"]["url"] = proxy
    yt.config["proxy"]["enable_proxy_discovery"] = False


def get_consumer_offset(tablet_index: int) -> int:
    rows = list(yt.select_rows(
        f"[offset] from [{CONSUMER_PATH}] "
        f"where [queue_path] = '{RAW_TABLE}' and [partition_index] = {tablet_index}"
    ))
    if not rows:
        return 0
    return int(rows[0].get("offset", 0))


def process_tablet(tablet_index: int) -> int:
    bronze_table = TABLET_TO_BRONZE[tablet_index]
    offset = get_consumer_offset(tablet_index)

    rows = list(yt.pull_consumer(
        consumer_path=CONSUMER_PATH,
        queue_path=RAW_TABLE,
        partition_index=tablet_index,
        offset=offset,
        max_row_count=BATCH_SIZE,
    ))
    if not rows:
        return 0

    processed_at = utc_now()
    bronze_rows = []
    last_row_index = offset - 1
    for row in rows:
        ridx = row.get("$row_index")
        bronze_rows.append({
            "source": row.get("source"),
            "stream_id": row.get("stream_id"),
            "event_type": row.get("event_type"),
            "event_ts": row.get("event_ts"),
            "entity_id": row.get("entity_id"),
            "payload_json": row.get("payload_json"),
            "queue_timestamp": row.get("$timestamp"),
            "tablet_index": tablet_index,
            "row_index": ridx,
            "processed_at": processed_at,
            "batch_id": row.get("batch_id"),
        })
        if ridx is not None:
            last_row_index = max(last_row_index, ridx)

    # bronze_t* — dynamic tables → insert_rows (а не write_table)
    yt.insert_rows(bronze_table, bronze_rows, raw=False)

    new_offset = last_row_index + 1
    yt.advance_consumer(
        consumer_path=CONSUMER_PATH,
        queue_path=RAW_TABLE,
        partition_index=tablet_index,
        old_offset=offset,
        new_offset=new_offset,
    )
    return len(rows)


def main():
    setup_yt_client()
    print("raw_to_bronze started")
    print(f"raw_table={RAW_TABLE}")
    print(f"consumer={CONSUMER_PATH}")
    for t, p in TABLET_TO_BRONZE.items():
        print(f"  tablet {t} -> {p}")

    try:
        while True:
            total = 0
            per_tablet = {}
            for tablet_index in TABLET_TO_BRONZE.keys():
                try:
                    n = process_tablet(tablet_index)
                except Exception as e:
                    print(f"tablet {tablet_index} error: {e}")
                    n = 0
                per_tablet[tablet_index] = n
                total += n

            if total > 0:
                print(
                    f"processed total={total} "
                    f"t0={per_tablet[0]} t1={per_tablet[1]} t2={per_tablet[2]} "
                    f"at={utc_now()}"
                )
            else:
                time.sleep(POLL_SLEEP_SEC)
    except KeyboardInterrupt:
        print("stopped by user")


if __name__ == "__main__":
    main()
