$parsed = (
    SELECT
        Yson::ParseJson(COALESCE(payload_json, "{}")) AS payload,
        source,
        stream_id,
        event_type,
        event_ts,
        entity_id,
        queue_timestamp,
        tablet_index,
        row_index,
        processed_at,
        batch_id
    FROM `//home/bronze_stage/bronze_t2`
    WHERE source = "jsonplaceholder_todos"
);

INSERT INTO `//home/silver_stage/streaming_todos` WITH TRUNCATE
SELECT
    CAST(Yson::LookupInt64(payload, "id") AS Int64) AS todo_id,
    CAST(Yson::LookupInt64(payload, "userId") AS Int64) AS user_id,
    Yson::LookupString(payload, "title") AS title,
    CAST(Yson::LookupBool(payload, "completed") AS Bool) AS completed,

    source,
    stream_id,
    event_type,
    event_ts,
    entity_id,
    queue_timestamp,
    tablet_index,
    row_index,
    processed_at,
    batch_id,

    CAST(CurrentUtcDatetime() AS String) AS silver_processed_at
FROM $parsed;
