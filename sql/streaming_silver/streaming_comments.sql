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
    FROM `//home/bronze_stage/bronze_t1`
    WHERE source = "jsonplaceholder_comments"
);

INSERT INTO `//home/silver_stage/streaming_comments` WITH TRUNCATE
SELECT
    CAST(Yson::LookupInt64(payload, "id") AS Int64) AS comment_id,
    CAST(Yson::LookupInt64(payload, "postId") AS Int64) AS post_id,
    Yson::LookupString(payload, "name") AS name,
    Yson::LookupString(payload, "email") AS email,
    Yson::LookupString(payload, "body") AS body,

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
