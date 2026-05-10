-- Build streaming silver table for JSONPlaceholder todos from bronze_t2.
-- Source: //home/bronze_stage/bronze_t2
-- Target: //home/silver_stage/streaming_todos

INSERT INTO `//home/silver_stage/streaming_todos` WITH TRUNCATE
SELECT
    CAST(JSON_VALUE(CAST(COALESCE(payload_json, "{}") AS Json), "$.id") AS Int64) AS todo_id,
    CAST(JSON_VALUE(CAST(COALESCE(payload_json, "{}") AS Json), "$.userId") AS Int64) AS user_id,
    JSON_VALUE(CAST(COALESCE(payload_json, "{}") AS Json), "$.title") AS title,
    CAST(JSON_VALUE(CAST(COALESCE(payload_json, "{}") AS Json), "$.completed") AS Bool) AS completed,

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
FROM `//home/bronze_stage/bronze_t2`
WHERE source = "jsonplaceholder_todos";
