-- Build streaming silver table for JSONPlaceholder posts from bronze_t0.
-- Source: //home/bronze_stage/bronze_t0
-- Target: //home/silver_stage/streaming_posts

INSERT INTO `//home/silver_stage/streaming_posts` WITH TRUNCATE
SELECT
    CAST(JSON_VALUE(CAST(COALESCE(payload_json, "{}") AS Json), "$.id") AS Int64) AS post_id,
    CAST(JSON_VALUE(CAST(COALESCE(payload_json, "{}") AS Json), "$.userId") AS Int64) AS user_id,
    JSON_VALUE(CAST(COALESCE(payload_json, "{}") AS Json), "$.title") AS title,
    JSON_VALUE(CAST(COALESCE(payload_json, "{}") AS Json), "$.body") AS body,

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
FROM `//home/bronze_stage/bronze_t0`
WHERE source = "jsonplaceholder_posts";
