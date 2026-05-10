-- Build streaming silver table for JSONPlaceholder comments from bronze_t1.
-- Source: //home/bronze_stage/bronze_t1
-- Target: //home/silver_stage/streaming_comments

INSERT INTO `//home/silver_stage/streaming_comments` WITH TRUNCATE
SELECT
    CAST(JSON_VALUE(CAST(COALESCE(payload_json, "{}") AS Json), "$.id") AS Int64) AS comment_id,
    CAST(JSON_VALUE(CAST(COALESCE(payload_json, "{}") AS Json), "$.postId") AS Int64) AS post_id,
    JSON_VALUE(CAST(COALESCE(payload_json, "{}") AS Json), "$.name") AS name,
    JSON_VALUE(CAST(COALESCE(payload_json, "{}") AS Json), "$.email") AS email,
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
FROM `//home/bronze_stage/bronze_t1`
WHERE source = "jsonplaceholder_comments";
