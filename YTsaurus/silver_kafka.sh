yt create map_node //home/silver_stage --ignore-existing || true

yt create table //home/silver_stage/streaming_posts \
  --ignore-existing \
  --attributes '{
    schema = [
      {name=post_id; type=int64;};
      {name=user_id; type=int64;};
      {name=title; type=string;};
      {name=body; type=string;};

      {name=source; type=string;};
      {name=stream_id; type=string;};
      {name=event_type; type=string;};
      {name=event_ts; type=string;};
      {name=entity_id; type=string;};
      {name=queue_timestamp; type=uint64;};
      {name=tablet_index; type=int64;};
      {name=row_index; type=int64;};
      {name=processed_at; type=string;};
      {name=batch_id; type=string;};

      {name=silver_processed_at; type=string;};
    ];
  }'

yt create table //home/silver_stage/streaming_comments \
  --ignore-existing \
  --attributes '{
    schema = [
      {name=comment_id; type=int64;};
      {name=post_id; type=int64;};
      {name=name; type=string;};
      {name=email; type=string;};
      {name=body; type=string;};

      {name=source; type=string;};
      {name=stream_id; type=string;};
      {name=event_type; type=string;};
      {name=event_ts; type=string;};
      {name=entity_id; type=string;};
      {name=queue_timestamp; type=uint64;};
      {name=tablet_index; type=int64;};
      {name=row_index; type=int64;};
      {name=processed_at; type=string;};
      {name=batch_id; type=string;};

      {name=silver_processed_at; type=string;};
    ];
  }'

yt create table //home/silver_stage/streaming_todos \
  --ignore-existing \
  --attributes '{
    schema = [
      {name=todo_id; type=int64;};
      {name=user_id; type=int64;};
      {name=title; type=string;};
      {name=completed; type=boolean;};

      {name=source; type=string;};
      {name=stream_id; type=string;};
      {name=event_type; type=string;};
      {name=event_ts; type=string;};
      {name=entity_id; type=string;};
      {name=queue_timestamp; type=uint64;};
      {name=tablet_index; type=int64;};
      {name=row_index; type=int64;};
      {name=processed_at; type=string;};
      {name=batch_id; type=string;};

      {name=silver_processed_at; type=string;};
    ];
  }'

  