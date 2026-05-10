#!/usr/bin/env bash
set -euo pipefail

export YT_PROXY="${YT_PROXY:-http://localhost:31103}"

yt create map_node //home/silver_stage --ignore-existing

yt create table //home/silver_stage/streaming_posts \
  --ignore-existing \
  --attributes '{
    schema = [
      {name=post_id; type=int64; required=%false;};
      {name=user_id; type=int64; required=%false;};
      {name=title; type=string; required=%false;};
      {name=body; type=string; required=%false;};
      {name=source; type=string; required=%false;};
      {name=stream_id; type=string; required=%false;};
      {name=event_type; type=string; required=%false;};
      {name=event_ts; type=string; required=%false;};
      {name=entity_id; type=string; required=%false;};
      {name=queue_timestamp; type=uint64; required=%false;};
      {name=tablet_index; type=int64; required=%false;};
      {name=row_index; type=int64; required=%false;};
      {name=processed_at; type=string; required=%false;};
      {name=batch_id; type=string; required=%false;};
      {name=silver_processed_at; type=string; required=%false;};
    ];
  }'

yt create table //home/silver_stage/streaming_comments \
  --ignore-existing \
  --attributes '{
    schema = [
      {name=comment_id; type=int64; required=%false;};
      {name=post_id; type=int64; required=%false;};
      {name=name; type=string; required=%false;};
      {name=email; type=string; required=%false;};
      {name=body; type=string; required=%false;};
      {name=source; type=string; required=%false;};
      {name=stream_id; type=string; required=%false;};
      {name=event_type; type=string; required=%false;};
      {name=event_ts; type=string; required=%false;};
      {name=entity_id; type=string; required=%false;};
      {name=queue_timestamp; type=uint64; required=%false;};
      {name=tablet_index; type=int64; required=%false;};
      {name=row_index; type=int64; required=%false;};
      {name=processed_at; type=string; required=%false;};
      {name=batch_id; type=string; required=%false;};
      {name=silver_processed_at; type=string; required=%false;};
    ];
  }'

yt create table //home/silver_stage/streaming_todos \
  --ignore-existing \
  --attributes '{
    schema = [
      {name=todo_id; type=int64; required=%false;};
      {name=user_id; type=int64; required=%false;};
      {name=title; type=string; required=%false;};
      {name=completed; type=boolean; required=%false;};
      {name=source; type=string; required=%false;};
      {name=stream_id; type=string; required=%false;};
      {name=event_type; type=string; required=%false;};
      {name=event_ts; type=string; required=%false;};
      {name=entity_id; type=string; required=%false;};
      {name=queue_timestamp; type=uint64; required=%false;};
      {name=tablet_index; type=int64; required=%false;};
      {name=row_index; type=int64; required=%false;};
      {name=processed_at; type=string; required=%false;};
      {name=batch_id; type=string; required=%false;};
      {name=silver_processed_at; type=string; required=%false;};
    ];
  }'
