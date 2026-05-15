#!/bin/bash
# scripts/init-streaming.sh
# Поднимает всё что нужно для streaming pipeline ПОСЛЕ установки Kafka helm:
#  1) Создаёт __consumer_offsets topic (Kafka 4.0 KRaft mode сам его иногда не создаёт)
#  2) Создаёт topic raw-events
#  3) Создаёт //home/raw_stage/raw_events (dynamic queue, 3 tablets)
#  4) Создаёт //home/raw_stage/raw_events_consumer (queue consumer)
#  5) Регистрирует consumer на queue
#
set -e

KAFKA_POD=${KAFKA_POD:-kafka-controller-0}
KAFKA_NS=${KAFKA_NS:-kafka}

echo "[init-streaming] === Kafka topics ==="

kubectl exec -n $KAFKA_NS $KAFKA_POD -- /opt/bitnami/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic __consumer_offsets \
  --partitions 50 --replication-factor 1 \
  --config cleanup.policy=compact 2>&1 | grep -v WARNING

kubectl exec -n $KAFKA_NS $KAFKA_POD -- /opt/bitnami/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic raw-events \
  --partitions 1 --replication-factor 1 2>&1 | grep -v WARNING

kubectl exec -n $KAFKA_NS $KAFKA_POD -- /opt/bitnami/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list

echo "[init-streaming] === YT raw_stage ==="

source /home/chig_k3s/yt-env/bin/activate
export YT_PROXY=http://localhost:31103
export YT_TOKEN=$(cat /home/chig_k3s/.yt/token)

yt create map_node //home/raw_stage --ignore-existing
yt create map_node //home/bronze_stage --ignore-existing

# raw_events queue (dynamic table with queue ordering)
yt create table //home/raw_stage/raw_events --attributes '{
  dynamic=%true;
  schema=[
    {name=source;          type=string};
    {name=stream_id;       type=string};
    {name=event_type;      type=string};
    {name=event_ts;        type=string};
    {name=entity_id;       type=string};
    {name=payload_json;    type=string};
    {name=kafka_topic;     type=string};
    {name=kafka_partition; type=int64};
    {name=kafka_offset;    type=int64};
    {name=ingested_at;     type=string};
    {name=batch_id;        type=string};
  ]
}' --ignore-existing

# Reshard на 3 tablets и mount
yt unmount-table //home/raw_stage/raw_events 2>/dev/null || true
sleep 3
yt reshard-table //home/raw_stage/raw_events --tablet-count 3
sleep 2
yt mount-table //home/raw_stage/raw_events
sleep 3

# Помечаем как queue
yt set //home/raw_stage/raw_events/@treat_as_queue_consumer '%false'
yt set //home/raw_stage/raw_events/@is_queue '%true'

# Consumer
yt create table //home/raw_stage/raw_events_consumer --attributes '{
  dynamic=%true;
  treat_as_queue_consumer=%true;
  schema=[
    {name=queue_cluster;   type=string; sort_order=ascending};
    {name=queue_path;      type=string; sort_order=ascending};
    {name=partition_index; type=uint64; sort_order=ascending};
    {name=offset;          type=uint64};
    {name=meta;            type=any};
  ]
}' --ignore-existing
yt mount-table //home/raw_stage/raw_events_consumer
sleep 3

# Регистрация
yt register-queue-consumer //home/raw_stage/raw_events //home/raw_stage/raw_events_consumer --vital true 2>/dev/null || true

echo "[init-streaming] === Bronze dynamic tables ==="

for i in 0 1 2; do
  yt create table //home/bronze_stage/bronze_t$i --attributes '{
    dynamic=%true;
    schema=[
      {name=stream_id;       type=string; sort_order=ascending};
      {name=row_index;       type=int64;  sort_order=ascending};
      {name=tablet_index;    type=int64};
      {name=source;          type=string};
      {name=event_type;      type=string};
      {name=event_ts;        type=string};
      {name=entity_id;       type=string};
      {name=payload_json;    type=string};
      {name=queue_timestamp; type=uint64};
      {name=processed_at;    type=string};
      {name=batch_id;        type=string};
    ]
  }' --ignore-existing
  yt mount-table //home/bronze_stage/bronze_t$i 2>/dev/null || true
done

sleep 3
echo "[init-streaming] === Состояние ==="
yt list //home/raw_stage
yt list //home/bronze_stage
yt get //home/raw_stage/raw_events/@tablet_count

echo "[init-streaming] DONE. Можно стартовать systemctl start greenhub-streaming-consumers"
