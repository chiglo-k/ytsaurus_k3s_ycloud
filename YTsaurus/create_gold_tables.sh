#!/usr/bin/env bash

set -euo pipefail

GOLD_ROOT="//home/gold_stage"

yt create map_node "${GOLD_ROOT}" --recursive --ignore-existing

GOLD_TABLE="${GOLD_ROOT}/gold_daily_country_stats"
yt exists "${GOLD_TABLE}" && yt remove --force "${GOLD_TABLE}"
yt create table "${GOLD_TABLE}" --attributes '{
  schema=[
    {name=date_local;             type=string;    required=%true};
    {name=country_code;           type=string};
    {name=n_events;               type=int64;     required=%true};
    {name=n_devices;              type=int64;     required=%true};
    {name=avg_battery_level;      type=double};
    {name=avg_temperature;        type=double};
    {name=avg_voltage;            type=double};
    {name=avg_memory_used_pct;    type=double};
    {name=avg_storage_used_pct;   type=double};
    {name=n_charging_events;      type=int64};
    {name=n_low_battery_events;   type=int64};
    {name=n_wifi_events;          type=int64};
    {name=n_night_events;         type=int64};
    {name=charging_rate;          type=double};
    {name=low_battery_rate;       type=double};
    {name=wifi_rate;              type=double};
    {name=_gold_batch_id;         type=string;    required=%true};
    {name=_gold_built_at;         type=string;    required=%true};
  ];
}'
echo "[OK] ${GOLD_TABLE}"

GOLD_TABLE="${GOLD_ROOT}/gold_country_overview"
yt exists "${GOLD_TABLE}" && yt remove --force "${GOLD_TABLE}"
yt create table "${GOLD_TABLE}" --attributes '{
  schema=[
    {name=country_code;           type=string;    required=%true};
    {name=n_devices;              type=int64;     required=%true};
    {name=n_events;               type=int64;     required=%true};
    {name=n_active_days;          type=int64};
    {name=first_date;             type=string};
    {name=last_date;              type=string};
    {name=avg_battery_level;      type=double};
    {name=avg_temperature;        type=double};
    {name=avg_memory_used_pct;    type=double};
    {name=avg_storage_used_pct;   type=double};
    {name=n_charging_events;      type=int64};
    {name=n_low_battery_events;   type=int64};
    {name=n_wifi_events;          type=int64};
    {name=charging_rate;          type=double};
    {name=low_battery_rate;       type=double};
    {name=wifi_rate;              type=double};
    {name=events_per_device;      type=double};
    {name=_gold_batch_id;         type=string;    required=%true};
    {name=_gold_built_at;         type=string;    required=%true};
  ];
}'
echo "[OK] ${GOLD_TABLE}"

GOLD_TABLE="${GOLD_ROOT}/gold_device_lifecycle"
yt exists "${GOLD_TABLE}" && yt remove --force "${GOLD_TABLE}"
yt create table "${GOLD_TABLE}" --attributes '{
  schema=[
    {name=device_id;              type=int64;     required=%true};
    {name=country_code;           type=string};
    {name=first_seen;             type=string;    required=%true};
    {name=last_seen;              type=string;    required=%true};
    {name=lifetime_days;          type=double;    required=%true};
    {name=n_events;               type=int64;     required=%true};
    {name=n_active_days;          type=int64;     required=%true};
    {name=avg_battery_level;      type=double};
    {name=min_battery_level;      type=double};
    {name=avg_temperature;        type=double};
    {name=avg_memory_used_pct;    type=double};
    {name=avg_storage_used_pct;   type=double};
    {name=n_charging_events;      type=int64};
    {name=n_wifi_events;          type=int64};
    {name=n_low_battery_events;   type=int64};
    {name=events_per_active_day;  type=double};
    {name=_gold_batch_id;         type=string;    required=%true};
    {name=_gold_built_at;         type=string;    required=%true};
  ];
}'
echo "[OK] ${GOLD_TABLE}"

GOLD_TABLE="${GOLD_ROOT}/gold_hourly_battery_health"
yt exists "${GOLD_TABLE}" && yt remove --force "${GOLD_TABLE}"
yt create table "${GOLD_TABLE}" --attributes '{
  schema=[
    {name=date_local;             type=string;    required=%true};
    {name=hour_of_day;            type=int64;     required=%true};
    {name=country_code;           type=string};
    {name=n_events;               type=int64;     required=%true};
    {name=n_devices;              type=int64;     required=%true};
    {name=avg_battery_level;      type=double};
    {name=avg_temperature;        type=double};
    {name=max_temperature;        type=double};
    {name=avg_voltage;            type=double};
    {name=n_charging_events;      type=int64};
    {name=n_low_battery_events;   type=int64};
    {name=n_night_events;         type=int64};
    {name=_gold_batch_id;         type=string;    required=%true};
    {name=_gold_built_at;         type=string;    required=%true};
  ];
}'
echo "[OK] ${GOLD_TABLE}"

echo ""
echo "Gold tables created:"
yt list "${GOLD_ROOT}"

echo ""
echo "Schema check:"
for tbl in gold_daily_country_stats gold_country_overview gold_device_lifecycle gold_hourly_battery_health; do
    echo ""
    echo "=== ${tbl} ==="
    yt get "${GOLD_ROOT}/${tbl}/@schema" --format json 2>/dev/null | python3 -c "
import json, sys
schema = json.load(sys.stdin)
print(f'  columns: {len(schema)}')
"
done
