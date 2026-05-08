#!/usr/bin/env bash

set -euo pipefail

yt create map_node //home/bronze_stage --ignore-existing
yt create map_node //home/bronze_stage/greenhub --ignore-existing

yt remove --force //home/bronze_stage/greenhub/fact_telemetry || true
yt remove --force //home/bronze_stage/greenhub/dim_device || true

for dim in country timezone battery_state network_status charger health \
           network_type mobile_network_type mobile_data_status \
           mobile_data_activity wifi_status; do
  yt remove --force //home/bronze_stage/greenhub/dim_${dim} || true
done

yt create table //home/bronze_stage/greenhub/fact_telemetry --attributes '{
  schema=[
    {name=fact_uid;                   type=string;  required=%true};
    {name=source_id;                  type=int64;   required=%true};
    {name=device_uid;                 type=string;  required=%true};
    {name=event_ts_str;               type=string;  required=%true};
    {name=event_date;                 type=string;  required=%true};
    {name=country_uid;                type=string;  required=%true};
    {name=timezone_uid;               type=string;  required=%true};
    {name=battery_state_uid;          type=string;  required=%true};
    {name=network_status_uid;         type=string;  required=%true};
    {name=charger_uid;                type=string};
    {name=health_uid;                 type=string};
    {name=network_type_uid;           type=string};
    {name=mobile_network_type_uid;    type=string};
    {name=mobile_data_status_uid;     type=string};
    {name=mobile_data_activity_uid;   type=string};
    {name=wifi_status_uid;            type=string};
    {name=battery_level;              type=double};
    {name=memory_active;              type=int64};
    {name=memory_inactive;            type=int64};
    {name=memory_free;                type=int64};
    {name=memory_user;                type=int64};
    {name=screen_brightness;          type=int64};
    {name=voltage;                    type=double};
    {name=temperature;                type=double};
    {name=usage;                      type=double};
    {name=up_time;                    type=double};
    {name=sleep_time;                 type=double};
    {name=wifi_signal_strength;       type=double};
    {name=wifi_link_speed;            type=double};
    {name=free;                       type=double};
    {name=total;                      type=double};
    {name=free_system;                type=double};
    {name=total_system;               type=double};
    {name=screen_on;                  type=boolean};
    {name=roaming_enabled;            type=boolean};
    {name=bluetooth_enabled;          type=boolean};
    {name=location_enabled;           type=boolean};
    {name=power_saver_enabled;        type=boolean};
    {name=nfc_enabled;                type=boolean};
    {name=developer_mode;             type=boolean};
    {name=_source_file;               type=string;  required=%true};
    {name=_file_hash;                 type=string;  required=%true};
    {name=_loaded_at;                 type=string;  required=%true};
    {name=_batch_id;                  type=string;  required=%true};
    {name=_part_index;                type=int64;   required=%true};
  ];
}'

for dim in country timezone battery_state network_status charger health \
           network_type mobile_network_type mobile_data_status \
           mobile_data_activity wifi_status; do
  yt create table //home/bronze_stage/greenhub/dim_${dim} --attributes "{
    schema=[
      {name=${dim}_uid; type=string; required=%true};
      {name=raw_value;  type=string; required=%true};
      {name=first_seen; type=string; required=%true};
      {name=_loaded_at; type=string; required=%true};
    ];
  }"
done

yt create table //home/bronze_stage/greenhub/dim_device --attributes '{
  schema=[
    {name=device_uid; type=string; required=%true};
    {name=device_id;  type=int64;  required=%true};
    {name=first_seen; type=string; required=%true};
    {name=_loaded_at; type=string; required=%true};
  ];
}'

yt list //home/bronze_stage/greenhub
