#!/usr/bin/env bash

set -euo pipefail

SILVER_ROOT="//home/silver_stage"
SILVER_TABLE="${SILVER_ROOT}/greenhub_telemetry"

yt create map_node "${SILVER_ROOT}" --recursive --ignore-existing

if yt exists "${SILVER_TABLE}"; then
    echo "[INFO] Removing existing ${SILVER_TABLE}"
    yt remove --force "${SILVER_TABLE}"
fi

yt create table "${SILVER_TABLE}" --attributes '{
  schema=[
    {name=fact_uid;                 type=string;  required=%true};
    {name=source_id;                type=int64;   required=%true};
    {name=device_id;                type=int64};
    {name=device_uid;               type=string;  required=%true};

    {name=event_ts_str;             type=string;  required=%true};
    {name=event_date;               type=string;  required=%true};

    {name=country_code;             type=string};
    {name=timezone_name;            type=string};
    {name=battery_state;            type=string};
    {name=network_status;           type=string};
    {name=charger;                  type=string};
    {name=health;                   type=string};
    {name=network_type;             type=string};
    {name=mobile_network_type;      type=string};
    {name=mobile_data_status;       type=string};
    {name=mobile_data_activity;     type=string};
    {name=wifi_status;              type=string};

    {name=battery_level;            type=double};
    {name=memory_active;            type=int64};
    {name=memory_inactive;          type=int64};
    {name=memory_free;              type=int64};
    {name=memory_user;              type=int64};
    {name=screen_brightness;        type=int64};
    {name=voltage;                  type=double};
    {name=temperature;              type=double};
    {name=usage;                    type=double};
    {name=up_time;                  type=double};
    {name=sleep_time;               type=double};
    {name=wifi_signal_strength;     type=double};
    {name=wifi_link_speed;          type=double};
    {name=free;                     type=double};
    {name=total;                    type=double};
    {name=free_system;              type=double};
    {name=total_system;             type=double};

    {name=screen_on;                type=boolean};
    {name=roaming_enabled;          type=boolean};
    {name=bluetooth_enabled;        type=boolean};
    {name=location_enabled;         type=boolean};
    {name=power_saver_enabled;      type=boolean};
    {name=nfc_enabled;              type=boolean};
    {name=developer_mode;           type=boolean};

    {name=date_local;               type=string};
    {name=hour_of_day;              type=int64};
    {name=day_of_week;              type=int64};
    {name=is_night;                 type=boolean};
    {name=is_charging;              type=boolean};
    {name=is_wifi;                  type=boolean};
    {name=battery_low;              type=boolean};
    {name=memory_used_pct;          type=double};
    {name=storage_used_pct;         type=double};

    {name=_source_file;             type=string};
    {name=_file_hash;               type=string};
    {name=_bronze_loaded_at;        type=string};
    {name=_silver_batch_id;         type=string;  required=%true};
    {name=_silver_built_at;         type=string;  required=%true};
  ];
}'

echo "[OK] Created ${SILVER_TABLE}"
echo ""
echo "Schema:"
yt get "${SILVER_TABLE}/@schema" --format json 2>/dev/null | python3 -c "
import json, sys
schema = json.load(sys.stdin)
print(f'  Total columns: {len(schema)}')
for col in schema:
    req = '*' if col.get('required') else ' '
    t = col['type'] if isinstance(col['type'], str) else col['type'].get('type_v3', col['type'])
    print(f'  {req} {col[\"name\"]:30s} {t}')
"
