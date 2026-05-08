#!/usr/bin/env python3

from __future__ import annotations

import argparse
import hashlib
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def yt_table_path(path: str) -> str:
    return "ytTable:" + path.removeprefix("/")


def stable_uuid_expr(namespace: str, *cols: str):
    parts = [F.lit(namespace)]
    for col in cols:
        parts.append(F.coalesce(F.col(col).cast("string"), F.lit("__null__")))

    h = F.sha2(F.concat_ws("|", *parts), 256)

    return F.concat(
        F.substring(h, 1, 8),
        F.lit("-"),
        F.substring(h, 9, 4),
        F.lit("-5"),
        F.substring(h, 14, 3),
        F.lit("-8"),
        F.substring(h, 17, 3),
        F.lit("-"),
        F.substring(h, 20, 12),
    )


def stable_uuid_from_value(namespace: str, col_expr):
    h = F.sha2(
        F.concat_ws(
            "|",
            F.lit(namespace),
            F.coalesce(col_expr.cast("string"), F.lit("__null__")),
        ),
        256,
    )

    return F.concat(
        F.substring(h, 1, 8),
        F.lit("-"),
        F.substring(h, 9, 4),
        F.lit("-5"),
        F.substring(h, 14, 3),
        F.lit("-8"),
        F.substring(h, 17, 3),
        F.lit("-"),
        F.substring(h, 20, 12),
    )


def to_bool(col_name: str):
    return (
        F.when(F.col(col_name).isNull(), F.lit(None).cast("boolean"))
        .when(F.col(col_name) == 0.0, F.lit(False))
        .otherwise(F.lit(True))
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--part-index", type=int, required=True)
    parser.add_argument("--file-hash", default="")
    parser.add_argument("--bronze-root", default="//home/bronze_stage/greenhub")
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName(f"raw_to_bronze_greenhub_{args.part_index}")
        .getOrCreate()
    )

    print(f"[INFO] Reading {args.input}", flush=True)

    df_raw = spark.read.parquet(args.input)

    df_raw = (
        df_raw
        .withColumn("event_ts", F.col("timestamp").cast("timestamp"))
        .withColumn("event_ts_str", F.date_format(F.col("event_ts"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("event_date", F.date_format(F.to_date(F.col("event_ts")), "yyyy-MM-dd"))
        .drop("event_ts")
        .cache()
    )

    rows_in = df_raw.count()
    print(f"[INFO] Source rows: {rows_in:,}", flush=True)

    file_hash = args.file_hash or hashlib.sha256(args.input.encode("utf-8")).hexdigest()
    print(f"[INFO] file_hash: {file_hash[:16]}...", flush=True)

    df_with_uids = (
        df_raw
        .withColumn("source_id", F.col("id").cast("long"))
        .withColumn("fact_uid", stable_uuid_expr("fact_telemetry", "id", "device_id", "event_ts_str"))
        .withColumn("device_uid", stable_uuid_expr("dim_device", "device_id"))
        .withColumn("country_uid", stable_uuid_expr("dim_country", "country_code"))
        .withColumn("timezone_uid", stable_uuid_expr("dim_timezone", "timezone"))
        .withColumn("battery_state_uid", stable_uuid_expr("dim_battery_state", "battery_state"))
        .withColumn("network_status_uid", stable_uuid_expr("dim_network_status", "network_status"))
        .withColumn("charger_uid", stable_uuid_expr("dim_charger", "charger"))
        .withColumn("health_uid", stable_uuid_expr("dim_health", "health"))
        .withColumn("network_type_uid", stable_uuid_expr("dim_network_type", "network_type"))
        .withColumn("mobile_network_type_uid", stable_uuid_expr("dim_mobile_network_type", "mobile_network_type"))
        .withColumn("mobile_data_status_uid", stable_uuid_expr("dim_mobile_data_status", "mobile_data_status"))
        .withColumn("mobile_data_activity_uid", stable_uuid_expr("dim_mobile_data_activity", "mobile_data_activity"))
        .withColumn("wifi_status_uid", stable_uuid_expr("dim_wifi_status", "wifi_status"))
        .withColumn("screen_on", (F.col("screen_on") == 1).cast("boolean"))
        .withColumn("roaming_enabled", to_bool("roaming_enabled"))
        .withColumn("bluetooth_enabled", to_bool("bluetooth_enabled"))
        .withColumn("location_enabled", to_bool("location_enabled"))
        .withColumn("power_saver_enabled", to_bool("power_saver_enabled"))
        .withColumn("nfc_enabled", to_bool("nfc_enabled"))
        .withColumn("developer_mode", to_bool("developer_mode"))
        .withColumn("_source_file", F.lit(args.input))
        .withColumn("_file_hash", F.lit(file_hash))
        .withColumn("_loaded_at", F.date_format(F.current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("_batch_id", F.lit(args.batch_id))
        .withColumn("_part_index", F.lit(args.part_index).cast("long"))
    )

    required_uid_cols = [
        "fact_uid",
        "device_uid",
        "country_uid",
        "timezone_uid",
        "battery_state_uid",
        "network_status_uid",
        "charger_uid",
        "health_uid",
        "network_type_uid",
        "mobile_network_type_uid",
        "mobile_data_status_uid",
        "mobile_data_activity_uid",
        "wifi_status_uid",
    ]

    missing = [c for c in required_uid_cols if c not in df_with_uids.columns]
    if missing:
        raise RuntimeError(f"Missing generated columns before fact select: {missing}")

    df_fact = df_with_uids.select(
        "fact_uid",
        "source_id",
        "device_uid",
        "event_ts_str",
        "event_date",
        "country_uid",
        "timezone_uid",
        "battery_state_uid",
        "network_status_uid",
        "charger_uid",
        "health_uid",
        "network_type_uid",
        "mobile_network_type_uid",
        "mobile_data_status_uid",
        "mobile_data_activity_uid",
        "wifi_status_uid",
        "battery_level",
        "memory_active",
        "memory_inactive",
        "memory_free",
        "memory_user",
        "screen_brightness",
        "voltage",
        "temperature",
        "usage",
        "up_time",
        "sleep_time",
        "wifi_signal_strength",
        "wifi_link_speed",
        "free",
        "total",
        "free_system",
        "total_system",
        "screen_on",
        "roaming_enabled",
        "bluetooth_enabled",
        "location_enabled",
        "power_saver_enabled",
        "nfc_enabled",
        "developer_mode",
        "_source_file",
        "_file_hash",
        "_loaded_at",
        "_batch_id",
        "_part_index",
    )

    fact_path = f"{args.bronze_root}/fact_telemetry"
    fact_count = df_fact.count()
    print(f"[INFO] fact rows ready: {fact_count:,} -> {fact_path}", flush=True)

    (
        df_fact.write
        .format("yt")
        .mode("append")
        .save(yt_table_path(fact_path))
    )

    print("[INFO] fact written", flush=True)

    dims_simple = [
        ("country", "country_code", "dim_country"),
        ("timezone", "timezone", "dim_timezone"),
        ("battery_state", "battery_state", "dim_battery_state"),
        ("network_status", "network_status", "dim_network_status"),
        ("charger", "charger", "dim_charger"),
        ("health", "health", "dim_health"),
        ("network_type", "network_type", "dim_network_type"),
        ("mobile_network_type", "mobile_network_type", "dim_mobile_network_type"),
        ("mobile_data_status", "mobile_data_status", "dim_mobile_data_status"),
        ("mobile_data_activity", "mobile_data_activity", "dim_mobile_data_activity"),
        ("wifi_status", "wifi_status", "dim_wifi_status"),
    ]

    for dim_name, raw_col, namespace in dims_simple:
        raw_value_expr = F.coalesce(F.col(raw_col).cast("string"), F.lit("__null__"))

        df_dim = (
            df_raw
            .withColumn("raw_value", raw_value_expr)
            .groupBy("raw_value")
            .agg(F.min("event_ts_str").alias("first_seen"))
            .withColumn(
                f"{dim_name}_uid",
                stable_uuid_from_value(
                    namespace,
                    F.when(F.col("raw_value") == "__null__", None).otherwise(F.col("raw_value")),
                ),
            )
            .withColumn("_loaded_at", F.date_format(F.current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
            .select(f"{dim_name}_uid", "raw_value", "first_seen", "_loaded_at")
        )

        path = f"{args.bronze_root}/dim_{dim_name}"
        n = df_dim.count()
        print(f"[INFO] dim_{dim_name} ({n} distinct rows) -> {path}", flush=True)

        (
            df_dim.write
            .format("yt")
            .mode("append")
            .save(yt_table_path(path))
        )

        print(f"[INFO] dim_{dim_name} written", flush=True)

    df_dim_device = (
        df_raw
        .groupBy("device_id")
        .agg(F.min("event_ts_str").alias("first_seen"))
        .withColumn("device_uid", stable_uuid_from_value("dim_device", F.col("device_id")))
        .withColumn("_loaded_at", F.date_format(F.current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
        .select("device_uid", "device_id", "first_seen", "_loaded_at")
    )

    path = f"{args.bronze_root}/dim_device"
    n_dev = df_dim_device.count()
    print(f"[INFO] dim_device ({n_dev} distinct devices) -> {path}", flush=True)

    (
        df_dim_device.write
        .format("yt")
        .mode("append")
        .save(yt_table_path(path))
    )

    print("[INFO] dim_device written", flush=True)

    print(f"[DONE] batch_id    : {args.batch_id}", flush=True)
    print(f"[DONE] part_index  : {args.part_index}", flush=True)
    print(f"[DONE] source rows : {rows_in:,}", flush=True)
    print(f"[DONE] fact rows   : {fact_count:,}", flush=True)
    print(f"[DONE] dim_device  : {n_dev:,}", flush=True)

    df_raw.unpersist()
    spark.stop()

    return 0


if __name__ == "__main__":
    sys.exit(main())
