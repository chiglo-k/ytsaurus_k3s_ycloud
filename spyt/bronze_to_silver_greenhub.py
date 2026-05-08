#!/usr/bin/env python3

from __future__ import annotations

import argparse
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


BRONZE_ROOT = "//home/bronze_stage/greenhub"
SILVER_PATH = "//home/silver_stage/greenhub_telemetry"


def yt_table_path(path: str) -> str:
    if path.startswith("ytTable:"):
        return path
    if path.startswith("//"):
        return "ytTable:" + path[1:]
    if path.startswith("/"):
        return "ytTable:" + path
    return "ytTable:/" + path


def read_yt(spark: SparkSession, path: str):
    return spark.read.format("yt").load(yt_table_path(path))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--bronze-root", default=BRONZE_ROOT)
    parser.add_argument("--silver-path", default=SILVER_PATH)
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName(f"bronze_to_silver_greenhub_{args.batch_id[:8]}")
        .getOrCreate()
    )

    print(f"[INFO] batch_id    = {args.batch_id}", flush=True)
    print(f"[INFO] bronze root = {args.bronze_root}", flush=True)
    print(f"[INFO] silver path = {args.silver_path}", flush=True)
    print(f"[INFO] bronze fact = {yt_table_path(f'{args.bronze_root}/fact_telemetry')}", flush=True)
    print(f"[INFO] silver out  = {yt_table_path(args.silver_path)}", flush=True)

    fact = read_yt(spark, f"{args.bronze_root}/fact_telemetry")
    fact_count_in = fact.count()
    print(f"[INFO] bronze fact rows: {fact_count_in:,}", flush=True)

    w = Window.partitionBy("fact_uid").orderBy(F.col("_loaded_at").desc())
    fact_dedup = (
        fact
        .withColumn("_rn", F.row_number().over(w))
        .where(F.col("_rn") == 1)
        .drop("_rn")
    )
    fact_count_dedup = fact_dedup.count()
    print(
        f"[INFO] after dedup:      {fact_count_dedup:,} "
        f"(removed {fact_count_in - fact_count_dedup:,} dupes)",
        flush=True,
    )

    def read_dim(name: str, value_col_alias: str):
        return (
            read_yt(spark, f"{args.bronze_root}/dim_{name}")
            .select(
                F.col(f"{name}_uid").alias(f"_join_{name}_uid"),
                F.col("raw_value").alias(value_col_alias),
            )
            .dropDuplicates([f"_join_{name}_uid"])
        )

    dim_country = read_dim("country", "country_code")
    dim_timezone = read_dim("timezone", "timezone_name")
    dim_battery_state = read_dim("battery_state", "battery_state")
    dim_network_status = read_dim("network_status", "network_status")
    dim_charger = read_dim("charger", "charger")
    dim_health = read_dim("health", "health")
    dim_network_type = read_dim("network_type", "network_type")
    dim_mobile_network_type = read_dim("mobile_network_type", "mobile_network_type")
    dim_mobile_data_status = read_dim("mobile_data_status", "mobile_data_status")
    dim_mobile_data_activity = read_dim("mobile_data_activity", "mobile_data_activity")
    dim_wifi_status = read_dim("wifi_status", "wifi_status")

    dim_device = (
        read_yt(spark, f"{args.bronze_root}/dim_device")
        .select(
            F.col("device_uid").alias("_join_device_uid"),
            F.col("device_id").alias("device_id_dim"),
        )
        .dropDuplicates(["_join_device_uid"])
    )

    silver = (
        fact_dedup
        .join(dim_device, F.col("device_uid") == F.col("_join_device_uid"), "left").drop("_join_device_uid")
        .join(dim_country, F.col("country_uid") == F.col("_join_country_uid"), "left").drop("_join_country_uid")
        .join(dim_timezone, F.col("timezone_uid") == F.col("_join_timezone_uid"), "left").drop("_join_timezone_uid")
        .join(dim_battery_state, F.col("battery_state_uid") == F.col("_join_battery_state_uid"), "left").drop("_join_battery_state_uid")
        .join(dim_network_status, F.col("network_status_uid") == F.col("_join_network_status_uid"), "left").drop("_join_network_status_uid")
        .join(dim_charger, F.col("charger_uid") == F.col("_join_charger_uid"), "left").drop("_join_charger_uid")
        .join(dim_health, F.col("health_uid") == F.col("_join_health_uid"), "left").drop("_join_health_uid")
        .join(dim_network_type, F.col("network_type_uid") == F.col("_join_network_type_uid"), "left").drop("_join_network_type_uid")
        .join(dim_mobile_network_type, F.col("mobile_network_type_uid") == F.col("_join_mobile_network_type_uid"), "left").drop("_join_mobile_network_type_uid")
        .join(dim_mobile_data_status, F.col("mobile_data_status_uid") == F.col("_join_mobile_data_status_uid"), "left").drop("_join_mobile_data_status_uid")
        .join(dim_mobile_data_activity, F.col("mobile_data_activity_uid") == F.col("_join_mobile_data_activity_uid"), "left").drop("_join_mobile_data_activity_uid")
        .join(dim_wifi_status, F.col("wifi_status_uid") == F.col("_join_wifi_status_uid"), "left").drop("_join_wifi_status_uid")
    )

    event_ts = F.to_timestamp(F.col("event_ts_str"), "yyyy-MM-dd HH:mm:ss")

    silver = (
        silver
        .withColumn("device_id", F.coalesce(F.col("device_id_dim"), F.lit(None).cast("long")))
        .withColumn("date_local", F.col("event_date"))
        .withColumn("hour_of_day", F.hour(event_ts).cast("long"))
        .withColumn("day_of_week", F.dayofweek(event_ts).cast("long"))
        .withColumn("is_night", F.hour(event_ts).between(0, 5))
        .withColumn("is_charging", F.col("battery_state").isin("Charging", "Full"))
        .withColumn("is_wifi", F.col("network_type") == F.lit("WIFI"))
        .withColumn("battery_low", F.col("battery_level") < F.lit(20.0))
        .withColumn(
            "memory_used_pct",
            F.when(
                (F.col("memory_active") + F.col("memory_inactive") + F.col("memory_free") + F.col("memory_user")) > 0,
                (F.col("memory_active") + F.col("memory_user")) /
                (F.col("memory_active") + F.col("memory_inactive") + F.col("memory_free") + F.col("memory_user")),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "storage_used_pct",
            F.when(F.col("total") > 0, (F.col("total") - F.col("free")) / F.col("total"))
            .otherwise(F.lit(None).cast("double")),
        )
        .withColumn("_silver_batch_id", F.lit(args.batch_id))
        .withColumn("_silver_built_at", F.date_format(F.current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("_bronze_loaded_at", F.col("_loaded_at"))
    )

    silver_out = silver.select(
        "fact_uid",
        "source_id",
        "device_id",
        "device_uid",
        "event_ts_str",
        "event_date",
        "country_code",
        "timezone_name",
        "battery_state",
        "network_status",
        "charger",
        "health",
        "network_type",
        "mobile_network_type",
        "mobile_data_status",
        "mobile_data_activity",
        "wifi_status",
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
        "date_local",
        "hour_of_day",
        "day_of_week",
        "is_night",
        "is_charging",
        "is_wifi",
        "battery_low",
        "memory_used_pct",
        "storage_used_pct",
        "_source_file",
        "_file_hash",
        "_bronze_loaded_at",
        "_silver_batch_id",
        "_silver_built_at",
    )

    silver_count = silver_out.count()
    print(f"[INFO] silver wide rows: {silver_count:,}", flush=True)

    print(f"[INFO] writing silver -> {yt_table_path(args.silver_path)}", flush=True)
    (
        silver_out.write
        .format("yt")
        .mode("overwrite")
        .save(yt_table_path(args.silver_path))
    )

    print(f"[DONE] batch_id        : {args.batch_id}", flush=True)
    print(f"[DONE] bronze fact in  : {fact_count_in:,}", flush=True)
    print(f"[DONE] after dedup     : {fact_count_dedup:,}", flush=True)
    print(f"[DONE] silver out      : {silver_count:,}", flush=True)

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
