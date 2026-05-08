#!/usr/bin/env python3

from __future__ import annotations

import argparse
import os
import sys

import yt.wrapper as yt
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


SILVER_PATH = "//home/silver_stage/greenhub_telemetry"
GOLD_ROOT = "//home/gold_stage"


def yt_table_path(path: str) -> str:
    if path.startswith("ytTable:"):
        return path
    if path.startswith("//"):
        return "ytTable:/" + path
    if path.startswith("/"):
        return "ytTable:///" + path.lstrip("/")
    return "ytTable:///" + path


def read_yt(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.format("yt").load(yt_table_path(path))


def write_yt_append(df: DataFrame, path: str) -> None:
    df.write.format("yt").mode("append").save(yt_table_path(path))


def mart_daily_country_stats(silver: DataFrame) -> DataFrame:
    return (
        silver
        .groupBy(F.col("date_local"), F.col("country_code"))
        .agg(
            F.count("*").alias("n_events"),
            F.countDistinct("device_id").alias("n_devices"),
            F.avg("battery_level").alias("avg_battery_level"),
            F.avg("temperature").alias("avg_temperature"),
            F.avg("voltage").alias("avg_voltage"),
            F.avg("memory_used_pct").alias("avg_memory_used_pct"),
            F.avg("storage_used_pct").alias("avg_storage_used_pct"),
            F.sum(F.when(F.col("is_charging"), 1).otherwise(0)).alias("n_charging_events"),
            F.sum(F.when(F.col("battery_low"), 1).otherwise(0)).alias("n_low_battery_events"),
            F.sum(F.when(F.col("is_wifi"), 1).otherwise(0)).alias("n_wifi_events"),
            F.sum(F.when(F.col("is_night"), 1).otherwise(0)).alias("n_night_events"),
        )
        .withColumn(
            "charging_rate",
            F.when(F.col("n_events") > 0, F.col("n_charging_events") / F.col("n_events"))
            .otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "low_battery_rate",
            F.when(F.col("n_events") > 0, F.col("n_low_battery_events") / F.col("n_events"))
            .otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "wifi_rate",
            F.when(F.col("n_events") > 0, F.col("n_wifi_events") / F.col("n_events"))
            .otherwise(F.lit(None).cast("double")),
        )
    )


def mart_country_overview(silver: DataFrame) -> DataFrame:
    return (
        silver
        .groupBy(F.col("country_code"))
        .agg(
            F.countDistinct("device_id").alias("n_devices"),
            F.count("*").alias("n_events"),
            F.countDistinct("date_local").alias("n_active_days"),
            F.min("date_local").alias("first_date"),
            F.max("date_local").alias("last_date"),
            F.avg("battery_level").alias("avg_battery_level"),
            F.avg("temperature").alias("avg_temperature"),
            F.avg("memory_used_pct").alias("avg_memory_used_pct"),
            F.avg("storage_used_pct").alias("avg_storage_used_pct"),
            F.sum(F.when(F.col("is_charging"), 1).otherwise(0)).alias("n_charging_events"),
            F.sum(F.when(F.col("battery_low"), 1).otherwise(0)).alias("n_low_battery_events"),
            F.sum(F.when(F.col("is_wifi"), 1).otherwise(0)).alias("n_wifi_events"),
        )
        .withColumn(
            "charging_rate",
            F.when(F.col("n_events") > 0, F.col("n_charging_events") / F.col("n_events"))
            .otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "low_battery_rate",
            F.when(F.col("n_events") > 0, F.col("n_low_battery_events") / F.col("n_events"))
            .otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "wifi_rate",
            F.when(F.col("n_events") > 0, F.col("n_wifi_events") / F.col("n_events"))
            .otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "events_per_device",
            F.when(F.col("n_devices") > 0, F.col("n_events") / F.col("n_devices"))
            .otherwise(F.lit(None).cast("double")),
        )
    )


def mart_device_lifecycle(silver: DataFrame) -> DataFrame:
    event_ts = F.to_timestamp(F.col("event_ts_str"), "yyyy-MM-dd HH:mm:ss")

    base = silver.withColumn("_event_ts", event_ts)

    return (
        base
        .groupBy(F.col("device_id"), F.col("country_code"))
        .agg(
            F.min("event_ts_str").alias("first_seen"),
            F.max("event_ts_str").alias("last_seen"),
            F.min("_event_ts").alias("_first_seen_ts"),
            F.max("_event_ts").alias("_last_seen_ts"),
            F.count("*").alias("n_events"),
            F.countDistinct("date_local").alias("n_active_days"),
            F.avg("battery_level").alias("avg_battery_level"),
            F.min("battery_level").alias("min_battery_level"),
            F.avg("temperature").alias("avg_temperature"),
            F.avg("memory_used_pct").alias("avg_memory_used_pct"),
            F.avg("storage_used_pct").alias("avg_storage_used_pct"),
            F.sum(F.when(F.col("is_charging"), 1).otherwise(0)).alias("n_charging_events"),
            F.sum(F.when(F.col("is_wifi"), 1).otherwise(0)).alias("n_wifi_events"),
            F.sum(F.when(F.col("battery_low"), 1).otherwise(0)).alias("n_low_battery_events"),
        )
        .withColumn(
            "lifetime_days",
            (F.unix_timestamp("_last_seen_ts") - F.unix_timestamp("_first_seen_ts")) / F.lit(86400.0),
        )
        .withColumn(
            "events_per_active_day",
            F.when(F.col("n_active_days") > 0, F.col("n_events") / F.col("n_active_days"))
            .otherwise(F.lit(None).cast("double")),
        )
        .drop("_first_seen_ts", "_last_seen_ts")
    )


def mart_hourly_battery_health(silver: DataFrame) -> DataFrame:
    return (
        silver
        .groupBy(F.col("date_local"), F.col("hour_of_day"), F.col("country_code"))
        .agg(
            F.count("*").alias("n_events"),
            F.countDistinct("device_id").alias("n_devices"),
            F.avg("battery_level").alias("avg_battery_level"),
            F.avg("temperature").alias("avg_temperature"),
            F.max("temperature").alias("max_temperature"),
            F.avg("voltage").alias("avg_voltage"),
            F.sum(F.when(F.col("is_charging"), 1).otherwise(0)).alias("n_charging_events"),
            F.sum(F.when(F.col("battery_low"), 1).otherwise(0)).alias("n_low_battery_events"),
            F.sum(F.when(F.col("is_night"), 1).otherwise(0)).alias("n_night_events"),
        )
    )


MARTS = {
    "daily_country_stats": mart_daily_country_stats,
    "country_overview": mart_country_overview,
    "device_lifecycle": mart_device_lifecycle,
    "hourly_battery_health": mart_hourly_battery_health,
}


def cast_to_ddl(mart: str, df: DataFrame, batch_id: str) -> DataFrame:
    df = (
        df
        .withColumn("_gold_batch_id", F.lit(batch_id))
        .withColumn("_gold_built_at", F.date_format(F.current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
    )

    if mart == "daily_country_stats":
        return df.select(
            F.col("date_local").cast("string"),
            F.col("country_code").cast("string"),
            F.col("n_events").cast("long"),
            F.col("n_devices").cast("long"),
            F.col("avg_battery_level").cast("double"),
            F.col("avg_temperature").cast("double"),
            F.col("avg_voltage").cast("double"),
            F.col("avg_memory_used_pct").cast("double"),
            F.col("avg_storage_used_pct").cast("double"),
            F.col("n_charging_events").cast("long"),
            F.col("n_low_battery_events").cast("long"),
            F.col("n_wifi_events").cast("long"),
            F.col("n_night_events").cast("long"),
            F.col("charging_rate").cast("double"),
            F.col("low_battery_rate").cast("double"),
            F.col("wifi_rate").cast("double"),
            F.col("_gold_batch_id").cast("string"),
            F.col("_gold_built_at").cast("string"),
        )

    if mart == "country_overview":
        return df.select(
            F.col("country_code").cast("string"),
            F.col("n_devices").cast("long"),
            F.col("n_events").cast("long"),
            F.col("n_active_days").cast("long"),
            F.col("first_date").cast("string"),
            F.col("last_date").cast("string"),
            F.col("avg_battery_level").cast("double"),
            F.col("avg_temperature").cast("double"),
            F.col("avg_memory_used_pct").cast("double"),
            F.col("avg_storage_used_pct").cast("double"),
            F.col("n_charging_events").cast("long"),
            F.col("n_low_battery_events").cast("long"),
            F.col("n_wifi_events").cast("long"),
            F.col("charging_rate").cast("double"),
            F.col("low_battery_rate").cast("double"),
            F.col("wifi_rate").cast("double"),
            F.col("events_per_device").cast("double"),
            F.col("_gold_batch_id").cast("string"),
            F.col("_gold_built_at").cast("string"),
        )

    if mart == "device_lifecycle":
        return df.select(
            F.col("device_id").cast("long"),
            F.col("country_code").cast("string"),
            F.col("first_seen").cast("string"),
            F.col("last_seen").cast("string"),
            F.col("lifetime_days").cast("double"),
            F.col("n_events").cast("long"),
            F.col("n_active_days").cast("long"),
            F.col("avg_battery_level").cast("double"),
            F.col("min_battery_level").cast("double"),
            F.col("avg_temperature").cast("double"),
            F.col("avg_memory_used_pct").cast("double"),
            F.col("avg_storage_used_pct").cast("double"),
            F.col("n_charging_events").cast("long"),
            F.col("n_wifi_events").cast("long"),
            F.col("n_low_battery_events").cast("long"),
            F.col("events_per_active_day").cast("double"),
            F.col("_gold_batch_id").cast("string"),
            F.col("_gold_built_at").cast("string"),
        )

    if mart == "hourly_battery_health":
        return df.select(
            F.col("date_local").cast("string"),
            F.col("hour_of_day").cast("long"),
            F.col("country_code").cast("string"),
            F.col("n_events").cast("long"),
            F.col("n_devices").cast("long"),
            F.col("avg_battery_level").cast("double"),
            F.col("avg_temperature").cast("double"),
            F.col("max_temperature").cast("double"),
            F.col("avg_voltage").cast("double"),
            F.col("n_charging_events").cast("long"),
            F.col("n_low_battery_events").cast("long"),
            F.col("n_night_events").cast("long"),
            F.col("_gold_batch_id").cast("string"),
            F.col("_gold_built_at").cast("string"),
        )

    raise ValueError(f"Unknown mart: {mart}")


def get_yt_client() -> yt.YtClient:
    yt_proxy = os.environ.get("YT_PROXY", "http://localhost:31103")
    yt_token = os.environ.get("YT_TOKEN")

    proxy = (
        yt_proxy
        .replace("http://", "")
        .replace("https://", "")
        .rstrip("/")
    )

    return yt.YtClient(
        proxy=proxy,
        token=yt_token,
        config={"force_use_python_client": True},
    )


def truncate_gold_keep_schema(yt_client: yt.YtClient, gold_path: str) -> None:
    if not yt_client.exists(gold_path):
        raise RuntimeError(f"Gold table {gold_path} does not exist. Run create_gold_tables.sh first.")

    schema = yt_client.get(f"{gold_path}/@schema")
    print(f"[INFO] gold existing schema: {len(schema)} columns", flush=True)

    yt_client.remove(gold_path)
    yt_client.create("table", gold_path, attributes={"schema": schema})

    print("[INFO] gold truncated, schema preserved", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mart", required=True, choices=sorted(MARTS.keys()))
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--silver-path", default=SILVER_PATH)
    parser.add_argument("--gold-root", default=GOLD_ROOT)
    args = parser.parse_args()

    mart_fn = MARTS[args.mart]
    gold_path = f"{args.gold_root}/gold_{args.mart}"

    spark = (
        SparkSession.builder
        .appName(f"silver_to_gold_greenhub_{args.mart}_{args.batch_id[:8]}")
        .getOrCreate()
    )

    print(f"[INFO] mart       = {args.mart}", flush=True)
    print(f"[INFO] batch_id   = {args.batch_id}", flush=True)
    print(f"[INFO] silver     = {args.silver_path}", flush=True)
    print(f"[INFO] silver yt  = {yt_table_path(args.silver_path)}", flush=True)
    print(f"[INFO] gold path  = {gold_path}", flush=True)
    print(f"[INFO] gold yt    = {yt_table_path(gold_path)}", flush=True)

    yt_client = get_yt_client()
    truncate_gold_keep_schema(yt_client, gold_path)

    silver = read_yt(spark, args.silver_path)
    silver_count = silver.count()
    print(f"[INFO] silver rows in: {silver_count:,}", flush=True)

    raw_mart = mart_fn(silver)
    typed_mart = cast_to_ddl(args.mart, raw_mart, args.batch_id)

    out_count = typed_mart.count()
    print(f"[INFO] mart rows out:  {out_count:,}", flush=True)

    print(f"[INFO] writing -> {yt_table_path(gold_path)}", flush=True)
    write_yt_append(typed_mart, gold_path)

    print("=" * 60, flush=True)
    print(f"[DONE] mart        : {args.mart}", flush=True)
    print(f"[DONE] batch_id    : {args.batch_id}", flush=True)
    print(f"[DONE] silver in   : {silver_count:,}", flush=True)
    print(f"[DONE] gold rows   : {out_count:,}", flush=True)
    print("=" * 60, flush=True)

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
