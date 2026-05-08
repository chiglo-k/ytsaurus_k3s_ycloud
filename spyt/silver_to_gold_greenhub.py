#!/usr/bin/env python3
"""
PySpark job: строит агрегатные витрины поверх silver_stage/greenhub_telemetry.

Каждая витрина — отдельный запуск:
  - параллельный запуск разных витрин (DAG .expand)
  - независимый rebuild (одна сломалась — другие живы)
  - чёткое имя витрины в логах / state

  - daily_battery_health : срез батареи по дням × странам
  - device_lifecycle : per-device first/last seen, активность
  - hourly_network_usage : сетевая активность по часам
  - country_overview : country-level summary (для DataLens map)

  spark-submit-yt --proxy localhost:31103 \\
    --discovery-path //home/spark/discovery/main \\
    silver_to_gold_greenhub.py \\
      --mart daily_battery_health --batch-id $(uuidgen)
"""

import argparse
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


SILVER_PATH = "//home/silver_stage/greenhub_telemetry"
GOLD_ROOT = "//home/gold_stage"


def mart_daily_battery_health(silver: DataFrame) -> DataFrame:
    return (silver
        .filter(F.col("battery_level").isNotNull())
        .groupBy(
            F.col("date_local"),
            F.col("country_code"),
            F.col("battery_state"),
        )
        .agg(
            F.count("*").alias("n_events"),
            F.countDistinct("device_id").alias("n_devices"),
            F.avg("battery_level").alias("avg_battery_level"),
            F.avg("temperature").alias("avg_temperature"),
            F.avg("voltage").alias("avg_voltage"),
            F.sum(F.when(F.col("battery_low"), 1).otherwise(0)).alias("n_low_battery_events"),
            F.sum(F.when(F.col("is_charging"), 1).otherwise(0)).alias("n_charging_events"),
        )
        .orderBy("date_local", "country_code")
    )


def mart_device_lifecycle(silver: DataFrame) -> DataFrame:
    return (silver
        .groupBy(F.col("device_id"), F.col("country_code"))
        .agg(
            F.min("timestamp").alias("first_seen"),
            F.max("timestamp").alias("last_seen"),
            F.count("*").alias("n_events"),
            F.countDistinct("date_local").alias("n_active_days"),
            F.avg("battery_level").alias("avg_battery_level"),
            F.avg("memory_used_pct").alias("avg_memory_used_pct"),
            F.avg("storage_used_pct").alias("avg_storage_used_pct"),
            F.sum(F.when(F.col("is_charging"), 1).otherwise(0)).alias("n_charging_events"),
            F.sum(F.when(F.col("is_wifi"), 1).otherwise(0)).alias("n_wifi_events"),
        )
        .withColumn(
            "lifetime_days",
            (F.unix_timestamp("last_seen") - F.unix_timestamp("first_seen")) / 86400.0
        )
    )


def mart_hourly_network_usage(silver: DataFrame) -> DataFrame:
    return (silver
        .filter(F.col("network_status").isNotNull())
        .groupBy(
            F.col("date_local"),
            F.col("hour_of_day"),
            F.col("country_code"),
            F.col("network_type"),
        )
        .agg(
            F.count("*").alias("n_events"),
            F.countDistinct("device_id").alias("n_devices"),
            F.avg("wifi_signal_strength").alias("avg_wifi_signal"),
            F.avg("wifi_link_speed").alias("avg_wifi_link_speed"),
            F.sum(F.when(F.col("mobile_data_status") == "CONNECTED", 1).otherwise(0)).alias("n_mobile_data_events"),
        )
    )


def mart_country_overview(silver: DataFrame) -> DataFrame:
    return (silver
        .groupBy(F.col("country_code"))
        .agg(
            F.countDistinct("device_id").alias("n_devices"),
            F.count("*").alias("n_events"),
            F.min("date_local").alias("first_date"),
            F.max("date_local").alias("last_date"),
            F.avg("battery_level").alias("avg_battery_level"),
            F.avg("temperature").alias("avg_temperature"),
            F.avg("memory_used_pct").alias("avg_memory_used_pct"),
            F.sum(F.when(F.col("is_charging"), 1).otherwise(0)).alias("n_charging_events"),
            F.sum(F.when(F.col("is_wifi"), 1).otherwise(0)).alias("n_wifi_events"),
            F.sum(F.when(F.col("battery_low"), 1).otherwise(0)).alias("n_low_battery_events"),
        )
        .orderBy(F.col("n_events").desc())
    )


MARTS = {
    "daily_battery_health": mart_daily_battery_health,
    "device_lifecycle":  mart_device_lifecycle,
    "hourly_network_usage": mart_hourly_network_usage,
    "country_overview": mart_country_overview,
}



def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mart", required=True, choices=sorted(MARTS.keys()))
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--silver-path", default=SILVER_PATH)
    parser.add_argument("--gold-root", default=GOLD_ROOT)
    args = parser.parse_args()

    mart_fn = MARTS[args.mart]
    gold_path = f"{args.gold_root}/{args.mart}"

    spark = (SparkSession.builder
             .appName(f"silver_to_gold_greenhub_{args.mart}_{args.batch_id[:8]}")
             .getOrCreate())

    print(f"[INFO] mart = {args.mart}")
    print(f"[INFO] batch_id = {args.batch_id}")
    print(f"[INFO] silver = {args.silver_path}")
    print(f"[INFO] gold path = {gold_path}")

    silver = spark.read.format("yt").load(f"yt://{args.silver_path}")
    silver_count = silver.count()
    print(f"[INFO] silver rows in: {silver_count:,}")

    df_mart = (mart_fn(silver)
               .withColumn("_gold_batch_id", F.lit(args.batch_id))
               .withColumn("_gold_built_at", F.current_timestamp()))

    out_count = df_mart.count()
    print(f"[INFO] mart rows out:  {out_count:,}")

    print(f"[INFO] writing → yt://{gold_path}")
    (df_mart.write.format("yt").mode("overwrite").save(f"yt://{gold_path}"))


    print(f"[DONE] mart : {args.mart}")
    print(f"[DONE] batch_id : {args.batch_id}")
    print(f"[DONE] silver in : {silver_count:,}")
    print(f"[DONE] gold rows : {out_count:,}")

    spark.stop()


if __name__ == "__main__":
    sys.exit(main())
