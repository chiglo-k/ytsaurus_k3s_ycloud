#!/usr/bin/env python3
"""
PySpark job (на SPYT cluster):
  - Читает yt://home/bronze_stage/greenhub/fact_telemetry
  - LEFT JOIN с 12 dim: fact - dim_country - dim_timezone
  - Получает wide-таблицу с raw_value вместо UID + бизнес-обогащения
    (час суток, день недели, флаг "ночь" и пр. — для аналитики gold)
  - Дедупликация по fact_uid (берём latest по _loaded_at)
  - Перезаписывает yt://home/silver_stage/greenhub_telemetry (overwrite)

Зачем overwrite а не append: silver — конформированный слой, всегда
актуальный snapshot bronze. При повторном bronze-обновлении silver
перестраивается — это даёт актуальност без риска потери-данных.

  spark-submit-yt --proxy localhost:31103 \\
    --discovery-path //home/spark/discovery/main \\
    bronze_to_silver_greenhub.py --batch-id $(uuidgen)
"""

import argparse
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


BRONZE_ROOT = "//home/bronze_stage/greenhub"
SILVER_PATH = "//home/silver_stage/greenhub_telemetry"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-id",     required=True)
    parser.add_argument("--bronze-root",  default=BRONZE_ROOT)
    parser.add_argument("--silver-path",  default=SILVER_PATH)
    args = parser.parse_args()

    spark = (SparkSession.builder
             .appName(f"bronze_to_silver_greenhub_{args.batch_id[:8]}")
             .getOrCreate())

    print(f"[INFO] batch_id    = {args.batch_id}")
    print(f"[INFO] bronze root = {args.bronze_root}")
    print(f"[INFO] silver path = {args.silver_path}")

    fact = spark.read.format("yt").load(f"yt://{args.bronze_root}/fact_telemetry")
    fact_count_in = fact.count()
    print(f"[INFO] bronze fact rows: {fact_count_in:,}")

    w = Window.partitionBy("fact_uid").orderBy(F.col("_loaded_at").desc())
    fact_dedup = (fact
        .withColumn("_rn", F.row_number().over(w))
        .where(F.col("_rn") == 1)
        .drop("_rn"))
    fact_count_dedup = fact_dedup.count()
    print(f"[INFO] after dedup:      {fact_count_dedup:,} (removed {fact_count_in - fact_count_dedup:,} dupes)")

    def read_dim(name: str, value_col_alias: str):
        df = (spark.read.format("yt").load(f"yt://{args.bronze_root}/dim_{name}")
              .select(
                  F.col(f"{name}_uid").alias(f"_join_{name}_uid"),
                  F.col("raw_value").alias(value_col_alias),
              ))

        return df.dropDuplicates([f"_join_{name}_uid"])

    dim_country = read_dim("country", "country_code")
    dim_timezone = read_dim("timezone", "timezone_name")
    dim_battery_state = read_dim("battery_state", "battery_state")
    dim_network_status = read_dim("network_status", "network_status")
    dim_charger = read_dim("charger", "charger")
    dim_health  = read_dim("health", "health")
    dim_network_type = read_dim("network_type", "network_type")
    dim_mobile_network_type  = read_dim("mobile_network_type", "mobile_network_type")
    dim_mobile_data_status = read_dim("mobile_data_status", "mobile_data_status")
    dim_mobile_data_activity = read_dim("mobile_data_activity", "mobile_data_activity")
    dim_wifi_status = read_dim("wifi_status", "wifi_status")

    dim_device = (spark.read.format("yt").load(f"yt://{args.bronze_root}/dim_device")
                  .select(
                      F.col("device_uid").alias("_join_device_uid"),
                      F.col("device_id"),
                  )
                  .dropDuplicates(["_join_device_uid"]))


    silver = (fact_dedup
        .join(dim_device, fact_dedup.device_uid == dim_device._join_device_uid, "left").drop("_join_device_uid")
        .join(dim_country, F.col("country_uid") == F.col("_join_country_uid"), "left").drop("_join_country_uid")
        .join(dim_timezone,  F.col("timezone_uid") == F.col("_join_timezone_uid"), "left").drop("_join_timezone_uid")
        .join(dim_battery_state, F.col("battery_state_uid") == F.col("_join_battery_state_uid"), "left").drop("_join_battery_state_uid")
        .join(dim_network_status, F.col("network_status_uid") == F.col("_join_network_status_uid"), "left").drop("_join_network_status_uid")
        .join(dim_charger, F.col("charger_uid")  == F.col("_join_charger_uid"), "left").drop("_join_charger_uid")
        .join(dim_health, F.col("health_uid") == F.col("_join_health_uid"), "left").drop("_join_health_uid")
        .join(dim_network_type, F.col("network_type_uid") == F.col("_join_network_type_uid"), "left").drop("_join_network_type_uid")
        .join(dim_mobile_network_type, F.col("mobile_network_type_uid") == F.col("_join_mobile_network_type_uid"), "left").drop("_join_mobile_network_type_uid")
        .join(dim_mobile_data_status, F.col("mobile_data_status_uid")  == F.col("_join_mobile_data_status_uid"), "left").drop("_join_mobile_data_status_uid")
        .join(dim_mobile_data_activity, F.col("mobile_data_activity_uid") == F.col("_join_mobile_data_activity_uid"), "left").drop("_join_mobile_data_activity_uid")
        .join(dim_wifi_status, F.col("wifi_status_uid") == F.col("_join_wifi_status_uid"), "left").drop("_join_wifi_status_uid")
    )

    silver = (silver
        .withColumn("hour_of_day", F.hour(F.col("timestamp")))
        .withColumn("day_of_week", F.dayofweek(F.col("timestamp")))
        .withColumn("date_local", F.to_date(F.col("timestamp")))
        .withColumn("is_night", (F.hour(F.col("timestamp")).between(0, 5)))
        .withColumn("is_charging", F.col("battery_state").isin("Charging", "Full"))
        .withColumn("is_wifi", F.col("network_type") == F.lit("WIFI"))
        .withColumn("battery_low", F.col("battery_level") < 0.2)
        .withColumn("memory_used_pct",
            F.when(
                (F.col("memory_active") + F.col("memory_inactive") + F.col("memory_free") + F.col("memory_user")) > 0,
                (F.col("memory_active") + F.col("memory_user")) /
                (F.col("memory_active") + F.col("memory_inactive") + F.col("memory_free") + F.col("memory_user"))
            ).otherwise(F.lit(None).cast("double"))
        )
        .withColumn("storage_used_pct",
            F.when(F.col("total") > 0, (F.col("total") - F.col("free")) / F.col("total"))
             .otherwise(F.lit(None).cast("double"))
        )
        .withColumn("_silver_batch_id", F.lit(args.batch_id))
        .withColumn("_silver_built_at", F.current_timestamp())
    )

    silver_count = silver.count()
    print(f"[INFO] silver wide rows: {silver_count:,}")

    print(f"[INFO] writing silver → yt://{args.silver_path}")
    (silver.write
        .format("yt")
        .mode("overwrite")
        .save(f"yt://{args.silver_path}"))

    print(f"[DONE] batch_id        : {args.batch_id}")
    print(f"[DONE] bronze fact in  : {fact_count_in:,}")
    print(f"[DONE] after dedup     : {fact_count_dedup:,}")
    print(f"[DONE] silver out      : {silver_count:,}")

    spark.stop()


if __name__ == "__main__":
    sys.exit(main())
