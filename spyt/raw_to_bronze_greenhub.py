#!/usr/bin/env python3
"""

PySpark job (запускается на SPYT cluster):
  - Читает один parquet-файл из S3 (SeaweedFS)
  - Генерирует UUIDv5 для всех 12 dim-полей по namespace из docs/uuid_namespaces.py
  - Формирует fact_telemetry с UUID-ссылками + meta-колонками
  - Извлекает distinct-значения для 12 dim таблиц и пишет append
  - Записывает в YT через SPYT writer (format='yt')

  spark-submit-yt \\
    --proxy localhost:31103 \\
    --discovery-path //home/spark/discovery/main \\
    --packages org.apache.hadoop:hadoop-aws:3.3.4 \\
    ... s3 confs ... \\
    raw_to_bronze_greenhub.py \\
      --input s3a://greenhub//part-0000.parquet \\
      --batch-id $(uuidgen) \\
      --part-index 0

"""

import argparse
import hashlib
import sys
import uuid

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


NS_FACT_TELEMETRY = uuid.UUID("8947f476-fb93-44bc-aec6-2b8199d66c77")
NS_DEVICE = uuid.UUID("21652137-ff4f-4545-972b-52f28561b2ef")
NS_COUNTRY = uuid.UUID("88222767-fca9-41a2-87c2-2e566edb8dd4")
NS_TIMEZONE = uuid.UUID("34911b4c-d853-42d4-96ac-a8d71999b12d")
NS_BATTERY_STATE = uuid.UUID("04c36863-474f-44b2-9df1-9da58e8bdf65")
NS_CHARGER = uuid.UUID("14f49acf-aece-4a76-8213-4729646dbb6a")
NS_HEALTH = uuid.UUID("294d1142-111e-4a51-b843-803554b1415f")
NS_NETWORK_STATUS = uuid.UUID("f4ee5df7-d3cf-4904-8905-fcb022f7538a")
NS_NETWORK_TYPE = uuid.UUID("51a5e98c-7573-4677-b71a-6e388fc91a5b")
NS_MOBILE_NETWORK_TYPE = uuid.UUID("bcfe5630-9228-4ff2-93f3-6f7c6831aaaa")
NS_MOBILE_DATA_STATUS = uuid.UUID("de2d9eee-9961-456c-a7f1-eb21edd63927")
NS_MOBILE_DATA_ACTIVITY = uuid.UUID("65c9bf2f-4d25-40f6-b1c6-05d91c6cf095")
NS_WIFI_STATUS = uuid.UUID("b78bd936-71ed-483a-ad3f-eab9eb57f9e3")



def make_uid_udf(ns: uuid.UUID):
    """Возвращает Spark UDF, генерящий UUIDv5 от raw_value в указанном namespace.
    None / "" / NaN → uuid5(ns, "__null__"), чтобы NULL стал явной dim-записью."""
    ns_local = ns
    def _f(value):
        if value is None:
            return str(uuid.uuid5(ns_local, "__null__"))
        sval = str(value)
        if sval == "" or sval.lower() == "nan":
            return str(uuid.uuid5(ns_local, "__null__"))
        return str(uuid.uuid5(ns_local, sval))
    return F.udf(_f, StringType())


@F.udf(StringType())
def fact_uid_udf(source_id, device_id, ts):
    """fact_uid = uuid5(NS_FACT, '<source_id>|<device_id>|<ts_iso>')"""
    if source_id is None or device_id is None or ts is None:
        return None
    natural_key = f"{int(source_id)}|{int(device_id)}|{ts.isoformat()}"
    return str(uuid.uuid5(NS_FACT_TELEMETRY, natural_key))

def double_to_bool(col_name):
    """double 0.0/1.0/NaN → boolean false/true/null."""
    return (
        F.when(F.col(col_name).isNull(), F.lit(None).cast("boolean"))
         .when(F.col(col_name) == 0.0, F.lit(False))
         .otherwise(F.lit(True))
    ).alias(col_name)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="s3a://bucket/.../part-NNNN.parquet")
    parser.add_argument("--batch-id", required=True, help="UUID запуска DAG")
    parser.add_argument("--part-index", type=int, required=True)
    parser.add_argument("--file-hash", default="", help="sha256 hex; пусто → считается от пути")
    parser.add_argument("--bronze-root", default="//home/bronze_stage/greenhub")
    args = parser.parse_args()

    spark = (SparkSession.builder
             .appName(f"raw_to_bronze_greenhub_{args.part_index}")
             .getOrCreate())

    print(f"[INFO] Reading {args.input}")
    df_raw = spark.read.parquet(args.input)
    df_raw = df_raw.cache()
    rows_in = df_raw.count()
    print(f"[INFO] Source rows: {rows_in:,}")

    file_hash = args.file_hash or hashlib.sha256(args.input.encode("utf-8")).hexdigest()
    print(f"[INFO] file_hash: {file_hash[:16]}...")

    uid_country = make_uid_udf(NS_COUNTRY)
    uid_timezone = make_uid_udf(NS_TIMEZONE)
    uid_battery = make_uid_udf(NS_BATTERY_STATE)
    uid_charger = make_uid_udf(NS_CHARGER)
    uid_health = make_uid_udf(NS_HEALTH)
    uid_netstatus = make_uid_udf(NS_NETWORK_STATUS)
    uid_nettype = make_uid_udf(NS_NETWORK_TYPE)
    uid_mobnettype = make_uid_udf(NS_MOBILE_NETWORK_TYPE)
    uid_mobdatastat = make_uid_udf(NS_MOBILE_DATA_STATUS)
    uid_mobdataact = make_uid_udf(NS_MOBILE_DATA_ACTIVITY)
    uid_wifistatus = make_uid_udf(NS_WIFI_STATUS)
    uid_device = make_uid_udf(NS_DEVICE)


    df_fact = (df_raw
        .withColumn("fact_uid",fact_uid_udf(F.col("id"), F.col("device_id"), F.col("timestamp")))
        .withColumnRenamed("id", "source_id")
        .withColumn("device_uid", uid_device(F.col("device_id")))
        .withColumn("country_uid", uid_country(F.col("country_code")))
        .withColumn("timezone_uid", uid_timezone(F.col("timezone")))
        .withColumn("battery_state_uid", uid_battery(F.col("battery_state")))
        .withColumn("network_status_uid",uid_netstatus(F.col("network_status")))
        .withColumn("charger_uid", uid_charger(F.col("charger")))
        .withColumn("health_uid", uid_health(F.col("health")))
        .withColumn("network_type_uid", uid_nettype(F.col("network_type")))
        .withColumn("mobile_network_type_uid", uid_mobnettype(F.col("mobile_network_type")))
        .withColumn("mobile_data_status_uid", uid_mobdatastat(F.col("mobile_data_status")))
        .withColumn("mobile_data_activity_uid", uid_mobdataact(F.col("mobile_data_activity")))
        .withColumn("wifi_status_uid", uid_wifistatus(F.col("wifi_status")))
        # --- Booleans ---
        .withColumn("screen_on", (F.col("screen_on") == 1).cast("boolean"))
        .withColumn("roaming_enabled", F.when(F.col("roaming_enabled").isNull(),     F.lit(None).cast("boolean")).when(F.col("roaming_enabled")     == 0.0, F.lit(False)).otherwise(F.lit(True)))
        .withColumn("bluetooth_enabled", F.when(F.col("bluetooth_enabled").isNull(),   F.lit(None).cast("boolean")).when(F.col("bluetooth_enabled")   == 0.0, F.lit(False)).otherwise(F.lit(True)))
        .withColumn("location_enabled", F.when(F.col("location_enabled").isNull(),    F.lit(None).cast("boolean")).when(F.col("location_enabled")    == 0.0, F.lit(False)).otherwise(F.lit(True)))
        .withColumn("power_saver_enabled", F.when(F.col("power_saver_enabled").isNull(), F.lit(None).cast("boolean")).when(F.col("power_saver_enabled") == 0.0, F.lit(False)).otherwise(F.lit(True)))
        .withColumn("nfc_enabled", F.when(F.col("nfc_enabled").isNull(),         F.lit(None).cast("boolean")).when(F.col("nfc_enabled")         == 0.0, F.lit(False)).otherwise(F.lit(True)))
        .withColumn("developer_mode", F.when(F.col("developer_mode").isNull(),      F.lit(None).cast("boolean")).when(F.col("developer_mode")      == 0.0, F.lit(False)).otherwise(F.lit(True)))
        # --- Meta ---
        .withColumn("_source_file", F.lit(args.input))
        .withColumn("_file_hash", F.lit(file_hash))
        .withColumn("_loaded_at", F.current_timestamp())
        .withColumn("_batch_id", F.lit(args.batch_id))
        .withColumn("_part_index", F.lit(args.part_index).cast("long"))
        .select(
            "fact_uid", "source_id", "device_uid", "timestamp",
            "country_uid", "timezone_uid", "battery_state_uid", "network_status_uid",
            "charger_uid", "health_uid", "network_type_uid", "mobile_network_type_uid",
            "mobile_data_status_uid", "mobile_data_activity_uid", "wifi_status_uid",
            "battery_level", "memory_active", "memory_inactive", "memory_free",
            "memory_user", "screen_brightness",
            "voltage", "temperature", "usage", "up_time", "sleep_time",
            "wifi_signal_strength", "wifi_link_speed",
            "free", "total", "free_system", "total_system",
            "screen_on", "roaming_enabled", "bluetooth_enabled", "location_enabled",
            "power_saver_enabled", "nfc_enabled", "developer_mode",
            "_source_file", "_file_hash", "_loaded_at", "_batch_id", "_part_index",
        )
    )

    fact_path = f"{args.bronze_root}/fact_telemetry"
    fact_count = df_fact.count()
    print(f"[INFO] fact rows ready: {fact_count:,} → {fact_path}")
    (df_fact.write
        .format("yt")
        .mode("append")
        .save(f"yt://{fact_path}"))
    print(f"[INFO] fact written ✓")

    dims_simple = [
        ("country", "country_code", uid_country),
        ("timezone", "timezone", uid_timezone),
        ("battery_state", "battery_state", uid_battery),
        ("network_status", "network_status", uid_netstatus),
        ("charger", "charger", uid_charger),
        ("health", "health",  uid_health),
        ("network_type", "network_type", uid_nettype),
        ("mobile_network_type", "mobile_network_type", uid_mobnettype),
        ("mobile_data_status", "mobile_data_status", uid_mobdatastat),
        ("mobile_data_activity", "mobile_data_activity", uid_mobdataact),
        ("wifi_status", "wifi_status", uid_wifistatus),
    ]

    for dim_name, raw_col, uid_fn in dims_simple:
        df_dim = (df_raw
            .withColumn("raw_value", F.coalesce(F.col(raw_col).cast("string"), F.lit("__null__")))
            .groupBy("raw_value")
            .agg(F.min("timestamp").alias("first_seen"))
            .withColumn(f"{dim_name}_uid", uid_fn(
                F.when(F.col("raw_value") == "__null__", None).otherwise(F.col("raw_value"))
            ))
            .withColumn("_loaded_at", F.current_timestamp())
            .select(f"{dim_name}_uid", "raw_value", "first_seen", "_loaded_at")
        )
        n = df_dim.count()
        path = f"{args.bronze_root}/dim_{dim_name}"
        print(f"[INFO] dim_{dim_name} ({n} distinct rows) → {path}")
        df_dim.write.format("yt").mode("append").save(f"yt://{path}")


    df_dim_device = (df_raw
        .groupBy("device_id")
        .agg(F.min("timestamp").alias("first_seen"))
        .withColumn("device_uid", uid_device(F.col("device_id")))
        .withColumn("_loaded_at", F.current_timestamp())
        .select("device_uid", "device_id", "first_seen", "_loaded_at")
    )
    n_dev = df_dim_device.count()
    path = f"{args.bronze_root}/dim_device"
    print(f"[INFO] dim_device ({n_dev} distinct devices) → {path}")
    df_dim_device.write.format("yt").mode("append").save(f"yt://{path}")


    print(f"[DONE] batch_id  : {args.batch_id}")
    print(f"[DONE] part_index  : {args.part_index}")
    print(f"[DONE] source rows : {rows_in:,}")
    print(f"[DONE] fact rows : {fact_count:,}")
    print(f"[DONE] dim_device : {n_dev:,}")

    spark.stop()


if __name__ == "__main__":
    sys.exit(main())
