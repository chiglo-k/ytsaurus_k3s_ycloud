#!/usr/bin/env python3

from __future__ import annotations

import argparse
import hashlib
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


NULL_SENTINEL = "__null__"


def yt_table_path(path: str) -> str:
    return "ytTable:" + path.removeprefix("/")


def norm_raw_col(col_name: str):
    return F.coalesce(F.trim(F.col(col_name).cast("string")), F.lit(NULL_SENTINEL))


def norm_raw_expr(col_expr):
    return F.coalesce(F.trim(col_expr.cast("string")), F.lit(NULL_SENTINEL))


def stable_uuid_expr(namespace: str, *cols: str):
    """Keep fact_uid deterministic. Dim UUIDs are read from dim tables."""
    parts = [F.lit(namespace)]
    for col in cols:
        parts.append(F.coalesce(F.col(col).cast("string"), F.lit(NULL_SENTINEL)))

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


def random_uuid():
    """Spark-side random UUID for new dimension rows."""
    return F.expr("uuid()")


def read_yt_table(spark: SparkSession, path: str):
    return spark.read.format("yt").load(yt_table_path(path))


def to_bool(col_name: str):
    return (
        F.when(F.col(col_name).isNull(), F.lit(None).cast("boolean"))
        .when(F.col(col_name) == 0.0, F.lit(False))
        .otherwise(F.lit(True))
    )


def append_simple_dim_only_new(
    df_raw,
    spark: SparkSession,
    path: str,
    dim_name: str,
    raw_col: str,
) -> int:
    """
    Ensure dim rows exist by natural key raw_value.

    If raw_value already exists in the dimension table, skip it.
    If raw_value is new, assign a random UUID and append one new row.
    """
    uid_col = f"{dim_name}_uid"

    df_candidate = (
        df_raw
        .withColumn("raw_value", norm_raw_col(raw_col))
        .groupBy("raw_value")
        .agg(F.min("event_ts_str").alias("first_seen"))
        .withColumn(uid_col, random_uuid())
        .withColumn("_loaded_at", F.date_format(F.current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
        .select(uid_col, "raw_value", "first_seen", "_loaded_at")
        .dropDuplicates(["raw_value"])
    )

    existing = read_yt_table(spark, path)
    if "raw_value" not in existing.columns:
        raise RuntimeError(f"Existing dim table {path} has no raw_value column. Columns: {existing.columns}")

    existing_values = (
        existing
        .select(norm_raw_expr(F.col("raw_value")).alias("raw_value"))
        .dropDuplicates(["raw_value"])
    )

    df_new = (
        df_candidate
        .join(F.broadcast(existing_values), on="raw_value", how="left_anti")
        .cache()
    )

    new_count = df_new.count()
    print(f"[INFO] dim_{dim_name}: new raw values={new_count:,}", flush=True)

    if new_count > 0:
        (
            df_new.write
            .format("yt")
            .mode("append")
            .save(yt_table_path(path))
        )
        print(f"[INFO] dim_{dim_name}: appended {new_count:,} rows", flush=True)
    else:
        print(f"[INFO] dim_{dim_name}: nothing to append", flush=True)

    df_new.unpersist()
    return new_count


def append_device_dim_only_new(df_raw, spark: SparkSession, path: str) -> int:
    """Ensure device dimension rows exist by natural key device_id."""
    df_candidate = (
        df_raw
        .select(F.col("device_id").cast("long").alias("device_id"), "event_ts_str")
        .where(F.col("device_id").isNotNull())
        .groupBy("device_id")
        .agg(F.min("event_ts_str").alias("first_seen"))
        .withColumn("device_uid", random_uuid())
        .withColumn("_loaded_at", F.date_format(F.current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
        .select("device_uid", "device_id", "first_seen", "_loaded_at")
        .dropDuplicates(["device_id"])
    )

    existing = read_yt_table(spark, path)
    if "device_id" not in existing.columns:
        raise RuntimeError(f"Existing dim table {path} has no device_id column. Columns: {existing.columns}")

    existing_devices = (
        existing
        .select(F.col("device_id").cast("long").alias("device_id"))
        .where(F.col("device_id").isNotNull())
        .dropDuplicates(["device_id"])
    )

    df_new = (
        df_candidate
        .join(F.broadcast(existing_devices), on="device_id", how="left_anti")
        .cache()
    )

    new_count = df_new.count()
    print(f"[INFO] dim_device: new devices={new_count:,}", flush=True)

    if new_count > 0:
        (
            df_new.write
            .format("yt")
            .mode("append")
            .save(yt_table_path(path))
        )
        print(f"[INFO] dim_device: appended {new_count:,} rows", flush=True)
    else:
        print("[INFO] dim_device: nothing to append", flush=True)

    df_new.unpersist()
    return new_count


def load_simple_dim_mapping(spark: SparkSession, path: str, dim_name: str):
    uid_col = f"{dim_name}_uid"
    raw_key_col = f"{dim_name}_raw_value"

    dim = read_yt_table(spark, path)
    missing = [c for c in [uid_col, "raw_value"] if c not in dim.columns]
    if missing:
        raise RuntimeError(f"Dim table {path} missing columns {missing}. Columns: {dim.columns}")

    # Existing duplicates are collapsed deterministically for lookup.
    return (
        dim
        .select(
            norm_raw_expr(F.col("raw_value")).alias(raw_key_col),
            F.col(uid_col).cast("string").alias(uid_col),
        )
        .where(F.col(uid_col).isNotNull())
        .groupBy(raw_key_col)
        .agg(F.min(uid_col).alias(uid_col))
    )


def load_device_dim_mapping(spark: SparkSession, path: str):
    dim = read_yt_table(spark, path)
    missing = [c for c in ["device_uid", "device_id"] if c not in dim.columns]
    if missing:
        raise RuntimeError(f"Dim table {path} missing columns {missing}. Columns: {dim.columns}")

    return (
        dim
        .select(
            F.col("device_id").cast("long").alias("device_id"),
            F.col("device_uid").cast("string").alias("device_uid"),
        )
        .where(F.col("device_id").isNotNull() & F.col("device_uid").isNotNull())
        .groupBy("device_id")
        .agg(F.min("device_uid").alias("device_uid"))
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

    dims_simple = [
        ("country", "country_code"),
        ("timezone", "timezone"),
        ("battery_state", "battery_state"),
        ("network_status", "network_status"),
        ("charger", "charger"),
        ("health", "health"),
        ("network_type", "network_type"),
        ("mobile_network_type", "mobile_network_type"),
        ("mobile_data_status", "mobile_data_status"),
        ("mobile_data_activity", "mobile_data_activity"),
        ("wifi_status", "wifi_status"),
    ]

    # 1. Dimensions first: compare by natural raw value/device_id, not UUID.
    dim_new_counts: dict[str, int] = {}
    for dim_name, raw_col in dims_simple:
        dim_new_counts[dim_name] = append_simple_dim_only_new(
            df_raw=df_raw,
            spark=spark,
            path=f"{args.bronze_root}/dim_{dim_name}",
            dim_name=dim_name,
            raw_col=raw_col,
        )

    n_dev = append_device_dim_only_new(
        df_raw=df_raw,
        spark=spark,
        path=f"{args.bronze_root}/dim_device",
    )

    # 2. Fact uses UUIDs from the dimension tables.
    df_fact_src = (
        df_raw
        .withColumn("source_id", F.col("id").cast("long"))
        .withColumn("fact_uid", stable_uuid_expr("fact_telemetry", "id", "device_id", "event_ts_str"))
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

    # Device lookup.
    device_mapping = load_device_dim_mapping(spark, f"{args.bronze_root}/dim_device")
    df_fact_src = df_fact_src.join(F.broadcast(device_mapping), on="device_id", how="left")

    # Simple dimension lookups.
    for dim_name, raw_col in dims_simple:
        raw_key_col = f"{dim_name}_raw_value"
        mapping = load_simple_dim_mapping(spark, f"{args.bronze_root}/dim_{dim_name}", dim_name)
        df_fact_src = (
            df_fact_src
            .withColumn(raw_key_col, norm_raw_col(raw_col))
            .join(F.broadcast(mapping), on=raw_key_col, how="left")
            .drop(raw_key_col)
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

    missing = [c for c in required_uid_cols if c not in df_fact_src.columns]
    if missing:
        raise RuntimeError(f"Missing generated/joined columns before fact select: {missing}")

    for uid_col in required_uid_cols:
        if uid_col == "fact_uid":
            continue
        miss_count = df_fact_src.where(F.col(uid_col).isNull()).limit(1).count()
        if miss_count > 0:
            raise RuntimeError(f"Fact lookup produced NULL {uid_col}; dim load/join is incomplete")

    df_fact = df_fact_src.select(
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
