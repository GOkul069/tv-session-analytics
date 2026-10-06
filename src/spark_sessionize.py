"""PySpark ETL: raw set-top box telemetry -> analysis-ready session fact table.

This is the production-scale version of the logic in ``src/sessionize.py`` (pandas, used by the
notebooks on a sample). It is written for a Spark 3.x cluster and is not executed in this
repository (PySpark is not part of requirements.txt).

Pipeline
--------
1. Read raw TUNE / STANDBY events (gzip CSV or Parquet landing zone, partitioned by event date).
2. Clean: drop exact duplicates (telemetry retries), drop events outside the extraction window
   (device clock resets), normalise empty strings to NULL.
3. Sessionize with window functions per device ordered by event time:
   session_end = next event time, capped at a 240-minute inactivity timeout when the next event is
   missing or later than the cap (boxes that lose power never send STANDBY).
4. Derive duration, zap flag (< 30 s), end reason, date/hour keys.
5. Write the fact table as Parquet partitioned by session_date, and optionally load it into
   Snowflake with the Spark-Snowflake connector (``sql/`` holds the Snowflake DDL and models).

Example
-------
    spark-submit --packages net.snowflake:spark-snowflake_2.12:2.16.0-spark_3.4 \\
        src/spark_sessionize.py \\
        --input s3://iptv-telemetry/raw/stb_events/ --input-format csv \\
        --output s3://iptv-analytics/curated/fact_session/ \\
        --window-start 2025-03-03 --window-end 2025-04-01 \\
        --snowflake-table ANALYTICS.CURATED.FACT_SESSION

Snowflake credentials are read from environment variables (SNOWFLAKE_URL, SNOWFLAKE_USER,
SNOWFLAKE_PASSWORD, SNOWFLAKE_ROLE, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA).
"""
from __future__ import annotations

import argparse
import os

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

INACTIVITY_TIMEOUT_MIN = 240
ZAP_THRESHOLD_SEC = 30

RAW_SCHEMA = T.StructType([
    T.StructField("device_id", T.StringType(), False),
    T.StructField("event_time", T.TimestampType(), False),
    T.StructField("event_type", T.StringType(), False),
    T.StructField("channel_id", T.StringType(), True),
    T.StructField("playback_mode", T.StringType(), True),
    T.StructField("content_age_min", T.IntegerType(), True),
])


def read_events(spark: SparkSession, path: str, fmt: str) -> DataFrame:
    if fmt == "csv":
        return (spark.read.option("header", True).option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
                .schema(RAW_SCHEMA).csv(path))
    return spark.read.parquet(path)


def clean_events(ev: DataFrame, window_start: str, window_end: str) -> DataFrame:
    """Deduplicate and drop out-of-window events; empty strings become NULL."""
    for c in ("channel_id", "playback_mode"):
        ev = ev.withColumn(c, F.when(F.trim(F.col(c)) == "", None).otherwise(F.col(c)))
    return (
        ev.dropDuplicates(["device_id", "event_time", "event_type", "channel_id", "playback_mode",
                           "content_age_min"])
        .filter((F.col("event_time") >= F.lit(window_start).cast("timestamp"))
                & (F.col("event_time") < F.lit(window_end).cast("timestamp")))
    )


def sessionize(ev: DataFrame) -> DataFrame:
    """One row per TUNE event, ended by the device's next event or the inactivity timeout."""
    # event_type as a tie-breaker so STANDBY sorts before TUNE at the same second, as in pandas
    w = Window.partitionBy("device_id").orderBy("event_time", "event_type")
    ev = (ev.withColumn("next_time", F.lead("event_time").over(w))
            .withColumn("next_type", F.lead("event_type").over(w)))

    cap = F.col("session_start") + F.expr(f"INTERVAL {INACTIVITY_TIMEOUT_MIN} MINUTES")
    timed_out = F.col("next_time").isNull() | (F.col("next_time") > cap)

    s = (
        ev.filter(F.col("event_type") == "TUNE")
        .withColumnRenamed("event_time", "session_start")
        .withColumn("session_end", F.when(timed_out, cap).otherwise(F.col("next_time")))
        .withColumn("end_reason", F.when(timed_out, "TIMEOUT")
                    .when(F.col("next_type") == "STANDBY", "STANDBY").otherwise("NEXT_TUNE"))
        .withColumn("duration_sec",
                    (F.col("session_end").cast("long") - F.col("session_start").cast("long")).cast("int"))
        .withColumn("is_zap", F.col("duration_sec") < ZAP_THRESHOLD_SEC)
        .withColumn("session_date", F.to_date("session_start"))
        .withColumn("start_hour", F.hour("session_start"))
        # deterministic surrogate key: stable across re-runs of the same input
        .withColumn("session_id", F.xxhash64("device_id", "session_start", "channel_id", "playback_mode"))
    )
    return s.select("session_id", "device_id", "session_start", "session_end", "session_date",
                    "start_hour", "channel_id", "playback_mode", "content_age_min",
                    "duration_sec", "is_zap", "end_reason")


def quality_report(raw: DataFrame, clean: DataFrame, sessions: DataFrame) -> dict:
    return {
        "raw_events": raw.count(),
        "clean_events": clean.count(),
        "sessions": sessions.count(),
        "timeout_sessions": sessions.filter(F.col("end_reason") == "TIMEOUT").count(),
        "negative_durations": sessions.filter(F.col("duration_sec") < 0).count(),
    }


def write_snowflake(df: DataFrame, table: str) -> None:
    opts = {
        "sfURL": os.environ["SNOWFLAKE_URL"],
        "sfUser": os.environ["SNOWFLAKE_USER"],
        "sfPassword": os.environ["SNOWFLAKE_PASSWORD"],
        "sfRole": os.environ.get("SNOWFLAKE_ROLE", "TRANSFORMER"),
        "sfWarehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "ETL_WH"),
        "sfDatabase": os.environ.get("SNOWFLAKE_DATABASE", "ANALYTICS"),
        "sfSchema": os.environ.get("SNOWFLAKE_SCHEMA", "CURATED"),
    }
    (df.write.format("net.snowflake.spark.snowflake").options(**opts)
       .option("dbtable", table.split(".")[-1]).mode("append").save())


def main() -> None:
    ap = argparse.ArgumentParser(description="Sessionize raw STB telemetry")
    ap.add_argument("--input", required=True)
    ap.add_argument("--input-format", choices=["csv", "parquet"], default="parquet")
    ap.add_argument("--output", required=True)
    ap.add_argument("--window-start", required=True)
    ap.add_argument("--window-end", required=True)
    ap.add_argument("--snowflake-table", default=None)
    ap.add_argument("--shuffle-partitions", type=int, default=2000)
    args = ap.parse_args()

    spark = (SparkSession.builder.appName("stb-sessionize")
             .config("spark.sql.shuffle.partitions", args.shuffle_partitions)
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())

    raw = read_events(spark, args.input, args.input_format)
    # repartition by device so the window's sort happens once per device, not across the cluster
    clean = clean_events(raw, args.window_start, args.window_end).repartition("device_id")
    sessions = sessionize(clean).cache()

    (sessions.write.mode("overwrite").partitionBy("session_date")
             .option("compression", "snappy").parquet(args.output))
    if args.snowflake_table:
        write_snowflake(sessions, args.snowflake_table)

    print(quality_report(raw, clean, sessions))
    spark.stop()


if __name__ == "__main__":
    main()
