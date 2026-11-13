"""Spark Structured Streaming: Kafka → silver vehicle positions (+ quarantine).

Runs two streaming queries over the same Kafka topic:

  silver_vehicle_positions   valid, deduplicated events
      s3a://<bucket>/silver/vehicle_positions/feed=*/event_date=*/
  quarantine_vehicle_positions   rows that failed a data-quality rule, with the reason
      s3a://<bucket>/quarantine/vehicle_positions/processed_date=*/dq_reason=*/

Exactly-once output: the Parquet file sink records committed files in
_spark_metadata, and offsets + dedupe state live in the checkpoint, so a
restart resumes exactly where it stopped.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

from jobs.transforms import add_quality_flags, parse_kafka, standardize, to_quarantine, to_silver


def env(name: str, default: str) -> str:
    return os.getenv(name, default)


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("silver_vehicle_positions")
        .config("spark.sql.session.timeZone", "UTC")
        # Small local machine: few shuffle partitions → few dedupe state stores.
        .config("spark.sql.shuffle.partitions", env("SPARK_SHUFFLE_PARTITIONS", "4"))
        .config("spark.sql.streaming.stateStore.providerClass",
                "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
        # S3A → local SeaweedFS (unset S3_ENDPOINT_URL for real AWS S3)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", env("S3_ENDPOINT_URL", ""))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled",
                str(env("S3_ENDPOINT_URL", "https").startswith("https")).lower())
        .config("spark.hadoop.fs.s3a.access.key", env("AWS_ACCESS_KEY_ID", ""))
        .config("spark.hadoop.fs.s3a.secret.key", env("AWS_SECRET_ACCESS_KEY", ""))
        .getOrCreate()
    )


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    bucket = env("LAKE_BUCKET", "transit-lake")
    lake = f"s3a://{bucket}"
    trigger = f"{env('SPARK_TRIGGER_SECONDS', '60')} seconds"
    watermark = env("DEDUPE_WATERMARK", "15 minutes")
    # Checkpoints need real directory semantics (atomic rename). Locally they live on a
    # Docker volume; in the cloud point this at HDFS/DBFS or GCS (see README).
    checkpoints = env("SPARK_CHECKPOINT_DIR", "/checkpoints")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", env("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"))
        .option("subscribe", env("KAFKA_TOPIC_VEHICLE_POSITIONS", "gtfs.vehicle_positions"))
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", env("SPARK_MAX_OFFSETS_PER_TRIGGER", "50000"))
        # Topic retention may delete old offsets while the job is down: keep going.
        .option("failOnDataLoss", "false")
        .load()
    )
    events = add_quality_flags(standardize(parse_kafka(raw)))

    (
        to_silver(events, watermark)
        # Dedupe shuffles rows over SPARK_SHUFFLE_PARTITIONS tasks, and each task
        # writes its own file per output partition: ~10 tiny files a minute.
        # Grouping by the partition column first gives one file per feed per batch.
        .repartition("feed", "event_date")
        .writeStream.queryName("silver_vehicle_positions")
        .format("parquet")
        .outputMode("append")
        .partitionBy("feed", "event_date")
        .option("path", f"{lake}/silver/vehicle_positions")
        .option("checkpointLocation", f"{checkpoints}/silver_vehicle_positions")
        .trigger(processingTime=trigger)
        .start()
    )
    (
        to_quarantine(events)
        .coalesce(1)  # a handful of rows per batch: one file is plenty
        .writeStream.queryName("quarantine_vehicle_positions")
        .format("parquet")
        .outputMode("append")
        .partitionBy("processed_date", "dq_reason")
        .option("path", f"{lake}/quarantine/vehicle_positions")
        .option("checkpointLocation", f"{checkpoints}/quarantine_vehicle_positions")
        .trigger(processingTime=trigger)
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
