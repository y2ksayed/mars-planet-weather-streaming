"""PySpark Structured Streaming: Mars weather bronze -> silver -> gold.

Reads JSON from Kafka and writes three medallion layers:
bronze (raw), silver (cleaned), gold (daily aggregates).

Run:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
        src/streaming_job.py

All logic is Python/PySpark — no Scala or JVM code.
"""

from __future__ import annotations

import argparse
import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType,
)

from config import settings

logger = logging.getLogger(__name__)

# Explicit schema so bad fields fail loudly instead of becoming nulls.
OBSERVATION_SCHEMA = StructType([
    StructField("sol", IntegerType(), False),
    StructField("terrestrial_date", StringType(), True),
    StructField("season", StringType(), True),
    StructField("air_temp_avg_c", DoubleType(), True),
    StructField("air_temp_min_c", DoubleType(), True),
    StructField("air_temp_max_c", DoubleType(), True),
    StructField("air_temp_samples", IntegerType(), True),
    StructField("pressure_pa", DoubleType(), True),
    StructField("pressure_samples", IntegerType(), True),
    StructField("wind_speed_ms", DoubleType(), True),
    StructField("wind_speed_max_ms", DoubleType(), True),
    StructField("ingested_at", StringType(), True),
    StructField("source", StringType(), True),
])

def build_spark(app_name: str, master: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name).master(master)
        .config("spark.sql.streaming.schemaInference", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

def read_kafka(spark: SparkSession, servers: str, topic: str) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

def to_bronze(kafka_df: DataFrame) -> DataFrame:
    """Bronze keeps the raw JSON plus Kafka metadata for debugging."""
    return kafka_df.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("raw_json"),
        F.col("topic"), F.col("partition"), F.col("offset"),
        F.col("timestamp").alias("kafka_timestamp"),
    )

def to_silver(bronze_df: DataFrame) -> DataFrame:
    """Silver: parse, validate, deduplicate — one row per sol."""
    parsed = bronze_df.select(
        F.from_json(F.col("raw_json"), OBSERVATION_SCHEMA).alias("d"),
        F.col("kafka_timestamp"),
    ).select("d.*", "kafka_timestamp")
    return (
        parsed.filter(F.col("sol").isNotNull())
        .filter(F.col("air_temp_avg_c").isNotNull())
        # Sanity bounds for Mars surface conditions.
        .filter((F.col("air_temp_avg_c") > -150)
                & (F.col("air_temp_avg_c") < 30))
        .filter((F.col("pressure_pa") > 400)
                & (F.col("pressure_pa") < 1000))
        .withWatermark("kafka_timestamp", settings.watermark_delay)
        .dropDuplicates(["sol"])
        .withColumn("processed_at", F.current_timestamp())
    )

def to_gold(silver_df: DataFrame) -> DataFrame:
    """Gold: one aggregated row per sol for dashboards."""
    return (
        silver_df.groupBy("sol", "terrestrial_date").agg(
            F.avg("air_temp_avg_c").alias("avg_temp_c"),
            F.min("air_temp_min_c").alias("min_temp_c"),
            F.max("air_temp_max_c").alias("max_temp_c"),
            F.avg("pressure_pa").alias("avg_pressure_pa"),
            F.avg("wind_speed_ms").alias("avg_wind_ms"),
            F.max("wind_speed_max_ms").alias("max_wind_ms"),
            F.count("*").alias("observation_count"),
        ).withColumn("computed_at", F.current_timestamp())
    )

def start_sink(df: DataFrame, path: str, checkpoint: str,
               name: str, mode: str) -> None:
    (df.writeStream.format("parquet").outputMode(mode)
       .option("path", path).option("checkpointLocation", checkpoint)
       .trigger(processingTime=settings.trigger_interval)
       .queryName(name).start())
    logger.info("Started %s -> %s", name, path)

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Mars weather streaming job")
    p.add_argument("--kafka-bootstrap-servers",
                   default=settings.kafka_bootstrap_servers)
    p.add_argument("--input-topic", default=settings.kafka_topic)
    p.add_argument("--output-base", default=settings.output_base)
    p.add_argument("--checkpoint-dir", default=settings.checkpoint_dir)
    p.add_argument("--spark-master", default=settings.spark_master)
    return p.parse_args()

def main() -> None:
    logging.basicConfig(level=logging.INFO)
    args = parse_args()
    spark = build_spark(settings.spark_app_name, args.spark_master)
    spark.sparkContext.setLogLevel("WARN")

    kafka_df = read_kafka(spark, args.kafka_bootstrap_servers,
                          args.input_topic)
    bronze_df = to_bronze(kafka_df)
    silver_df = to_silver(bronze_df)
    gold_df = to_gold(silver_df)

    start_sink(bronze_df, f"{args.output_base}/bronze",
               f"{args.checkpoint_dir}/bronze", "mars_bronze", "append")
    start_sink(silver_df, f"{args.output_base}/silver",
               f"{args.checkpoint_dir}/silver", "mars_silver", "append")
    start_sink(gold_df, f"{args.output_base}/gold",
               f"{args.checkpoint_dir}/gold", "mars_gold", "complete")
    logger.info("All queries started — awaiting termination")
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
