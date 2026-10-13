"""Bronze: Kafka -> Delta, raw payload kept as-is plus Kafka lineage metadata. No cleaning here."""
from pyspark.sql import functions as F

from streaming import config
from streaming.metrics import MetricsListener
from streaming.spark_session import get_spark


def main() -> None:
    spark = get_spark("bronze_trips")
    spark.streams.addListener(MetricsListener())

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
        .option("subscribe", config.KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 50_000)
        .option("failOnDataLoss", "false")
        .load()
    )
    bronze = raw.select(
        F.col("key").cast("string").alias("event_key"),
        F.col("value").cast("string").alias("payload"),
        F.col("topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_ts"),
        F.current_timestamp().alias("ingest_ts"),
    )
    query = (
        bronze.writeStream.format("delta")
        .outputMode("append")
        .queryName("bronze_trips")
        .option("checkpointLocation", f"{config.CHECKPOINT_BASE}/bronze_trips")
        .trigger(processingTime="10 seconds")
        .start(config.BRONZE_PATH)
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
