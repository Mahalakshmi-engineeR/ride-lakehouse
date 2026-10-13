"""Silver: bronze Delta -> parsed, validated, de-duplicated trips; bad rows go to a quarantine table."""
import json
import time

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from streaming import config
from streaming.metrics import MetricsListener
from streaming.spark_session import get_spark
from streaming.transforms import add_validation, parse_trips


def wait_for_delta(spark, path: str) -> None:
    while not DeltaTable.isDeltaTable(spark, path):
        print(f"waiting for {path} ...", flush=True)
        time.sleep(5)


def process_batch(batch_df, batch_id: int) -> None:
    """Split a micro-batch into valid / rejected and write both idempotently."""
    flagged = add_validation(batch_df).cache()
    valid = flagged.filter(F.col("reject_reason").isNull()).drop("reject_reason")
    rejects = flagged.filter(F.col("reject_reason").isNotNull())
    n_valid, n_rejected = valid.count(), rejects.count()

    # txnAppId/txnVersion make the Delta write idempotent if Spark retries this batch.
    (valid.withColumn("processed_ts", F.current_timestamp())
        .write.format("delta").mode("append")
        .option("txnAppId", "silver_valid").option("txnVersion", batch_id)
        .partitionBy("pickup_date").save(config.SILVER_PATH))
    (rejects.withColumn("processed_ts", F.current_timestamp())
        .write.format("delta").mode("append")
        .option("txnAppId", "silver_quarantine").option("txnVersion", batch_id)
        .save(config.QUARANTINE_PATH))

    total = n_valid + n_rejected
    print(json.dumps({
        "event": "silver_batch", "batch_id": batch_id, "valid": n_valid, "rejected": n_rejected,
        "reject_rate": round(n_rejected / total, 4) if total else 0.0,
    }), flush=True)
    flagged.unpersist()


def main() -> None:
    spark = get_spark("silver_trips")
    spark.streams.addListener(MetricsListener())
    wait_for_delta(spark, config.BRONZE_PATH)

    bronze = spark.readStream.format("delta").option("maxFilesPerTrigger", 20).load(config.BRONZE_PATH)
    deduped = (
        parse_trips(bronze.select("payload"))
        # State for a given (event_id, pickup_ts) is dropped once the watermark passes it.
        .withWatermark("pickup_ts", "30 minutes")
        .dropDuplicates(["event_id", "pickup_ts"])
    )
    query = (
        deduped.writeStream.foreachBatch(process_batch)
        .queryName("silver_trips")
        .option("checkpointLocation", f"{config.CHECKPOINT_BASE}/silver_trips")
        .trigger(processingTime="15 seconds")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
