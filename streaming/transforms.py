"""Pure DataFrame transformations (no I/O) so they can be unit-tested with a local SparkSession."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

_STR = ["event_id", "tpep_pickup_datetime", "tpep_dropoff_datetime", "store_and_fwd_flag"]
_NUM = [
    "VendorID", "passenger_count", "trip_distance", "RatecodeID", "PULocationID", "DOLocationID",
    "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
    "improvement_surcharge", "total_amount", "congestion_surcharge", "Airport_fee",
]
# Numerics are read as double and cast later: a single "1.0" where an int is expected would otherwise
# null the whole record in from_json's PERMISSIVE mode.
TRIP_SCHEMA = StructType(
    [StructField(c, StringType()) for c in _STR] + [StructField(c, DoubleType()) for c in _NUM]
)


def parse_trips(raw: DataFrame, payload_col: str = "payload") -> DataFrame:
    """JSON string -> typed, renamed columns."""
    t = raw.select(F.from_json(F.col(payload_col), TRIP_SCHEMA).alias("t")).select("t.*")
    return t.select(
        "event_id",
        F.col("VendorID").cast("int").alias("vendor_id"),
        F.to_timestamp("tpep_pickup_datetime").alias("pickup_ts"),
        F.to_timestamp("tpep_dropoff_datetime").alias("dropoff_ts"),
        F.col("passenger_count").cast("int").alias("passenger_count"),
        F.col("trip_distance"),
        F.col("RatecodeID").cast("int").alias("rate_code_id"),
        F.col("store_and_fwd_flag"),
        F.col("PULocationID").cast("int").alias("pu_location_id"),
        F.col("DOLocationID").cast("int").alias("do_location_id"),
        F.col("payment_type").cast("int").alias("payment_type"),
        "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
        "improvement_surcharge", "total_amount", "congestion_surcharge",
        F.col("Airport_fee").alias("airport_fee"),
    ).withColumn("pickup_date", F.to_date("pickup_ts"))


def add_validation(df: DataFrame) -> DataFrame:
    """Adds `reject_reason` (NULL = valid). First matching rule wins."""
    reason = (
        F.when(F.col("pickup_ts").isNull() | F.col("dropoff_ts").isNull(), "missing_timestamp")
        .when(F.col("pu_location_id").isNull() | F.col("do_location_id").isNull(), "missing_location")
        .when(F.col("fare_amount").isNull() | (F.col("fare_amount") < 0), "invalid_fare")
        .when(F.col("dropoff_ts") <= F.col("pickup_ts"), "non_positive_duration")
        .when(F.col("trip_distance").isNull() | (F.col("trip_distance") < 0) | (F.col("trip_distance") > 500),
              "invalid_distance")
    )
    return df.withColumn("reject_reason", reason)
