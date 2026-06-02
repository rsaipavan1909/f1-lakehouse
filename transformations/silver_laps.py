import os
import sys

# Always run from project root
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from spark_session import get_spark
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType, BooleanType

BRONZE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../data/bronze/laps")
SILVER_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../data/silver/laps")

def parse_lap_time_to_seconds(col_name: str):
    """
    FastF1 stores lap times as strings like '0 days 00:01:32.456000000'.
    Converts to float seconds for easy querying and charting.
    """
    return (
        F.regexp_extract(col_name, r'(\d+):(\d+)\.(\d+)', 1).cast("float") * 60 +
        F.regexp_extract(col_name, r'(\d+):(\d+)\.(\d+)', 2).cast("float") +
        F.regexp_extract(col_name, r'(\d+):(\d+)\.(\d+)', 3).cast("float") / 1e9
    )

def transform_laps(spark):
    print("Transforming laps: Bronze → Silver...")

    # Check Bronze path exists
    if not os.path.exists(BRONZE_PATH):
        print(f"ERROR: Bronze path not found: {BRONZE_PATH}")
        print("Make sure you ran bronze_ingestion.py first.")
        return

    # 1. Read all Bronze JSON files
    df = spark.read \
        .option("recursiveFileLookup", "true") \
        .option("multiLine", "true") \
        .option("mode", "PERMISSIVE") \
        .json(BRONZE_PATH)

    df = df.cache()
    row_count = df.count()
    print(f"  Bronze row count: {row_count}")

    if row_count == 0:
        print("ERROR: No data found in Bronze laps folder.")
        return

    # 2. Print schema so we can see what columns exist
    print("  Bronze schema:")
    df.printSchema()

    # 3. Select only columns that exist — handle missing columns gracefully
    available = df.columns

    select_cols = []

    if "year" in available:
        select_cols.append(F.col("year").cast(IntegerType()))
    if "round" in available:
        select_cols.append(F.col("round").cast(IntegerType()))
    if "event_name" in available:
        select_cols.append(F.col("event_name"))
    if "Driver" in available:
        select_cols.append(F.col("Driver").alias("driver_code"))
    if "LapNumber" in available:
        select_cols.append(F.col("LapNumber").cast(IntegerType()).alias("lap_number"))
    if "LapTime" in available:
        select_cols.append(F.col("LapTime").alias("lap_time_raw"))
    if "Sector1Time" in available:
        select_cols.append(F.col("Sector1Time").alias("sector1_raw"))
    if "Sector2Time" in available:
        select_cols.append(F.col("Sector2Time").alias("sector2_raw"))
    if "Sector3Time" in available:
        select_cols.append(F.col("Sector3Time").alias("sector3_raw"))
    if "SpeedI1" in available:
        select_cols.append(F.col("SpeedI1").cast(FloatType()).alias("speed_sector1"))
    if "SpeedI2" in available:
        select_cols.append(F.col("SpeedI2").cast(FloatType()).alias("speed_sector2"))
    if "SpeedFL" in available:
        select_cols.append(F.col("SpeedFL").cast(FloatType()).alias("speed_finish_line"))
    if "SpeedST" in available:
        select_cols.append(F.col("SpeedST").cast(FloatType()).alias("speed_longest_straight"))
    if "Compound" in available:
        select_cols.append(F.col("Compound").alias("tyre_compound"))
    if "TyreLife" in available:
        select_cols.append(F.col("TyreLife").cast(IntegerType()).alias("tyre_life_laps"))
    if "FreshTyre" in available:
        select_cols.append(F.col("FreshTyre").cast(BooleanType()).alias("is_fresh_tyre"))
    if "IsPersonalBest" in available:
        select_cols.append(F.col("IsPersonalBest").cast(BooleanType()).alias("is_personal_best"))
    if "_ingested_at" in available:
        select_cols.append(F.col("_ingested_at"))

    df = df.select(select_cols)

    # 4. Parse time strings to seconds
    if "lap_time_raw" in df.columns:
        df = df.withColumn("lap_time_seconds", parse_lap_time_to_seconds("lap_time_raw"))
        df = df.drop("lap_time_raw")
    if "sector1_raw" in df.columns:
        df = df.withColumn("sector1_seconds", parse_lap_time_to_seconds("sector1_raw"))
        df = df.drop("sector1_raw")
    if "sector2_raw" in df.columns:
        df = df.withColumn("sector2_seconds", parse_lap_time_to_seconds("sector2_raw"))
        df = df.drop("sector2_raw")
    if "sector3_raw" in df.columns:
        df = df.withColumn("sector3_seconds", parse_lap_time_to_seconds("sector3_raw"))
        df = df.drop("sector3_raw")

    # 5. Data quality flags
    if "lap_time_seconds" in df.columns:
        df = df.withColumn("dq_valid_lap_time",
                F.col("lap_time_seconds").isNotNull() & (F.col("lap_time_seconds") > 0))
        df = df.withColumn("dq_reasonable_lap_time",
                F.col("lap_time_seconds").between(60, 300))
    if all(c in df.columns for c in ["sector1_seconds", "sector2_seconds", "sector3_seconds"]):
        df = df.withColumn("dq_valid_sectors",
                F.col("sector1_seconds").isNotNull() &
                F.col("sector2_seconds").isNotNull() &
                F.col("sector3_seconds").isNotNull())

    # 6. Deduplicate
    dedup_cols = [c for c in ["year", "round", "driver_code", "lap_number"] if c in df.columns]
    if dedup_cols:
        df = df.dropDuplicates(dedup_cols)

    # 7. Add Silver metadata
    df = df.withColumn("_silver_processed_at", F.current_timestamp())
    df = df.withColumn("_layer", F.lit("silver"))

    print(f"  Silver row count: {df.count()}")

    # 8. Write as Delta table
    os.makedirs(SILVER_PATH, exist_ok=True)
    partition_cols = ["year"] if "year" in df.columns else []

    writer = df.write.format("delta").mode("overwrite")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(SILVER_PATH)

    print(f"  Written to {SILVER_PATH}")

    # 9. Show sample and quality summary
    print("\nSample rows:")
    show_cols = [c for c in ["year", "round", "driver_code", "lap_number",
                              "lap_time_seconds", "tyre_compound",
                              "dq_valid_lap_time"] if c in df.columns]
    df.select(show_cols).show(5)

    if "dq_valid_lap_time" in df.columns:
        print("\nData quality summary:")
        df.agg(
            F.count("*").alias("total_laps"),
            F.sum(F.col("dq_valid_lap_time").cast("int")).alias("valid_lap_times"),
            F.sum(F.col("dq_reasonable_lap_time").cast("int")).alias("reasonable_times")
        ).show()

if __name__ == "__main__":
    spark = get_spark("F1-Silver-Laps")
    transform_laps(spark)
    spark.stop()
    print("Laps Silver transformation complete!")