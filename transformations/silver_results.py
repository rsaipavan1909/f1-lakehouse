import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from spark_session import get_spark
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType

BRONZE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../data/bronze/results")
SILVER_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../data/silver/results")

def transform_results(spark):
    print("Transforming results: Bronze → Silver...")

    if not os.path.exists(BRONZE_PATH):
        print(f"ERROR: Bronze path not found: {BRONZE_PATH}")
        return

    # 1. Read Bronze JSON
    df = spark.read \
        .option("recursiveFileLookup", "true") \
        .option("multiLine", "true") \
        .option("mode", "PERMISSIVE") \
        .json(BRONZE_PATH)

    df = df.cache()
    row_count = df.count()
    print(f"  Bronze row count: {row_count}")

    if row_count == 0:
        print("ERROR: No data found in Bronze results folder.")
        return

    print("  Bronze schema:")
    df.printSchema()

    available = df.columns
    select_cols = []

    if "year" in available:
        select_cols.append(F.col("year").cast(IntegerType()))
    if "round" in available:
        select_cols.append(F.col("round").cast(IntegerType()))
    if "event_name" in available:
        select_cols.append(F.col("event_name"))
    if "Abbreviation" in available:
        select_cols.append(F.col("Abbreviation").alias("driver_code"))
    if "FullName" in available:
        select_cols.append(F.col("FullName").alias("driver_name"))
    if "TeamName" in available:
        select_cols.append(F.col("TeamName").alias("constructor_name"))
    if "TeamColor" in available:
        select_cols.append(F.col("TeamColor").alias("constructor_color"))
    if "Position" in available:
        select_cols.append(F.col("Position").cast(IntegerType()).alias("finish_position"))
    if "GridPosition" in available:
        select_cols.append(F.col("GridPosition").cast(IntegerType()).alias("grid_position"))
    if "Points" in available:
        select_cols.append(F.col("Points").cast(FloatType()).alias("points"))
    if "Status" in available:
        select_cols.append(F.col("Status").alias("race_status"))
    if "ClassifiedPosition" in available:
        select_cols.append(F.col("ClassifiedPosition").alias("classified_position"))
    if "_ingested_at" in available:
        select_cols.append(F.col("_ingested_at"))

    df = df.select(select_cols)

    # 2. Derive useful columns
    if all(c in df.columns for c in ["grid_position", "finish_position"]):
        df = df.withColumn("positions_gained",
                F.col("grid_position") - F.col("finish_position"))
    if "points" in df.columns:
        df = df.withColumn("is_points_finish", F.col("points") > 0)
    if "race_status" in df.columns:
        df = df.withColumn("is_classified", F.col("race_status") == "Finished")

    # 3. Data quality flags
    if "finish_position" in df.columns:
        df = df.withColumn("dq_valid_position",
                F.col("finish_position").between(1, 20))
    if "points" in df.columns:
        df = df.withColumn("dq_valid_points", F.col("points") >= 0)

    # 4. Deduplicate
    dedup_cols = [c for c in ["year", "round", "driver_code"] if c in df.columns]
    if dedup_cols:
        df = df.dropDuplicates(dedup_cols)

    # 5. Silver metadata
    df = df.withColumn("_silver_processed_at", F.current_timestamp())
    df = df.withColumn("_layer", F.lit("silver"))

    print(f"  Silver row count: {df.count()}")

    # 6. Write Delta table
    os.makedirs(SILVER_PATH, exist_ok=True)
    partition_cols = ["year"] if "year" in df.columns else []

    writer = df.write.format("delta").mode("overwrite")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(SILVER_PATH)

    print(f"  Written to {SILVER_PATH}")

    print("\nSample rows:")
    show_cols = [c for c in ["year", "round", "driver_code", "finish_position",
                              "points", "race_status", "positions_gained"] if c in df.columns]
    df.select(show_cols).show(5)

    if "dq_valid_position" in df.columns:
        print("\nData quality summary:")
        df.agg(
            F.count("*").alias("total_results"),
            F.sum(F.col("dq_valid_position").cast("int")).alias("valid_positions"),
            F.sum(F.col("dq_valid_points").cast("int")).alias("valid_points")
        ).show()

if __name__ == "__main__":
    spark = get_spark("F1-Silver-Results")
    transform_results(spark)
    spark.stop()
    print("Results Silver transformation complete!")