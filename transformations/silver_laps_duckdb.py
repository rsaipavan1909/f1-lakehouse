import os
import duckdb
import glob

BASE     = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH  = os.path.join(BASE, "data", "f1_warehouse.duckdb")
BRONZE   = os.path.join(BASE, "data", "bronze", "laps")

def transform_laps():
    print("Transforming laps: Bronze → Silver (DuckDB)...")

    files = glob.glob(os.path.join(BRONZE, "**", "*.json"), recursive=True)
    if not files:
        raise Exception(f"No Bronze laps files found at {BRONZE}")

    print(f"  Found {len(files)} files")
    files_str = ", ".join([f"'{f}'" for f in files])

    con = duckdb.connect(DB_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute("DROP TABLE IF EXISTS silver.laps")

    con.execute(f"""
        CREATE TABLE silver.laps AS
        SELECT
            CAST(year AS INTEGER)                       AS year,
            CAST(round AS INTEGER)                      AS race_round,
            event_name,
            "Driver"                                    AS driver_code,
            "Team"                                      AS team_name,
            CAST("LapNumber" AS INTEGER)                AS lap_number,
            "Compound"                                  AS tyre_compound,
            CAST("TyreLife" AS INTEGER)                 AS tyre_life_laps,
            CAST("FreshTyre" AS BOOLEAN)                AS is_fresh_tyre,
            CAST("IsPersonalBest" AS BOOLEAN)           AS is_personal_best,
            CAST("SpeedI1" AS DOUBLE)                   AS speed_sector1,
            CAST("SpeedI2" AS DOUBLE)                   AS speed_sector2,
            CAST("SpeedFL" AS DOUBLE)                   AS speed_finish_line,
            CAST("SpeedST" AS DOUBLE)                   AS speed_longest_straight,
            "LapTime"                                   AS lap_time_raw,
            "Sector1Time"                               AS sector1_raw,
            "Sector2Time"                               AS sector2_raw,
            "Sector3Time"                               AS sector3_raw,
            _ingested_at,
            current_timestamp                           AS _silver_processed_at,
            "LapTime" IS NOT NULL                       AS dq_valid_lap_time,
            "Compound" IS NOT NULL                      AS dq_valid_compound,
            CAST("SpeedST" AS DOUBLE) > 0               AS dq_valid_speed
        FROM read_json_auto([{files_str}],
            union_by_name = true,
            ignore_errors = true
        )
        WHERE year IS NOT NULL
    """)

    count = con.execute("SELECT COUNT(*) FROM silver.laps").fetchone()[0]
    print(f"  Silver laps rows: {count:,}")
    print(con.execute("""
        SELECT COUNT(*) AS total_laps,
               SUM(CASE WHEN dq_valid_lap_time THEN 1 END) AS valid_lap_times
        FROM silver.laps
    """).df().to_string())

    con.close()
    print("  Laps Silver transformation complete!")

if __name__ == "__main__":
    transform_laps()