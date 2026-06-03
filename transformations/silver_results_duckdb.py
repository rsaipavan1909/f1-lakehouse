import os
import duckdb
import glob

BASE    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE, "data", "f1_warehouse.duckdb")
BRONZE  = os.path.join(BASE, "data", "bronze", "results")

def transform_results():
    print("Transforming results: Bronze → Silver (DuckDB)...")

    files = glob.glob(os.path.join(BRONZE, "**", "*.json"), recursive=True)
    if not files:
        raise Exception(f"No Bronze results files found at {BRONZE}")

    print(f"  Found {len(files)} files")
    files_str = ", ".join([f"'{f}'" for f in files])

    con = duckdb.connect(DB_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute("DROP TABLE IF EXISTS silver.results")

    con.execute(f"""
        CREATE TABLE silver.results AS
        SELECT
            CAST(year AS INTEGER)               AS year,
            CAST(round AS INTEGER)              AS race_round,
            event_name,
            "Abbreviation"                      AS driver_code,
            "FullName"                          AS driver_name,
            "TeamName"                          AS constructor_name,
            "TeamColor"                         AS constructor_color,
            CAST("Position" AS INTEGER)         AS finish_position,
            CAST("GridPosition" AS INTEGER)     AS grid_position,
            CAST("Points" AS DOUBLE)            AS points,
            "Status"                            AS race_status,
            "ClassifiedPosition"                AS classified_position,
            CAST("Points" AS DOUBLE) > 0        AS is_points_finish,
            _ingested_at,
            current_timestamp                   AS _silver_processed_at,
            "Abbreviation" IS NOT NULL          AS dq_valid_driver,
            CAST("Points" AS DOUBLE) >= 0       AS dq_valid_points,
            CAST("Position" AS INTEGER)
                BETWEEN 1 AND 20                AS dq_valid_position
        FROM read_json_auto([{files_str}],
            union_by_name = true,
            ignore_errors = true
        )
        WHERE year IS NOT NULL
    """)

    count = con.execute("SELECT COUNT(*) FROM silver.results").fetchone()[0]
    print(f"  Silver results rows: {count:,}")
    print(con.execute("""
        SELECT COUNT(*) AS total_results,
               SUM(CASE WHEN dq_valid_driver THEN 1 END) AS valid_drivers
        FROM silver.results
    """).df().to_string())

    con.close()
    print("  Results Silver transformation complete!")

if __name__ == "__main__":
    transform_results()