import duckdb
import os

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE, "data", "f1_warehouse.duckdb")
OUT_DIR = os.path.join(BASE, "dashboards", "data")

os.makedirs(OUT_DIR, exist_ok=True)

con = duckdb.connect(DB_PATH)

exports = {
    "driver_standings": """
        SELECT * FROM gold.gold_driver_standings
    """,

    "race_results": """
        SELECT * FROM gold.fact_race_results
    """,

    "dim_driver": """
        SELECT * FROM gold.dim_driver
    """,

    "dim_race": """
        SELECT * FROM gold.dim_race
    """,

    "lap_summary": """
        SELECT
            f.race_year,
            f.race_round,
            f.race_name,
            f.driver_code,
            f.team_name,

            COUNT(l.LapNumber) AS total_laps,

            ROUND(
                AVG(
                    CASE
                        WHEN l.LapTime IS NOT NULL
                        THEN EXTRACT(EPOCH FROM TRY_CAST(l.LapTime AS INTERVAL))
                        ELSE NULL
                    END
                ), 3
            ) AS avg_lap_time_seconds,

            MAX(l.SpeedST) AS top_speed,

            SUM(
                CASE
                    WHEN l.IsPersonalBest = TRUE THEN 1
                    ELSE 0
                END
            ) AS personal_bests

        FROM gold.fact_race_results f
        LEFT JOIN silver.laps l
            ON f.race_year = l.year
            AND f.race_round = l.round
            AND f.driver_code = l.Driver

        GROUP BY
            f.race_year,
            f.race_round,
            f.race_name,
            f.driver_code,
            f.team_name

        ORDER BY
            f.race_year,
            f.race_round,
            f.driver_code
    """
}

for filename, query in exports.items():
    try:
        df = con.execute(query).df()
        path = os.path.join(OUT_DIR, f"{filename}.csv")
        df.to_csv(path, index=False)
        print(f"Exported {len(df):,} rows → {filename}.csv")
    except Exception as e:
        print(f"Skipping {filename}: {e}")

con.close()

print(f"\nAll files saved to {OUT_DIR}")