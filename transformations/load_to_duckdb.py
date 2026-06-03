import os
import duckdb
import glob

BASE    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE, "data", "f1_warehouse.duckdb")

def load_json_to_duckdb(bronze_path: str, table_name: str, schema: str):
    """
    Read JSON files directly into DuckDB — no Spark needed.
    DuckDB can read JSON natively and is very fast.
    """
    print(f"\nLoading {table_name} → DuckDB.{schema}...")

    # Find all JSON files recursively
    pattern = os.path.join(bronze_path, "**", "*.json")
    files   = glob.glob(pattern, recursive=True)

    if not files:
        print(f"  No files found at {bronze_path}")
        return

    print(f"  Found {len(files)} files")

    con = duckdb.connect(DB_PATH)
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    con.execute(f"DROP TABLE IF EXISTS {schema}.{table_name}")

    # DuckDB reads JSON natively — pass the file list directly
    files_str = ", ".join([f"'{f}'" for f in files])
    con.execute(f"""
        CREATE TABLE {schema}.{table_name} AS
        SELECT * FROM read_json_auto([{files_str}],
            union_by_name = true,
            ignore_errors = true
        )
    """)

    count = con.execute(f"SELECT COUNT(*) FROM {schema}.{table_name}").fetchone()[0]
    cols  = len(con.execute(f"DESCRIBE {schema}.{table_name}").fetchall())
    con.close()

    print(f"  Loaded {count:,} rows, {cols} columns")

if __name__ == "__main__":
    os.makedirs(os.path.join(BASE, "data"), exist_ok=True)

    BRONZE = os.path.join(BASE, "data", "bronze")

    load_json_to_duckdb(
        bronze_path=os.path.join(BRONZE, "laps"),
        table_name="laps",
        schema="silver"
    )

    load_json_to_duckdb(
        bronze_path=os.path.join(BRONZE, "results"),
        table_name="results",
        schema="silver"
    )

    # Show summary
    con = duckdb.connect(DB_PATH)
    print("\nDuckDB warehouse summary:")
    print(con.execute("SHOW ALL TABLES").df().to_string())

    print("\nSample — top drivers 2024:")
    try:
        print(con.execute("""
            SELECT driver_code, constructor_name, finish_position, points
            FROM silver.results
            WHERE year = 2024
            ORDER BY points DESC
            LIMIT 5
        """).df().to_string())
    except Exception as e:
        print(f"  (sample query skipped: {e})")

    con.close()
    print(f"\nDuckDB saved at: {DB_PATH}")
    print("Done!")