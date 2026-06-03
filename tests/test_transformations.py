import pytest
import duckdb

@pytest.fixture
def test_db():
    """Create an in-memory DuckDB for testing."""
    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA silver")

    con.execute("""
        CREATE TABLE silver.results AS SELECT
            2024 AS year,
            1 AS round,
            'Bahrain Grand Prix' AS event_name,
            'VER' AS driver_code,
            'Max Verstappen' AS driver_name,
            'Red Bull Racing' AS constructor_name,
            1 AS finish_position,
            1 AS grid_position,
            25.0 AS points,
            'Finished' AS race_status,
            true AS is_points_finish,
            current_timestamp AS _silver_processed_at
    """)

    con.execute("""
        CREATE TABLE silver.laps AS SELECT
            2024 AS year,
            1 AS round,
            'VER' AS driver_code,
            1 AS lap_number,
            94.5 AS lap_time_seconds,
            'SOFT' AS tyre_compound,
            true AS dq_valid_lap_time
    """)

    yield con
    con.close()

def test_results_table_has_data(test_db):
    count = test_db.execute(
        "SELECT COUNT(*) FROM silver.results").fetchone()[0]
    assert count > 0, "Results table should have data"

def test_results_points_non_negative(test_db):
    bad = test_db.execute(
        "SELECT COUNT(*) FROM silver.results WHERE points < 0").fetchone()[0]
    assert bad == 0, "No negative points allowed"

def test_results_position_valid(test_db):
    bad = test_db.execute("""
        SELECT COUNT(*) FROM silver.results
        WHERE finish_position < 1 OR finish_position > 20
    """).fetchone()[0]
    assert bad == 0, "Position must be between 1 and 20"

def test_laps_table_has_data(test_db):
    count = test_db.execute(
        "SELECT COUNT(*) FROM silver.laps").fetchone()[0]
    assert count > 0, "Laps table should have data"

def test_laps_valid_lap_times(test_db):
    valid = test_db.execute("""
        SELECT COUNT(*) FROM silver.laps
        WHERE dq_valid_lap_time = true
    """).fetchone()[0]
    assert valid > 0, "Should have valid lap times"

def test_no_duplicate_results(test_db):
    dupes = test_db.execute("""
        SELECT COUNT(*) FROM (
            SELECT year, round, driver_code, COUNT(*) AS cnt
            FROM silver.results
            GROUP BY year, round, driver_code
            HAVING cnt > 1
        )
    """).fetchone()[0]
    assert dupes == 0, "No duplicate results per driver per race"