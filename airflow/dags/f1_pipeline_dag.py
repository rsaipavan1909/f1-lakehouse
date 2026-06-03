from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner":            "f1_lakehouse",
    "depends_on_past":  False,
    "start_date":       datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=2),
}

with DAG(
    dag_id="f1_full_pipeline",
    default_args=default_args,
    description="Full F1 Lakehouse pipeline: Bronze → Silver → Gold",
    schedule_interval="0 6 * * 1",
    catchup=False,
    tags=["f1", "lakehouse", "etl"],
) as dag:

    t_bronze = BashOperator(
        task_id="bronze_ingestion",
        bash_command="cd /opt/airflow && python ingestion/bronze_ingestion.py",
    )

    t_silver_laps = BashOperator(
        task_id="silver_laps_transformation",
        bash_command="cd /opt/airflow && python transformations/silver_laps_duckdb.py",
    )

    t_silver_results = BashOperator(
        task_id="silver_results_transformation",
        bash_command="cd /opt/airflow && python transformations/silver_results_duckdb.py",
    )

    t_dq = BashOperator(
        task_id="data_quality_checks",
        bash_command="""python -c "
import duckdb
con = duckdb.connect('/opt/airflow/data/f1_warehouse.duckdb')
count = con.execute('SELECT COUNT(*) FROM silver.results').fetchone()[0]
assert count > 0, 'Results table is empty!'
laps = con.execute('SELECT COUNT(*) FROM silver.laps').fetchone()[0]
assert laps > 0, 'Laps table is empty!'
print(f'Quality checks passed — {count} results, {laps} laps')
con.close()
"
""",
    )

    t_duckdb = BashOperator(
        task_id="load_to_duckdb",
        bash_command="cd /opt/airflow && python transformations/load_to_duckdb.py",
    )

    t_dbt = BashOperator(
        task_id="dbt_gold_models",
        bash_command="cd /opt/airflow/dbt/f1_gold && dbt run --profiles-dir /opt/airflow/dags",
    )

    t_dbt_test = BashOperator(
        task_id="dbt_tests",
        bash_command="cd /opt/airflow/dbt/f1_gold && dbt test --profiles-dir /opt/airflow/dags",
    )

    t_bronze >> [t_silver_laps, t_silver_results] >> t_dq >> t_duckdb >> t_dbt >> t_dbt_test