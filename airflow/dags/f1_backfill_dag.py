from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner":           "f1_lakehouse",
    "start_date":      datetime(2024, 1, 1),
    "retries":         1,
    "retry_delay":     timedelta(minutes=5),
}

def run_historical_ingestion(**context):
    """Ingest historical F1 data for 2018-2023."""
    import fastf1
    import pandas as pd
    import os
    from datetime import datetime

    fastf1.Cache.enable_cache("/opt/airflow/data/cache")
    BRONZE_PATH = "/opt/airflow/data/bronze"

    def save_bronze(df, entity, partition=None):
        df = df.copy()
        df["_ingested_at"] = datetime.utcnow().isoformat()
        df["_source"]      = "fastf1"
        df["_entity"]      = entity
        for col in df.columns:
            if pd.api.types.is_timedelta64_dtype(df[col]):
                df[col] = df[col].astype(str)
            elif df[col].dtype == object:
                df[col] = df[col].astype(str)
        folder = f"{BRONZE_PATH}/{entity}"
        if partition:
            folder = f"{folder}/{partition}"
        os.makedirs(folder, exist_ok=True)
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        df.to_json(f"{folder}/{entity}_{timestamp}.json",
                   orient="records", lines=False)

    for year in range(2018, 2023):
        print(f"Ingesting {year}...")
        try:
            schedule = fastf1.get_event_schedule(
                year, include_testing=False)
            for _, event in schedule.iterrows():
                try:
                    session = fastf1.get_session(
                        year, int(event["RoundNumber"]), "R")
                    session.load(laps=True, telemetry=False,
                                weather=False, messages=False)
                    results = session.results.reset_index(drop=True)
                    results["year"]       = year
                    results["round"]      = int(event["RoundNumber"])
                    results["event_name"] = event["EventName"]
                    save_bronze(results, "results", f"year={year}")

                    laps = session.laps.reset_index(drop=True)
                    laps["year"]       = year
                    laps["round"]      = int(event["RoundNumber"])
                    laps["event_name"] = event["EventName"]
                    save_bronze(laps, "laps", f"year={year}")

                    print(f"  {year} R{event['RoundNumber']} done")
                except Exception as e:
                    print(f"  Skipping {year} R{event['RoundNumber']}: {e}")
        except Exception as e:
            print(f"  {year} failed: {e}")

with DAG(
    dag_id="f1_historical_backfill",
    default_args=default_args,
    description="One-time historical backfill for 2018-2023",
    schedule_interval=None,  # manual trigger only
    catchup=False,
    tags=["f1", "backfill"],
) as dag:

    t_backfill = PythonOperator(
        task_id="historical_ingestion",
        python_callable=run_historical_ingestion,
        execution_timeout=timedelta(hours=3),
    )