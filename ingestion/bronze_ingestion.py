import fastf1
import pandas as pd
import os
import json
from datetime import datetime

BRONZE_PATH = "./data/bronze"
CACHE_PATH  = "./data/cache"

fastf1.Cache.enable_cache(CACHE_PATH)

# ── Helper ────────────────────────────────────────────────────────────────────

def save_bronze(df: pd.DataFrame, entity: str, partition: str = None):
    """
    Save a DataFrame as JSON to the Bronze layer.
    Always adds metadata columns for lineage tracking.
    """
    df = df.copy()
    df["_ingested_at"] = datetime.utcnow().isoformat()
    df["_source"]      = "fastf1"
    df["_entity"]      = entity

    # Convert timedelta columns to strings — JSON can't serialize them natively
    for col in df.columns:
        if pd.api.types.is_timedelta64_dtype(df[col]):
            df[col] = df[col].astype(str)

    folder = f"{BRONZE_PATH}/{entity}"
    if partition:
        folder = f"{folder}/{partition}"
    os.makedirs(folder, exist_ok=True)

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filepath  = f"{folder}/{entity}_{timestamp}.json"

    df.to_json(filepath, orient="records", indent=2)
    print(f"  Saved {len(df)} records → {filepath}")

# ── Ingestion functions ───────────────────────────────────────────────────────

def ingest_schedule(start_year: int = 2018, end_year: int = 2024):
    """Race calendar — all events per season."""
    print("\nIngesting race schedules...")
    for year in range(start_year, end_year + 1):
        schedule = fastf1.get_event_schedule(year, include_testing=False)
        save_bronze(schedule, "schedule", f"year={year}")
        print(f"  {year}: {len(schedule)} events")

def ingest_laps(start_year: int = 2023, end_year: int = 2024):
    """
    Lap times for every driver in every race.
    This is your highest-volume table — millions of rows across all years.
    Start with 2023–2024 for speed, expand later.
    """
    print("\nIngesting lap times...")
    for year in range(start_year, end_year + 1):
        schedule = fastf1.get_event_schedule(year, include_testing=False)

        for _, event in schedule.iterrows():
            try:
                session = fastf1.get_session(year, event["RoundNumber"], "R")
                session.load(telemetry=False, weather=False, messages=False)

                laps = session.laps.reset_index(drop=True)
                laps["year"]       = year
                laps["round"]      = event["RoundNumber"]
                laps["event_name"] = event["EventName"]

                save_bronze(laps, "laps", f"year={year}")
                print(f"  {year} R{event['RoundNumber']} {event['EventName']}: {len(laps)} laps")

            except Exception as e:
                print(f"  Skipping {year} R{event['RoundNumber']}: {e}")

def ingest_telemetry(year: int = 2024, round_num: int = 1, driver: str = "VER"):
    """
    Car telemetry — speed, throttle, brake, gear every ~0.1 seconds.
    Very large dataset — ingest one driver per race to start.
    """
    print(f"\nIngesting telemetry ({year} R{round_num} {driver})...")
    try:
        session = fastf1.get_session(year, round_num, "R")
        session.load(telemetry=True)

        fastest_lap = session.laps.pick_driver(driver).pick_fastest()
        telemetry   = fastest_lap.get_telemetry()
        telemetry["year"]       = year
        telemetry["round"]      = round_num
        telemetry["driver"]     = driver
        telemetry["event_name"] = session.event["EventName"]

        save_bronze(telemetry, "telemetry", f"year={year}/round={round_num}")
        print(f"  {len(telemetry)} telemetry points saved")

    except Exception as e:
        print(f"  Error: {e}")

def ingest_results(start_year: int = 2018, end_year: int = 2024):
    """Final race classification — positions, points, status."""
    print("\nIngesting race results...")
    for year in range(start_year, end_year + 1):
        schedule = fastf1.get_event_schedule(year, include_testing=False)

        for _, event in schedule.iterrows():
            try:
                session = fastf1.get_session(year, event["RoundNumber"], "R")
                session.load(laps=False, telemetry=False, weather=False, messages=False)

                results = session.results.reset_index(drop=True)
                results["year"]       = year
                results["round"]      = event["RoundNumber"]
                results["event_name"] = event["EventName"]

                save_bronze(results, "results", f"year={year}")
                print(f"  {year} R{event['RoundNumber']}: {len(results)} results")

            except Exception as e:
                print(f"  Skipping {year} R{event['RoundNumber']}: {e}")

def ingest_weather(start_year: int = 2023, end_year: int = 2024):
    """Weather data per race — temperature, humidity, rainfall."""
    print("\nIngesting weather data...")
    for year in range(start_year, end_year + 1):
        schedule = fastf1.get_event_schedule(year, include_testing=False)

        for _, event in schedule.iterrows():
            try:
                session = fastf1.get_session(year, event["RoundNumber"], "R")
                session.load(laps=False, telemetry=False, weather=True, messages=False)

                weather = session.weather_data.reset_index(drop=True)
                weather["year"]       = year
                weather["round"]      = event["RoundNumber"]
                weather["event_name"] = event["EventName"]

                save_bronze(weather, "weather", f"year={year}")
                print(f"  {year} R{event['RoundNumber']}: {len(weather)} weather records")

            except Exception as e:
                print(f"  Skipping {year} R{event['RoundNumber']}: {e}")

# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("Starting F1 Bronze ingestion with FastF1...")

    ingest_schedule(start_year=2018, end_year=2024)
    ingest_results(start_year=2018,  end_year=2024)
    ingest_laps(start_year=2023,     end_year=2024)    # recent years first
    ingest_weather(start_year=2023,  end_year=2024)
    ingest_telemetry(year=2024, round_num=1, driver="VER")  # one sample

    print("\nBronze ingestion complete!")