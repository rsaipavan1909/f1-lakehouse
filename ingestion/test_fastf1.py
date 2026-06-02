import os
import fastf1
os.makedirs("./data/cache", exist_ok=True)  # add this BEFORE enable_cache

fastf1.Cache.enable_cache("./data/cache")

# FastF1 caches data locally so it doesn't re-download
# Create a cache folder first
fastf1.Cache.enable_cache("./data/cache")
# Load the 2024 Bahrain race
session = fastf1.get_session(2024, "Bahrain", "R")
session.load()

print(f"Session: {session.event['EventName']}")
print(f"Total laps: {len(session.laps)}")
print(session.laps[["Driver", "LapTime", "Sector1Time", "Sector2Time"]].head())