"""
Fetch real weather forecast data from MET Norway (api.met.no)
and store it in DuckDB for the chrontext demo.

No API key needed — just a proper User-Agent header.
Uses Locationforecast 2.0 (compact) which provides ~9 days
of hourly forecasts for any lat/lon coordinate.

Run this once before main.py.
"""

import duckdb
import polars as pl
import json
import os
import time
from urllib.request import Request, urlopen

# -----------------------------------------------------------
# Station definitions — real Norwegian weather stations
# -----------------------------------------------------------
# These match the stations in data/stations.csv.
# Coordinates are used to query the Locationforecast API.

stations = {
    "ST001": {"lat": 59.9423, "lon": 10.7200, "alt": 94},    # Blindern, Oslo
    "ST002": {"lat": 60.3833, "lon":  5.3322, "alt": 12},    # Florida, Bergen
    "ST003": {"lat": 63.4571, "lon": 10.9239, "alt": 12},    # Værnes, Trondheim
    "ST004": {"lat": 69.6533, "lon": 18.9553, "alt": 100},   # Tromsø
    "ST005": {"lat": 62.1135, "lon":  9.2847, "alt": 952},   # Fokstugu, Dovre
    "ST006": {"lat": 61.5647, "lon":  7.9939, "alt": 1413},  # Sognefjellet, Luster
    "ST007": {"lat": 60.2900, "lon":  5.2283, "alt": 48},    # Flesland, Bergen
    "ST008": {"lat": 58.8767, "lon":  5.6378, "alt": 7},     # Sola, Stavanger
    "ST009": {"lat": 67.2692, "lon": 14.3651, "alt": 11},    # Bodø
    "ST010": {"lat": 69.0100, "lon": 23.0400, "alt": 307},   # Kautokeino
    "ST011": {"lat": 61.1500, "lon": 11.4700, "alt": 240},   # Rena, Åmot
    "ST012": {"lat": 70.6634, "lon": 23.6821, "alt": 10},    # Hammerfest
}

BASE_URL = "https://api.met.no/weatherapi/locationforecast/2.0/compact"
USER_AGENT = "chrontext-demo/1.0 github.com/DataTreehouse/maplib"


def fetch_forecast(station_id: str, lat: float, lon: float, alt: int) -> list[dict]:
    """Fetch hourly forecast data from MET Norway for one station."""
    url = f"{BASE_URL}?lat={lat:.4f}&lon={lon:.4f}&altitude={alt}"
    req = Request(url, headers={"User-Agent": USER_AGENT})

    print(f"  Fetching {station_id} ({lat:.2f}°N, {lon:.2f}°E)...", end=" ", flush=True)
    resp = urlopen(req, timeout=30)
    data = json.loads(resp.read())

    rows = []
    for entry in data["properties"]["timeseries"]:
        ts = entry["time"]
        instant = entry["data"]["instant"]["details"]

        temperature = instant.get("air_temperature")
        wind_speed = instant.get("wind_speed")

        # Precipitation is in next_1_hours (aggregated over 1h).
        # Not all timesteps have next_1_hours (later ones use 6h intervals).
        precip = None
        if "next_1_hours" in entry["data"]:
            precip = entry["data"]["next_1_hours"]["details"].get("precipitation_amount")
        elif "next_6_hours" in entry["data"]:
            # For 6-hour intervals, divide by 6 for a rough hourly estimate
            p6 = entry["data"]["next_6_hours"]["details"].get("precipitation_amount")
            precip = round(p6 / 6, 1) if p6 is not None else None

        if temperature is not None:
            rows.append({
                "station_id":    station_id,
                "timestamp":     ts,
                "temperature":   temperature,
                "wind_speed":    wind_speed,
                "precipitation": precip,
            })

    print(f"{len(rows)} data points")
    return rows


# -----------------------------------------------------------
# Fetch data for all stations
# -----------------------------------------------------------
print("Fetching real weather data from MET Norway (api.met.no)...")
print(f"  API: Locationforecast 2.0 (compact)")
print(f"  Stations: {len(stations)}\n")

all_rows = []
for sid, info in stations.items():
    rows = fetch_forecast(sid, info["lat"], info["lon"], info["alt"])
    all_rows.extend(rows)
    time.sleep(1)  # Be polite to the API

print(f"\nTotal: {len(all_rows):,} data points from {len(stations)} stations")


# -----------------------------------------------------------
# Write to DuckDB
# -----------------------------------------------------------
os.makedirs("data", exist_ok=True)
db_path = "data/weather.duckdb"
if os.path.exists(db_path):
    os.remove(db_path)

con = duckdb.connect(db_path)

con.execute("""
    CREATE TABLE measurements (
        station_id    VARCHAR,
        timestamp     TIMESTAMP,
        temperature   DOUBLE,
        wind_speed    DOUBLE,
        precipitation DOUBLE
    )
""")

df = pl.DataFrame(all_rows)
# Parse ISO timestamps to proper datetime
df = df.with_columns(pl.col("timestamp").str.to_datetime("%Y-%m-%dT%H:%M:%SZ"))

con.execute("INSERT INTO measurements SELECT * FROM df")

# Sanity check
count = con.execute("SELECT COUNT(*) FROM measurements").fetchone()[0]
date_range = con.execute("""
    SELECT MIN(timestamp) AS first, MAX(timestamp) AS last
    FROM measurements
""").fetchone()

print(f"\nDuckDB: {count:,} rows in measurements table")
print(f"Time range: {date_range[0]} → {date_range[1]}")

# Show a quick sample
sample = con.execute("""
    SELECT station_id,
           ROUND(AVG(temperature), 1) AS avg_temp,
           ROUND(MIN(temperature), 1) AS min_temp,
           ROUND(MAX(temperature), 1) AS max_temp
    FROM measurements
    GROUP BY station_id
    ORDER BY avg_temp DESC
    LIMIT 5
""").fetchall()
print("\nWarmest stations:")
for row in sample:
    print(f"  {row[0]}: avg {row[1]}°C (min {row[2]}, max {row[3]})")

con.close()
print(f"\nSaved to {db_path}")
