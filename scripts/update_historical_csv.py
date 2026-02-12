#!/usr/bin/env python3
"""
Update the historical weather CSV with new records from Environment Canada's API.
Also regenerates daily_records.json from the full dataset.

Runs daily via GitHub Actions. Fetches any new days since the last CSV entry
for the CROWSNEST station (3051R4R) and appends them.
"""

import csv
import json
import os
import sys
from datetime import datetime, timedelta
from collections import defaultdict

import requests

# Configuration
CLIMATE_ID = "3051R4R"
STATION_NAME = "CROWSNEST"
STATION_X = "-114.48195"
STATION_Y = "49.627525"
PROVINCE = "AB"

CSV_PATH = "data/CNP_weather_history_combined.csv"
RECORDS_JSON_PATH = "data/daily_records.json"

EC_API_BASE = "https://api.weather.gc.ca/collections/climate-daily/items"

# CSV column order (must match existing file)
CSV_COLUMNS = [
    "x", "y", "STATION_NAME", "CLIMATE_IDENTIFIER", "ID", "LOCAL_DATE",
    "PROVINCE_CODE", "LOCAL_YEAR", "LOCAL_MONTH", "LOCAL_DAY",
    "MEAN_TEMPERATURE", "MEAN_TEMPERATURE_FLAG",
    "MIN_TEMPERATURE", "MIN_TEMPERATURE_FLAG",
    "MAX_TEMPERATURE", "MAX_TEMPERATURE_FLAG",
    "TOTAL_PRECIPITATION", "TOTAL_PRECIPITATION_FLAG",
    "TOTAL_RAIN", "TOTAL_RAIN_FLAG",
    "TOTAL_SNOW", "TOTAL_SNOW_FLAG",
    "SNOW_ON_GROUND", "SNOW_ON_GROUND_FLAG",
    "DIRECTION_MAX_GUST", "DIRECTION_MAX_GUST_FLAG",
    "SPEED_MAX_GUST", "SPEED_MAX_GUST_FLAG",
    "COOLING_DEGREE_DAYS", "COOLING_DEGREE_DAYS_FLAG",
    "HEATING_DEGREE_DAYS", "HEATING_DEGREE_DAYS_FLAG",
    "MIN_REL_HUMIDITY", "MIN_REL_HUMIDITY_FLAG",
    "MAX_REL_HUMIDITY", "MAX_REL_HUMIDITY_FLAG",
]


def find_latest_date():
    """Find the most recent date in the CSV for the CROWSNEST station."""
    latest = None
    with open(CSV_PATH, "r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["STATION_NAME"].strip() == STATION_NAME:
                try:
                    d = datetime.strptime(row["LOCAL_DATE"].strip(), "%m/%d/%Y %H:%M")
                    if latest is None or d > latest:
                        latest = d
                except ValueError:
                    continue
    return latest


def fetch_new_records(since_date):
    """Fetch records from EC API newer than since_date."""
    start = (since_date + timedelta(days=1)).strftime("%Y-%m-%d")
    # Don't fetch today - it may be incomplete
    end = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

    if start > end:
        print(f"No new dates to fetch (last: {since_date.strftime('%Y-%m-%d')}, yesterday: {end})")
        return []

    print(f"Fetching EC API records from {start} to {end}...")

    all_records = []
    offset = 0
    limit = 500

    while True:
        url = (
            f"{EC_API_BASE}?f=json"
            f"&CLIMATE_IDENTIFIER={CLIMATE_ID}"
            f"&datetime={start} 00:00:00/{end} 00:00:00"
            f"&limit={limit}&offset={offset}"
            f"&sortby=LOCAL_DATE"
        )
        print(f"  Fetching offset={offset}...")
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        features = data.get("features", [])
        if not features:
            break

        for feat in features:
            props = feat.get("properties", {})
            all_records.append(props)

        if len(features) < limit:
            break
        offset += limit

    print(f"  Got {len(all_records)} new records from EC API")
    return all_records


def api_record_to_csv_row(props):
    """Convert an EC API JSON record to a CSV row matching our format."""
    local_date = props.get("LOCAL_DATE", "")
    try:
        dt = datetime.strptime(local_date[:10], "%Y-%m-%d")
    except (ValueError, TypeError):
        return None

    date_str = f"{dt.month}/{dt.day}/{dt.year} 0:00"

    def val(key):
        v = props.get(key)
        if v is None:
            return ""
        return str(v)

    def flag(key):
        v = props.get(key)
        if v is None:
            return ""
        return str(v)

    return {
        "x": STATION_X,
        "y": STATION_Y,
        "STATION_NAME": STATION_NAME,
        "CLIMATE_IDENTIFIER": CLIMATE_ID,
        "ID": f"{CLIMATE_ID}.{dt.year}.{dt.month}.{dt.day}",
        "LOCAL_DATE": date_str,
        "PROVINCE_CODE": PROVINCE,
        "LOCAL_YEAR": str(dt.year),
        "LOCAL_MONTH": str(dt.month),
        "LOCAL_DAY": str(dt.day),
        "MEAN_TEMPERATURE": val("MEAN_TEMPERATURE"),
        "MEAN_TEMPERATURE_FLAG": flag("MEAN_TEMPERATURE_FLAG"),
        "MIN_TEMPERATURE": val("MIN_TEMPERATURE"),
        "MIN_TEMPERATURE_FLAG": flag("MIN_TEMPERATURE_FLAG"),
        "MAX_TEMPERATURE": val("MAX_TEMPERATURE"),
        "MAX_TEMPERATURE_FLAG": flag("MAX_TEMPERATURE_FLAG"),
        "TOTAL_PRECIPITATION": val("TOTAL_PRECIPITATION"),
        "TOTAL_PRECIPITATION_FLAG": flag("TOTAL_PRECIPITATION_FLAG"),
        "TOTAL_RAIN": val("TOTAL_RAIN"),
        "TOTAL_RAIN_FLAG": flag("TOTAL_RAIN_FLAG"),
        "TOTAL_SNOW": val("TOTAL_SNOW"),
        "TOTAL_SNOW_FLAG": flag("TOTAL_SNOW_FLAG"),
        "SNOW_ON_GROUND": val("SNOW_ON_GROUND"),
        "SNOW_ON_GROUND_FLAG": flag("SNOW_ON_GROUND_FLAG"),
        "DIRECTION_MAX_GUST": val("DIRECTION_MAX_GUST"),
        "DIRECTION_MAX_GUST_FLAG": flag("DIRECTION_MAX_GUST_FLAG"),
        "SPEED_MAX_GUST": val("SPEED_MAX_GUST"),
        "SPEED_MAX_GUST_FLAG": flag("SPEED_MAX_GUST_FLAG"),
        "COOLING_DEGREE_DAYS": val("COOLING_DEGREE_DAYS"),
        "COOLING_DEGREE_DAYS_FLAG": flag("COOLING_DEGREE_DAYS_FLAG"),
        "HEATING_DEGREE_DAYS": val("HEATING_DEGREE_DAYS"),
        "HEATING_DEGREE_DAYS_FLAG": flag("HEATING_DEGREE_DAYS_FLAG"),
        "MIN_REL_HUMIDITY": val("MIN_REL_HUMIDITY"),
        "MIN_REL_HUMIDITY_FLAG": flag("MIN_REL_HUMIDITY_FLAG"),
        "MAX_REL_HUMIDITY": val("MAX_REL_HUMIDITY"),
        "MAX_REL_HUMIDITY_FLAG": flag("MAX_REL_HUMIDITY_FLAG"),
    }


def append_rows_to_csv(rows):
    """Append new rows to the CSV file."""
    if not rows:
        return 0
    with open(CSV_PATH, "a", newline="\r\n") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        for row in rows:
            writer.writerow(row)
    return len(rows)


def regenerate_daily_records():
    """Regenerate daily_records.json from the full CSV dataset (all stations)."""
    print("Regenerating daily_records.json from full CSV...")

    day_data = defaultdict(lambda: {"highs": [], "lows": [], "years": set()})

    with open(CSV_PATH, "r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                dt = datetime.strptime(row["LOCAL_DATE"].strip(), "%m/%d/%Y %H:%M")
                mm_dd = f"{dt.month:02d}-{dt.day:02d}"
                year = dt.year

                max_t = row.get("MAX_TEMPERATURE", "").strip()
                min_t = row.get("MIN_TEMPERATURE", "").strip()

                if max_t:
                    day_data[mm_dd]["highs"].append((float(max_t), year))
                if min_t:
                    day_data[mm_dd]["lows"].append((float(min_t), year))
                day_data[mm_dd]["years"].add(year)
            except (ValueError, KeyError):
                continue

    records_by_day = {}
    for mm_dd, data in sorted(day_data.items()):
        if data["highs"] and data["lows"]:
            max_pair = max(data["highs"], key=lambda x: x[0])
            min_pair = min(data["lows"], key=lambda x: x[0])
            records_by_day[mm_dd] = {
                "record_high": max_pair[0],
                "record_high_year": max_pair[1],
                "record_low": min_pair[0],
                "record_low_year": min_pair[1],
                "years_of_data": len(data["years"]),
            }

    output = {
        "generated": datetime.utcnow().isoformat(),
        "data_source": "CNP_weather_history_combined.csv (1893-2026)",
        "records_by_day": records_by_day,
    }

    with open(RECORDS_JSON_PATH, "w") as f:
        json.dump(output, f)

    print(f"\u2713 daily_records.json regenerated: {len(records_by_day)} days")
    return len(records_by_day)


def main():
    if not os.path.exists(CSV_PATH):
        print(f"ERROR: CSV not found at {CSV_PATH}")
        sys.exit(1)

    latest = find_latest_date()
    if latest is None:
        print("ERROR: Could not find any CROWSNEST records in CSV")
        sys.exit(1)

    print(f"Latest CROWSNEST record: {latest.strftime('%Y-%m-%d')}")

    new_records = fetch_new_records(latest)

    if not new_records:
        print("No new records to add. CSV is up to date.")
        regenerate_daily_records()
        return

    csv_rows = []
    for rec in new_records:
        row = api_record_to_csv_row(rec)
        if row:
            csv_rows.append(row)

    if csv_rows:
        count = append_rows_to_csv(csv_rows)
        print(f"\u2713 Appended {count} new records to CSV")
        for row in csv_rows[:5]:
            print(f"  {row['LOCAL_DATE']}: High {row['MAX_TEMPERATURE']}\u00b0C, Low {row['MIN_TEMPERATURE']}\u00b0C")
        if len(csv_rows) > 5:
            print(f"  ... and {len(csv_rows) - 5} more")

    regenerate_daily_records()
    print("\u2713 Historical data update complete")


if __name__ == "__main__":
    main()
