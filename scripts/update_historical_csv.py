#!/usr/bin/env python3
"""
Update historical weather data from Environment Canada's API.

Does two things:
  1. Appends new rows to data/CNP_weather_history_combined.csv (if it exists)
  2. Merges new records into data/daily_records.json (always)

Tracks last fetch date in data/last_historical_update.json.
On first run (no tracker), reads the latest date from the CSV if present,
otherwise defaults to 2026-01-04 (last date in original combined CSV).
"""

import csv
import json
import os
from datetime import datetime, timedelta

import requests

# Configuration
CLIMATE_ID = "3051R4R"
STATION_NAME = "CROWSNEST"
STATION_X = "-114.48195"
STATION_Y = "49.627525"
PROVINCE = "AB"

CSV_PATH = "data/CNP_weather_history_combined.csv"
LAST_UPDATE_PATH = "data/last_historical_update.json"
RECORDS_JSON_PATH = "data/daily_records.json"

DEFAULT_LAST_DATE = "2026-01-04"

EC_API_BASE = "https://api.weather.gc.ca/collections/climate-daily/items"

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


def get_last_date():
    """Get the last successfully fetched date.
    Priority: last_historical_update.json > latest date in CSV > default."""
    if os.path.exists(LAST_UPDATE_PATH):
        with open(LAST_UPDATE_PATH) as f:
            data = json.load(f)
            d = datetime.strptime(data["last_date"], "%Y-%m-%d")
            print(f"Last update tracker: {d.strftime('%Y-%m-%d')}")
            return d

    if os.path.exists(CSV_PATH):
        print("No tracker found, scanning CSV for latest date...")
        latest = None
        with open(CSV_PATH, "r", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("STATION_NAME", "").strip() == STATION_NAME:
                    try:
                        d = datetime.strptime(row["LOCAL_DATE"].strip(), "%m/%d/%Y %H:%M")
                        if latest is None or d > latest:
                            latest = d
                    except ValueError:
                        continue
        if latest:
            print(f"Latest date in CSV: {latest.strftime('%Y-%m-%d')}")
            return latest

    print(f"No tracker or CSV found, defaulting to {DEFAULT_LAST_DATE}")
    return datetime.strptime(DEFAULT_LAST_DATE, "%Y-%m-%d")


def save_last_date(date):
    with open(LAST_UPDATE_PATH, "w") as f:
        json.dump({
            "last_date": date.strftime("%Y-%m-%d"),
            "updated": datetime.utcnow().isoformat()
        }, f)


def fetch_new_records(since_date):
    start = (since_date + timedelta(days=1)).strftime("%Y-%m-%d")
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
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        features = data.get("features", [])
        if not features:
            break

        for feat in features:
            all_records.append(feat.get("properties", {}))

        if len(features) < limit:
            break
        offset += limit

    print(f"  Got {len(all_records)} new records")
    return all_records


def api_record_to_csv_row(props):
    """Convert an EC API JSON record to a CSV row dict."""
    local_date = props.get("LOCAL_DATE", "")
    try:
        dt = datetime.strptime(local_date[:10], "%Y-%m-%d")
    except (ValueError, TypeError):
        return None

    def v(key):
        val = props.get(key)
        return "" if val is None else str(val)

    return {
        "x": STATION_X,
        "y": STATION_Y,
        "STATION_NAME": STATION_NAME,
        "CLIMATE_IDENTIFIER": CLIMATE_ID,
        "ID": f"{CLIMATE_ID}.{dt.year}.{dt.month}.{dt.day}",
        "LOCAL_DATE": f"{dt.month}/{dt.day}/{dt.year} 0:00",
        "PROVINCE_CODE": PROVINCE,
        "LOCAL_YEAR": str(dt.year),
        "LOCAL_MONTH": str(dt.month),
        "LOCAL_DAY": str(dt.day),
        "MEAN_TEMPERATURE": v("MEAN_TEMPERATURE"),
        "MEAN_TEMPERATURE_FLAG": v("MEAN_TEMPERATURE_FLAG"),
        "MIN_TEMPERATURE": v("MIN_TEMPERATURE"),
        "MIN_TEMPERATURE_FLAG": v("MIN_TEMPERATURE_FLAG"),
        "MAX_TEMPERATURE": v("MAX_TEMPERATURE"),
        "MAX_TEMPERATURE_FLAG": v("MAX_TEMPERATURE_FLAG"),
        "TOTAL_PRECIPITATION": v("TOTAL_PRECIPITATION"),
        "TOTAL_PRECIPITATION_FLAG": v("TOTAL_PRECIPITATION_FLAG"),
        "TOTAL_RAIN": v("TOTAL_RAIN"),
        "TOTAL_RAIN_FLAG": v("TOTAL_RAIN_FLAG"),
        "TOTAL_SNOW": v("TOTAL_SNOW"),
        "TOTAL_SNOW_FLAG": v("TOTAL_SNOW_FLAG"),
        "SNOW_ON_GROUND": v("SNOW_ON_GROUND"),
        "SNOW_ON_GROUND_FLAG": v("SNOW_ON_GROUND_FLAG"),
        "DIRECTION_MAX_GUST": v("DIRECTION_MAX_GUST"),
        "DIRECTION_MAX_GUST_FLAG": v("DIRECTION_MAX_GUST_FLAG"),
        "SPEED_MAX_GUST": v("SPEED_MAX_GUST"),
        "SPEED_MAX_GUST_FLAG": v("SPEED_MAX_GUST_FLAG"),
        "COOLING_DEGREE_DAYS": v("COOLING_DEGREE_DAYS"),
        "COOLING_DEGREE_DAYS_FLAG": v("COOLING_DEGREE_DAYS_FLAG"),
        "HEATING_DEGREE_DAYS": v("HEATING_DEGREE_DAYS"),
        "HEATING_DEGREE_DAYS_FLAG": v("HEATING_DEGREE_DAYS_FLAG"),
        "MIN_REL_HUMIDITY": v("MIN_REL_HUMIDITY"),
        "MIN_REL_HUMIDITY_FLAG": v("MIN_REL_HUMIDITY_FLAG"),
        "MAX_REL_HUMIDITY": v("MAX_REL_HUMIDITY"),
        "MAX_REL_HUMIDITY_FLAG": v("MAX_REL_HUMIDITY_FLAG"),
    }


def append_to_csv(new_records):
    """Append new records to the CSV if it exists."""
    if not os.path.exists(CSV_PATH):
        print("CSV not found in repo - skipping CSV update (push it manually to enable)")
        return 0

    rows = [api_record_to_csv_row(r) for r in new_records]
    rows = [r for r in rows if r]

    with open(CSV_PATH, "a", newline="\r\n") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        for row in rows:
            writer.writerow(row)

    print(f"✓ Appended {len(rows)} rows to CSV")
    return len(rows)


def update_daily_records(new_records):
    """Merge new records into daily_records.json."""
    if os.path.exists(RECORDS_JSON_PATH):
        with open(RECORDS_JSON_PATH) as f:
            existing = json.load(f)
    else:
        existing = {"records_by_day": {}}

    records_by_day = existing.get("records_by_day", {})
    updated_days = []

    for props in new_records:
        local_date = props.get("LOCAL_DATE", "")
        try:
            dt = datetime.strptime(local_date[:10], "%Y-%m-%d")
        except (ValueError, TypeError):
            continue

        mm_dd = f"{dt.month:02d}-{dt.day:02d}"
        year = dt.year
        max_t = props.get("MAX_TEMPERATURE")
        min_t = props.get("MIN_TEMPERATURE")

        if mm_dd not in records_by_day:
            records_by_day[mm_dd] = {
                "record_high": None, "record_high_year": None,
                "record_low": None, "record_low_year": None,
                "years_of_data": 0,
            }

        entry = records_by_day[mm_dd]
        changed = False

        if max_t is not None:
            if entry["record_high"] is None or float(max_t) > entry["record_high"]:
                entry["record_high"] = float(max_t)
                entry["record_high_year"] = year
                changed = True

        if min_t is not None:
            if entry["record_low"] is None or float(min_t) < entry["record_low"]:
                entry["record_low"] = float(min_t)
                entry["record_low_year"] = year
                changed = True

        entry["years_of_data"] = entry.get("years_of_data", 0) + (1 if (max_t is not None or min_t is not None) else 0)

        if changed:
            updated_days.append(f"{mm_dd} ({year}): High {max_t}, Low {min_t}")

    if updated_days:
        print(f"  Records broken/updated for {len(updated_days)} days")

    output = {
        "generated": datetime.utcnow().isoformat(),
        "data_source": "CNP combined CSV (1893-2026) + EC API daily updates",
        "records_by_day": records_by_day,
    }

    with open(RECORDS_JSON_PATH, "w") as f:
        json.dump(output, f)

    print(f"✓ daily_records.json updated ({len(records_by_day)} days total)")


def main():
    last_date = get_last_date()

    new_records = fetch_new_records(last_date)

    if not new_records:
        print("Nothing to do - all files are up to date.")
        return

    # Update CSV (if present in repo)
    append_to_csv(new_records)

    # Always update daily_records.json
    update_daily_records(new_records)

    # Save tracker
    latest_fetched = max(
        datetime.strptime(r.get("LOCAL_DATE", "")[:10], "%Y-%m-%d")
        for r in new_records
        if r.get("LOCAL_DATE")
    )
    save_last_date(latest_fetched)
    print(f"✓ Tracker updated to {latest_fetched.strftime('%Y-%m-%d')}")


if __name__ == "__main__":
    main()
