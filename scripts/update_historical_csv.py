#!/usr/bin/env python3
"""
Update daily_records.json with new records from Environment Canada's API.

No CSV required. Tracks last fetch date in data/last_historical_update.json.
On first run, defaults to 2026-01-04 (last date in original combined CSV).
Fetches any new completed days since then and merges into daily_records.json.
"""

import json
import os
from datetime import datetime, timedelta
from collections import defaultdict

import requests

# Configuration
CLIMATE_ID = "3051R4R"

LAST_UPDATE_PATH = "data/last_historical_update.json"
RECORDS_JSON_PATH = "data/daily_records.json"

# Default: last date in the original combined CSV
DEFAULT_LAST_DATE = "2026-01-04"

EC_API_BASE = "https://api.weather.gc.ca/collections/climate-daily/items"


def get_last_date():
    """Get the last successfully fetched date."""
    if os.path.exists(LAST_UPDATE_PATH):
        with open(LAST_UPDATE_PATH) as f:
            data = json.load(f)
            return datetime.strptime(data["last_date"], "%Y-%m-%d")
    return datetime.strptime(DEFAULT_LAST_DATE, "%Y-%m-%d")


def save_last_date(date):
    """Save the last successfully fetched date."""
    with open(LAST_UPDATE_PATH, "w") as f:
        json.dump({"last_date": date.strftime("%Y-%m-%d"), "updated": datetime.utcnow().isoformat()}, f)


def fetch_new_records(since_date):
    """Fetch records from EC API newer than since_date."""
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
            props = feat.get("properties", {})
            all_records.append(props)

        if len(features) < limit:
            break
        offset += limit

    print(f"  Got {len(all_records)} new records")
    return all_records


def load_daily_records():
    """Load existing daily_records.json."""
    if os.path.exists(RECORDS_JSON_PATH):
        with open(RECORDS_JSON_PATH) as f:
            return json.load(f)
    return {"records_by_day": {}}


def merge_new_records(existing, new_api_records):
    """Merge new API records into the existing records_by_day structure."""
    records_by_day = existing.get("records_by_day", {})

    # Convert existing structure into mutable working data
    # We need highs/lows lists to properly update records
    # Build a minimal update: check each new record against existing record high/low
    updated_days = []

    for props in new_api_records:
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
                "record_high": None,
                "record_high_year": None,
                "record_low": None,
                "record_low_year": None,
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

        # Increment years_of_data if this is a new year for this day
        # (approximate - we don't track which years contributed without full CSV)
        entry["years_of_data"] = entry.get("years_of_data", 0) + (1 if (max_t or min_t) else 0)

        if changed:
            updated_days.append(f"{mm_dd} ({year}): High {max_t}, Low {min_t}")

    if updated_days:
        print(f"  Records broken/updated: {len(updated_days)}")
        for d in updated_days[:5]:
            print(f"    {d}")

    return records_by_day


def main():
    last_date = get_last_date()
    print(f"Last historical update: {last_date.strftime('%Y-%m-%d')}")

    new_records = fetch_new_records(last_date)

    if not new_records:
        print("No new records. daily_records.json is up to date.")
        return

    existing = load_daily_records()
    updated_records = merge_new_records(existing, new_records)

    output = {
        "generated": datetime.utcnow().isoformat(),
        "data_source": "CNP combined CSV (1893-2026) + EC API daily updates",
        "records_by_day": updated_records,
    }

    with open(RECORDS_JSON_PATH, "w") as f:
        json.dump(output, f)

    # Save the latest date we fetched
    latest_fetched = max(
        datetime.strptime(r.get("LOCAL_DATE", "")[:10], "%Y-%m-%d")
        for r in new_records
        if r.get("LOCAL_DATE")
    )
    save_last_date(latest_fetched)

    print(f"✓ daily_records.json updated with {len(new_records)} new records")
    print(f"✓ Last date now: {latest_fetched.strftime('%Y-%m-%d')}")


if __name__ == "__main__":
    main()
