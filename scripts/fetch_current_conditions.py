#!/usr/bin/env python3
"""
Fetch complete current weather conditions from Environment Canada RSS feed
and historical data using the env_canada package.

This version fetches: temperature, conditions, wind, gusts, pressure, humidity, dewpoint
Uses env_canada package for proper historical data from Environment Canada.
"""

import requests
import xml.etree.ElementTree as ET
import json
import re
import asyncio
from datetime import datetime, timedelta
import os
import html as html_module

# Configuration
RSS_URL = "https://weather.gc.ca/rss/weather/49.631_-114.693_e.xml"

# Crowsnest Pass coordinates
CROWSNEST_LAT = 49.631
CROWSNEST_LON = -114.693

OUTPUT_DIR = "data"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "current_conditions.json")
HISTORY_FILE = os.path.join(OUTPUT_DIR, "temperature_history.json")
DAILY_STATS_FILE = os.path.join(OUTPUT_DIR, "daily_stats.json")

# Use Mountain Time for day boundaries (UTC-7)
MT_OFFSET_HOURS = -7


def get_local_date():
    """Get current date in Mountain Time."""
    utc_now = datetime.utcnow()
    mt_now = utc_now + timedelta(hours=MT_OFFSET_HOURS)
    return mt_now.date().isoformat()


async def fetch_historical_temperatures_ec():
    """Fetch 7-day historical temperature data using env_canada package."""
    try:
        from env_canada import ECHistorical
        from env_canada.ec_historical import get_historical_stations
        
        print("Fetching 7-day temperature history using env_canada...")
        
        # Find nearest station to Crowsnest Pass
        coordinates = [CROWSNEST_LAT, CROWSNEST_LON]
        stations = await get_historical_stations(coordinates, radius=100, limit=10)
        
        if not stations:
            print("⚠ No historical stations found near Crowsnest Pass")
            return None
        
        # Get the closest station
        station_id = stations[0]['station_id']
        station_name = stations[0].get('station_name', 'Unknown')
        print(f"Using station: {station_name} (ID: {station_id})")
        
        # Get current and previous month data
        today = datetime.now()
        history_records = []
        
        for month_offset in range(2):
            target_date = today - timedelta(days=30 * month_offset)
            year = target_date.year
            month = target_date.month
            
            try:
                ec_hist = ECHistorical(
                    station_id=station_id,
                    year=year,
                    month=month,
                    language="english",
                    format="csv"
                )
                await ec_hist.update()
                
                if ec_hist.station_data:
                    # Parse CSV data
                    import io
                    import csv
                    
                    reader = csv.DictReader(io.StringIO(ec_hist.station_data))
                    for row in reader:
                        try:
                            date_str = row.get('Date/Time', row.get('Date/Time (LST)', ''))
                            max_temp = row.get('Max Temp (°C)', row.get('Max Temp', ''))
                            min_temp = row.get('Min Temp (°C)', row.get('Min Temp', ''))
                            
                            if not date_str or not max_temp or not min_temp:
                                continue
                            
                            record_date = datetime.strptime(date_str.split()[0], '%Y-%m-%d').date()
                            days_ago = (today.date() - record_date).days
                            
                            if 0 < days_ago <= 7:
                                history_records.append({
                                    "date": record_date.isoformat(),
                                    "high": float(max_temp),
                                    "low": float(min_temp)
                                })
                        except (ValueError, KeyError) as e:
                            continue
                            
            except Exception as e:
                print(f"  Warning: Could not fetch {year}-{month:02d}: {e}")
                continue
        
        if not history_records:
            print("⚠ No historical records found")
            return None
        
        # Remove duplicates and sort
        seen_dates = set()
        unique_records = []
        for record in history_records:
            if record['date'] not in seen_dates:
                seen_dates.add(record['date'])
                unique_records.append(record)
        
        unique_records.sort(key=lambda x: x['date'], reverse=True)
        unique_records = unique_records[:7]
        
        history_data = {
            "daily_records": unique_records,
            "last_updated": datetime.utcnow().isoformat() + 'Z',
            "source": f"Environment Canada - {station_name}",
            "station_id": station_id
        }
        
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        with open(HISTORY_FILE, 'w') as f:
            json.dump(history_data, f, indent=2)
        
        print(f"✓ Fetched {len(unique_records)} days of temperature history")
        return history_data
        
    except ImportError:
        print("⚠ env_canada package not available, skipping historical data")
        return None
    except Exception as e:
        print(f"⚠ Error fetching historical data: {e}")
        import traceback
        traceback.print_exc()
        return None


def load_daily_stats():
    """Load daily stats from file, reset if it's a new day."""
    today = get_local_date()
    
    try:
        if os.path.exists(DAILY_STATS_FILE):
            with open(DAILY_STATS_FILE, 'r') as f:
                stats = json.load(f)
                
            if stats.get('date') == today:
                return stats
            else:
                print(f"New day detected ({today}), resetting daily stats")
    except (json.JSONDecodeError, IOError) as e:
        print(f"Could not load daily stats: {e}")
    
    return {
        'date': today,
        'max_gust_kmh': None,
        'max_gust_time': None,
        'max_wind_kmh': None,
        'max_wind_time': None,
        'high_temp': None,
        'high_temp_time': None,
        'low_temp': None,
        'low_temp_time': None
    }


def save_daily_stats(stats):
    """Save daily stats to file."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(DAILY_STATS_FILE, 'w') as f:
        json.dump(stats, f, indent=2)


def update_daily_stats(conditions):
    """Update daily statistics with current conditions."""
    stats = load_daily_stats()
    now_str = datetime.utcnow().strftime('%H:%M UTC')
    updated = False
    
    current_gust = conditions.get('wind_gust_kmh')
    if current_gust is not None:
        if stats['max_gust_kmh'] is None or current_gust > stats['max_gust_kmh']:
            stats['max_gust_kmh'] = current_gust
            stats['max_gust_time'] = now_str
            print(f"New daily max gust: {current_gust} km/h")
            updated = True
    
    current_wind = conditions.get('wind_speed_kmh')
    if current_wind is not None:
        if stats['max_wind_kmh'] is None or current_wind > stats['max_wind_kmh']:
            stats['max_wind_kmh'] = current_wind
            stats['max_wind_time'] = now_str
            updated = True
    
    current_temp = conditions.get('temperature')
    if current_temp is not None:
        if stats['high_temp'] is None or current_temp > stats['high_temp']:
            stats['high_temp'] = current_temp
            stats['high_temp_time'] = now_str
            updated = True
        if stats['low_temp'] is None or current_temp < stats['low_temp']:
            stats['low_temp'] = current_temp
            stats['low_temp_time'] = now_str
            updated = True
    
    if updated:
        save_daily_stats(stats)
    
    return stats


def extract_condition_from_forecast_title(title):
    """Extract the weather condition from a forecast entry title."""
    if not title:
        return None
    
    if ':' in title:
        _, forecast_part = title.split(':', 1)
        forecast_part = forecast_part.strip()
    else:
        return None
    
    condition = re.sub(r'\s*(High|Low|POP).*$', '', forecast_part, flags=re.IGNORECASE)
    condition = condition.strip().rstrip('.')
    
    if condition:
        return condition
    
    return None


def get_current_forecast_condition(entries, namespaces):
    """Find the current forecast entry and extract its condition."""
    now = datetime.now()
    current_hour = now.hour
    is_night = current_hour >= 18 or current_hour < 6
    
    weekdays = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    today_name = weekdays[now.weekday()]
    
    for entry in entries:
        title_elem = entry.find('atom:title', namespaces)
        category_elem = entry.find('atom:category', namespaces)
        
        if title_elem is None or category_elem is None:
            continue
            
        if category_elem.get('term') != 'Weather Forecasts':
            continue
        
        title = title_elem.text
        if not title:
            continue
        
        title_lower = title.lower()
        today_lower = today_name.lower()
        
        if is_night:
            if today_lower in title_lower and 'night' in title_lower:
                condition = extract_condition_from_forecast_title(title)
                if condition:
                    print(f"Found current night condition from: {title}")
                    return condition
        else:
            if today_lower in title_lower and 'night' not in title_lower:
                condition = extract_condition_from_forecast_title(title)
                if condition:
                    print(f"Found current day condition from: {title}")
                    return condition
    
    # Fallback: use first forecast entry
    for entry in entries:
        title_elem = entry.find('atom:title', namespaces)
        category_elem = entry.find('atom:category', namespaces)
        
        if title_elem is None or category_elem is None:
            continue
            
        if category_elem.get('term') == 'Weather Forecasts':
            condition = extract_condition_from_forecast_title(title_elem.text)
            if condition:
                print(f"Using first forecast condition from: {title_elem.text}")
                return condition
    
    return None


def get_forecast_gust(entries, namespaces):
    """Extract forecast wind gust from the current forecast period."""
    now = datetime.now()
    current_hour = now.hour
    is_night = current_hour >= 18 or current_hour < 6
    
    weekdays = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    today_name = weekdays[now.weekday()].lower()
    
    for entry in entries:
        title_elem = entry.find('atom:title', namespaces)
        category_elem = entry.find('atom:category', namespaces)
        summary_elem = entry.find('atom:summary', namespaces)
        
        if category_elem is None or category_elem.get('term') != 'Weather Forecasts':
            continue
        
        if title_elem is None or summary_elem is None:
            continue
        
        title = title_elem.text.lower() if title_elem.text else ''
        
        matches = False
        if is_night and today_name in title and 'night' in title:
            matches = True
        elif not is_night and today_name in title and 'night' not in title:
            matches = True
        
        if matches and summary_elem.text:
            summary = html_module.unescape(summary_elem.text)
            gust_match = re.search(r'gust(?:ing)?\s+(?:to\s+)?(\d+)', summary, re.IGNORECASE)
            if gust_match:
                return int(gust_match.group(1))
    
    return None


def parse_current_conditions(summary_text):
    """Parse the current conditions from RSS summary text."""
    print(f"Raw summary text: {summary_text[:200]}...")
    
    summary_text = html_module.unescape(summary_text)
    summary_text = re.sub(r'<[^>]+>', ' ', summary_text)
    
    conditions = {}
    
    temp_match = re.search(r'Temperature[:\s]+(-?\d+\.?\d*)', summary_text, re.IGNORECASE)
    if temp_match:
        conditions['temperature'] = float(temp_match.group(1))
        print(f"Found temperature: {conditions['temperature']}")
    
    press_match = re.search(r'Pressure[^:]*[:\s]+(\d+\.?\d*)\s*kPa\s*(\w+)?', summary_text, re.IGNORECASE)
    if press_match:
        conditions['pressure_kpa'] = float(press_match.group(1))
        print(f"Found pressure: {conditions['pressure_kpa']}")
        if press_match.group(2):
            conditions['pressure_tendency'] = press_match.group(2).lower()
            print(f"Found tendency: {conditions['pressure_tendency']}")
    
    hum_match = re.search(r'Humidity[:\s]+(\d+)\s*%', summary_text, re.IGNORECASE)
    if hum_match:
        conditions['humidity_percent'] = int(hum_match.group(1))
        print(f"Found humidity: {conditions['humidity_percent']}")
    
    dew_match = re.search(r'Dewpoint[:\s]+(-?\d+\.?\d*)', summary_text, re.IGNORECASE)
    if dew_match:
        conditions['dewpoint'] = float(dew_match.group(1))
        print(f"Found dewpoint: {conditions['dewpoint']}")
    
    wind_match = re.search(r'Wind[:\s]+(.+?)(?=Air Quality|Pressure|Humidity|Dewpoint|Observed|$)', summary_text, re.IGNORECASE | re.DOTALL)
    if wind_match:
        wind_text = wind_match.group(1).strip()
        print(f"Wind text: {wind_text}")
        
        if 'calm' in wind_text.lower():
            conditions['wind_speed_kmh'] = 0
            conditions['wind_direction'] = 'CALM'
        else:
            speed_match = re.search(r'(\d+)\s*km/h', wind_text, re.IGNORECASE)
            dir_match = re.search(r'\b([NSEW]{1,3})\b', wind_text, re.IGNORECASE)
            
            if speed_match:
                conditions['wind_speed_kmh'] = int(speed_match.group(1))
            if dir_match:
                conditions['wind_direction'] = dir_match.group(1).upper()
            
            gust_match = re.search(r'gust(?:s|ing)?(?:\s+to)?\s+(\d+)(?:\s*km/h)?', wind_text, re.IGNORECASE)
            if gust_match:
                conditions['wind_gust_kmh'] = int(gust_match.group(1))
    
    return conditions


async def fetch_weather_data():
    """Fetch weather data from Environment Canada RSS feed."""
    try:
        print(f"Fetching weather data from {RSS_URL}...")
        response = requests.get(RSS_URL, timeout=10)
        response.raise_for_status()
        
        root = ET.fromstring(response.content)
        namespaces = {'atom': 'http://www.w3.org/2005/Atom'}
        entries = root.findall('.//atom:entry', namespaces)
        observation_time = None
        
        current_condition = get_current_forecast_condition(entries, namespaces)
        print(f"Current condition from forecast: {current_condition}")
        
        forecast_gust = get_forecast_gust(entries, namespaces)
        print(f"Forecast gust: {forecast_gust}")
        
        for entry in entries:
            title_elem = entry.find('atom:title', namespaces)
            if title_elem is not None and 'Current Conditions' in title_elem.text:
                print(f"Found Current Conditions entry with title: {title_elem.text}")
                
                summary_elem = entry.find('atom:summary', namespaces)
                updated_elem = entry.find('atom:updated', namespaces)
                
                if updated_elem is not None:
                    observation_time = updated_elem.text
                
                if summary_elem is not None:
                    summary_text = summary_elem.text
                    
                    if summary_text:
                        conditions = parse_current_conditions(summary_text)
                        
                        if current_condition:
                            conditions['condition'] = current_condition
                        
                        if 'wind_gust_kmh' not in conditions and forecast_gust:
                            conditions['wind_gust_kmh'] = forecast_gust
                            print(f"Using forecast gust: {forecast_gust} km/h")
                        
                        daily_stats = update_daily_stats(conditions)
                        
                        if daily_stats.get('max_gust_kmh') is not None:
                            conditions['daily_max_gust_kmh'] = daily_stats['max_gust_kmh']
                        
                        # Fetch historical temperatures from Environment Canada
                        history_data = await fetch_historical_temperatures_ec()
                        
                        full_data = {
                            "timestamp": datetime.utcnow().isoformat() + 'Z',
                            "source": "Environment Canada",
                            "location": "Crowsnest Pass, AB",
                            "conditions": conditions,
                            "daily_stats": {
                                "date": daily_stats.get('date'),
                                "max_gust_kmh": daily_stats.get('max_gust_kmh'),
                                "max_gust_time": daily_stats.get('max_gust_time'),
                                "high_temp": daily_stats.get('high_temp'),
                                "low_temp": daily_stats.get('low_temp')
                            },
                            "observation_time": observation_time or "Unknown",
                            "fetch_time_utc": datetime.utcnow().isoformat() + 'Z'
                        }
                        
                        os.makedirs(OUTPUT_DIR, exist_ok=True)
                        
                        with open(OUTPUT_FILE, 'w') as f:
                            json.dump(full_data, f, indent=2)
                        
                        print(f"✓ Weather data saved to {OUTPUT_FILE}")
                        print(f"  Condition: {conditions.get('condition', 'N/A')}")
                        print(f"  Temperature: {conditions.get('temperature', 'N/A')}°C")
                        print(f"  Wind: {conditions.get('wind_direction', 'N/A')} {conditions.get('wind_speed_kmh', 'N/A')} km/h")
                        if 'wind_gust_kmh' in conditions:
                            print(f"  Current Gust: {conditions['wind_gust_kmh']} km/h")
                        if daily_stats.get('max_gust_kmh'):
                            print(f"  Daily Max Gust: {daily_stats['max_gust_kmh']} km/h")
                        print(f"  Pressure: {conditions.get('pressure_kpa', 'N/A')} kPa")
                        if history_data:
                            print(f"  Temperature History: {len(history_data.get('daily_records', []))} days")
                        return True
                    else:
                        print("✗ Summary text was empty")
                else:
                    print("✗ No summary element found")
        
        print("✗ Could not find current conditions in RSS feed")
        return False
        
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = asyncio.run(fetch_weather_data())
    exit(0 if success else 1)
