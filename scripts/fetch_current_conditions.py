#!/usr/bin/env python3
"""
Fetch complete current weather conditions from Environment Canada RSS feed
and save as JSON for the weather dashboard.

This version fetches: temperature, conditions, wind, gusts, pressure, humidity, dewpoint
Also fetches 7-day historical high/low temperatures from Environment Canada.
"""

import requests
import xml.etree.ElementTree as ET
import json
import re
from datetime import datetime, timedelta
import os
import html as html_module

# Configuration
RSS_URL = "https://weather.gc.ca/rss/weather/49.631_-114.693_e.xml"

# CROWSNEST PASS STATION
# Climate Identifier: 3051R4R
# Coordinates: 49.627525, -114.48195

STATION_IDS_TO_TRY = [
    "2695",   # CROWSNEST (try this first - Climate ID 3051R4R)
    "48844",  # PINCHER CREEK (backup)
    "2696",   # Possible alternate Crowsnest station
]

OUTPUT_DIR = "data"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "current_conditions.json")
HISTORY_FILE = os.path.join(OUTPUT_DIR, "temperature_history.json")


def extract_condition_from_forecast_title(title):
    """
    Extract the weather condition from a forecast entry title.
    Title format examples:
    - "Wednesday night: Partly cloudy. Low 7."
    - "Thursday: Mainly sunny. High 16."
    - "Friday: A mix of sun and cloud. High 13."
    
    Returns the condition part (e.g., "Partly cloudy", "Mainly sunny", "A mix of sun and cloud")
    """
    if not title:
        return None
    
    # Split on the colon to get the forecast part
    if ':' in title:
        _, forecast_part = title.split(':', 1)
        forecast_part = forecast_part.strip()
    else:
        return None
    
    # The condition is everything before "High" or "Low" or "POP"
    # Remove the temperature/probability part
    condition = re.sub(r'\s*(High|Low|POP).*$', '', forecast_part, flags=re.IGNORECASE)
    condition = condition.strip().rstrip('.')
    
    if condition:
        return condition
    
    return None


def get_current_forecast_condition(entries, namespaces):
    """
    Find the current forecast entry based on time of day and extract its condition.
    Environment Canada provides forecasts for periods like:
    - "Wednesday night" (evening/overnight)
    - "Thursday" (daytime)
    """
    now = datetime.now()
    current_hour = now.hour
    
    # Determine if we're in "day" or "night" period
    # Night typically starts around 6 PM (18:00)
    is_night = current_hour >= 18 or current_hour < 6
    
    # Get day names
    weekdays = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    today_name = weekdays[now.weekday()]
    
    # Look for the matching forecast entry
    for entry in entries:
        title_elem = entry.find('atom:title', namespaces)
        category_elem = entry.find('atom:category', namespaces)
        
        if title_elem is None or category_elem is None:
            continue
            
        # Only look at forecast entries
        if category_elem.get('term') != 'Weather Forecasts':
            continue
        
        title = title_elem.text
        if not title:
            continue
        
        # Check if this is the current period's forecast
        title_lower = title.lower()
        today_lower = today_name.lower()
        
        if is_night:
            # Look for "today night" forecast
            if today_lower in title_lower and 'night' in title_lower:
                condition = extract_condition_from_forecast_title(title)
                if condition:
                    print(f"Found current night condition from: {title}")
                    return condition
        else:
            # Look for today's daytime forecast (no "night" in title)
            if today_lower in title_lower and 'night' not in title_lower:
                condition = extract_condition_from_forecast_title(title)
                if condition:
                    print(f"Found current day condition from: {title}")
                    return condition
    
    # Fallback: just use the first forecast entry's condition
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


def parse_current_conditions(summary_text):
    """Parse the current conditions from RSS summary text."""
    print(f"Raw summary text: {summary_text[:200]}...")  # Debug
    
    # Unescape HTML entities (&deg; -> °, etc.)
    summary_text = html_module.unescape(summary_text)
    
    # Remove HTML tags but keep the text
    summary_text = re.sub(r'<[^>]+>', ' ', summary_text)
    
    conditions = {}
    
    # Temperature
    temp_match = re.search(r'Temperature[:\s]+(-?\d+\.?\d*)', summary_text, re.IGNORECASE)
    if temp_match:
        conditions['temperature'] = float(temp_match.group(1))
        print(f"Found temperature: {conditions['temperature']}")
    
    # Pressure and Tendency
    press_match = re.search(r'Pressure[^:]*[:\s]+(\d+\.?\d*)\s*kPa\s*(\w+)?', summary_text, re.IGNORECASE)
    if press_match:
        conditions['pressure_kpa'] = float(press_match.group(1))
        print(f"Found pressure: {conditions['pressure_kpa']}")
        if press_match.group(2):
            conditions['pressure_tendency'] = press_match.group(2).lower()
            print(f"Found tendency: {conditions['pressure_tendency']}")
    
    # Humidity
    hum_match = re.search(r'Humidity[:\s]+(\d+)\s*%', summary_text, re.IGNORECASE)
    if hum_match:
        conditions['humidity_percent'] = int(hum_match.group(1))
        print(f"Found humidity: {conditions['humidity_percent']}")
    
    # Dewpoint
    dew_match = re.search(r'Dewpoint[:\s]+(-?\d+\.?\d*)', summary_text, re.IGNORECASE)
    if dew_match:
        conditions['dewpoint'] = float(dew_match.group(1))
        print(f"Found dewpoint: {conditions['dewpoint']}")
    
    # Wind
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
            
            # Gusts
            gust_match = re.search(r'gust(?:s|ing)?(?:\s+to)?\s+(\d+)(?:\s*km/h)?', wind_text, re.IGNORECASE)
            if gust_match:
                conditions['wind_gust_kmh'] = int(gust_match.group(1))
    
    return conditions


def fetch_historical_temperatures():
    """Fetch 7-day historical temperature data from Environment Canada."""
    try:
        print("Fetching 7-day temperature history from Environment Canada...")
        
        history_records = []
        today = datetime.now()
        
        for station_id in STATION_IDS_TO_TRY:
            for month_offset in range(0, 2):
                target_date = today - timedelta(days=30 * month_offset)
                year = target_date.year
                month = target_date.month
                
                csv_url = f"https://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID={station_id}&Year={year}&Month={month}&timeframe=2&submit=Download+Data"
                
                try:
                    response = requests.get(csv_url, timeout=15)
                    if response.status_code != 200:
                        continue
                    
                    lines = response.text.strip().split('\n')
                    
                    header_idx = None
                    for i, line in enumerate(lines):
                        if 'Date/Time' in line and ('Max Temp' in line or 'Min Temp' in line):
                            header_idx = i
                            break
                    
                    if header_idx is None:
                        continue
                    
                    header_line = lines[header_idx]
                    if '","' in header_line:
                        header = header_line.strip('"').split('","')
                    else:
                        header = [h.strip('"') for h in header_line.split(',')]
                    
                    try:
                        date_idx = header.index('Date/Time')
                        max_temp_idx = None
                        min_temp_idx = None
                        for i, col in enumerate(header):
                            if 'Max Temp' in col:
                                max_temp_idx = i
                            if 'Min Temp' in col:
                                min_temp_idx = i
                        
                        if max_temp_idx is None or min_temp_idx is None:
                            continue
                            
                    except ValueError:
                        continue
                    
                    records_found = 0
                    for line in lines[header_idx + 1:]:
                        if not line.strip():
                            continue
                        
                        if '","' in line:
                            parts = line.strip('"').split('","')
                        else:
                            parts = [p.strip('"') for p in line.split(',')]
                            
                        if len(parts) <= max(date_idx, max_temp_idx, min_temp_idx):
                            continue
                        
                        date_str = parts[date_idx]
                        max_temp_str = parts[max_temp_idx]
                        min_temp_str = parts[min_temp_idx]
                        
                        if not max_temp_str or not min_temp_str or max_temp_str == '' or min_temp_str == '':
                            continue
                        
                        try:
                            record_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                            days_ago = (today.date() - record_date).days
                            if days_ago < 0 or days_ago > 8:
                                continue
                            
                            max_temp = float(max_temp_str)
                            min_temp = float(min_temp_str)
                            
                            history_records.append({
                                "date": record_date.isoformat(),
                                "high": max_temp,
                                "low": min_temp
                            })
                            records_found += 1
                            
                        except (ValueError, AttributeError) as e:
                            continue
                    
                    if records_found > 0:
                        print(f"  Found {records_found} records from station {station_id}")
                
                except requests.exceptions.RequestException as e:
                    continue
            
            if history_records:
                print(f"✓ Using station ID {station_id}")
                break
        
        if not history_records:
            print("⚠ No historical temperature data available")
            return False
        
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
            "source": "Environment Canada - Crowsnest Pass Station"
        }
        
        with open(HISTORY_FILE, 'w') as f:
            json.dump(history_data, f, indent=2)
        
        print(f"✓ Fetched {len(unique_records)} days of temperature history")
        return True
            
    except Exception as e:
        print(f"⚠ Warning: Could not fetch temperature history: {e}")
        return False


def fetch_weather_data():
    """Fetch weather data from Environment Canada RSS feed."""
    try:
        print(f"Fetching weather data from {RSS_URL}...")
        response = requests.get(RSS_URL, timeout=10)
        response.raise_for_status()
        
        root = ET.fromstring(response.content)
        namespaces = {'atom': 'http://www.w3.org/2005/Atom'}
        entries = root.findall('.//atom:entry', namespaces)
        observation_time = None
        
        # First, get the current condition from the forecast
        # (Environment Canada doesn't include condition text in Current Conditions!)
        current_condition = get_current_forecast_condition(entries, namespaces)
        print(f"Current condition from forecast: {current_condition}")
        
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
                        # Parse the numeric conditions
                        conditions = parse_current_conditions(summary_text)
                        
                        # Add the condition from the forecast
                        if current_condition:
                            conditions['condition'] = current_condition
                        
                        # Fetch historical temperatures
                        fetch_historical_temperatures()
                        
                        # Build full data structure
                        full_data = {
                            "timestamp": datetime.utcnow().isoformat() + 'Z',
                            "source": "Environment Canada",
                            "location": "Crowsnest Pass, AB",
                            "conditions": conditions,
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
                            print(f"  Gusts: {conditions['wind_gust_kmh']} km/h")
                        print(f"  Pressure: {conditions.get('pressure_kpa', 'N/A')} kPa")
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
    success = fetch_weather_data()
    exit(0 if success else 1)
