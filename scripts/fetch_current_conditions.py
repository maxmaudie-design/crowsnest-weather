#!/usr/bin/env python3
"""
Fetch complete current weather conditions from Environment Canada RSS feed
and save as JSON for the weather dashboard.

This version fetches: temperature, conditions, wind, gusts, pressure, humidity, dewpoint
Tracks daily high/low temps and builds a rolling 7-day history.
Tracks daily max gust speed.
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


def load_temperature_history():
    """Load temperature history from file."""
    try:
        if os.path.exists(HISTORY_FILE):
            with open(HISTORY_FILE, 'r') as f:
                return json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        print(f"Could not load temperature history: {e}")
    
    return {
        "daily_records": [],
        "last_updated": None,
        "source": "Self-tracked from Environment Canada observations"
    }


def save_temperature_history(history):
    """Save temperature history to file."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    history["last_updated"] = datetime.utcnow().isoformat() + 'Z'
    with open(HISTORY_FILE, 'w') as f:
        json.dump(history, f, indent=2)


def archive_yesterday_to_history(yesterday_stats):
    """Archive yesterday's high/low to the 7-day rolling history."""
    if not yesterday_stats:
        return
    
    yesterday_date = yesterday_stats.get('date')
    high_temp = yesterday_stats.get('high_temp')
    low_temp = yesterday_stats.get('low_temp')
    
    # Only archive if we have both high and low
    if yesterday_date and high_temp is not None and low_temp is not None:
        history = load_temperature_history()
        
        # Check if this date is already in history
        existing_dates = [r['date'] for r in history['daily_records']]
        if yesterday_date not in existing_dates:
            # Add yesterday's record
            history['daily_records'].append({
                "date": yesterday_date,
                "high": high_temp,
                "low": low_temp
            })
            
            # Sort by date descending (newest first)
            history['daily_records'].sort(key=lambda x: x['date'], reverse=True)
            
            # Keep only the most recent 7 days
            history['daily_records'] = history['daily_records'][:7]
            
            save_temperature_history(history)
            print(f"✓ Archived {yesterday_date}: High {high_temp}°C, Low {low_temp}°C")


def load_daily_stats():
    """Load daily stats from file, archive and reset if it's a new day."""
    today = get_local_date()
    
    try:
        if os.path.exists(DAILY_STATS_FILE):
            with open(DAILY_STATS_FILE, 'r') as f:
                stats = json.load(f)
                
            # Check if it's still the same day
            if stats.get('date') == today:
                return stats
            else:
                # New day - archive yesterday's data to history before resetting
                print(f"New day detected ({today}), archiving yesterday's stats...")
                archive_yesterday_to_history(stats)
    except (json.JSONDecodeError, IOError) as e:
        print(f"Could not load daily stats: {e}")
    
    # Return fresh stats for new day
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
    
    # Track max gust
    current_gust = conditions.get('wind_gust_kmh')
    if current_gust is not None:
        if stats['max_gust_kmh'] is None or current_gust > stats['max_gust_kmh']:
            stats['max_gust_kmh'] = current_gust
            stats['max_gust_time'] = now_str
            print(f"New daily max gust: {current_gust} km/h")
            updated = True
    
    # Track max wind speed
    current_wind = conditions.get('wind_speed_kmh')
    if current_wind is not None:
        if stats['max_wind_kmh'] is None or current_wind > stats['max_wind_kmh']:
            stats['max_wind_kmh'] = current_wind
            stats['max_wind_time'] = now_str
            updated = True
    
    # Track high/low temperature
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


def get_forecast_gust(entries, namespaces):
    """
    Extract forecast wind gust from the current forecast period.
    Forecasts contain text like: "Wind west 60 km/h gusting to 80"
    """
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
        
        # Check if this is the current period
        matches = False
        if is_night and today_name in title and 'night' in title:
            matches = True
        elif not is_night and today_name in title and 'night' not in title:
            matches = True
        
        if matches and summary_elem.text:
            summary = html_module.unescape(summary_elem.text)
            # Look for "gusting to XX" pattern
            gust_match = re.search(r'gust(?:ing)?\s+(?:to\s+)?(\d+)', summary, re.IGNORECASE)
            if gust_match:
                return int(gust_match.group(1))
    
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
            
            # Gusts from current conditions
            gust_match = re.search(r'gust(?:s|ing)?(?:\s+to)?\s+(\d+)(?:\s*km/h)?', wind_text, re.IGNORECASE)
            if gust_match:
                conditions['wind_gust_kmh'] = int(gust_match.group(1))
    
    return conditions


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
        
        # Get forecast gust if current conditions don't have it
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
                        # Parse the numeric conditions
                        conditions = parse_current_conditions(summary_text)
                        
                        # Add the condition from the forecast
                        if current_condition:
                            conditions['condition'] = current_condition
                        
                        # If no gust in current conditions, use forecast gust
                        if 'wind_gust_kmh' not in conditions and forecast_gust:
                            conditions['wind_gust_kmh'] = forecast_gust
                            print(f"Using forecast gust: {forecast_gust} km/h")
                        
                        # Update daily stats and get current daily max gust
                        # This also archives yesterday's data if it's a new day
                        daily_stats = update_daily_stats(conditions)
                        
                        # Add daily max gust to conditions
                        if daily_stats.get('max_gust_kmh') is not None:
                            conditions['daily_max_gust_kmh'] = daily_stats['max_gust_kmh']
                        
                        # Load temperature history for output
                        history = load_temperature_history()
                        
                        # Build full data structure
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
                        print(f"  Temperature History: {len(history.get('daily_records', []))} days")
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
