#!/usr/bin/env python3
"""
Fetch pressure forecast from OpenWeatherMap One Call API 3.0
for Crowsnest Pass, Alberta.

Output: data/pressure_forecast.json

Requires: OPENWEATHER_API_KEY environment variable
Get your free API key at: https://openweathermap.org/api
"""

import json
import os
from datetime import datetime
from pathlib import Path

try:
    import requests
except ImportError:
    print("Error: requests library not installed. Run: pip install requests")
    exit(1)

# Configuration
CROWSNEST_LAT = 49.63
CROWSNEST_LON = -114.69
OUTPUT_FILE = Path("data/pressure_forecast.json")

def fetch_openweather_forecast(api_key):
    """Fetch forecast data from OpenWeatherMap One Call API."""
    url = "https://api.openweathermap.org/data/3.0/onecall"
    
    params = {
        'lat': CROWSNEST_LAT,
        'lon': CROWSNEST_LON,
        'exclude': 'minutely',
        'appid': api_key,
        'units': 'metric'
    }
    
    print(f"Fetching forecast from OpenWeatherMap...")
    print(f"Location: {CROWSNEST_LAT}, {CROWSNEST_LON}")
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching from OpenWeatherMap: {e}")
        return None

def extract_pressure_data(raw_data):
    """Extract pressure data from OpenWeatherMap response."""
    result = {
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'source': 'OpenWeatherMap One Call API 3.0',
        'location': {
            'lat': CROWSNEST_LAT,
            'lon': CROWSNEST_LON,
            'name': 'Crowsnest Pass, AB'
        },
        'current': None,
        'hourly_forecast': [],
        'daily_forecast': []
    }
    
    # Current conditions
    if 'current' in raw_data:
        current = raw_data['current']
        result['current'] = {
            'time': datetime.fromtimestamp(current['dt']).isoformat() + 'Z',
            'pressure_hpa': current.get('pressure'),
            'temperature_c': current.get('temp'),
            'humidity_percent': current.get('humidity'),
            'wind_speed_ms': current.get('wind_speed'),
            'wind_deg': current.get('wind_deg')
        }
    
    # Hourly forecast (48 hours)
    if 'hourly' in raw_data:
        for hour in raw_data['hourly'][:48]:
            result['hourly_forecast'].append({
                'time': datetime.fromtimestamp(hour['dt']).isoformat() + 'Z',
                'pressure_hpa': hour.get('pressure'),
                'temperature_c': hour.get('temp'),
                'humidity_percent': hour.get('humidity'),
                'wind_speed_ms': hour.get('wind_speed'),
                'description': hour.get('weather', [{}])[0].get('description', '')
            })
    
    # Daily forecast (8 days)
    if 'daily' in raw_data:
        for day in raw_data['daily'][:8]:
            result['daily_forecast'].append({
                'date': datetime.fromtimestamp(day['dt']).strftime('%Y-%m-%d'),
                'pressure_hpa': day.get('pressure'),
                'temp_min_c': day.get('temp', {}).get('min'),
                'temp_max_c': day.get('temp', {}).get('max'),
                'humidity_percent': day.get('humidity'),
                'wind_speed_ms': day.get('wind_speed'),
                'description': day.get('weather', [{}])[0].get('description', '')
            })
    
    return result

def calculate_pressure_trends(forecast_data):
    """Calculate pressure change trends from forecast data."""
    if not forecast_data.get('hourly_forecast'):
        return None
    
    hourly = forecast_data['hourly_forecast']
    
    # Calculate pressure change over next 24 hours
    if len(hourly) >= 24:
        current_p = hourly[0]['pressure_hpa']
        future_p = hourly[23]['pressure_hpa']
        
        if current_p and future_p:
            change_24h = future_p - current_p
            
            return {
                'pressure_change_24h_hpa': round(change_24h, 1),
                'trend_24h': 'rising' if change_24h > 1 else 'falling' if change_24h < -1 else 'steady',
                'current_pressure_hpa': current_p,
                'predicted_pressure_24h_hpa': future_p
            }
    
    return None

def fetch_and_save_forecast():
    """Fetch pressure forecast and save to JSON file."""
    # Get API key from environment
    api_key = os.environ.get('OPENWEATHER_API_KEY')
    
    if not api_key:
        print("Warning: OPENWEATHER_API_KEY not set in environment")
        print("Skipping pressure forecast fetch")
        print("Get your free API key at: https://openweathermap.org/api")
        return False
    
    # Fetch data
    raw_data = fetch_openweather_forecast(api_key)
    
    if raw_data is None:
        print("Failed to fetch forecast data")
        return False
    
    # Extract and process pressure data
    forecast_data = extract_pressure_data(raw_data)
    
    # Add pressure trends
    trends = calculate_pressure_trends(forecast_data)
    if trends:
        forecast_data['pressure_trends'] = trends
    
    # Create output directory
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    
    # Save to file
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(forecast_data, f, indent=2)
    
    print(f"✓ Pressure forecast saved to {OUTPUT_FILE}")
    
    # Print summary
    if forecast_data.get('current'):
        curr = forecast_data['current']
        print(f"\nCurrent pressure: {curr['pressure_hpa']} hPa")
    
    if trends:
        print(f"24-hour trend: {trends['trend_24h']} ({trends['pressure_change_24h_hpa']:+.1f} hPa)")
    
    print(f"Hourly forecast points: {len(forecast_data['hourly_forecast'])}")
    print(f"Daily forecast points: {len(forecast_data['daily_forecast'])}")
    
    return True

if __name__ == "__main__":
    success = fetch_and_save_forecast()
    exit(0 if success else 1)
