#!/usr/bin/env python3
"""
Fetch current weather conditions from Environment Canada RSS feed
for Crowsnest Pass, Alberta.

Output: data/current_conditions.json
"""

import json
import re
from datetime import datetime
from pathlib import Path
import xml.etree.ElementTree as ET

try:
    import requests
except ImportError:
    print("Error: requests library not installed. Run: pip install requests")
    exit(1)

# Configuration
RSS_URL = "https://weather.gc.ca/rss/weather/49.631_-114.693_e.xml"
OUTPUT_FILE = Path("data/current_conditions.json")

def parse_current_conditions(xml_content):
    """Parse current conditions from Environment Canada RSS feed."""
    root = ET.fromstring(xml_content)
    
    # Find the current conditions entry
    for entry in root.findall('.//{http://www.w3.org/2005/Atom}entry'):
        title = entry.find('{http://www.w3.org/2005/Atom}title')
        if title is not None and 'Current Conditions' in title.text:
            summary = entry.find('{http://www.w3.org/2005/Atom}summary')
            updated = entry.find('{http://www.w3.org/2005/Atom}updated')
            
            if summary is not None:
                return parse_conditions_text(summary.text, updated.text if updated is not None else None)
    
    return None

def parse_conditions_text(html_text, updated_time):
    """Extract weather data from HTML summary text."""
    data = {
        'timestamp': updated_time or datetime.utcnow().isoformat() + 'Z',
        'source': 'Environment Canada',
        'location': 'Crowsnest Pass, AB',
        'conditions': {}
    }
    
    # Parse temperature
    temp_match = re.search(r'Temperature:</b>\s*([-+]?\d+\.?\d*)\s*°C', html_text)
    if temp_match:
        data['conditions']['temperature_c'] = float(temp_match.group(1))
    
    # Parse pressure and tendency
    pressure_match = re.search(r'Pressure / Tendency:</b>\s*([\d.]+)\s*kPa\s*(\w+)', html_text)
    if pressure_match:
        data['conditions']['pressure_kpa'] = float(pressure_match.group(1))
        data['conditions']['pressure_tendency'] = pressure_match.group(2)
    
    # Parse humidity
    humidity_match = re.search(r'Humidity:</b>\s*(\d+)\s*%', html_text)
    if humidity_match:
        data['conditions']['humidity_percent'] = int(humidity_match.group(1))
    
    # Parse wind chill
    windchill_match = re.search(r'Wind Chill:</b>\s*([-+]?\d+)', html_text)
    if windchill_match:
        data['conditions']['wind_chill'] = int(windchill_match.group(1))
    
    # Parse dewpoint
    dewpoint_match = re.search(r'Dewpoint:</b>\s*([-+]?\d+\.?\d*)\s*°C', html_text)
    if dewpoint_match:
        data['conditions']['dewpoint_c'] = float(dewpoint_match.group(1))
    
    # Parse wind
    wind_match = re.search(r'Wind:</b>\s*([A-Z]+)\s*(\d+)\s*km/h', html_text)
    if wind_match:
        data['conditions']['wind_direction'] = wind_match.group(1)
        data['conditions']['wind_speed_kmh'] = int(wind_match.group(2))
    
    # Parse observation time
    obs_match = re.search(r'Observed at:</b>\s*(.+?)<br', html_text)
    if obs_match:
        data['observation_time'] = obs_match.group(1).strip()
    
    return data

def fetch_and_save_conditions():
    """Fetch current conditions and save to JSON file."""
    print(f"Fetching weather data from: {RSS_URL}")
    
    try:
        response = requests.get(RSS_URL, timeout=10)
        response.raise_for_status()
        
        print(f"Response received (status: {response.status_code})")
        
        # Parse the RSS feed
        weather_data = parse_current_conditions(response.content)
        
        if weather_data is None:
            print("Error: Could not find current conditions in RSS feed")
            return False
        
        # Create output directory if needed
        OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
        
        # Add metadata
        weather_data['fetch_time_utc'] = datetime.utcnow().isoformat() + 'Z'
        
        # Save to file
        with open(OUTPUT_FILE, 'w') as f:
            json.dump(weather_data, f, indent=2)
        
        print(f"✓ Weather data saved to {OUTPUT_FILE}")
        
        # Print summary
        if 'conditions' in weather_data:
            cond = weather_data['conditions']
            print(f"\nCurrent conditions:")
            if 'temperature_c' in cond:
                print(f"  Temperature: {cond['temperature_c']}°C")
            if 'pressure_kpa' in cond:
                print(f"  Pressure: {cond['pressure_kpa']} kPa ({cond.get('pressure_tendency', 'unknown')})")
            if 'wind_speed_kmh' in cond:
                print(f"  Wind: {cond.get('wind_direction', '?')} {cond['wind_speed_kmh']} km/h")
            if 'humidity_percent' in cond:
                print(f"  Humidity: {cond['humidity_percent']}%")
        
        return True
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching RSS feed: {e}")
        return False
    except Exception as e:
        print(f"Error processing data: {e}")
        return False

if __name__ == "__main__":
    success = fetch_and_save_conditions()
    exit(0 if success else 1)
