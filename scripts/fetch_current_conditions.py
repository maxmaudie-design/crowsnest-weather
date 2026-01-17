#!/usr/bin/env python3
"""
Fetch complete current weather conditions from Environment Canada RSS feed
and save as JSON for the weather dashboard.

This version fetches: temperature, conditions, wind, pressure, humidity, dewpoint
"""

import requests
import xml.etree.ElementTree as ET
import json
import re
from datetime import datetime
import os

# Configuration
RSS_URL = "https://weather.gc.ca/rss/weather/49.631_-114.693_e.xml"
OUTPUT_DIR = "data"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "current_conditions.json")

def parse_current_conditions(summary_text):
    """Parse the current conditions from RSS summary text."""
    conditions = {}
    
    # Temperature
    temp_match = re.search(r'Temperature[:\s]+(-?\d+\.?\d*)\s*°C', summary_text, re.IGNORECASE)
    if temp_match:
        conditions['temperature'] = float(temp_match.group(1))
    
    # Condition
    cond_match = re.search(r'Condition[:\s]+([^:]+?)(?=Temperature|Pressure|Tendency|Wind|$)', summary_text, re.IGNORECASE)
    if cond_match:
        conditions['condition'] = cond_match.group(1).strip()
    
    # Pressure
    press_match = re.search(r'Pressure[:\s]+(\d+\.?\d*)\s*kPa', summary_text, re.IGNORECASE)
    if press_match:
        conditions['pressure_kpa'] = float(press_match.group(1))
    
    # Pressure Tendency
    tend_match = re.search(r'Tendency[:\s]+(\w+)', summary_text, re.IGNORECASE)
    if tend_match:
        conditions['pressure_tendency'] = tend_match.group(1).lower()
    
    # Wind
    wind_match = re.search(r'Wind[:\s]+([^:]+?)(?=Temperature|Pressure|Humidity|$)', summary_text, re.IGNORECASE)
    if wind_match:
        wind_text = wind_match.group(1).strip()
        speed_match = re.search(r'(\d+)\s*km/h', wind_text, re.IGNORECASE)
        dir_match = re.search(r'\b([NSEW]{1,3})\b', wind_text, re.IGNORECASE)
        
        if speed_match:
            conditions['wind_speed_kmh'] = int(speed_match.group(1))
        if dir_match:
            conditions['wind_direction'] = dir_match.group(1).upper()
    
    # Humidity
    hum_match = re.search(r'Humidity[:\s]+(\d+)%', summary_text, re.IGNORECASE)
    if hum_match:
        conditions['humidity_percent'] = int(hum_match.group(1))
    
    # Dewpoint
    dew_match = re.search(r'Dewpoint[:\s]+(-?\d+\.?\d*)\s*°C', summary_text, re.IGNORECASE)
    if dew_match:
        conditions['dewpoint'] = float(dew_match.group(1))
    
    return conditions

def fetch_weather_data():
    """Fetch weather data from Environment Canada RSS feed."""
    try:
        print(f"Fetching weather data from {RSS_URL}...")
        response = requests.get(RSS_URL, timeout=10)
        response.raise_for_status()
        
        # Parse XML
        root = ET.fromstring(response.content)
        
        # Find namespace
        namespaces = {'atom': 'http://www.w3.org/2005/Atom'}
        
        # Find current conditions entry
        entries = root.findall('.//atom:entry', namespaces)
        observation_time = None
        
        for entry in entries:
            title_elem = entry.find('atom:title', namespaces)
            if title_elem is not None and 'Current Conditions' in title_elem.text:
                summary_elem = entry.find('atom:summary', namespaces)
                updated_elem = entry.find('atom:updated', namespaces)
                
                if updated_elem is not None:
                    observation_time = updated_elem.text
                
                if summary_elem is not None:
                    summary_text = summary_elem.text
                    conditions = parse_current_conditions(summary_text)
                    
                    # Build full data structure
                    full_data = {
                        "timestamp": datetime.utcnow().isoformat() + 'Z',
                        "source": "Environment Canada",
                        "location": "Crowsnest Pass, AB",
                        "conditions": conditions,
                        "observation_time": observation_time or "Unknown",
                        "fetch_time_utc": datetime.utcnow().isoformat() + 'Z'
                    }
                    
                    # Ensure output directory exists
                    os.makedirs(OUTPUT_DIR, exist_ok=True)
                    
                    # Save to JSON
                    with open(OUTPUT_FILE, 'w') as f:
                        json.dump(full_data, f, indent=2)
                    
                    print(f"✓ Weather data saved to {OUTPUT_FILE}")
                    print(f"  Temperature: {conditions.get('temperature', 'N/A')}°C")
                    print(f"  Condition: {conditions.get('condition', 'N/A')}")
                    print(f"  Wind: {conditions.get('wind_direction', 'N/A')} {conditions.get('wind_speed_kmh', 'N/A')} km/h")
                    print(f"  Pressure: {conditions.get('pressure_kpa', 'N/A')} kPa")
                    print(f"  Humidity: {conditions.get('humidity_percent', 'N/A')}%")
                    return True
        
        print("✗ Could not find current conditions in RSS feed")
        return False
        
    except requests.exceptions.RequestException as e:
        print(f"✗ Error fetching RSS feed: {e}")
        return False
    except ET.ParseError as e:
        print(f"✗ Error parsing XML: {e}")
        return False
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = fetch_weather_data()
    exit(0 if success else 1)
