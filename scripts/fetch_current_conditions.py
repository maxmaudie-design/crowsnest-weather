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
import html as html_module

# Configuration
RSS_URL = "https://weather.gc.ca/rss/weather/49.631_-114.693_e.xml"
OUTPUT_DIR = "data"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "current_conditions.json")

def extract_condition_from_title(title_text):
    """Extract weather condition from title like 'Current Conditions: Mainly Cloudy, 5.3°C'"""
    if not title_text or 'Current Conditions' not in title_text:
        return None
    
    # Split on colon and get the part after "Current Conditions:"
    parts = title_text.split(':', 1)
    if len(parts) < 2:
        return None
    
    # Get everything after the colon
    condition_part = parts[1].strip()
    
    # Split on comma to separate condition from temperature
    # Example: "Mainly Cloudy, 5.3°C" -> "Mainly Cloudy"
    condition_parts = condition_part.split(',')
    if condition_parts:
        condition = condition_parts[0].strip()
        # Clean up any extra text
        condition = re.sub(r'\s+', ' ', condition)
        return condition if condition else None
    
    return None

def parse_current_conditions(summary_text):
    """Parse the current conditions from RSS summary text."""
    print(f"Raw summary text: {summary_text[:200]}...")  # Debug
    
    # Unescape HTML entities (&deg; -> °, etc.)
    summary_text = html_module.unescape(summary_text)
    print(f"After unescape: {summary_text[:200]}...")  # Debug
    
    # Remove HTML tags but keep the text
    summary_text = re.sub(r'<[^>]+>', ' ', summary_text)
    print(f"After tag removal: {summary_text[:200]}...")  # Debug
    
    conditions = {}
    
    # Temperature - be more flexible with format
    temp_match = re.search(r'Temperature[:\s]+(-?\d+\.?\d*)', summary_text, re.IGNORECASE)
    if temp_match:
        conditions['temperature'] = float(temp_match.group(1))
        print(f"Found temperature: {conditions['temperature']}")
    
    # Pressure and Tendency (format: "103.8 kPa rising")
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
    
    # Wind - handle "calm" and numeric speeds
    wind_match = re.search(r'Wind[:\s]+(.+?)(?=Air Quality|Observed|$)', summary_text, re.IGNORECASE | re.DOTALL)
    if wind_match:
        wind_text = wind_match.group(1).strip()
        print(f"Wind text: {wind_text}")
        
        # Check for calm
        if 'calm' in wind_text.lower():
            conditions['wind_speed_kmh'] = 0
            conditions['wind_direction'] = 'CALM'
            print("Wind is calm")
        else:
            # Try to extract direction and speed
            speed_match = re.search(r'(\d+)\s*km/h', wind_text, re.IGNORECASE)
            dir_match = re.search(r'\b([NSEW]{1,3})\b', wind_text, re.IGNORECASE)
            
            if speed_match:
                conditions['wind_speed_kmh'] = int(speed_match.group(1))
                print(f"Found wind speed: {conditions['wind_speed_kmh']}")
            if dir_match:
                conditions['wind_direction'] = dir_match.group(1).upper()
                print(f"Found wind direction: {conditions['wind_direction']}")
    
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
                print(f"Found Current Conditions entry with title: {title_elem.text}")
                
                # Extract condition from title
                condition_text = extract_condition_from_title(title_elem.text)
                print(f"Extracted condition: {condition_text}")
                
                summary_elem = entry.find('atom:summary', namespaces)
                updated_elem = entry.find('atom:updated', namespaces)
                
                if updated_elem is not None:
                    observation_time = updated_elem.text
                
                if summary_elem is not None:
                    summary_text = summary_elem.text
                    
                    if summary_text:
                        conditions = parse_current_conditions(summary_text)
                        
                        # Add the condition text extracted from the title
                        if condition_text:
                            conditions['condition'] = condition_text
                        
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
                        print(f"  Condition: {conditions.get('condition', 'N/A')}")
                        print(f"  Temperature: {conditions.get('temperature', 'N/A')}°C")
                        print(f"  Wind: {conditions.get('wind_direction', 'N/A')} {conditions.get('wind_speed_kmh', 'N/A')} km/h")
                        print(f"  Pressure: {conditions.get('pressure_kpa', 'N/A')} kPa")
                        print(f"  Humidity: {conditions.get('humidity_percent', 'N/A')}%")
                        print(f"  Dewpoint: {conditions.get('dewpoint', 'N/A')}°C")
                        return True
                    else:
                        print("✗ Summary text was empty")
                else:
                    print("✗ No summary element found")
        
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
