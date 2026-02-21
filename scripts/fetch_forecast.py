import requests
import json
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
import os

RSS_URL = 'https://weather.gc.ca/rss/weather/49.631_-114.693_e.xml'
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'forecast.json')

NAMESPACE = {'atom': 'http://www.w3.org/2005/Atom'}

def get_weather_icon(condition):
    if not condition:
        return '🌡️'
    c = condition.lower()
    if 'sunny' in c or 'clear' in c:
        return '☀️'
    if 'partly' in c and 'cloud' in c:
        return '⛅'
    if 'cloudy' in c or 'overcast' in c:
        return '☁️'
    if 'rain' in c and 'snow' in c:
        return '🌨️'
    if 'rain' in c or 'shower' in c:
        return '🌧️'
    if 'snow' in c or 'flurr' in c:
        return '❄️'
    if 'thunder' in c or 'storm' in c:
        return '⛈️'
    if 'fog' in c or 'mist' in c:
        return '🌫️'
    if 'wind' in c:
        return '💨'
    if 'mix' in c:
        return '🌨️'
    return '🌤️'

def parse_temp(title, keyword):
    """Parse High or Low temp from title string."""
    pattern = rf'{keyword}\s+(minus\s+)?(\d+|zero)'
    m = re.search(pattern, title, re.IGNORECASE)
    if not m:
        return None
    raw = m.group(2).lower()
    val = 0 if raw == 'zero' else int(raw)
    if m.group(1):  # "minus"
        val = -val
    return val

def is_night(title):
    t = title.lower()
    return 'night' in t or 'tonight' in t

def fetch_forecast():
    resp = requests.get(RSS_URL, timeout=15)
    resp.raise_for_status()
    root = ET.fromstring(resp.content)

    entries = []
    for entry in root.findall('atom:entry', NAMESPACE):
        title_el = entry.find('atom:title', NAMESPACE)
        summary_el = entry.find('atom:summary', NAMESPACE)
        category_el = entry.find('atom:category', NAMESPACE)
        if category_el is None or category_el.get('term') != 'Weather Forecasts':
            continue
        title = title_el.text if title_el is not None else ''
        summary = summary_el.text if summary_el is not None else ''

        night = is_night(title)
        high = parse_temp(title, 'High')
        low = parse_temp(title, 'Low')

        day_match = re.match(r'^(\w+)', title)
        day = day_match.group(1) if day_match else ''
        cond_match = re.search(r':\s*(.+?)(?:\.\s*(?:High|Low|POP)|$)', title)
        condition = cond_match.group(1).strip() if cond_match else ''

        entries.append({
            'day': day,
            'condition': condition,
            'high': high,
            'low': low,
            'is_night': night,
            'icon': get_weather_icon(condition)
        })

    # Skip any leading night entries (remainder of current night)
    while entries and entries[0]['is_night']:
        entries.pop(0)

    # Pair day + night entries
    combined = []
    i = 0
    offset = 0
    while i < len(entries) and len(combined) < 7:
        cur = entries[i]
        nxt = entries[i + 1] if i + 1 < len(entries) else None

        if not cur['is_night'] and nxt and nxt['is_night']:
            combined.append({
                'day': cur['day'],
                'condition': cur['condition'],
                'icon': cur['icon'],
                'high': cur['high'],
                'low': nxt['low'],
                'offset': offset
            })
            i += 2
        elif not cur['is_night']:
            combined.append({
                'day': cur['day'],
                'condition': cur['condition'],
                'icon': cur['icon'],
                'high': cur['high'],
                'low': None,
                'offset': offset
            })
            i += 1
        else:
            # Orphaned night entry, skip
            i += 1
        offset += 1

    result = {
        'fetched_at': datetime.now(timezone.utc).isoformat(),
        'forecast': combined
    }

    with open(OUTPUT_PATH, 'w') as f:
        json.dump(result, f, indent=2)

    print(f"Forecast saved: {len(combined)} days")

if __name__ == '__main__':
    fetch_forecast()
