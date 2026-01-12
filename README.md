# Crowsnest Pass Weather Data

Automated weather data fetching for Crowsnest Pass, Alberta using GitHub Actions.

## What This Does

- ✅ Fetches current weather from Environment Canada **every hour**
- ✅ Fetches 48-hour pressure forecast from OpenWeatherMap **every hour**
- ✅ Saves data as JSON files
- ✅ Automatically commits updates to this repository
- ✅ Hosts data via GitHub Pages (free CDN)

## Data Available

Your weather data is available at:

```
https://maxmaudie-design.github.io/crowsnest-weather/data/current_conditions.json
https://maxmaudie-design.github.io/crowsnest-weather/data/pressure_forecast.json
```

### Current Conditions

Includes:
- Temperature (°C)
- Air pressure (kPa) and tendency (rising/falling/steady)
- Wind speed and direction
- Humidity
- Wind chill
- Dewpoint

Updated hourly from Environment Canada RSS feed.

### Pressure Forecast

Includes:
- Current pressure
- 48-hour hourly pressure forecast
- 8-day daily forecast
- 24-hour pressure trend analysis

Updated hourly from OpenWeatherMap API.

## Setup Complete! ✓

Your automated weather pipeline is now running. Weather data updates every hour automatically.

## Monitoring

- Check the **Actions** tab to see workflow runs
- Look for green checkmarks ✓ (success) or red X (failure)
- Each update creates a new commit from github-actions bot

## Using the Data

### Simple JavaScript Example

```javascript
// Fetch current weather
fetch('https://maxmaudie-design.github.io/crowsnest-weather/data/current_conditions.json')
  .then(response => response.json())
  .then(data => {
    console.log(`Temperature: ${data.conditions.temperature_c}°C`);
    console.log(`Pressure: ${data.conditions.pressure_kpa} kPa (${data.conditions.pressure_tendency})`);
    console.log(`Wind: ${data.conditions.wind_direction} ${data.conditions.wind_speed_kmh} km/h`);
  });
```

### Fetch Pressure Forecast

```javascript
fetch('https://maxmaudie-design.github.io/crowsnest-weather/data/pressure_forecast.json')
  .then(response => response.json())
  .then(data => {
    console.log(`Current: ${data.current.pressure_hpa} hPa`);
    console.log(`24h trend: ${data.pressure_trends.trend_24h}`);
    
    // Next 12 hours
    data.hourly_forecast.slice(0, 12).forEach(hour => {
      console.log(`${hour.time}: ${hour.pressure_hpa} hPa`);
    });
  });
```

## Data Structure

### current_conditions.json

```json
{
  "timestamp": "2026-01-11T20:00:00Z",
  "source": "Environment Canada",
  "location": "Crowsnest Pass, AB",
  "conditions": {
    "temperature_c": -2.0,
    "pressure_kpa": 100.0,
    "pressure_tendency": "falling",
    "humidity_percent": 72,
    "wind_direction": "NW",
    "wind_speed_kmh": 13
  }
}
```

### pressure_forecast.json

```json
{
  "timestamp": "2026-01-11T20:00:00Z",
  "source": "OpenWeatherMap One Call API 3.0",
  "current": {
    "pressure_hpa": 1000
  },
  "pressure_trends": {
    "trend_24h": "falling",
    "pressure_change_24h_hpa": -5.2
  },
  "hourly_forecast": [
    {
      "time": "2026-01-11T21:00:00Z",
      "pressure_hpa": 999.5,
      "temperature_c": -2.5
    }
  ]
}
```

## Costs

Everything is **FREE**:
- GitHub Actions: 2,000 minutes/month free (we use ~1,440)
- GitHub Pages: Free for public repos
- OpenWeatherMap: 1,000 calls/day free (we use 24)
- Environment Canada: Free government data

## Troubleshooting

**No data files?**
- Wait for first workflow run (check Actions tab)
- Trigger manually: Actions → Fetch Weather Data → Run workflow

**Workflow failing?**
- Check Actions tab for error logs
- Verify OpenWeatherMap API key is set correctly

**GitHub Pages not working?**
- Wait 2-3 minutes after first commit
- Check Settings → Pages is enabled

## Technical Details

**Update Frequency**: Every hour at :00  
**Location**: 49.63°N, 114.69°W (Crowsnest Pass)  
**Data Sources**: Environment Canada, OpenWeatherMap  
**Automation**: GitHub Actions  
**Hosting**: GitHub Pages  

## Next Steps

1. Use these JSON endpoints in your weather website
2. Build pressure forecast visualizations
3. Add historical comparisons
4. Monitor the Actions tab for successful runs

---

**Repository**: https://github.com/maxmaudie-design/crowsnest-weather  
**Data**: Updates hourly automatically  
**Status**: Operational ✓
