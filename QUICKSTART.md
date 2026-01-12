# Quick Setup Instructions

## ✅ Repository Created!

Your repository is at: https://github.com/maxmaudie-design/crowsnest-weather

## Next Steps (15 minutes)

### 1. Get OpenWeatherMap API Key (5 min)

1. Go to https://openweathermap.org/api
2. Click "Get API Key" or "Sign Up"
3. Create free account
4. Copy your API key from dashboard

### 2. Add API Key to GitHub (2 min)

1. In your repo: **Settings** → **Secrets and variables** → **Actions**
2. Click **"New repository secret"**
3. Name: `OPENWEATHER_API_KEY`
4. Value: (paste your API key)
5. Click **"Add secret"**

### 3. Enable Workflow Permissions (2 min)

1. **Settings** → **Actions** → **General**
2. Scroll to "Workflow permissions"
3. Select: **"Read and write permissions"**
4. Check: **"Allow GitHub Actions to create and approve pull requests"**
5. Click **"Save"**

### 4. Enable GitHub Pages (2 min)

1. **Settings** → **Pages**
2. Source: **"Deploy from a branch"**
3. Branch: **main**, Folder: **/ (root)**
4. Click **"Save"**

### 5. Run First Workflow (3 min)

1. Go to **Actions** tab
2. Click **"Fetch Weather Data"** (left sidebar)
3. Click **"Run workflow"** button (right side)
4. Select branch: **main**
5. Click green **"Run workflow"**
6. Wait ~2 minutes for green checkmark ✓

### 6. Check Your Data! (1 min)

Visit:
```
https://maxmaudie-design.github.io/crowsnest-weather/data/current_conditions.json
```

If you see JSON data - **SUCCESS!** 🎉

## What Happens Now?

- ✅ Weather data fetches **every hour** automatically
- ✅ Files update in `data/` folder
- ✅ GitHub Pages serves fresh data
- ✅ Your website can fetch the JSON

## Troubleshooting

**Workflow failed?**
- Check if API key secret is named exactly: `OPENWEATHER_API_KEY`
- Verify "Read and write permissions" are enabled

**No data files?**
- Wait for workflow to complete (2-3 minutes)
- Check Actions tab for green checkmark

**GitHub Pages not working?**
- Wait 2-3 minutes after enabling
- Repository must be Public
- Check Settings → Pages for status

## Your Data URLs

```
Current: https://maxmaudie-design.github.io/crowsnest-weather/data/current_conditions.json
Forecast: https://maxmaudie-design.github.io/crowsnest-weather/data/pressure_forecast.json
```

Use these in your weather app!

---

**Need help?** Check the README.md for more details.
