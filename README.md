# TSA Travel Dashboard

An automated travel data pipeline that scrapes daily TSA passenger volumes, EIA gas prices, FAA delay data, and NWS weather alerts — stores them in SQLite, publishes an interactive Plotly dashboard to GitHub Pages, and sends a weekly Claude-written narrative recap via Buttondown.

## How It Works

**Daily (10am ET):** GitHub Actions scrapes all four data sources, writes to SQLite, regenerates the dashboard HTML, and commits everything back to the repo.

**Weekly (Monday 11am ET):** GitHub Actions queries the last 7 days from SQLite, calls Claude with web search enabled, and publishes the AI-written recap to Buttondown for newsletter delivery.

## Data Sources

| Source | Data | Cadence |
|--------|------|---------|
| TSA | Daily passenger throughput | Daily |
| EIA | National avg regular unleaded gas price | Weekly |
| FAA | Ground delays and ground stops snapshot | Daily |
| NWS | Significant weather alerts (aviation-relevant) | Daily |

## Repo Structure

```
tsa-travel-dashboard/
├── .github/
│   └── workflows/
│       ├── daily.yml          # Daily scrape + dashboard refresh
│       └── weekly.yml         # Weekly recap + Buttondown publish
├── data/
│   └── travel_data.db         # SQLite database (auto-generated)
├── scripts/
│   ├── init_db.py             # Run once to create DB schema
│   ├── daily_scrape.py        # All four source pulls + dashboard regen
│   └── weekly_recap.py        # Claude recap + Buttondown publish
├── docs/
│   └── index.html             # GitHub Pages dashboard (auto-generated)
├── requirements.txt
└── README.md
```

## Setup Instructions

### 1. Clone and initialize

```bash
git clone https://github.com/YOUR_USERNAME/tsa-travel-dashboard.git
cd tsa-travel-dashboard
pip install -r requirements.txt
python scripts/init_db.py
```

### 2. Get API keys

| Key | Where to get it |
|-----|----------------|
| `EIA_API_KEY` | https://www.eia.gov/opendata/ — free registration |
| `ANTHROPIC_API_KEY` | https://console.anthropic.com |
| `BUTTONDOWN_API_KEY` | https://buttondown.email/settings/programming |

### 3. Add GitHub Secrets

In your repo: Settings → Secrets and variables → Actions → New repository secret

Add each key from the table above plus:
- `NOTIFY_EMAIL` — your email address for failure notifications

### 4. Enable GitHub Pages

Settings → Pages → Source: Deploy from branch → Branch: `main` → Folder: `/docs`

### 5. Set Anthropic spend cap

In your Anthropic account dashboard, set a monthly spend limit (recommend $5–10 for this project).

## Local Development

Run the daily scrape manually:
```bash
python scripts/daily_scrape.py
```

Run the weekly recap manually:
```bash
python scripts/weekly_recap.py
```

## Notes

- The SQLite database is committed to the repo as a single file (`data/travel_data.db`)
- GitHub Pages serves the dashboard from the `/docs` folder
- The dashboard auto-updates daily — no manual intervention needed
- Buttondown handles all subscriber management and unsubscribe compliance
