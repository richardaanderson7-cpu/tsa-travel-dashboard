"""
generate_dashboard.py
Reads from SQLite and generates the GitHub Pages dashboard HTML.
Called at the end of the daily scrape workflow.
Also calls Claude API to generate the daily narrative block.

Usage:
  python scripts/generate_dashboard.py

Required environment variables:
  ANTHROPIC_API_KEY
"""

import os
import sqlite3
import json
from datetime import datetime, date, timedelta

ANTHROPIC_API_KEY = os.environ.get('ANTHROPIC_API_KEY', '')
DB_PATH           = os.path.join(os.path.dirname(__file__), '..', 'data', 'travel_data.db')
OUTPUT_PATH       = os.path.join(os.path.dirname(__file__), '..', 'docs', 'index.html')


# ---------------------------------------------------------------------------
# Data queries
# ---------------------------------------------------------------------------

def get_conn():
    return sqlite3.connect(DB_PATH)


def query_tsa(conn, days=400):
    rows = conn.execute('''
        SELECT date, travelers, prior_year_travelers
        FROM tsa_daily
        WHERE travelers IS NOT NULL
        ORDER BY date ASC
    ''').fetchall()
    return rows


def query_eia(conn):
    rows = conn.execute('''
        SELECT date, national_avg_price
        FROM eia_gas_prices
        ORDER BY date ASC
    ''').fetchall()
    return rows


def query_faa(conn, days=90):
    cutoff = (date.today() - timedelta(days=days)).strftime('%Y-%m-%d')
    rows = conn.execute('''
        SELECT date, ground_delays, ground_stops
        FROM faa_delays
        WHERE date >= ?
        ORDER BY date ASC
    ''', (cutoff,)).fetchall()
    return rows


def query_nws(conn, days=90):
    cutoff = (date.today() - timedelta(days=days)).strftime('%Y-%m-%d')
    rows = conn.execute('''
        SELECT date, event_type, region, severity
        FROM nws_alerts
        WHERE date >= ?
        ORDER BY date ASC
    ''', (cutoff,)).fetchall()
    return rows


def query_recent_tsa(conn, days=30):
    cutoff = (date.today() - timedelta(days=days)).strftime('%Y-%m-%d')
    rows = conn.execute('''
        SELECT date, travelers, prior_year_travelers
        FROM tsa_daily
        WHERE date >= ? AND travelers IS NOT NULL
        ORDER BY date DESC
    ''', (cutoff,)).fetchall()
    return rows


def query_recent_eia(conn, weeks=8):
    rows = conn.execute('''
        SELECT date, national_avg_price
        FROM eia_gas_prices
        ORDER BY date DESC
        LIMIT ?
    ''', (weeks,)).fetchall()
    return rows


# ---------------------------------------------------------------------------
# Claude narrative block
# ---------------------------------------------------------------------------

def generate_narrative(conn):
    """
    Calls Claude API with recent data to generate a short dashboard narrative.
    Data-only — no web search, just summarizing what the numbers show.
    Falls back to a plain summary if API key not set.
    """
    if not ANTHROPIC_API_KEY:
        print("  ANTHROPIC_API_KEY not set — using fallback narrative")
        return generate_fallback_narrative(conn)

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

        recent_tsa = query_recent_tsa(conn, days=14)
        recent_eia = query_recent_eia(conn, weeks=4)
        recent_faa = conn.execute('''
            SELECT date, ground_delays, ground_stops
            FROM faa_delays ORDER BY date DESC LIMIT 7
        ''').fetchall()
        recent_nws = conn.execute('''
            SELECT date, event_type, severity
            FROM nws_alerts ORDER BY date DESC LIMIT 10
        ''').fetchall()

        # Build context
        lines = []
        lines.append(f"Today: {date.today().strftime('%B %d, %Y')}")
        lines.append("")
        lines.append("RECENT TSA PASSENGER DATA (last 14 days):")
        for row in recent_tsa[:7]:
            travelers = f"{row[1]:,}" if row[1] else "N/A"
            prior     = f"{row[2]:,}" if row[2] else "N/A"
            yoy = ""
            if row[1] and row[2]:
                pct = ((row[1] - row[2]) / row[2]) * 100
                yoy = f" ({pct:+.1f}% YoY)"
            lines.append(f"  {row[0]}: {travelers}{yoy}")

        lines.append("")
        lines.append("GAS PRICES (recent weeks):")
        for row in recent_eia[:4]:
            lines.append(f"  {row[0]}: ${row[1]:.3f}/gal")

        lines.append("")
        lines.append("FAA DELAYS (recent days):")
        for row in recent_faa:
            lines.append(f"  {row[0]}: {row[1]} ground delays, {row[2]} ground stops")

        if recent_nws:
            lines.append("")
            lines.append("ACTIVE WEATHER ALERTS:")
            for row in recent_nws[:5]:
                lines.append(f"  {row[0]}: {row[1]} ({row[2]})")

        context = "\n".join(lines)

        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=400,
            system="""You write the daily data summary for a travel industry dashboard 
called Travel Pulse. Based on the data provided, write a concise 2-3 paragraph 
analyst note (150-200 words) summarizing current US air travel conditions. 

Focus on:
- Current passenger volume trend and what it means
- Whether the FAA system is running cleanly or under stress  
- Any weather or gas price context worth noting
- One sentence forward observation if relevant

Write in a confident, present-tense analytical voice. No bullet points. 
No headers. Plain prose only. Do not start with 'Today' or the date.""",
            messages=[{
                "role": "user",
                "content": f"Here is today's travel data:\n\n{context}\n\nWrite the dashboard summary."
            }]
        )

        narrative = response.content[0].text.strip()
        print(f"  Claude narrative generated ({len(narrative)} chars)")
        return narrative

    except Exception as e:
        print(f"  Claude narrative error: {e} — using fallback")
        return generate_fallback_narrative(conn)


def generate_fallback_narrative(conn):
    """Simple data-driven fallback if Claude API is unavailable."""
    recent = query_recent_tsa(conn, days=3)
    if recent:
        latest = recent[0]
        travelers = f"{latest[1]:,}" if latest[1] else "N/A"
        yoy = ""
        if latest[1] and latest[2]:
            pct = ((latest[1] - latest[2]) / latest[2]) * 100
            yoy = f", {pct:+.1f}% vs prior year"
        return (
            f"Latest TSA data shows {travelers} travelers screened on {latest[0]}{yoy}. "
            f"Dashboard updated daily with TSA passenger volumes, EIA gas prices, "
            f"FAA system delay snapshots, and NWS aviation weather alerts."
        )
    return "Dashboard updated daily. Data loading in progress."


# ---------------------------------------------------------------------------
# Build chart data
# ---------------------------------------------------------------------------

def build_chart_data(conn):
    tsa_rows = query_tsa(conn)
    eia_rows = query_eia(conn)
    faa_rows = query_faa(conn, days=180)
    nws_rows = query_nws(conn, days=180)

    # TSA — full history
    tsa_dates     = [r[0] for r in tsa_rows]
    tsa_travelers = [r[1] for r in tsa_rows]

    # Build a date->travelers lookup for fast prior year calculation
    travelers_by_date = {r[0]: r[1] for r in tsa_rows}

    # Rolling 7-day average
    tsa_rolling = []
    for i in range(len(tsa_travelers)):
        window = [v for v in tsa_travelers[max(0, i-6):i+1] if v is not None]
        tsa_rolling.append(round(sum(window) / len(window)) if window else None)

    # YoY % difference — calculated from our own database
    # Start from 2022 to avoid COVID-era distortion skewing the chart
    # Strategy: look up same date exactly 364 days ago (52 weeks = same weekday)
    # Fall back to 365/366/363 days if exact match not found
    from datetime import timedelta
    tsa_yoy       = []
    tsa_yoy_dates = []
    tsa_prior     = []
    for d, curr in zip(tsa_dates, tsa_travelers):
        if d < '2022-01-01':
            tsa_prior.append(None)
            continue
        prior = None
        dt = datetime.strptime(d, '%Y-%m-%d')
        for delta in [364, 365, 366, 363]:
            prior_date = (dt - timedelta(days=delta)).strftime('%Y-%m-%d')
            if prior_date in travelers_by_date and travelers_by_date[prior_date]:
                prior = travelers_by_date[prior_date]
                break
        tsa_prior.append(prior)
        if curr and prior and prior > 0:
            tsa_yoy.append(round(((curr - prior) / prior) * 100, 1))
            tsa_yoy_dates.append(d)
        else:
            tsa_yoy.append(None)
            tsa_yoy_dates.append(d)

    # EIA
    eia_dates  = [r[0] for r in eia_rows]
    eia_prices = [r[1] for r in eia_rows]

    # FAA
    faa_dates  = [r[0] for r in faa_rows]
    faa_delays = [r[1] for r in faa_rows]
    faa_stops  = [r[2] for r in faa_rows]

    # NWS — aggregate by date
    nws_by_date = {}
    for row in nws_rows:
        d = row[0]
        if d not in nws_by_date:
            nws_by_date[d] = []
        nws_by_date[d].append(f"{row[1]} ({row[2]})")
    nws_dates  = sorted(nws_by_date.keys())
    nws_events = [' | '.join(nws_by_date[d]) for d in nws_dates]
    nws_counts = [len(nws_by_date[d]) for d in nws_dates]

    return {
        'tsa_dates':     tsa_dates,
        'tsa_travelers': tsa_travelers,
        'tsa_prior':     tsa_prior,
        'tsa_rolling':   tsa_rolling,
        'tsa_yoy':       tsa_yoy,
        'tsa_yoy_dates': tsa_yoy_dates,
        'eia_dates':     eia_dates,
        'eia_prices':    eia_prices,
        'faa_dates':     faa_dates,
        'faa_delays':    faa_delays,
        'faa_stops':     faa_stops,
        'nws_dates':     nws_dates,
        'nws_events':    nws_events,
        'nws_counts':    nws_counts,
    }


# ---------------------------------------------------------------------------
# HTML generation
# ---------------------------------------------------------------------------

def generate_html(narrative, chart_data, updated_at):
    data_json = json.dumps(chart_data)

    html = f'''<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Travel Pulse — US Air Travel Dashboard</title>
  <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link href="https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=Syne:wght@400;600;700;800&display=swap" rel="stylesheet">
  <style>
    :root {{
      --bg:         #080b10;
      --surface:    #0e1420;
      --border:     #1c2535;
      --accent:     #3b82f6;
      --accent2:    #06b6d4;
      --green:      #10b981;
      --red:        #ef4444;
      --text:       #e2e8f0;
      --muted:      #64748b;
      --narrative:  #94a3b8;
    }}

    * {{ box-sizing: border-box; margin: 0; padding: 0; }}

    body {{
      background: var(--bg);
      color: var(--text);
      font-family: 'Syne', sans-serif;
      min-height: 100vh;
      padding: 0 0 4rem 0;
    }}

    /* Header */
    .header {{
      border-bottom: 1px solid var(--border);
      padding: 2rem 2.5rem;
      display: flex;
      align-items: baseline;
      gap: 1.5rem;
      background: linear-gradient(180deg, #0b1018 0%, transparent 100%);
    }}
    .header-title {{
      font-size: 1.6rem;
      font-weight: 800;
      letter-spacing: -0.02em;
      color: #fff;
    }}
    .header-title span {{
      color: var(--accent);
    }}
    .header-sub {{
      font-family: 'DM Mono', monospace;
      font-size: 0.75rem;
      color: var(--muted);
      letter-spacing: 0.05em;
      text-transform: uppercase;
    }}
    .header-updated {{
      margin-left: auto;
      font-family: 'DM Mono', monospace;
      font-size: 0.7rem;
      color: var(--muted);
    }}

    /* Narrative block */
    .narrative-wrap {{
      margin: 2rem 2.5rem;
      border: 1px solid var(--border);
      border-left: 3px solid var(--accent);
      background: var(--surface);
      border-radius: 4px;
      padding: 1.5rem 2rem;
      display: grid;
      grid-template-columns: auto 1fr;
      gap: 1.5rem;
      align-items: start;
    }}
    .narrative-label {{
      font-family: 'DM Mono', monospace;
      font-size: 0.65rem;
      letter-spacing: 0.12em;
      text-transform: uppercase;
      color: var(--accent);
      writing-mode: vertical-rl;
      transform: rotate(180deg);
      padding-top: 0.25rem;
    }}
    .narrative-text {{
      font-size: 0.95rem;
      line-height: 1.75;
      color: var(--narrative);
      font-weight: 400;
    }}

    /* Stats row */
    .stats-row {{
      display: grid;
      grid-template-columns: repeat(4, 1fr);
      gap: 1px;
      margin: 0 2.5rem 2rem;
      border: 1px solid var(--border);
      border-radius: 4px;
      overflow: hidden;
      background: var(--border);
    }}
    .stat {{
      background: var(--surface);
      padding: 1.25rem 1.5rem;
    }}
    .stat-label {{
      font-family: 'DM Mono', monospace;
      font-size: 0.65rem;
      letter-spacing: 0.1em;
      text-transform: uppercase;
      color: var(--muted);
      margin-bottom: 0.5rem;
    }}
    .stat-value {{
      font-size: 1.6rem;
      font-weight: 700;
      color: #fff;
      letter-spacing: -0.02em;
    }}
    .stat-sub {{
      font-family: 'DM Mono', monospace;
      font-size: 0.7rem;
      color: var(--muted);
      margin-top: 0.25rem;
    }}
    .stat-up   {{ color: var(--green); }}
    .stat-down {{ color: var(--red); }}

    /* Charts grid */
    .charts-grid {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 1px;
      margin: 0 2.5rem;
      background: var(--border);
      border: 1px solid var(--border);
      border-radius: 4px;
      overflow: hidden;
    }}
    .chart-block {{
      background: var(--surface);
      padding: 1.5rem;
    }}
    .chart-block.full {{
      grid-column: 1 / -1;
    }}
    .chart-title {{
      font-family: 'DM Mono', monospace;
      font-size: 0.7rem;
      letter-spacing: 0.1em;
      text-transform: uppercase;
      color: var(--muted);
      margin-bottom: 1rem;
    }}
    .chart-div {{ width: 100%; }}

    /* Footer */
    .footer {{
      margin: 2rem 2.5rem 0;
      padding-top: 1.5rem;
      border-top: 1px solid var(--border);
      display: flex;
      justify-content: space-between;
      align-items: center;
    }}
    .footer-text {{
      font-family: 'DM Mono', monospace;
      font-size: 0.7rem;
      color: var(--muted);
    }}
    .footer-sources {{
      font-family: 'DM Mono', monospace;
      font-size: 0.65rem;
      color: var(--muted);
      text-align: right;
      line-height: 1.8;
    }}

    @media (max-width: 900px) {{
      .charts-grid {{ grid-template-columns: 1fr; }}
      .chart-block.full {{ grid-column: 1; }}
      .stats-row {{ grid-template-columns: repeat(2, 1fr); }}
      .header {{ flex-wrap: wrap; }}
    }}
  </style>
</head>
<body>

<div class="header">
  <div class="header-title">Travel<span>Pulse</span></div>
  <div class="header-sub">US Air Travel Intelligence</div>
  <div class="header-updated">Updated {updated_at}</div>
</div>

<div class="narrative-wrap">
  <div class="narrative-label">Analysis</div>
  <div class="narrative-text" id="narrative-text">{narrative}</div>
</div>

<div class="stats-row" id="stats-row">
  <!-- populated by JS -->
</div>

<div class="charts-grid">
  <div class="chart-block full">
    <div class="chart-title">TSA Daily Passenger Throughput — Full History</div>
    <div class="chart-div" id="chart-tsa-history"></div>
  </div>
  <div class="chart-block">
    <div class="chart-title">Year-over-Year % Difference</div>
    <div class="chart-div" id="chart-yoy"></div>
  </div>
  <div class="chart-block">
    <div class="chart-title">National Avg Gas Price (Regular Unleaded)</div>
    <div class="chart-div" id="chart-eia"></div>
  </div>
  <div class="chart-block">
    <div class="chart-title">FAA Ground Delays &amp; Ground Stops</div>
    <div class="chart-div" id="chart-faa"></div>
  </div>
  <div class="chart-block">
    <div class="chart-title">NWS Aviation Weather Alerts</div>
    <div class="chart-div" id="chart-nws"></div>
  </div>
</div>

<div class="footer">
  <div class="footer-text">Travel Pulse · Auto-updated daily via GitHub Actions</div>
  <div class="footer-sources">
    Sources: TSA · EIA · FAA NASSTATUS · NWS api.weather.gov
  </div>
</div>

<script>
const D = {data_json};

const LAYOUT_BASE = {{
  paper_bgcolor: 'transparent',
  plot_bgcolor:  'transparent',
  font:          {{ family: 'DM Mono, monospace', color: '#64748b', size: 11 }},
  margin:        {{ t: 10, r: 20, b: 40, l: 60 }},
  xaxis: {{
    gridcolor: '#1c2535',
    linecolor: '#1c2535',
    tickcolor: '#1c2535',
    tickfont:  {{ color: '#475569', size: 10 }},
  }},
  yaxis: {{
    gridcolor: '#1c2535',
    linecolor: '#1c2535',
    tickcolor: '#1c2535',
    tickfont:  {{ color: '#475569', size: 10 }},
    zerolinecolor: '#1c2535',
  }},
  legend: {{
    bgcolor: 'transparent',
    font: {{ color: '#94a3b8', size: 10 }},
  }},
  hovermode: 'x unified',
  hoverlabel: {{
    bgcolor: '#0e1420',
    bordercolor: '#1c2535',
    font: {{ family: 'DM Mono, monospace', color: '#e2e8f0', size: 11 }},
  }},
}};

const CONFIG = {{
  displayModeBar: false,
  responsive: true,
}};

// --- Stats row ---
function buildStats() {{
  const lastIdx = D.tsa_travelers.length - 1;
  let latestTravelers = null;
  for (let i = lastIdx; i >= 0; i--) {{
    if (D.tsa_travelers[i]) {{ latestTravelers = D.tsa_travelers[i]; break; }}
  }}
  let latestDate = D.tsa_dates[lastIdx];
  let latestEia  = D.eia_prices[D.eia_prices.length - 1];
  let latestYoy  = null;
  for (let i = D.tsa_yoy.length - 1; i >= 0; i--) {{
    if (D.tsa_yoy[i] !== null) {{ latestYoy = D.tsa_yoy[i]; break; }}
  }}

  // Total FAA events last 7 days
  const faaTotal = D.faa_delays.slice(-7).reduce((a,b) => a + (b||0), 0)
                 + D.faa_stops.slice(-7).reduce((a,b) => a + (b||0), 0);

  const yoyClass = latestYoy > 0 ? 'stat-up' : latestYoy < 0 ? 'stat-down' : '';
  const yoySign  = latestYoy > 0 ? '+' : '';

  document.getElementById('stats-row').innerHTML = `
    <div class="stat">
      <div class="stat-label">Latest Travelers</div>
      <div class="stat-value">${{(latestTravelers/1e6).toFixed(2)}}M</div>
      <div class="stat-sub">${{latestDate}}</div>
    </div>
    <div class="stat">
      <div class="stat-label">vs Prior Year</div>
      <div class="stat-value ${{yoyClass}}">${{latestYoy !== null ? yoySign + latestYoy.toFixed(1) + '%' : 'N/A'}}</div>
      <div class="stat-sub">same weekday</div>
    </div>
    <div class="stat">
      <div class="stat-label">Gas Price</div>
      <div class="stat-value">$${{latestEia ? latestEia.toFixed(3) : 'N/A'}}</div>
      <div class="stat-sub">national avg / gal</div>
    </div>
    <div class="stat">
      <div class="stat-label">FAA Events</div>
      <div class="stat-value">${{faaTotal}}</div>
      <div class="stat-sub">delays + stops, 7 days</div>
    </div>
  `;
}}
buildStats();

// --- TSA History chart ---
Plotly.newPlot('chart-tsa-history', [
  {{
    x: D.tsa_dates, y: D.tsa_travelers,
    type: 'scatter', mode: 'lines',
    name: 'Daily Travelers',
    line: {{ color: '#1c2d4a', width: 1 }},
    hovertemplate: '%{{y:,.0f}}<extra>Daily</extra>',
  }},
  {{
    x: D.tsa_dates, y: D.tsa_rolling,
    type: 'scatter', mode: 'lines',
    name: '7-Day Avg',
    line: {{ color: '#3b82f6', width: 2 }},
    hovertemplate: '%{{y:,.0f}}<extra>7-Day Avg</extra>',
  }},
  {{
    x: D.tsa_dates, y: D.tsa_prior,
    type: 'scatter', mode: 'lines',
    name: 'Prior Year',
    line: {{ color: '#06b6d4', width: 1.5, dash: 'dot' }},
    hovertemplate: '%{{y:,.0f}}<extra>Prior Year</extra>',
  }},
], {{
  ...LAYOUT_BASE,
  margin: {{ t: 10, r: 20, b: 40, l: 75 }},
  height: 280,
  yaxis: {{
    ...LAYOUT_BASE.yaxis,
    tickformat: ',.2s',
  }},
}}, CONFIG);

// --- YoY chart ---
const yoyColors = D.tsa_yoy.map(v => v === null ? 'transparent' : v >= 0 ? '#10b981' : '#ef4444');
Plotly.newPlot('chart-yoy', [
  {{
    x: D.tsa_yoy_dates, y: D.tsa_yoy,
    type: 'bar',
    name: 'YoY %',
    marker: {{ color: yoyColors, opacity: 0.85 }},
    hovertemplate: '%{{y:+.1f}}%<extra>YoY</extra>',
  }},
], {{
  ...LAYOUT_BASE,
  height: 240,
  yaxis: {{
    ...LAYOUT_BASE.yaxis,
    ticksuffix: '%',
    zerolinecolor: '#334155',
    zerolinewidth: 1,
  }},
}}, CONFIG);

// --- EIA chart ---
Plotly.newPlot('chart-eia', [
  {{
    x: D.eia_dates, y: D.eia_prices,
    type: 'scatter', mode: 'lines',
    name: '$/gal',
    line: {{ color: '#f59e0b', width: 2 }},
    fill: 'tozeroy',
    fillcolor: 'rgba(245,158,11,0.06)',
    hovertemplate: '$%{{y:.3f}}<extra>Avg Price</extra>',
  }},
], {{
  ...LAYOUT_BASE,
  height: 240,
  yaxis: {{
    ...LAYOUT_BASE.yaxis,
    tickprefix: '$',
    rangemode: 'tozero',
  }},
}}, CONFIG);

// --- FAA chart ---
Plotly.newPlot('chart-faa', [
  {{
    x: D.faa_dates, y: D.faa_delays,
    type: 'bar', name: 'Ground Delays',
    marker: {{ color: '#f59e0b', opacity: 0.8 }},
    hovertemplate: '%{{y}}<extra>Ground Delays</extra>',
  }},
  {{
    x: D.faa_dates, y: D.faa_stops,
    type: 'bar', name: 'Ground Stops',
    marker: {{ color: '#ef4444', opacity: 0.8 }},
    hovertemplate: '%{{y}}<extra>Ground Stops</extra>',
  }},
], {{
  ...LAYOUT_BASE,
  height: 240,
  barmode: 'stack',
}}, CONFIG);

// --- NWS chart ---
Plotly.newPlot('chart-nws', [
  {{
    x: D.nws_dates, y: D.nws_counts,
    type: 'bar', name: 'Alert Count',
    marker: {{ color: '#8b5cf6', opacity: 0.8 }},
    text: D.nws_events,
    hovertemplate: '%{{text}}<extra>Weather Alerts</extra>',
  }},
], {{
  ...LAYOUT_BASE,
  height: 240,
  yaxis: {{
    ...LAYOUT_BASE.yaxis,
    dtick: 1,
  }},
}}, CONFIG);

</script>
</body>
</html>'''
    return html


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print(f"\n{'='*60}")
    print(f"Dashboard generation starting: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")

    conn = get_conn()

    print("Querying data...")
    chart_data = build_chart_data(conn)
    print(f"  TSA rows:  {len(chart_data['tsa_dates'])}")
    print(f"  EIA rows:  {len(chart_data['eia_dates'])}")
    print(f"  FAA rows:  {len(chart_data['faa_dates'])}")
    print(f"  NWS rows:  {len(chart_data['nws_dates'])}")

    print("\nGenerating Claude narrative...")
    narrative = generate_narrative(conn)

    conn.close()

    updated_at = datetime.now().strftime('%B %d, %Y at %H:%M UTC')
    print(f"\nBuilding HTML...")
    html = generate_html(narrative, chart_data, updated_at)

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f"  Dashboard written to: {OUTPUT_PATH}")
    print(f"  File size: {os.path.getsize(OUTPUT_PATH):,} bytes")

    print(f"\n{'='*60}")
    print("Dashboard generation complete.")
    print(f"{'='*60}\n")


if __name__ == '__main__':
    main()
