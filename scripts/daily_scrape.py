"""
daily_scrape.py
Runs daily via GitHub Actions at 10am ET.
Pulls all four data sources and writes to SQLite.

Sources:
  - TSA:  tsa.gov/travel/passenger-volumes
  - EIA:  EIA APIv2 (requires EIA_API_KEY env var)
  - FAA:  nasstatus.faa.gov/api/airport-status-information
  - NWS:  api.weather.gov/alerts/active
"""

import os
import sqlite3
import requests
import pandas as pd
from io import StringIO
from datetime import datetime, date
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'travel_data.db')

TSA_URL     = 'https://www.tsa.gov/travel/passenger-volumes'
TSA_HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/120.0.0.0 Safari/537.36'
    )
}

EIA_API_KEY = os.environ.get('EIA_API_KEY', '')
EIA_URL     = (
    'https://api.eia.gov/v2/petroleum/pri/gnd/data/'
    '?api_key={key}'
    '&frequency=weekly'
    '&data[0]=value'
    '&facets[product][]=EPM0'
    '&facets[duoarea][]=NUS'
    '&facets[series][]=EMM_EPM0_PTE_NUS_DPG'
    '&sort[0][column]=period'
    '&sort[0][direction]=desc'
    '&length=2'
)

FAA_URL     = 'https://nasstatus.faa.gov/api/airport-status-information'
FAA_HEADERS = {'Accept': 'application/xml'}

NWS_URL     = 'https://api.weather.gov/alerts/active'
NWS_PARAMS  = {
    'status': 'actual',
    'message_type': 'alert',
    'urgency': 'Immediate,Expected',
    'severity': 'Extreme,Severe',
    'certainty': 'Observed,Likely'
}
NWS_EVENT_TYPES = {
    'Winter Storm Warning', 'Winter Storm Watch', 'Blizzard Warning',
    'Dense Fog Advisory', 'Severe Thunderstorm Warning',
    'High Wind Warning', 'High Wind Watch', 'Ice Storm Warning',
}


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def get_conn():
    return sqlite3.connect(DB_PATH)

def upsert_tsa(conn, row_date, travelers, prior_year_travelers):
    conn.execute('''
        INSERT OR IGNORE INTO tsa_daily (date, travelers, prior_year_travelers)
        VALUES (?, ?, ?)
    ''', (row_date, travelers, prior_year_travelers))
    conn.commit()

def upsert_eia(conn, row_date, price):
    conn.execute('''
        INSERT OR IGNORE INTO eia_gas_prices (date, national_avg_price)
        VALUES (?, ?)
    ''', (row_date, price))
    conn.commit()

def upsert_faa(conn, row_date, ground_delays, ground_stops, affected_airports, delay_causes):
    conn.execute('''
        INSERT OR REPLACE INTO faa_delays
            (date, ground_delays, ground_stops, affected_airports, delay_causes)
        VALUES (?, ?, ?, ?, ?)
    ''', (row_date, ground_delays, ground_stops, affected_airports, delay_causes))
    conn.commit()

def insert_nws(conn, row_date, event_type, region, severity, onset, expires):
    conn.execute('''
        INSERT INTO nws_alerts (date, event_type, region, severity, onset, expires)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (row_date, event_type, region, severity, onset, expires))
    conn.commit()


# ---------------------------------------------------------------------------
# TSA scraper
# ---------------------------------------------------------------------------

def scrape_tsa(conn):
    print("Scraping TSA...")
    try:
        response = requests.get(TSA_URL, headers=TSA_HEADERS, timeout=15)
        response.raise_for_status()

        # StringIO wrapper avoids pandas deprecation warning
        tables = pd.read_html(StringIO(response.text))
        if not tables:
            raise ValueError("No tables found on TSA page")

        df = tables[0]
        print(f"  TSA table shape: {df.shape}")
        print(f"  TSA columns: {list(df.columns)}")

        df.columns  = [str(c).strip() for c in df.columns]
        date_col    = df.columns[0]
        current_col = df.columns[1]
        prior_col   = df.columns[2] if len(df.columns) > 2 else None

        df = df.dropna(subset=[date_col])
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        df = df.dropna(subset=[date_col])

        df[current_col] = pd.to_numeric(
            df[current_col].astype(str).str.replace(',', '', regex=False).str.strip(),
            errors='coerce'
        )
        if prior_col:
            df[prior_col] = pd.to_numeric(
                df[prior_col].astype(str).str.replace(',', '', regex=False).str.strip(),
                errors='coerce'
            )

        df      = df.sort_values(date_col, ascending=False)
        latest  = df.iloc[0]

        row_date  = latest[date_col].strftime('%Y-%m-%d')
        travelers = int(latest[current_col]) if pd.notna(latest[current_col]) else None
        prior     = int(latest[prior_col]) if prior_col and pd.notna(latest[prior_col]) else None

        upsert_tsa(conn, row_date, travelers, prior)

        travelers_str = f"{travelers:,}" if travelers is not None else "N/A"
        prior_str     = f"{prior:,}"     if prior is not None     else "not yet published"
        print(f"  TSA: {row_date} | {travelers_str} travelers | prior year: {prior_str}")

    except Exception as e:
        print(f"  TSA ERROR: {e}")
        raise


# ---------------------------------------------------------------------------
# EIA scraper
# ---------------------------------------------------------------------------

def scrape_eia(conn):
    print("Scraping EIA...")
    if not EIA_API_KEY:
        print("  EIA_API_KEY not set — skipping")
        return
    try:
        url      = EIA_URL.format(key=EIA_API_KEY)
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        data     = response.json()
        rows     = data.get('response', {}).get('data', [])
        if not rows:
            raise ValueError("No EIA data returned")
        latest   = rows[0]
        row_date = latest.get('period')
        price    = float(latest.get('value'))
        upsert_eia(conn, row_date, price)
        print(f"  EIA: {row_date} | ${price:.3f}/gal national avg")
    except Exception as e:
        print(f"  EIA ERROR: {e}")
        raise


# ---------------------------------------------------------------------------
# FAA scraper
# ---------------------------------------------------------------------------

def scrape_faa(conn):
    print("Scraping FAA...")
    today = date.today().strftime('%Y-%m-%d')
    try:
        response = requests.get(FAA_URL, headers=FAA_HEADERS, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'xml')

        ground_delay_nodes = soup.find_all('Ground_Delay')
        ground_delays      = len(ground_delay_nodes)
        delay_airports     = []
        delay_causes       = []
        for node in ground_delay_nodes:
            ap     = node.find('ARPT')
            reason = node.find('Reason')
            if ap:     delay_airports.append(ap.text.strip())
            if reason: delay_causes.append(reason.text.strip())

        ground_stop_nodes = soup.find_all('Ground_Stop')
        ground_stops      = len(ground_stop_nodes)
        for node in ground_stop_nodes:
            ap = node.find('ARPT')
            if ap: delay_airports.append(ap.text.strip() + '(GS)')

        affected_airports = ', '.join(delay_airports) if delay_airports else ''
        causes_str        = '; '.join(set(delay_causes)) if delay_causes else ''

        upsert_faa(conn, today, ground_delays, ground_stops, affected_airports, causes_str)
        print(f"  FAA: {today} | {ground_delays} ground delays | {ground_stops} ground stops | airports: {affected_airports or 'none'}")

    except Exception as e:
        print(f"  FAA ERROR: {e}")
        raise


# ---------------------------------------------------------------------------
# NWS scraper
# ---------------------------------------------------------------------------

def scrape_nws(conn):
    print("Scraping NWS...")
    today = date.today().strftime('%Y-%m-%d')
    try:
        response = requests.get(
            NWS_URL, params=NWS_PARAMS,
            headers={'User-Agent': 'tsa-travel-dashboard/1.0'},
            timeout=15
        )
        response.raise_for_status()
        features = response.json().get('features', [])
        relevant = [f for f in features if f.get('properties', {}).get('event') in NWS_EVENT_TYPES]

        if not relevant:
            print(f"  NWS: {today} | No significant aviation weather alerts active")
            return

        for alert in relevant:
            props = alert.get('properties', {})
            insert_nws(conn, today,
                props.get('event', ''),
                props.get('areaDesc', '')[:255],
                props.get('severity', ''),
                props.get('onset', ''),
                props.get('expires', '')
            )
            print(f"  NWS: {props.get('event')} | {props.get('severity')} | {props.get('areaDesc','')[:60]}...")

    except Exception as e:
        print(f"  NWS ERROR: {e}")
        raise


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print(f"\n{'='*60}")
    print(f"Daily scrape starting: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")

    conn = get_conn()
    scrape_tsa(conn)
    print()
    scrape_eia(conn)
    print()
    scrape_faa(conn)
    print()
    scrape_nws(conn)
    conn.close()

    print(f"\n{'='*60}")
    print("Daily scrape complete.")
    print(f"{'='*60}\n")


if __name__ == '__main__':
    main()
