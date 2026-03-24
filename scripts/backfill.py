"""
backfill.py
One-time script to load historical data into the SQLite database.
Safe to run multiple times — uses INSERT OR IGNORE so no duplicates.

Sources:
  - TSA:  tsa.gov/travel/passenger-volumes/YYYY (2019 through last year)
  - EIA:  EIA APIv2 — 5 years of weekly national avg gas prices

Usage:
  python scripts/backfill.py

Required environment variables:
  EIA_API_KEY
"""

import os
import sqlite3
import requests
import pandas as pd
from io import StringIO
from datetime import datetime
import time

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DB_PATH     = os.path.join(os.path.dirname(__file__), '..', 'data', 'travel_data.db')
EIA_API_KEY = os.environ.get('EIA_API_KEY', '')

TSA_HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/120.0.0.0 Safari/537.36'
    )
}

CURRENT_YEAR = datetime.now().year

# Years to backfill — 2019 is as far back as TSA publishes
TSA_YEARS = list(range(2019, CURRENT_YEAR))  # excludes current year (daily scraper handles that)


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


def upsert_eia(conn, row_date, price):
    conn.execute('''
        INSERT OR IGNORE INTO eia_gas_prices (date, national_avg_price)
        VALUES (?, ?)
    ''', (row_date, price))


# ---------------------------------------------------------------------------
# TSA historical backfill
# ---------------------------------------------------------------------------

def backfill_tsa(conn):
    """
    Scrapes TSA passenger volume pages for each year from 2019 to last year.
    Each year page has columns: Date | {year} | {prior year}
    Polite delay between requests to avoid hammering the TSA server.
    """
    print("\nBackfilling TSA historical data...")
    print(f"Years to process: {TSA_YEARS}\n")

    total_rows = 0

    for year in TSA_YEARS:
        if year == CURRENT_YEAR:
            url = 'https://www.tsa.gov/travel/passenger-volumes'
        else:
            url = f'https://www.tsa.gov/travel/passenger-volumes/{year}'

        print(f"  Fetching {year}... ", end='', flush=True)

        try:
            response = requests.get(url, headers=TSA_HEADERS, timeout=15)
            response.raise_for_status()

            tables = pd.read_html(StringIO(response.text))
            if not tables:
                print(f"No table found — skipping")
                continue

            df = tables[0]
            df.columns = [str(c).strip() for c in df.columns]

            date_col    = df.columns[0]
            current_col = df.columns[1]
            prior_col   = df.columns[2] if len(df.columns) > 2 else None

            # Clean dates
            df = df.dropna(subset=[date_col])
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            df = df.dropna(subset=[date_col])

            # Clean numeric columns
            df[current_col] = pd.to_numeric(
                df[current_col].astype(str).str.replace(',', '', regex=False).str.strip(),
                errors='coerce'
            )
            if prior_col:
                df[prior_col] = pd.to_numeric(
                    df[prior_col].astype(str).str.replace(',', '', regex=False).str.strip(),
                    errors='coerce'
                )

            # Write each row
            year_rows = 0
            for _, row in df.iterrows():
                if pd.isna(row[date_col]):
                    continue
                row_date  = row[date_col].strftime('%Y-%m-%d')
                travelers = int(row[current_col]) if pd.notna(row[current_col]) else None
                prior     = int(row[prior_col])   if prior_col and pd.notna(row[prior_col]) else None
                upsert_tsa(conn, row_date, travelers, prior)
                year_rows += 1

            conn.commit()
            total_rows += year_rows
            print(f"{year_rows} rows loaded")

            # Polite delay between requests
            time.sleep(2)

        except Exception as e:
            print(f"ERROR: {e}")
            continue

    print(f"\n  TSA backfill complete — {total_rows} total rows loaded")


# ---------------------------------------------------------------------------
# EIA historical backfill
# ---------------------------------------------------------------------------

def backfill_eia(conn):
    """
    Pulls 5 years of weekly national average regular unleaded gas prices
    from EIA APIv2 in a single request.
    Requires EIA_API_KEY environment variable.
    """
    print("\nBackfilling EIA historical gas prices...")

    if not EIA_API_KEY:
        print("  EIA_API_KEY not set — skipping")
        print("  Set it with: export EIA_API_KEY=your_key_here")
        return

    try:
        # EIA APIv2 — pull last 5 years of weekly data (260 weeks)
        url = (
            'https://api.eia.gov/v2/petroleum/pri/gnd/data/'
            f'?api_key={EIA_API_KEY}'
            '&frequency=weekly'
            '&data[0]=value'
            '&facets[product][]=EPM0'
            '&facets[duoarea][]=NUS'
            '&facets[series][]=EMM_EPM0_PTE_NUS_DPG'
            '&sort[0][column]=period'
            '&sort[0][direction]=desc'
            '&length=260'           # 5 years of weekly data
        )

        print("  Fetching 5 years of weekly gas prices from EIA API... ", end='', flush=True)
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()

        rows = data.get('response', {}).get('data', [])
        if not rows:
            raise ValueError("No EIA data returned")

        count = 0
        for row in rows:
            row_date = row.get('period')    # YYYY-MM-DD
            value    = row.get('value')
            if row_date and value is not None:
                upsert_eia(conn, row_date, float(value))
                count += 1

        conn.commit()
        print(f"{count} weeks loaded")

        # Show date range loaded
        earliest = min(r.get('period') for r in rows if r.get('period'))
        latest   = max(r.get('period') for r in rows if r.get('period'))
        print(f"  Date range: {earliest} to {latest}")
        print(f"  EIA backfill complete — {count} rows loaded")

    except Exception as e:
        print(f"  EIA ERROR: {e}")
        raise


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------

def verify(conn):
    """Print row counts and date ranges for all tables after backfill."""
    print("\n" + "="*60)
    print("Verification — row counts and date ranges:")
    print("="*60)

    tables = ['tsa_daily', 'eia_gas_prices', 'faa_delays', 'nws_alerts']
    for table in tables:
        count = conn.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]
        if count > 0:
            min_date = conn.execute(f'SELECT MIN(date) FROM {table}').fetchone()[0]
            max_date = conn.execute(f'SELECT MAX(date) FROM {table}').fetchone()[0]
            print(f"  {table:<20} {count:>5} rows | {min_date} to {max_date}")
        else:
            print(f"  {table:<20}     0 rows | (empty — starts from daily scrape)")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("="*60)
    print("TSA Travel Dashboard — Historical Backfill")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)

    if not os.path.exists(DB_PATH):
        print(f"\nERROR: Database not found at {DB_PATH}")
        print("Run python scripts/init_db.py first")
        return

    conn = get_conn()

    backfill_tsa(conn)
    backfill_eia(conn)
    verify(conn)

    conn.close()

    print(f"\nBackfill complete: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)


if __name__ == '__main__':
    main()
