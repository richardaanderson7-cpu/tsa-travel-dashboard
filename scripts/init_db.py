"""
init_db.py
Run once to initialize the SQLite database and create all four tables.
Usage: python scripts/init_db.py
"""

import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'travel_data.db')


def init_db():
    # Make sure the data directory exists
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # -------------------------------------------------------------------------
    # TSA daily passenger throughput
    # scraped from tsa.gov/travel/passenger-volumes
    # one row per day
    # -------------------------------------------------------------------------
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tsa_daily (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            date                TEXT UNIQUE NOT NULL,   -- YYYY-MM-DD
            travelers           INTEGER,                -- current year throughput
            prior_year_travelers INTEGER,               -- same weekday prior year
            created_at          TEXT DEFAULT (datetime('now'))
        )
    ''')

    # -------------------------------------------------------------------------
    # EIA weekly national average regular unleaded gas price
    # pulled from EIA APIv2
    # one row per week (only writes when value changes)
    # -------------------------------------------------------------------------
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS eia_gas_prices (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            date                TEXT UNIQUE NOT NULL,   -- YYYY-MM-DD (week of)
            national_avg_price  REAL,                   -- dollars per gallon
            created_at          TEXT DEFAULT (datetime('now'))
        )
    ''')

    # -------------------------------------------------------------------------
    # FAA daily ground delay / ground stop snapshot
    # pulled from nasstatus.faa.gov/api/airport-status-information
    # daily snapshot at scrape time — live feed so we capture a daily photo
    # -------------------------------------------------------------------------
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS faa_delays (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            date                TEXT UNIQUE NOT NULL,   -- YYYY-MM-DD
            ground_delays       INTEGER,                -- count of active ground delays
            ground_stops        INTEGER,                -- count of active ground stops
            affected_airports   TEXT,                   -- comma-separated airport codes
            delay_causes        TEXT,                   -- comma-separated cause descriptions
            created_at          TEXT DEFAULT (datetime('now'))
        )
    ''')

    # -------------------------------------------------------------------------
    # NWS significant weather alerts (aviation-relevant)
    # pulled from api.weather.gov/alerts/active
    # filtered to: winter storms, blizzards, dense fog, severe thunderstorms,
    #              high wind warnings
    # sparse table — only rows when something significant is active
    # -------------------------------------------------------------------------
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS nws_alerts (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            date                TEXT NOT NULL,          -- YYYY-MM-DD (date captured)
            event_type          TEXT,                   -- e.g. "Winter Storm Warning"
            region              TEXT,                   -- affected area description
            severity            TEXT,                   -- Extreme / Severe / Moderate
            onset               TEXT,                   -- ISO datetime alert starts
            expires             TEXT,                   -- ISO datetime alert ends
            created_at          TEXT DEFAULT (datetime('now'))
        )
    ''')

    # -------------------------------------------------------------------------
    # Indexes for fast date-range queries used by the weekly recap
    # -------------------------------------------------------------------------
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_tsa_date ON tsa_daily(date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_eia_date ON eia_gas_prices(date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_faa_date ON faa_delays(date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_nws_date ON nws_alerts(date)')

    conn.commit()
    conn.close()

    print(f"Database initialized at: {os.path.abspath(DB_PATH)}")
    print("Tables created:")
    print("  - tsa_daily")
    print("  - eia_gas_prices")
    print("  - faa_delays")
    print("  - nws_alerts")


if __name__ == '__main__':
    init_db()
