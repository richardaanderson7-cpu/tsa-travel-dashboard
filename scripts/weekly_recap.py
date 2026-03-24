"""
weekly_recap.py
Runs every Monday at 11am ET via GitHub Actions.
Queries SQLite for the past 7 days across all four tables,
calls Claude API with web search enabled to write a narrative recap,
and publishes to Buttondown for newsletter delivery.

Required environment variables:
  ANTHROPIC_API_KEY
  BUTTONDOWN_API_KEY
"""

import os
import sqlite3
import json
import requests
from datetime import datetime, date, timedelta
import anthropic

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DB_PATH             = os.path.join(os.path.dirname(__file__), '..', 'data', 'travel_data.db')
ANTHROPIC_API_KEY   = os.environ.get('ANTHROPIC_API_KEY', '')
BUTTONDOWN_API_KEY  = os.environ.get('BUTTONDOWN_API_KEY', '')
BUTTONDOWN_URL      = 'https://api.buttondown.email/v1/emails'


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

def get_week_range():
    """Returns (start_date, end_date) strings for the past 7 days."""
    end   = date.today() - timedelta(days=1)        # yesterday
    start = end - timedelta(days=6)                 # 7 days back
    return start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d')


def get_prior_year_range(start_str, end_str):
    """Returns the same 7-day window one year ago."""
    start = datetime.strptime(start_str, '%Y-%m-%d').date() - timedelta(weeks=52)
    end   = datetime.strptime(end_str,   '%Y-%m-%d').date() - timedelta(weeks=52)
    return start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d')


# ---------------------------------------------------------------------------
# Data queries
# ---------------------------------------------------------------------------

def query_week(conn, start, end):
    """Pull all four data sources for a given date range."""

    tsa = conn.execute('''
        SELECT date, travelers, prior_year_travelers
        FROM tsa_daily
        WHERE date BETWEEN ? AND ?
        ORDER BY date ASC
    ''', (start, end)).fetchall()

    eia = conn.execute('''
        SELECT date, national_avg_price
        FROM eia_gas_prices
        WHERE date BETWEEN ? AND ?
        ORDER BY date DESC
        LIMIT 1
    ''', (start, end)).fetchall()

    faa = conn.execute('''
        SELECT date, ground_delays, ground_stops, affected_airports
        FROM faa_delays
        WHERE date BETWEEN ? AND ?
        ORDER BY date ASC
    ''', (start, end)).fetchall()

    nws = conn.execute('''
        SELECT date, event_type, region, severity
        FROM nws_alerts
        WHERE date BETWEEN ? AND ?
        ORDER BY date ASC
    ''', (start, end)).fetchall()

    return {
        'tsa': tsa,
        'eia': eia,
        'faa': faa,
        'nws': nws
    }


def format_data_for_prompt(current, prior, start, end, prior_start, prior_end):
    """Format all queried data into a clear context block for Claude."""

    lines = []
    lines.append(f"CURRENT WEEK: {start} to {end}")
    lines.append("")

    # TSA
    lines.append("TSA PASSENGER THROUGHPUT:")
    if current['tsa']:
        total = sum(r[1] for r in current['tsa'] if r[1])
        lines.append(f"  Total travelers this week: {total:,}")
        for row in current['tsa']:
            travelers = f"{row[1]:,}" if row[1] else "N/A"
            prior_yr  = f"{row[2]:,}" if row[2] else "N/A"
            yoy = ""
            if row[1] and row[2]:
                pct = ((row[1] - row[2]) / row[2]) * 100
                yoy = f" ({pct:+.1f}% vs prior year)"
            lines.append(f"  {row[0]}: {travelers} travelers | prior year: {prior_yr}{yoy}")
    else:
        lines.append("  No TSA data available for this week")

    lines.append("")

    # EIA
    lines.append("GAS PRICES (EIA National Average Regular Unleaded):")
    if current['eia']:
        price = current['eia'][0][1]
        lines.append(f"  Current week: ${price:.3f}/gal")
        if prior['eia']:
            prior_price = prior['eia'][0][1]
            diff = price - prior_price
            lines.append(f"  Prior year same period: ${prior_price:.3f}/gal ({diff:+.3f} YoY)")
    else:
        lines.append("  No EIA data available for this week")

    lines.append("")

    # FAA
    lines.append("FAA SYSTEM DELAYS:")
    if current['faa']:
        total_gd = sum(r[1] for r in current['faa'] if r[1])
        total_gs = sum(r[2] for r in current['faa'] if r[2])
        lines.append(f"  Total ground delays across week: {total_gd}")
        lines.append(f"  Total ground stops across week: {total_gs}")
        for row in current['faa']:
            lines.append(
                f"  {row[0]}: {row[1]} ground delays | {row[2]} ground stops"
                + (f" | airports: {row[3]}" if row[3] else "")
            )
    else:
        lines.append("  No FAA delay data available for this week")

    lines.append("")

    # NWS
    lines.append("SIGNIFICANT WEATHER ALERTS (aviation-relevant):")
    if current['nws']:
        for row in current['nws']:
            lines.append(f"  {row[0]}: {row[1]} ({row[3]}) — {row[2][:80]}")
    else:
        lines.append("  No significant aviation weather alerts this week")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Claude API call
# ---------------------------------------------------------------------------

def generate_recap(data_context, start, end):
    """
    Calls Claude with the week's data and web search enabled.
    Claude uses the data as analytical context while researching
    the week's major news events affecting travel.
    Returns the generated newsletter content as a string.
    """
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    system_prompt = """You are the writer of a weekly newsletter called "Travel Pulse" 
that tracks US air travel conditions. Your readers are travel industry professionals, 
frequent flyers, and data enthusiasts.

Each week you receive structured data on TSA passenger volumes, gas prices, FAA system 
delays, and significant weather alerts. You also use web search to find major news events 
from the week that would have affected US air travel — things like major sporting events, 
political disruptions, government actions, severe weather events, airline news, or 
anything else materially relevant to travel demand or operations.

Write a weekly recap newsletter that:
- Opens with a compelling 1-2 sentence lede that captures the week's travel story
- Synthesizes the data with the news context — don't just report numbers, explain what 
  they mean together
- Highlights the most interesting or unexpected finding
- Notes any year-over-year trends worth calling out
- Closes with a brief forward look if anything notable is on the horizon
- Is written in a confident, clear, analytical voice — informed but not dry
- Is approximately 350-450 words
- Does NOT use bullet points — this is narrative prose
- Does NOT include a subject line — just the body content

The data you receive reflects actual measured values. Use web search to find the news 
context that explains the patterns in that data."""

    user_prompt = f"""Here is this week's travel data ({start} to {end}):

{data_context}

Please search the web for major news events and context from this week that would affect 
US air travel demand or operations, then write this week's Travel Pulse newsletter recap."""

    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1000,
        system=system_prompt,
        tools=[{"type": "web_search_20250305", "name": "web_search"}],
        messages=[{"role": "user", "content": user_prompt}]
    )

    # Extract the text content from the response
    # Response may contain web_search tool use blocks — we want the final text
    recap_text = ""
    for block in response.content:
        if block.type == "text":
            recap_text += block.text

    return recap_text.strip()


# ---------------------------------------------------------------------------
# Buttondown publish
# ---------------------------------------------------------------------------

def publish_to_buttondown(subject, body):
    """
    POSTs the recap to Buttondown as a new email.
    Status 'draft' means it queues for review before sending.
    Change to 'scheduled' with a send_at time to fully automate.
    """
    headers = {
        'Authorization': f'Token {BUTTONDOWN_API_KEY}',
        'Content-Type': 'application/json'
    }
    payload = {
        'subject': subject,
        'body': body,
        'status': 'draft'      # change to 'scheduled' once confirmed working
    }

    response = requests.post(BUTTONDOWN_URL, headers=headers, json=payload)
    response.raise_for_status()
    data = response.json()
    print(f"  Buttondown: draft created — ID {data.get('id')}")
    return data


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print(f"\n{'='*60}")
    print(f"Weekly recap starting: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")

    start, end             = get_week_range()
    prior_start, prior_end = get_prior_year_range(start, end)

    print(f"Current week:    {start} to {end}")
    print(f"Prior year week: {prior_start} to {prior_end}\n")

    conn = get_conn()
    current = query_week(conn, start, end)
    prior   = query_week(conn, prior_start, prior_end)
    conn.close()

    data_context = format_data_for_prompt(
        current, prior, start, end, prior_start, prior_end
    )
    print("Data context prepared:\n")
    print(data_context)
    print()

    print("Calling Claude API...")
    recap = generate_recap(data_context, start, end)
    print("\nGenerated recap:\n")
    print(recap)
    print()

    # Build subject line with the week's date range
    subject = (
        f"Travel Pulse: Week of "
        f"{datetime.strptime(start, '%Y-%m-%d').strftime('%B %d, %Y')}"
    )

    print(f"Publishing to Buttondown: '{subject}'")
    publish_to_buttondown(subject, recap)

    print(f"\n{'='*60}")
    print("Weekly recap complete.")
    print(f"{'='*60}\n")


def get_conn():
    return sqlite3.connect(DB_PATH)


if __name__ == '__main__':
    main()
