import requests
import time
import random
# Remove unnecessary print statements about curl_cffi

import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
import time
import random
import json
import duckdb

DB_PATH = r"C:\DuckDB\my_db.duckdb"
HOURS = [0, 6, 12, 18]
RATE_LIMIT_SECONDS = 5.0  # More conservative delay
MAX_COOLDOWN_SECONDS = 300  # Reduced from 30 min to 5 min
STATUS_CACHE_FLUSH_EVERY = 25  # write to DuckDB every N newly-fetched statuses
# Standard requests with no fingerprint spoofing

# Prevents duplicate LIST-endpoint sightings (same flight can appear once as a
# departure at its origin airport and once as an arrival at its destination
# airport -- both are legitimate, separate rows we want).
_seen_flights = set()

# In-memory mirror of the persistent STATUS_CACHE table (see get_duckdb_connection).
# Populated from disk at startup so a re-run after a block/crash skips flights
# already fetched today, and updated as new flights are fetched during the run.
_status_cache = {}
_status_cache_pending = []  # rows waiting to be flushed to DuckDB

# NOTE: when impersonating a specific browser's TLS/JA3 fingerprint via
# curl_cffi, the User-Agent header should match that same browser -- mixing a
# Chrome TLS handshake with a random Safari/Firefox User-Agent is itself a
# mismatch anti-bot systems can flag. curl_cffi's impersonate profile already
# sets a consistent, correct header set, so we no longer override User-Agent
# manually when curl_cffi is available.
# Single consistent user agent
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15"
]

# Persistent session so cookies (e.g. any anti-bot "verified" cookie) carry
# across requests instead of looking like a fresh, cookie-less client every time.
session = requests.Session()

_cooldown_until = 0
_last_request_time = 0
# Tracks blocks ACROSS different calls (not just retries of the same call), so
# repeated blocking actually escalates instead of resetting to the base wait
# every time a new flight's request happens to be the first attempt.
_consecutive_block_count = 0

def get_browser_headers():
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.flightstats.com/v2/flight-tracker",
    }
    if not _USING_CURL_CFFI:
        # Fallback path only: plain `requests` has no real browser TLS
        # fingerprint anyway, so a rotating User-Agent is the best we can do.
        headers["User-Agent"] = random.choice(web_user_agents)
    return headers

def get_duckdb_connection():
    conn = duckdb.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS IRREGULAR_FLIGHTS (
            FlightDate DATE,
            FlightNumber VARCHAR,
            AirlineCode VARCHAR,
            DepartureAirport VARCHAR,
            ArrivalAirport VARCHAR,
            ScheduledDeparture TIMESTAMP,
            ActualDeparture TIMESTAMP,
            ScheduledArrival TIMESTAMP,
            ActualArrival TIMESTAMP,
            StatusCode VARCHAR,
            Status VARCHAR,
            FinalStatus VARCHAR,
            DelayDepartureMin DOUBLE,
            DelayArrivalMin DOUBLE,
            IsCanceled BOOLEAN,
            IsDiverted BOOLEAN,
            IsDelayed BOOLEAN
        )
    """)
    # Resumability: every fetched flight status (even "no data") is recorded
    # here so a re-run for the same date skips flights already fetched.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS STATUS_CACHE (
            AirlineCode VARCHAR,
            FlightNumber VARCHAR,
            FlightDate DATE,
            RawJson VARCHAR,
            FetchedAt TIMESTAMP
        )
    """)
    return conn

def eu_airports(conn):
    result = conn.execute("SELECT IATA FROM Airports60").fetchall()
    return [row[0] for row in result]

def target_airlines(conn):
    # Get your specific airlines here (e.g., German airlines)
    result = conn.execute("""
        SELECT IataCode FROM airlines 
        WHERE country='Germany' AND IataCode IS NOT NULL
    """).fetchall()
    return set(row[0] for row in result) # Use a set for O(1) instant lookups

def load_status_cache(conn, flight_date):
    """Load any previously-fetched statuses for this date so we don't refetch them."""
    rows = conn.execute(
        "SELECT AirlineCode, FlightNumber, RawJson FROM STATUS_CACHE WHERE FlightDate = ?",
        [flight_date],
    ).fetchall()
    for airline, flight_num, raw in rows:
        key = (airline, str(flight_num), flight_date.strftime("%Y-%m-%d"))
        if raw is None or raw == "":
            _status_cache[key] = None
        else:
            try:
                _status_cache[key] = json.loads(raw)
            except Exception:
                _status_cache[key] = None
    if rows:
        print(f"Resuming: loaded {len(rows)} cached flight statuses already fetched for {flight_date}.\n")

def flush_status_cache_pending(conn):
    global _status_cache_pending
    if not _status_cache_pending:
        return
    conn.executemany(
        "INSERT INTO STATUS_CACHE (AirlineCode, FlightNumber, FlightDate, RawJson, FetchedAt) VALUES (?, ?, ?, ?, ?)",
        _status_cache_pending,
    )
    _status_cache_pending = []

# Remove explicit bot detection markers

def rate_limit():
    global _last_request_time
    elapsed = time.time() - _last_request_time
    _last_request_time = time.time()
def request_json_with_backoff(url, params=None):
    # Random delay between 3-8 seconds to mimic human behavior
    delay = random.uniform(3, 8)
    time.sleep(delay)
    
    # Rotate user agent for each request
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://www.google.com/",
        "Connection": "keep-alive"
    }
    
    try:
        response = session.get(url, params=params, headers=headers, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Request failed: {e}")
        return None
    """
    url = f"https://www.flightstats.com/v2/api-next/flight-tracker/{direction}/{airport}/{flight_date.year}/{flight_date.month}/{flight_date.day}/{hour}"
    data = request_json_with_backoff(url, {"numHours": 6})
    return data.get("flights", []) if data else []

def fetch_status(carrier_fs, flight_number, flight_date):
    """
    PHASE 2 -- per-flight lookup. Returns "schedule" (scheduled + actual
    times), "status" (statusCode/diverted), and "flightNote" (canceled).
    """
    url = (f"https://www.flightstats.com/v2/api-next/flight-tracker/"
           f"{carrier_fs}/{flight_number}/{flight_date.year}/{flight_date.month}/{flight_date.day}")
    return request_json_with_backoff(url)

def get_flight_status_cached(conn, carrier_fs, flight_number, flight_date):
    key = (carrier_fs, str(flight_number), flight_date.strftime("%Y-%m-%d"))
    if key in _status_cache:
        return _status_cache[key]

    status_data = fetch_status(carrier_fs, flight_number, flight_date)
    _status_cache[key] = status_data  # cache None too, so we don't retry a flight with genuinely no status

    _status_cache_pending.append((
        carrier_fs,
        str(flight_number),
        flight_date,
        json.dumps(status_data) if status_data is not None else None,
        datetime.now(),
    ))
    if len(_status_cache_pending) >= STATUS_CACHE_FLUSH_EVERY:
        flush_status_cache_pending(conn)

    return status_data

def extract_record(conn, list_flight, flight_date, direction, airport):
    """
    Builds the base identity of the flight from the cheap list-endpoint entry,
    then fetches (or reuses a cached) full status lookup for the actual
    schedule/status/flightNote data needed to classify it as irregular.
    """
    carrier = list_flight.get("carrier", {})
    fs = carrier.get("fs")
    flight_num = carrier.get("flightNumber")

    if list_flight.get("isCodeshare") or not fs or not flight_num:
        return None

    global_key = (fs, str(flight_num), flight_date.strftime("%Y-%m-%d"), direction, airport)
    if global_key in _seen_flights:
        return None
    _seen_flights.add(global_key)

    status_data = get_flight_status_cached(conn, fs, flight_num, flight_date)
    if not status_data:
        return None

    schedule = status_data.get("schedule", {})
    status = status_data.get("status", {})
    flight_note = status_data.get("flightNote", {})

    dep_airport = status_data.get("departureAirport", {}).get("fs")
    arr_airport = status_data.get("arrivalAirport", {}).get("fs")

    # Override with the queried airport to ensure accuracy
    if direction == "dep":
        dep_airport = airport
    else:
        arr_airport = airport

    # Handle actual times (only if confirmed "Actual")
    dep_actual = schedule.get("estimatedActualDeparture") if schedule.get("estimatedActualDepartureTitle") == "Actual" else None
    arr_actual = schedule.get("estimatedActualArrival") if schedule.get("estimatedActualArrivalTitle") == "Actual" else None

    # -----------------------------------------------------------------
    # CALCULATE DELAYS: compare Scheduled vs Actual/Estimated timestamps.
    # -----------------------------------------------------------------
    delay_dep = None
    delay_arr = None

    sched_dep_str = schedule.get("scheduledDeparture")
    est_act_dep_str = schedule.get("estimatedActualDeparture")
    if sched_dep_str and est_act_dep_str and schedule.get("estimatedActualDepartureTitle") in ["Actual", "Estimated"]:
        try:
            dt_s = pd.to_datetime(sched_dep_str)
            dt_a = pd.to_datetime(est_act_dep_str)
            delay_dep = (dt_a - dt_s).total_seconds() / 60.0
        except Exception:
            pass

    sched_arr_str = schedule.get("scheduledArrival")
    est_act_arr_str = schedule.get("estimatedActualArrival")
    if sched_arr_str and est_act_arr_str and schedule.get("estimatedActualArrivalTitle") in ["Actual", "Estimated"]:
        try:
            dt_s = pd.to_datetime(sched_arr_str)
            dt_a = pd.to_datetime(est_act_arr_str)
            delay_arr = (dt_a - dt_s).total_seconds() / 60.0
        except Exception:
            pass

    # -----------------------------------------------------------------
    # STATUS FLAGS: FlightStats/Cirium documented statusCodes are:
    #   S=Scheduled, A=Active, U=Unknown, R=Redirected (in-flight, diverting),
    #   L=Landed (at the SCHEDULED airport -- i.e. arrived normally, NOT "Late"),
    #   D=Diverted (landed at an unscheduled airport), C=Cancelled, NO=Not Operational.
    # Source: https://helpdesk.flightglobal.com/hc/en-us/articles/217613238
    # -----------------------------------------------------------------
    status_code = status.get("statusCode")
    is_canceled = bool(flight_note.get("canceled")) or status_code == "C"
    is_diverted = bool(status.get("diverted")) or status_code == "D"

    is_delayed = bool((delay_dep or 0) > 10 or (delay_arr or 0) > 10)

    return {
        "FlightDate": flight_date,
        "FlightNumber": f"{fs}{flight_num}",
        "AirlineCode": fs,
        "DepartureAirport": dep_airport,
        "ArrivalAirport": arr_airport,
        "ScheduledDeparture": sched_dep_str,
        "ActualDeparture": dep_actual,
        "ScheduledArrival": sched_arr_str,
        "ActualArrival": arr_actual,
        "StatusCode": status_code,
        "Status": status.get("status"),
        "FinalStatus": status.get("finalStatus"),
        "DelayDepartureMin": delay_dep,
        "DelayArrivalMin": delay_arr,
        "IsCanceled": is_canceled,
        "IsDiverted": is_diverted,
        "IsDelayed": is_delayed,
    }

def save_to_duckdb(conn, records):
    if not records:
        return 0, 0

    df = pd.DataFrame(records)

    for c in ["ScheduledDeparture", "ActualDeparture", "ScheduledArrival", "ActualArrival"]:
        df[c] = pd.to_datetime(df[c], errors="coerce")
    df["FlightDate"] = pd.to_datetime(df["FlightDate"]).dt.date

    # Only save irregular flights
    irregular_df = df[df["IsCanceled"] | df["IsDiverted"] | df["IsDelayed"]]

    if irregular_df.empty:
        return len(df), 0

    try:
        conn.execute("INSERT INTO IRREGULAR_FLIGHTS SELECT * FROM irregular_df")
    except Exception as e:
        print(f"DB Error: {e}")

    return len(df), len(irregular_df)

def _format_duration(seconds):
    seconds = int(round(seconds))
    hours, remainder = divmod(seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    return f"{hours}h {minutes}m {secs}s"

def main():
    run_start = time.perf_counter()

    conn = get_duckdb_connection()
    airports = eu_airports(conn)
    # Scoped back down to specific airline(s) -- Phase 2 costs one status API
    # call per unique matching flight, so this keeps total request volume sane.
    airlines_set = {'LH'}  # e.g., {'LH', 'EW', '4U', 'DE'}

    # STRICT YESTERDAY DATE
    target_date = datetime.now().date() - timedelta(days=1)

    print(f"Target Date: {target_date}")
    print(f"Airports: {len(airports)} | Target Airlines: {len(airlines_set)} {airlines_set}")
    print(f"Phase 1 (list) API calls: {len(airports) * len(HOURS) * 2}")
    print("Phase 2 (status) API calls: one per unique matching flight (varies by day).")
    print(f"TLS impersonation: {'curl_cffi (' + IMPERSONATE_PROFILE + ')' if _USING_CURL_CFFI else 'none -- plain requests'}")
    print("Progress is saved continuously to STATUS_CACHE, so if this run gets blocked or")
    print("interrupted, just re-run the script -- already-fetched flights for today's")
    print("target date will be skipped automatically.\n")

    _seen_flights.clear()
    load_status_cache(conn, target_date)

    total_flights, total_irregular = 0, 0
    batch_records = []

    try:
        for airport in airports:
            airport_start = time.perf_counter()
            print(f"Processing {airport}...", end=" ", flush=True)
            airport_flight_count = 0

            for hour in HOURS:
                for direction in ["dep", "arr"]:
                    flights = fetch_flights(airport, target_date, hour, direction)

                    for f in flights:
                        # Skip non-target airlines BEFORE the expensive status call
                        carrier_fs = f.get("carrier", {}).get("fs")
                        if carrier_fs not in airlines_set:
                            continue

                        rec = extract_record(conn, f, target_date, direction, airport)
                        if rec:
                            batch_records.append(rec)
                            airport_flight_count += 1

            airport_elapsed = time.perf_counter() - airport_start
            print(f"found {airport_flight_count} target airline flights ({_format_duration(airport_elapsed)}).")

            # Save every batch to keep memory low and persist progress
            if len(batch_records) >= 50:
                n, ni = save_to_duckdb(conn, batch_records)
                total_flights += n
                total_irregular += ni
                batch_records = []

            flush_status_cache_pending(conn)

    finally:
        # Always flush whatever we have, even on Ctrl+C or an unexpected error,
        # so a long unattended run never loses progress.
        if batch_records:
            n, ni = save_to_duckdb(conn, batch_records)
            total_flights += n
            total_irregular += ni
        flush_status_cache_pending(conn)
        conn.close()

    elapsed = time.perf_counter() - run_start
    print(f"\n=== DONE ===")
    print(f"Total matching flights: {total_flights}")
    print(f"Irregular flights saved: {total_irregular}")
    print(f"Time taken: {_format_duration(elapsed)}")

if __name__ == "__main__":
    main()