import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
import time
import random
import duckdb

DB_PATH = r"C:\DuckDB\my_db.duckdb"
HOURS = [0, 6, 12, 18]

# Prevents duplicate LIST-endpoint sightings (same flight can appear once as a
# departure at its origin airport and once as an arrival at its destination
# airport -- both are legitimate, separate rows we want).
_seen_flights = set()

# Caches per-flight STATUS lookups so a physical flight that shows up in both
# a departure list and an arrival list only costs one status API call, not two.
_status_cache = {}

web_user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:129.0) Gecko/20100101 Firefox/129.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36 OPR/112.0.0.0",
]

_cooldown_until = 0
_last_request_time = 0

def get_browser_headers():
    return {
        "User-Agent": random.choice(web_user_agents),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.flightstats.com/v2/flight-tracker",
    }

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

BOT_DETECTION_MARKERS = ["captcha", "access denied", "blocked", "cloudflare", "challenge", "awswaf"]

def rate_limit():
    global _last_request_time
    elapsed = time.time() - _last_request_time
    if elapsed < 1.5:  # Strict 1.5s delay to avoid timeouts/bans
        time.sleep(1.5 - elapsed)
    _last_request_time = time.time()

def request_json_with_backoff(url, params=None, max_retries=3):
    global _cooldown_until
    
    for attempt in range(max_retries):
        # Respect global cooldown
        while time.time() < _cooldown_until:
            time.sleep(5)
            
        rate_limit()
        
        try:
            r = requests.get(url, params=params, headers=get_browser_headers(), timeout=30)
        except requests.exceptions.Timeout:
            print(f"  Timeout (attempt {attempt+1})")
            time.sleep(10)
            continue
        except requests.exceptions.ConnectionError:
            print("  Connection reset by FlightStats. Cooling down 60s...")
            _cooldown_until = time.time() + 60
            continue

        if r.status_code in (403, 429):
            wait = 60 * (2 ** attempt)
            print(f"  {r.status_code} Blocked. Cooling down {wait}s...")
            _cooldown_until = time.time() + wait
            continue

        if r.status_code in (500, 502, 503, 504):
            if any(x in r.text.lower() for x in BOT_DETECTION_MARKERS):
                _cooldown_until = time.time() + 120
                continue
            return None # Genuine "no data" response

        if r.status_code != 200:
            return None
            
        if any(x in r.text.lower() for x in BOT_DETECTION_MARKERS):
            _cooldown_until = time.time() + 120
            continue

        try:
            return r.json().get("data")
        except ValueError:
            return None
            
    return None

def fetch_flights(airport, flight_date, hour, direction):
    """
    PHASE 1 -- cheap discovery call. NOTE: this list endpoint only returns
    carrier/flightNumber/scheduled-clock-time/airport fields -- it does NOT
    include "schedule"/"status"/"flightNote" (confirmed via live response:
    e.g. {"carrier": {...}, "departureTime": {"time24": "21:20"}, "url": "/flight-tracker/MU/552?...", ...}).
    So it cannot tell you delay/cancel/divert status on its own -- use it only
    to discover which flights exist, then call fetch_status() per flight.
    """
    url = f"https://www.flightstats.com/v2/api-next/flight-tracker/{direction}/{airport}/{flight_date.year}/{flight_date.month}/{flight_date.day}/{hour}"
    data = request_json_with_backoff(url, {"numHours": 6})
    return data.get("flights", []) if data else []

def fetch_status(carrier_fs, flight_number, flight_date):
    """
    PHASE 2 -- per-flight lookup. This is the endpoint that actually returns
    "schedule" (scheduled + actual times), "status" (statusCode/diverted), and
    "flightNote" (canceled) -- verified against a real flightstats.com URL
    (v2/flight-tracker/{carrier}/{number}?year=..&month=..&date=..).
    """
    url = (f"https://www.flightstats.com/v2/api-next/flight-tracker/"
           f"{carrier_fs}/{flight_number}/{flight_date.year}/{flight_date.month}/{flight_date.day}")
    return request_json_with_backoff(url)

def get_flight_status_cached(carrier_fs, flight_number, flight_date):
    key = (carrier_fs, str(flight_number), flight_date.strftime("%Y-%m-%d"))
    if key in _status_cache:
        return _status_cache[key]
    status_data = fetch_status(carrier_fs, flight_number, flight_date)
    _status_cache[key] = status_data  # cache None too, so we don't retry a flight that genuinely has no status
    return status_data

def extract_record(list_flight, flight_date, direction, airport):
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

    status_data = get_flight_status_cached(fs, flight_num, flight_date)
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

def main():
    run_start = time.perf_counter()
    
    conn = get_duckdb_connection()
    airports = eu_airports(conn)
    # IMPORTANT: this filter is now load-bearing, not optional. Phase 2 costs
    # one extra API call per unique flight that survives it, so leaving it
    # disabled means a status call for every single flight at every airport
    # (36,000+ on a typical day) -- that's what caused the earlier 40-minute,
    # heavily-blocked run. Keep this scoped to the airlines you actually need.
    #airlines_set = {'LH'}  # e.g., {'LH', 'EW', '4U', 'DE'}

    # STRICT YESTERDAY DATE
    target_date = datetime.now().date() - timedelta(days=1)
    
    print(f"Target Date: {target_date}")
    #print(f"Airports: {len(airports)} | Target Airlines: {len(airlines_set)} {airlines_set}")
    print(f"Airports: {len(airports)}")
    print(f"Phase 1 (list) API calls: {len(airports) * len(HOURS) * 2}")
    print(f"Phase 2 (status) API calls: one per unique matching flight (varies by day)\n")
    
    _seen_flights.clear()
    _status_cache.clear()
    total_flights, total_irregular = 0, 0
    batch_records = []
    
    for airport in airports:
        print(f"Processing {airport}...", end=" ")
        airport_flight_count = 0
        
        for hour in HOURS:
            for direction in ["dep", "arr"]:
                flights = fetch_flights(airport, target_date, hour, direction)
                
                for f in flights:
                    # Skip non-target airlines BEFORE the expensive status call
                    #carrier_fs = f.get("carrier", {}).get("fs")
                    #if carrier_fs not in airlines_set:
                    #    continue

                    rec = extract_record(f, target_date, direction, airport)
                    if rec:
                        batch_records.append(rec)
                        airport_flight_count += 1
        
        print(f"found {airport_flight_count} target airline flights.")
        
        # Save every 5 airports to keep memory low
        if len(batch_records) >= 50:
            n, ni = save_to_duckdb(conn, batch_records)
            total_flights += n
            total_irregular += ni
            batch_records = []
            
    # Save any remaining
    if batch_records:
        n, ni = save_to_duckdb(conn, batch_records)
        total_flights += n
        total_irregular += ni
        
    conn.close()
    
    elapsed = time.perf_counter() - run_start
    print(f"\n=== DONE ===")
    print(f"Total matching flights: {total_flights}")
    print(f"Irregular flights saved: {total_irregular}")
    print(f"Time taken: {elapsed/60:.1f} minutes")

if __name__ == "__main__":
    main()