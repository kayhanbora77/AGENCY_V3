import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time, random
import duckdb
import threading

HEADERS = {"User-Agent": "Mozilla/5.0"}
HOURS = [0, 6, 12, 18]
DB_PATH = r"C:\DuckDB\my_db.duckdb"
# Global dedup cache: (carrier_fs, flight_number, yyyy-mm-dd)
_seen_flights = set()


# ========== ANTI-BOT: Rotating User Agents ==========
web_user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:129.0) Gecko/20100101 Firefox/129.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/127.0.6533.107 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.6; rv:129.0) Gecko/20100101 Firefox/129.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36 OPR/112.0.0.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_6_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.6533.103 Mobile Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36 Edg/127.0.2651.98",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36 OPR/112.0.0.0",
]


def get_browser_headers():
    return {
        "User-Agent": random.choice(web_user_agents),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0",
    }


def get_duckdb_connection():
    """Create and return a DuckDB connection, ensuring the target table exists."""
    conn = duckdb.connect(DB_PATH)
    
    # Create the IRREGULAR_FLIGHTS table if it doesn't exist
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


def eu_airlines(conn):
    result = conn.execute("select IataCode from airlines where country='Germany' and IataCode is not null").fetchall()
    return [row[0] for row in result]


session = requests.Session()

_lock = threading.Lock()
_cooldown_until = 0

def wait_for_global_cooldown():
    while True:
        with _lock:
            remaining = _cooldown_until - time.time()
        if remaining <= 0:
            return
        time.sleep(min(remaining, 5))

def trigger_global_cooldown(seconds):
    with _lock:
        global _cooldown_until
        _cooldown_until = max(_cooldown_until, time.time() + seconds)


# Text/markers that indicate a bot-detection / challenge page
BOT_DETECTION_MARKERS = ["captcha", "access denied", "blocked", "cloudflare", "challenge", "awswaf"]


def request_json_with_backoff(url, params=None, max_retries=4):
    """
    Hits FlightStats' actual JSON data API with backoff and bot detection handling.
    """
    for attempt in range(max_retries):
        wait_for_global_cooldown()
        headers = get_browser_headers()
        headers["Accept"] = "application/json, text/plain, */*"
        headers["Referer"] = "https://www.flightstats.com/v2/flight-tracker"
        try:
            r = session.get(url, params=params, headers=headers, timeout=20)
        except Exception as e:
            print(f"  Network error: {e}")
            time.sleep(5)
            continue

        if r.status_code in (403, 429):
            wait = 30 * (2 ** attempt)
            print(f"  {r.status_code} on {url} -- global cooldown {wait}s (attempt {attempt+1}/{max_retries})")
            trigger_global_cooldown(wait)
            time.sleep(wait)
            continue

        if r.status_code in (500, 502, 503, 504):
            body_lower = r.text.lower()
            if any(x in body_lower for x in BOT_DETECTION_MARKERS):
                wait = 60 * (2 ** attempt)
                print(f"  {r.status_code} on {url} looks like a bot-detection page "
                      f"(not a real API response) -- cooldown {wait}s")
                trigger_global_cooldown(wait)
                time.sleep(wait)
                continue
            return None

        try:
            r.raise_for_status()
        except Exception as e:
            print(f"  HTTP error on {url}: {e}")
            return None

        text_lower = r.text.lower()
        if any(x in text_lower for x in BOT_DETECTION_MARKERS):
            wait = 60 * (2 ** attempt)
            print(f"  Bot detection page on {url} -- cooldown {wait}s")
            trigger_global_cooldown(wait)
            time.sleep(wait)
            continue

        try:
            payload = r.json()
        except ValueError:
            snippet = r.text[:200].replace("\n", " ")
            print(f"  Non-JSON response on {url}, retrying... Body snippet: {snippet!r}")
            time.sleep(5)
            continue

        return payload.get("data")

    raise RuntimeError(f"Repeated failures, giving up on {url}")


def fetch_departures(airport, date, hour):
    url = f"https://www.flightstats.com/v2/api-next/flight-tracker/dep/{airport}/{date.year}/{date.month}/{date.day}/{hour}"
    data = request_json_with_backoff(url, {"numHours": 6})
    if not data:
        return []
    return data.get("flights", [])


def fetch_arrivals(airport, date, hour):
    url = f"https://www.flightstats.com/v2/api-next/flight-tracker/arr/{airport}/{date.year}/{date.month}/{date.day}/{hour}"
    data = request_json_with_backoff(url, {"numHours": 6})
    if not data:
        return []
    return data.get("flights", [])


def fetch_airline_departures(airline, airport, date, hour):
    url = f"https://www.flightstats.com/v2/api-next/flight-tracker/dep/{airport}/{date.year}/{date.month}/{date.day}/{hour}"
    data = request_json_with_backoff(url, {"carrierCode": airline, "numHours": 6})
    if not data:
        return []
    return data.get("flights", [])


def fetch_airline_arrivals(airline, airport, date, hour):
    url = f"https://www.flightstats.com/v2/api-next/flight-tracker/arr/{airport}/{date.year}/{date.month}/{date.day}/{hour}"
    data = request_json_with_backoff(url, {"carrierCode": airline, "numHours": 6})
    if not data:
        return []
    return data.get("flights", [])


def fetch_status(carrier_fs, flight_number, date):
    url = f"https://www.flightstats.com/v2/api-next/flight-tracker/{carrier_fs}/{flight_number}/{date.year}/{date.month}/{date.day}"
    return request_json_with_backoff(url)


def extract_record(base, status_data):
    sched = status_data.get("schedule", {})
    st = status_data.get("status", {})
    note = status_data.get("flightNote", {})
    dep_actual = sched.get("estimatedActualDeparture") if sched.get("estimatedActualDepartureTitle") == "Actual" else None
    arr_actual = sched.get("estimatedActualArrival") if sched.get("estimatedActualArrivalTitle") == "Actual" else None
    return {
        **base,
        "departure_airport_iatacode": status_data.get("departureAirport", {}).get("fs"),
        "arrival_airport_iatacode": status_data.get("arrivalAirport", {}).get("fs"),
        "scheduled_departure": sched.get("scheduledDeparture"),
        "actual_departure": dep_actual,
        "scheduled_arrival": sched.get("scheduledArrival"),
        "actual_arrival": arr_actual,
        "status_code": st.get("statusCode"),
        "status": st.get("status"),
        "final_status": st.get("finalStatus"),
        "delay_departure_min": st.get("delay", {}).get("departure", {}).get("minutes"),
        "delay_arrival_min": st.get("delay", {}).get("arrival", {}).get("minutes"),
        "canceled": note.get("canceled"),
        "diverted": st.get("diverted"),
    }


def process_flight(f, date, target_airport=None, is_arrival=False):
    carrier = f.get("carrier", {})
    fs, num = carrier.get("fs"), carrier.get("flightNumber")
    if f.get("isCodeshare") or not fs or not num:
        return None

    global_key = (fs, str(num), date.strftime("%Y-%m-%d"))
    if global_key in _seen_flights:
        return None

    base = {
        "date": date.strftime("%Y-%m-%d"),
        "flight_number": fs + str(num),
        "carrier_code": fs,
        "airline": carrier.get("name"),
        "destination_airport_iatacode": f.get("airport", {}).get("fs"),
    }
    status_data = fetch_status(fs, num, date)
    if status_data is None:
        return None

    _seen_flights.add(global_key)
    record = extract_record(base, status_data)

    if target_airport:
        if is_arrival:
            record["arrival_airport_iatacode"] = target_airport
        else:
            record["departure_airport_iatacode"] = target_airport
    return record


def build_dataframe(records):
    df = pd.DataFrame(records)
    df.rename(columns={
        "date": "FlightDate",
        "flight_number": "FlightNumber",
        "carrier_code": "AirlineCode",
        "departure_airport_iatacode": "DepartureAirport",
        "arrival_airport_iatacode": "ArrivalAirport",
        "scheduled_departure": "ScheduledDeparture",
        "actual_departure": "ActualDeparture",
        "scheduled_arrival": "ScheduledArrival",
        "actual_arrival": "ActualArrival",
        "status_code": "StatusCode",
        "status": "Status",
        "final_status": "FinalStatus",
        "delay_departure_min": "DelayDepartureMin",
        "delay_arrival_min": "DelayArrivalMin",
        "canceled": "IsCanceled",
        "diverted": "IsDiverted",
    }, inplace=True)

    for c in ["ScheduledDeparture", "ActualDeparture", "ScheduledArrival", "ActualArrival"]:
        df[c] = pd.to_datetime(df[c], errors="coerce")

    df["FlightDate"] = pd.to_datetime(df["FlightDate"]).dt.date
    df["IsCanceled"] = df["IsCanceled"].fillna(False).astype(bool)
    df["IsDiverted"] = df["IsDiverted"].fillna(False).astype(bool)
    df["IsDelayed"] = (df["DelayArrivalMin"].fillna(0) > 10) | (df["DelayDepartureMin"].fillna(0) > 10)

    cols = [
        "FlightDate", "FlightNumber", "AirlineCode", "DepartureAirport", "ArrivalAirport",
        "ScheduledDeparture", "ActualDeparture", "ScheduledArrival", "ActualArrival",
        "StatusCode", "Status", "FinalStatus", "DelayDepartureMin", "DelayArrivalMin",
        "IsCanceled", "IsDiverted", "IsDelayed",
    ]
    return df[[c for c in cols if c in df.columns]]


def _to_native(value):
    """Convert pandas/numpy types to Python native types for DuckDB."""
    if value is None:
        return None
    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return None
        return value.to_pydatetime()
    if isinstance(value, (np.floating, float)):
        if pd.isna(value):
            return None
        return float(value)
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.bool_, bool)):
        return bool(value)
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return value
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def save_delaycanceldivert(conn, records):
    """Save irregular flights to DuckDB."""
    if not records:
        return 0, 0

    df = build_dataframe(records)
    insert_df = df.drop(columns=["DelayInMin"], errors="ignore")
    irregular_df = insert_df[insert_df["IsCanceled"] | insert_df["IsDiverted"] | insert_df["IsDelayed"]]
    n_total = len(df)
    n_irregular = len(irregular_df)

    if n_irregular == 0:
        return n_total, 0

    # Convert DataFrame to list of tuples with native Python types
    rows = [
        tuple(_to_native(v) for v in row)
        for row in irregular_df.itertuples(index=False, name=None)
    ]

    try:
        # DuckDB supports executemany with ? placeholders
        conn.executemany(
            """INSERT INTO IRREGULAR_FLIGHTS
               (FlightDate, FlightNumber, AirlineCode, DepartureAirport, ArrivalAirport,
                ScheduledDeparture, ActualDeparture, ScheduledArrival, ActualArrival,
                StatusCode, Status, FinalStatus, DelayDepartureMin, DelayArrivalMin,
                IsCanceled, IsDiverted, IsDelayed)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            rows
        )
    except Exception as e:
        print(f"Error inserting records: {e}")
        raise

    return n_total, n_irregular


def _format_duration(seconds):
    """Human-readable Hh Mm Ss for the terminal log."""
    seconds = int(round(seconds))
    hours, remainder = divmod(seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    return f"{hours}h {minutes}m {secs}s"


# ---- run ----
def main():
    run_start = time.perf_counter()
    run_start_wall = datetime.now()
    print(f"Run started at {run_start_wall:%Y-%m-%d %H:%M:%S}")

    # Use DuckDB connection instead of SQL Server
    conn = get_duckdb_connection()
    airports = eu_airports(conn)
    airlines = eu_airlines(conn)
    print(f"Loaded {len(airports)} airports, {len(airlines)} airlines")

    end_date = datetime.now()
    start_date = end_date - timedelta(days=1)

    global _seen_flights
    _seen_flights.clear()

    consecutive_failures = 0
    STOP_AFTER_N_FAILURES = 2
    total_flights_checked, total_irregular = 0, 0

    def _log_and_exit(reason, day_records):
        n, ni = save_delaycanceldivert(conn, day_records)
        nonlocal_totals[0] += n
        nonlocal_totals[1] += ni
        conn.close()
        elapsed = time.perf_counter() - run_start
        print(f"Checked {nonlocal_totals[0]} flights, inserted {nonlocal_totals[1]} irregular into DuckDB "
              f"before stopping.")
        print(f"Execution time: {_format_duration(elapsed)} ({elapsed:.1f}s)")
        raise SystemExit(reason)

    nonlocal_totals = [total_flights_checked, total_irregular]

    # ===================================================================
    # PHASE 1: Airline-centric fetching
    # ===================================================================
    LIMIT_AIRLINES = ["LH"]  # <-- Change to ["LH", "AF", "BA"] for testing

    phase1_airlines = LIMIT_AIRLINES if LIMIT_AIRLINES else airlines
    print(f"\n=== PHASE 1: Airline-centric fetching ({len(phase1_airlines)} airlines) ===")

    for airline in phase1_airlines:
        current_date = start_date
        while current_date < end_date:
            day_flights = []

            for airport in airports:
                for hour in HOURS:
                    try:
                        deps = fetch_airline_departures(airline, airport, current_date, hour)
                        day_flights.extend(deps)
                        consecutive_failures = 0
                    except Exception as e:
                        deps = []
                        print(f"  Airline dep error {airline}/{airport} {current_date:%Y-%m-%d} H{hour}: {e}")
                        consecutive_failures += 1

                    try:
                        arrs = fetch_airline_arrivals(airline, airport, current_date, hour)
                        day_flights.extend(arrs)
                        consecutive_failures = 0
                    except Exception as e:
                        arrs = []
                        print(f"  Airline arr error {airline}/{airport} {current_date:%Y-%m-%d} H{hour}: {e}")
                        consecutive_failures += 1

                    print(f"  [{airline}] {airport} {current_date:%Y-%m-%d} H{hour}: "
                          f"{len(deps)} dep, {len(arrs)} arr fetched")

                    time.sleep(random.uniform(2, 4))

                    if consecutive_failures >= STOP_AFTER_N_FAILURES:
                        print(f"Hit {STOP_AFTER_N_FAILURES} consecutive failures -- stopping early.")
                        _log_and_exit("Stopped: likely rate-limited. Re-run later / resume.", day_flights)

            to_process, seen_today = [], set()
            for f in day_flights:
                carrier = f.get("carrier", {})
                key = (carrier.get("fs"), carrier.get("flightNumber"))
                if key in seen_today or not all(key):
                    continue
                seen_today.add(key)
                to_process.append(f)

            print(f"{airline} {current_date:%Y-%m-%d}: {len(to_process)} unique flights to check")

            day_records = []
            for i, f in enumerate(to_process, 1):
                try:
                    rec = process_flight(f, current_date)
                    if rec:
                        day_records.append(rec)
                    consecutive_failures = 0
                except Exception as e:
                    print(f"  Status error: {e}")
                    consecutive_failures += 1

                if i % 25 == 0 or i == len(to_process):
                    print(f"  ...{i}/{len(to_process)} flights checked")
                time.sleep(random.uniform(1.0, 2.5))

                if consecutive_failures >= STOP_AFTER_N_FAILURES:
                    print(f"Hit {STOP_AFTER_N_FAILURES} consecutive failures -- stopping early.")
                    _log_and_exit("Stopped: likely rate-limited. Re-run later / resume.", day_records)

            n, ni = save_delaycanceldivert(conn, day_records)
            nonlocal_totals[0] += n
            nonlocal_totals[1] += ni
            current_date += timedelta(days=1)

    # ===================================================================
    # PHASE 2: Airport-wide fetching
    # ===================================================================
    print(f"\n=== PHASE 2: Airport-wide fetching ({len(airports)} airports) ===")

    for airport in airports:
        current_date = start_date
        while current_date < end_date:
            day_departures = []
            day_arrivals = []

            for hour in HOURS:
                try:
                    deps = fetch_departures(airport, current_date, hour)
                    day_departures.extend(deps)
                    consecutive_failures = 0
                except Exception as e:
                    deps = []
                    print(f"Dep list error {airport} {current_date:%Y-%m-%d} hour {hour}: {e}")
                    consecutive_failures += 1

                try:
                    arrs = fetch_arrivals(airport, current_date, hour)
                    day_arrivals.extend(arrs)
                    consecutive_failures = 0
                except Exception as e:
                    arrs = []
                    print(f"Arr list error {airport} {current_date:%Y-%m-%d} hour {hour}: {e}")
                    consecutive_failures += 1

                print(f"  [{airport}] {current_date:%Y-%m-%d} H{hour}: "
                      f"{len(deps)} dep, {len(arrs)} arr fetched")

                time.sleep(random.uniform(2, 4))

            to_process, seen_today = [], set()

            for f in day_departures:
                carrier = f.get("carrier", {})
                key = (carrier.get("fs"), carrier.get("flightNumber"), "dep")
                if key in seen_today or not all(key[:2]):
                    continue
                seen_today.add(key)
                to_process.append((f, False))

            for f in day_arrivals:
                carrier = f.get("carrier", {})
                key = (carrier.get("fs"), carrier.get("flightNumber"), "arr")
                if key in seen_today or not all(key[:2]):
                    continue
                seen_today.add(key)
                to_process.append((f, True))

            print(f"{airport} {current_date:%Y-%m-%d}: {len(to_process)} unique flights to check")

            day_records = []
            for i, (f, is_arr) in enumerate(to_process, 1):
                try:
                    rec = process_flight(f, current_date, target_airport=airport, is_arrival=is_arr)
                    if rec:
                        day_records.append(rec)
                    consecutive_failures = 0
                except Exception as e:
                    print(f"  Status error: {e}")
                    consecutive_failures += 1

                if i % 25 == 0 or i == len(to_process):
                    print(f"  ...{i}/{len(to_process)} flights checked")
                time.sleep(random.uniform(1.0, 2.5))

                if consecutive_failures >= STOP_AFTER_N_FAILURES:
                    print(f"Hit {STOP_AFTER_N_FAILURES} consecutive failures -- stopping early.")
                    _log_and_exit("Stopped: likely rate-limited. Re-run later / resume.", day_records)

            n, ni = save_delaycanceldivert(conn, day_records)
            nonlocal_totals[0] += n
            nonlocal_totals[1] += ni
            current_date += timedelta(days=1)

    conn.close()
    elapsed = time.perf_counter() - run_start
    print(f"Done. Checked {nonlocal_totals[0]} flights, inserted {nonlocal_totals[1]} irregular into DuckDB.")
    print(f"Run finished at {datetime.now():%Y-%m-%d %H:%M:%S}")
    print(f"Execution time: {_format_duration(elapsed)} ({elapsed:.1f}s)")


if __name__ == "__main__":
    main()