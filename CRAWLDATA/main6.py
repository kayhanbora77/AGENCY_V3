import time
import random
import json
from datetime import datetime, timedelta

import pandas as pd
import duckdb
from curl_cffi import requests as cffi_requests   # pip install curl_cffi

# ====================== CONFIG ======================
DB_PATH = r"C:\DuckDB\my_db.duckdb"
HOURS = [0, 6, 12, 18]
STATUS_CACHE_FLUSH_EVERY = 25

BASE_DELAY = (4.0, 9.0)          # random delay between requests
MAX_RETRIES = 4
IMPERSONATE_PROFILES = ["chrome124", "chrome123", "chrome120", "chrome116"]

# ====================================================

_seen_flights = set()
_status_cache = {}
_status_cache_pending = []

session = cffi_requests.Session()
_cooldown_until = 0.0
_consecutive_blocks = 0


def get_headers():
    return {
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.flightstats.com/v2/flight-tracker",
        "Origin": "https://www.flightstats.com",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
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


def load_status_cache(conn, flight_date):
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


def request_json_with_backoff(url, params=None):
    global _cooldown_until, _consecutive_blocks

    now = time.time()
    if now < _cooldown_until:
        sleep_for = _cooldown_until - now
        print(f"Cooling down {sleep_for:.0f}s after previous blocks...")
        time.sleep(sleep_for)

    for attempt in range(1, MAX_RETRIES + 1):
        time.sleep(random.uniform(*BASE_DELAY))

        profile = random.choice(IMPERSONATE_PROFILES)
        try:
            resp = session.get(
                url,
                params=params,
                headers=get_headers(),
                impersonate=profile,
                timeout=35,
            )

            if resp.status_code in (403, 429, 503):
                _consecutive_blocks += 1
                wait = min(300, (2 ** _consecutive_blocks) * 8 + random.uniform(5, 20))
                _cooldown_until = time.time() + wait
                print(f"Blocked ({resp.status_code}) – attempt {attempt}/{MAX_RETRIES}. "
                      f"Backing off {wait:.0f}s (consecutive blocks={_consecutive_blocks})")
                time.sleep(wait)
                continue

            resp.raise_for_status()
            _consecutive_blocks = 0
            return resp.json()

        except Exception as e:
            print(f"Request error (attempt {attempt}): {e}")
            if attempt == MAX_RETRIES:
                return None
            time.sleep(random.uniform(5, 15))

    return None


def fetch_flights(airport, flight_date, hour, direction):
    """Phase 1 – list endpoint."""
    url = (
        f"https://www.flightstats.com/v2/api-next/flight-tracker/"
        f"{direction}/{airport}/{flight_date.year}/{flight_date.month}/{flight_date.day}/{hour}"
    )
    data = request_json_with_backoff(url, {"numHours": 6})
    return data.get("flights", []) if data else []


def fetch_status(carrier_fs, flight_number, flight_date):
    """Phase 2 – detailed status."""
    url = (
        f"https://www.flightstats.com/v2/api-next/flight-tracker/"
        f"{carrier_fs}/{flight_number}/{flight_date.year}/{flight_date.month}/{flight_date.day}"
    )
    return request_json_with_backoff(url)


def get_flight_status_cached(conn, carrier_fs, flight_number, flight_date):
    key = (carrier_fs, str(flight_number), flight_date.strftime("%Y-%m-%d"))
    if key in _status_cache:
        return _status_cache[key]

    status_data = fetch_status(carrier_fs, flight_number, flight_date)
    _status_cache[key] = status_data

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

    if direction == "dep":
        dep_airport = airport
    else:
        arr_airport = airport

    dep_actual = schedule.get("estimatedActualDeparture") if schedule.get("estimatedActualDepartureTitle") == "Actual" else None
    arr_actual = schedule.get("estimatedActualArrival") if schedule.get("estimatedActualArrivalTitle") == "Actual" else None

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
    airlines_set = {'LH'}  # change this set if you want more airlines

    target_date = datetime.now().date() - timedelta(days=1)

    print(f"Target Date: {target_date}")
    print(f"Airports: {len(airports)} | Target Airlines: {len(airlines_set)} {airlines_set}")
    print(f"Phase 1 (list) API calls: {len(airports) * len(HOURS) * 2}")
    print("Phase 2 (status) API calls: one per unique matching flight (varies by day).")
    print(f"TLS impersonation: curl_cffi (profiles: {', '.join(IMPERSONATE_PROFILES)})")
    print("Progress is saved continuously to STATUS_CACHE.\n")

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
                        carrier_fs = f.get("carrier", {}).get("fs")
                        if carrier_fs not in airlines_set:
                            continue

                        rec = extract_record(conn, f, target_date, direction, airport)
                        if rec:
                            batch_records.append(rec)
                            airport_flight_count += 1

            airport_elapsed = time.perf_counter() - airport_start
            print(f"found {airport_flight_count} target airline flights ({_format_duration(airport_elapsed)}).")

            if len(batch_records) >= 50:
                n, ni = save_to_duckdb(conn, batch_records)
                total_flights += n
                total_irregular += ni
                batch_records = []

            flush_status_cache_pending(conn)

    finally:
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