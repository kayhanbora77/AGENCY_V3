"""
process_flight_csv.py

1. Reads the source CSV
2. Adds a UUID per row
3. Extracts AirlineCode1 / AirlineCode2 from FlightNumber1 / FlightNumber2
   e.g. "BA 2065" -> AirlineCode1 = "BA"
4. Builds the *cleaned* flight number columns that get inserted into DuckDB
   e.g. "BA 2065" -> "BA2065"   (source keeps the space, target does not)
5. Parses DepartureDate1 / DepartureDate2 carefully (day-first, blank-safe)
6. Inserts the result into a DuckDB table

Config constants are hardcoded at the top - edit these instead of passing CLI args.
"""

import re
import uuid
import duckdb
import pandas as pd

# ----------------------------------------------------------------------
# CONFIG
# ----------------------------------------------------------------------
CSV_PATH = r"C:\Users\cagri\Desktop\Agency_Data\TravelPack\filter0\Travel Pack UK - C2R Format.csv"
DB_PATH = r"C:\DuckDB\my_db.duckdb"
TABLE_NAME = "TRAVELPACK_RAW"        # target table name
CSV_SEP = ","                         # sample data is tab-separated; change to "," if needed
TRUNCATE_BEFORE_LOAD = True            # avoid silent row-count corruption across re-runs

# Columns that hold "AA 1234" style flight numbers
FLIGHT_NUMBER_COLS = ["FlightNumber1", "FlightNumber2"]

# Columns that hold dates in D/M/YYYY (day-first) format, blanks allowed
DATE_COLS = ["DepartureDate1", "DepartureDate2"]

# Encoding fallback chain (matches the pattern used across the other agency pipelines)
ENCODING_CHAIN = ["utf-8-sig", "cp1252", "latin-1"]


# ----------------------------------------------------------------------
# STEP 1 - READ CSV (with encoding fallback)
# ----------------------------------------------------------------------
def read_csv_with_fallback(path: str, sep: str) -> pd.DataFrame:
    last_err = None
    for enc in ENCODING_CHAIN:
        try:
            df = pd.read_csv(path, sep=sep, dtype=str, encoding=enc, keep_default_na=False)
            #print(f"Loaded CSV using encoding='{enc}'")
            return df
        except (UnicodeDecodeError, UnicodeError) as e:
            last_err = e
            continue
    raise RuntimeError(f"Could not read {path} with any encoding in {ENCODING_CHAIN}: {last_err}")


# ----------------------------------------------------------------------
# STEP 2 - UUID PER ROW
# ----------------------------------------------------------------------
def add_uuid_column(df: pd.DataFrame) -> pd.DataFrame:    
    df.insert(0, "Id", [str(uuid.uuid4()) for _ in range(len(df))])
    return df


# ----------------------------------------------------------------------
# STEP 3 - EXTRACT AIRLINE CODE ("BA 2065" -> "BA")
# ----------------------------------------------------------------------
AIRLINE_CODE_RE = re.compile(r"^\s*([A-Za-z0-9]+)")


def extract_airline_code(flight_number: str) -> str | None:
    """
    'BA 2065' -> 'BA'
    ''        -> None
    NaN/None  -> None
    Also tolerant of no-space formats like 'BA2065' -> 'BA'
    (splits at first run of digits if there's no space)
    """
    if flight_number is None:
        return None
    s = str(flight_number).strip()
    if s == "" or s.lower() == "nan":
        return None

    if " " in s:
        return s.split(" ", 1)[0].strip()

    # no space fallback: letters up to first digit
    m = re.match(r"^([A-Za-z]+)", s)
    return m.group(1) if m else s


def clean_flight_number(flight_number: str) -> str | None:
    """
    'BA 2065' -> 'BA2065'   (what actually gets inserted into DuckDB)
    ''        -> None
    """
    if flight_number is None:
        return None
    s = str(flight_number).strip()
    if s == "" or s.lower() == "nan":
        return None
    return s.replace(" ", "")


def process_flight_columns(df: pd.DataFrame) -> pd.DataFrame:
    for col in FLIGHT_NUMBER_COLS:
        idx = col[-1]  # "1" or "2" from FlightNumber1 / FlightNumber2
        airline_col = f"AirlineCode{idx}"
        clean_col = f"{col}_Clean"  # this is what gets inserted as the "target" FlightNumberN

        df[airline_col] = df[col].apply(extract_airline_code)
        df[clean_col] = df[col].apply(clean_flight_number)
    return df


# ----------------------------------------------------------------------
# STEP 4 - PARSE DATES CAREFULLY (day-first, blank-safe)
# ----------------------------------------------------------------------
def parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    for col in DATE_COLS:
        # blank strings -> NaN first, so pd.to_datetime doesn't choke / silently misparse
        raw = df[col].replace("", pd.NA)
        parsed = pd.to_datetime(raw, dayfirst=True, errors="coerce")

        # sanity check: anything non-blank that failed to parse is a real problem, surface it
        bad_mask = raw.notna() & parsed.isna()
        if bad_mask.any():
            bad_rows = df.loc[bad_mask, [col]]
            print(f"WARNING: {bad_mask.sum()} unparseable values in {col}:")
            print(bad_rows.to_string())

        df[col] = parsed  # datetime64[ns], NaT for blanks -> becomes NULL in DuckDB
    return df


# ----------------------------------------------------------------------
# STEP 5 - INSERT INTO DUCKDB
# ----------------------------------------------------------------------
def insert_into_duckdb(df: pd.DataFrame, db_path: str, table_name: str):
    con = duckdb.connect(db_path)

    # Build the final column set actually going into the table.
    # FlightNumber1/2 target columns use the CLEANED (no-space) values.
    final_df = df.copy()
    for col in FLIGHT_NUMBER_COLS:
        clean_col = f"{col}_Clean"
        final_df[col] = final_df[clean_col]   # overwrite with target/no-space version
        final_df.drop(columns=[clean_col], inplace=True)

    con.register("final_df_view", final_df)

    if TRUNCATE_BEFORE_LOAD:
        con.execute(f"DROP TABLE IF EXISTS {table_name}")

    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} AS
        SELECT * FROM final_df_view WHERE 1=0
    """)

    con.execute(f"INSERT INTO {table_name} SELECT * FROM final_df_view")

    row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    print(f"Inserted rows into {table_name}: {row_count}")

    con.close()


# ----------------------------------------------------------------------
# MAIN
# ----------------------------------------------------------------------
def main():
    df = read_csv_with_fallback(CSV_PATH, CSV_SEP)
    df = add_uuid_column(df)
    df = process_flight_columns(df)
    df = parse_dates(df)

    print(df[[
        "Id", "FlightNumber1", "AirlineCode1", "FlightNumber1_Clean",
        "FlightNumber2", "AirlineCode2", "FlightNumber2_Clean",
        "DepartureDate1", "DepartureDate2",
    ]].to_string())

    insert_into_duckdb(df, DB_PATH, TABLE_NAME)


if __name__ == "__main__":
    main()