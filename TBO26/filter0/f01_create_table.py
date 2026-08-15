"""
Load BookingData_01May2026_31Jul2026.csv into DuckDB table TBO26_RAW.

- All non-date columns are loaded as VARCHAR (raw, untouched) to avoid
  DuckDB's type-inference guessing wrong on a messy source file.
- FlightDate1..FlightDate7 are parsed into proper TIMESTAMP columns.
  Sample values observed: "5/10/2026 15:25" and "6/5/2026 0:00" ->
  M/D/YYYY H:MM (24h), with month/day/hour NOT reliably zero-padded.
  We try the full padded/non-padded format cross-product with
  TRY_STRPTIME + COALESCE, and the literal text "NULL" (present in the
  source, e.g. FlightDate2) is treated as SQL NULL via the CSV reader's
  nullstr option -- not something the date parser needs to special-case.
  Rows that genuinely fail to parse end up as NULL instead of crashing
  the load.
"""

import duckdb

DB_PATH = r"C:\DuckDB\my_db.duckdb"
CSV_PATH = r"C:\Users\cagri\Desktop\Agency_Data\TBO_2026\filter0\BookingData_01May2026_31Jul2026.csv"
TABLE_NAME = "TBO26_RAW"

# Source file has FlightNumber1-7 / FlightDate1-7 (Airport goes to 8, but
# there is no FlightDate8 column in this file).
FLIGHT_DATE_COLS = [f"FlightDate{i}" for i in range(1, 8)]

# Candidate strptime formats, tried in order via COALESCE.
# DuckDB supports GNU-style non-padded specifiers (%-m, %-d, %-H).
# Source data mixes padded and non-padded month/day/hour in the SAME
# column (e.g. "6/5/2026 0:00" vs "5/10/2026 15:25"), so we build the
# full cross-product of padded/non-padded variants rather than guessing.
_MONTHS = ["%-m", "%m"]
_DAYS = ["%-d", "%d"]
_HOURS = ["%-H", "%H"]
_SECONDS = ["", ":%S"]

DATE_FORMATS = [
    f"{m}/{d}/%Y {h}:%M{s}"
    for m in _MONTHS
    for d in _DAYS
    for h in _HOURS
    for s in _SECONDS
]

# Encodings to try, in order, matching the fallback chain used across
# the other agency pipelines (Tripjack/MiddleEast/etc.)
ENCODING_CANDIDATES = ["utf-8-sig", "utf-8", "cp1252", "latin-1"]

# The source file uses the literal text "NULL" (not just blank cells) to
# represent missing values in several columns, e.g. FlightDate2 = "NULL".
# Tell DuckDB to treat both as real SQL NULL on ingest.
NULL_STRINGS = ["NULL", ""]

# Airport1-8 columns are populated with whitespace-only placeholders
# (e.g. "   ") instead of being truly empty, so nullstr='' does not
# catch them. These get trimmed and NULLIF'd explicitly.
AIRPORT_COLS = [f"Airport{i}" for i in range(1, 9)]


def try_read_header(con: duckdb.DuckDBPyConnection, csv_path: str):
    """Try each encoding until DuckDB can read the header row."""
    last_err = None
    for enc in ENCODING_CANDIDATES:
        try:
            df = con.execute(
                f"""
                SELECT * FROM read_csv_auto(
                    '{csv_path}',
                    all_varchar=True,
                    sample_size=-1,
                    encoding='{enc}',
                    nullstr={NULL_STRINGS}
                )
                LIMIT 0
                """
            ).df()
            print(f"CSV header read OK with encoding='{enc}'")
            return list(df.columns), enc
        except Exception as e:  # noqa: BLE001
            last_err = e
            continue
    raise RuntimeError(f"Could not read CSV header with any encoding: {last_err}")


def build_date_expr(col: str) -> str:
    col_esc = f'"{col}"'
    coalesce_parts = ", ".join(
        f"TRY_STRPTIME({col_esc}, '{fmt}')" for fmt in DATE_FORMATS
    )
    return f"COALESCE({coalesce_parts}) AS {col_esc}"


def build_airport_expr(col: str) -> str:
    col_esc = f'"{col}"'
    # NULLIF(TRIM(x), '') -> whitespace-only or truly empty becomes NULL,
    # otherwise the trimmed airport code is kept.
    return f"NULLIF(TRIM({col_esc}), '') AS {col_esc}"

def main():
    con = duckdb.connect(DB_PATH)

    con.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")

    all_cols, encoding = try_read_header(con, CSV_PATH)

    select_parts = ["uuid() AS Id"]
    for col in all_cols:
        col_esc = f'"{col}"'
        if col in FLIGHT_DATE_COLS:
            select_parts.append(build_date_expr(col))
        elif col in AIRPORT_COLS:
            select_parts.append(build_airport_expr(col))
        else:
            select_parts.append(col_esc)

    select_sql = ",\n    ".join(select_parts)

    # Create the table without the PK constraint
    create_sql = f"""
        CREATE TABLE {TABLE_NAME} AS
        SELECT
            {select_sql}
        FROM read_csv_auto(
            '{CSV_PATH}',
            all_varchar=True,
            sample_size=-1,
            encoding='{encoding}',
            nullstr={NULL_STRINGS}
        )
    """
    con.execute(create_sql)

    # Add the primary key constraint to the Id column
    con.execute(f"ALTER TABLE {TABLE_NAME} ADD PRIMARY KEY (Id)")

    row_count = con.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
    print(f"Loaded {row_count} rows into {TABLE_NAME}")

    # Sanity checks (unchanged)
    for col in FLIGHT_DATE_COLS:
        if col not in all_cols:
            print(f"WARNING: {col} not found in source CSV, skipping")
            continue
        parsed_null = con.execute(
            f'SELECT COUNT(*) FROM {TABLE_NAME} WHERE "{col}" IS NULL'
        ).fetchone()[0]
        print(f"{col}: {parsed_null} NULL after parsing (out of {row_count} rows)")

    for col in AIRPORT_COLS:
        if col not in all_cols:
            print(f"WARNING: {col} not found in source CSV, skipping")
            continue
        null_count = con.execute(
            f'SELECT COUNT(*) FROM {TABLE_NAME} WHERE "{col}" IS NULL'
        ).fetchone()[0]
        print(f"{col}: {null_count} NULL (out of {row_count} rows)")

    con.close()
    print("Done.")


if __name__ == "__main__":
    main()