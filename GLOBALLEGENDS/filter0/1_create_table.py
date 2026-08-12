"""
GLOBALLEGENDS_RAW loader
-------------------------
Reads the 5-year International Global tickets CSV (tab-separated) and loads it
into DuckDB as GLOBALLEGENDS_RAW.

Steps:
  1. Read CSV -> add Id (UUID) as the first column.
  2. Derive AirlineCode from FLIGHT_NUMBER1 (chars before the first space)
     BEFORE the flight-number columns are cleaned of spaces.
  3. Clean FLIGHT_NUMBER1..4: strip, collapse internal spaces
     (e.g. "EK 787" -> "EK787", "VS 301" -> "VS301"). Blank/whitespace-only
     values become NULL.
  4. Parse FLIGHT_DATE1..4 (and BILL_DATE) as proper DATE values (source is
     M/D/YYYY, sometimes blank).
  5. Load into DuckDB table GLOBALLEGENDS_RAW at DB_PATH, schema-first
     (explicit CREATE TABLE), Id as column 1.
"""

import csv
import re
import uuid
import pandas as pd
import duckdb

# ----------------------------------------------------------------------------
# Config
# ----------------------------------------------------------------------------
CSV_FILE = r"C:\Users\cagri\Desktop\Agency_Data\GlobalLegend\filter0\5YEARS_INTERNATIONAL_GLOBAL_TKTS_DATA.csv"
DB_PATH = r"C:\DuckDB\my_db.duckdb"
TABLE_NAME = "GLOBALLEGENDS_RAW"

FLIGHT_NUMBER_COLS = ["FLIGHT_NUMBER1", "FLIGHT_NUMBER2", "FLIGHT_NUMBER3", "FLIGHT_NUMBER4"]
FLIGHT_DATE_COLS = ["FLIGHT_DATE1", "FLIGHT_DATE2", "FLIGHT_DATE3", "FLIGHT_DATE4"]

# Encoding fallback chain (same pattern used across the other AGENCY_V2 loaders)
ENCODING_FALLBACKS = ["utf-8-sig", "cp1252", "latin-1"]


# ----------------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------------
def _bad_line_handler(bad_line):
    """
    Called by pandas' python engine for any row whose field count doesn't
    match the header after proper quote/newline handling — a genuinely
    malformed row (unbalanced quote, truncated line, etc), not a multi-line
    quoted field (those are handled correctly by quoting=QUOTE_MINIMAL below).
    Logged and dropped rather than letting the whole load crash.
    """
    print(f"[read_csv] Skipping malformed row (field count mismatch): {bad_line}")
    return None  # None = skip the row


def sniff_delimiter(path: str, encoding: str) -> str:
    """
    Detect whether the file is actually tab- or comma-delimited by comparing
    delimiter counts on the header line. Source files have arrived as both in
    the past (a raw .csv export is usually comma; a copy/paste out of Excel
    is usually tab), so don't assume — check.
    """
    with open(path, "r", encoding=encoding, errors="replace") as f:
        header = f.readline()
    tab_count = header.count("\t")
    comma_count = header.count(",")
    delim = "\t" if tab_count > comma_count else ","
    print(f"[read_csv] Header has {tab_count} tabs / {comma_count} commas -> delimiter='{delim!r}'")
    return delim


def read_csv_with_fallback(path: str) -> pd.DataFrame:
    """
    Read the CSV, trying several encodings, with the real delimiter
    auto-detected from the header row.

    engine='python' + on_bad_lines=<handler> makes this tolerant of rows with
    a genuinely inconsistent field count (logged and skipped) while
    quoting=csv.QUOTE_MINIMAL (the CSV-standard default) still correctly
    handles fields that are quoted and contain embedded delimiters or
    embedded newlines (e.g. a company name spanning multiple physical
    lines) — those are NOT malformed and must not be split into extra rows.
    """
    last_err = None
    for enc in ENCODING_FALLBACKS:
        try:
            delim = sniff_delimiter(path, enc)
            df = pd.read_csv(
                path,
                sep=delim,
                encoding=enc,
                dtype=str,                    # read everything as text first; we cast later
                keep_default_na=False,
                engine="python",              # required for on_bad_lines callback
                quoting=csv.QUOTE_MINIMAL,     # CSV-standard: respects quotes, incl. multi-line fields
                on_bad_lines=_bad_line_handler,
            )
            print(f"[read_csv] Loaded with encoding='{enc}', rows={len(df)}, cols={len(df.columns)}")
            if "FLIGHT_NUMBER1" not in df.columns:
                raise RuntimeError(
                    f"Parsed {len(df.columns)} column(s) but 'FLIGHT_NUMBER1' is missing "
                    f"-- wrong delimiter detected. Columns seen: {list(df.columns)[:5]}..."
                )
            return df
        except (UnicodeDecodeError, UnicodeError) as e:
            last_err = e
            print(f"[read_csv] encoding='{enc}' failed, trying next...")
    raise RuntimeError(f"Failed to read {path} with fallbacks {ENCODING_FALLBACKS}") from last_err


def clean_flight_number(value: str) -> str | None:
    """Strip and remove all internal whitespace. '' / whitespace-only -> None."""
    if value is None:
        return None
    v = str(value).strip()
    if v == "":
        return None
    v = re.sub(r"\s+", "", v)
    return v if v else None


def extract_airline_code(raw_value: str) -> str | None:
    """
    AirlineCode = characters of FLIGHT_NUMBER1 up to (not including) the first
    space, taken from the ORIGINAL (uncleaned) value.
    e.g. 'EK 787' -> 'EK', 'VS 301' -> 'VS'
    """
    if raw_value is None:
        return None
    v = str(raw_value).strip()
    if v == "":
        return None
    return v.split(" ")[0].strip() or None


def parse_date_col(series: pd.Series) -> pd.Series:
    """
    Parse M/D/YYYY dates, blank -> NaT. Falls back to general parsing for any
    values that don't match the primary format (mirrors the mixed-date-format
    handling used in the other pipelines).
    """
    s = series.replace("", pd.NA)
    parsed = pd.to_datetime(s, format="%m/%d/%Y", errors="coerce")
    still_na = parsed.isna() & s.notna()
    if still_na.any():
        parsed.loc[still_na] = pd.to_datetime(s[still_na], errors="coerce", dayfirst=False)
    return parsed.dt.date


# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------
def main():
    df = read_csv_with_fallback(CSV_FILE)

    # Normalize column headers (strip stray whitespace, e.g. "NET FARE ")
    df.columns = [c.strip() for c in df.columns]

    # Some source rows have text fields (e.g. CUST_NAME) as CSV-quoted values
    # spanning multiple physical lines (embedded \n/\r). Flatten those to a
    # single space so the field reads as one line, then trim only the
    # leading/trailing whitespace on every cell -- internal spacing (e.g.
    # intentional double spaces inside a name) is left untouched.
    def _trim_cell(v):
        if not isinstance(v, str):
            return v
        v = v.replace("\r\n", " ").replace("\r", " ").replace("\n", " ")
        return v.strip()

    obj_cols = df.select_dtypes(include="object").columns
    for col in obj_cols:
        df[col] = df[col].apply(_trim_cell)

    # --- Step 2/3: AirlineCode derived from ORIGINAL FLIGHT_NUMBER1, before cleaning
    df["AirlineCode"] = df["FLIGHT_NUMBER1"].apply(extract_airline_code)

    # --- Step 2: clean FLIGHT_NUMBER1..4 (remove spaces, e.g. 'EK 787' -> 'EK787')
    for col in FLIGHT_NUMBER_COLS:
        df[col] = df[col].apply(clean_flight_number)

    # --- Step 4: parse date columns
    for col in FLIGHT_DATE_COLS:
        df[col] = parse_date_col(df[col])
    if "BILL_DATE" in df.columns:
        df["BILL_DATE"] = parse_date_col(df["BILL_DATE"])

    # --- Numeric columns: cast fare/tax/amount fields
    numeric_cols = ["BASIC_FARE", "TAX_AMT", "TK_CXL_AMT", "NET FARE"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].replace("", pd.NA), errors="coerce")

    # --- Step 1: Id as first column (UUID per row)
    df.insert(0, "Id", [str(uuid.uuid4()) for _ in range(len(df))])

    # Replace remaining empty strings with NA so they load as NULL in DuckDB
    df = df.replace("", pd.NA)

    print(f"[transform] Final columns ({len(df.columns)}): {list(df.columns)}")
    print(df.head(3).to_string())

    # ------------------------------------------------------------------------
    # Load into DuckDB (schema-first: explicit CREATE TABLE, Id is column 1)
    # ------------------------------------------------------------------------
    con = duckdb.connect(DB_PATH)

    other_cols = [c for c in df.columns if c != "Id"]
    select_cols = ["CAST(Id AS UUID) AS Id"] + [f'"{c}"' for c in other_cols]

    con.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    con.register("staging_df", df)
    con.execute(f"CREATE TABLE {TABLE_NAME} AS SELECT {', '.join(select_cols)} FROM staging_df")
    con.unregister("staging_df")

    row_count = con.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
    print(f"[duckdb] Loaded {row_count} rows into {TABLE_NAME} at {DB_PATH}")

    # Quick sanity check on the transformations
    print(con.execute(
        f"""
        SELECT Id, FLIGHT_NUMBER1, AirlineCode, FLIGHT_DATE1, FLIGHT_DATE2, FLIGHT_DATE3, FLIGHT_DATE4
        FROM {TABLE_NAME}
        LIMIT 5
        """
    ).df())

    con.close()


if __name__ == "__main__":
    main()