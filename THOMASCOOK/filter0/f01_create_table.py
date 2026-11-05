import glob
import os
import re
import uuid

import duckdb
import pandas as pd
import polars as pl

# ----------------------------------------------------------------------
# CONFIG
# ----------------------------------------------------------------------
EXCEL_FOLDER = r"C:\Users\cagri\Desktop\Group2_2026_September\ThomasCook\filter-0\splits"
EXCEL_GLOB   = "*.xlsb"

DB_PATH      = r"C:\DuckDB\my_db.duckdb"
TABLE_NAME   = "THOMASCOOK_RAW"

TRUNCATE_BEFORE_LOAD = True

# ----------------------------------------------------------------------
# IMPORTANT: the Excel loader UPPERCASES every column name.
# So every constant below must be UPPERCASE too.
# ----------------------------------------------------------------------
AIRLINE_COL        = "AIRLINE_CODE"
FLIGHT_NUMBER_COLS = [f"FLIGHTNO{i}" for i in range(1, 14)]
DATE_COLS          = [f"DEPARTURE_DATE{i}" for i in range(1, 14)]
AIRPORT_COLS       = [f"AIRPORT{i}" for i in range(1, 15)]

FLIGHTDATE_FORMATS = [
    "%Y-%m-%d - %H%M",            # 2019-01-01 - 1235  (ThomasCook format)
    "%m/%d/%Y %H:%M:%S", "%m/%d/%Y %H:%M", "%m/%d/%Y",
    "%d-%b-%y", "%d-%b-%Y",
    "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d",
    "%d/%m/%Y",
]

VARCHAR_COLS = {
    "COMPANY", "AIRLINE_PNR", "GDS_PNR", "TICKET_NO", "INVOICE_AND_REFUNDID", "GROUP_NAME",
    AIRLINE_COL, "AIRLINE_NAME", "STATUS",
    *FLIGHT_NUMBER_COLS,
    *AIRPORT_COLS,
}

# ----------------------------------------------------------------------
# HELPERS
# ----------------------------------------------------------------------
def quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'

def validate_table_name(name: str) -> None:
    parts = name.split(".")
    if not parts or any(not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", p) for p in parts):
        raise ValueError(f"Invalid DuckDB table name: {name!r}")

# ----------------------------------------------------------------------
# STEP 1 - FIND + READ EXCEL FILES (ALL SHEETS)
# ----------------------------------------------------------------------
def list_excel_files(folder: str, pattern: str) -> list[str]:
    if not os.path.isdir(folder):
        raise FileNotFoundError(f"Excel folder not found: {folder}")
    files = sorted(glob.glob(os.path.join(folder, pattern)))
    if not files:
        raise FileNotFoundError(f"No files matching {pattern!r} in {folder}")
    return files

def read_excel_all_sheets(path: str) -> pl.DataFrame:
    """Read every sheet, everything as String, header normalised to UPPERCASE."""
    ext = os.path.splitext(path)[1].lower()
    engine = "pyxlsb" if ext == ".xlsb" else "openpyxl"

    sheets = pd.read_excel(
        path,
        sheet_name=None,
        dtype=str,
        keep_default_na=False,
        na_values=[""],
        engine=engine,
    )

    frames: list[pl.DataFrame] = []
    for sheet_name, pdf in sheets.items():
        if pdf is None or pdf.empty:
            print(f"  {os.path.basename(path)} [{sheet_name}]: empty, skipped")
            continue

        pdf.columns = [str(c).strip().upper() for c in pdf.columns]
        df = pl.from_pandas(pdf, include_index=False)
        df = df.with_columns(pl.col(c).cast(pl.String) for c in df.columns)
        df = df.with_columns(pl.lit(sheet_name).alias("SourceSheet"))
        frames.append(df)
        print(f"  {os.path.basename(path)} [{sheet_name}]: "
              f"{df.height:,} rows, {df.width} cols")

    return pl.concat(frames, how="diagonal") if frames else pl.DataFrame()

# ----------------------------------------------------------------------
# STEP 2 - CLEAN TEXT, ADD Id / SourceFile
# ----------------------------------------------------------------------
def clean_text_columns(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.when(pl.col(c).str.strip_chars() == "")
          .then(None)
          .otherwise(pl.col(c).str.strip_chars())
          .alias(c)
        for c in df.columns
    )

def add_uuid_and_source(df: pl.DataFrame, source_name: str) -> pl.DataFrame:
    if "Id" in df.columns:
        raise ValueError("Source data already contains an 'Id' column.")

    ids = pl.Series("Id", [str(uuid.uuid4()) for _ in range(df.height)], dtype=pl.String)
    df = df.with_columns(ids, pl.lit(source_name).alias("SourceFile"))
    return df.select(["Id", "SourceFile",
                      *[c for c in df.columns if c not in ("Id", "SourceFile")]])

# ----------------------------------------------------------------------
# STEP 2b - NORMALISE FLIGHT NUMBERS ("AK 585 " -> "AK585")
# ----------------------------------------------------------------------
def normalize_flight_numbers(df: pl.DataFrame) -> pl.DataFrame:
    cols = [c for c in FLIGHT_NUMBER_COLS if c in df.columns]
    return df.with_columns(
        pl.col(c).str.replace_all(r"\s+", "").str.to_uppercase().alias(c)
        for c in cols
    )

# ----------------------------------------------------------------------
# STEP 2c - AIRPORTS: split sectors into single airports, drop repeats
#   AIRPORT1..4 = MNL-KUL, KUL-MAA, MAA-KUL, KUL-MNL
#   -> AIRPORT1..5 = MNL, KUL, MAA, KUL, MNL
#   (a sector's origin equal to the previous airport is removed; the
#    result is re-split safely if the script runs on already-split data)
# ----------------------------------------------------------------------
def _expand_airports(values: list) -> list:
    seq: list[str] = []
    for v in values:
        if v is None:
            continue
        for token in re.split(r"\s*-\s*", str(v).strip()):
            token = token.strip().upper()
            if not token:
                continue
            if seq and seq[-1] == token:      # same as previous -> drop
                continue
            seq.append(token)
    return seq

def split_airports(df: pl.DataFrame, source_name: str) -> pl.DataFrame:
    present = [c for c in AIRPORT_COLS if c in df.columns]
    if not present:
        return df

    rows = df.select(present).rows()
    n = len(AIRPORT_COLS)
    new_cols: list[list] = [[] for _ in range(n)]
    overflow = 0

    for row in rows:
        seq = _expand_airports(list(row))
        if len(seq) > n:
            overflow += 1
            seq = seq[:n]
        seq += [None] * (n - len(seq))
        for i in range(n):
            new_cols[i].append(seq[i])

    if overflow:
        print(f"\nWARNING [{source_name}]: {overflow:,} row(s) had more than {n} "
              f"airports after splitting; extra airports were truncated.")

    return df.with_columns(
        pl.Series(AIRPORT_COLS[i], new_cols[i], dtype=pl.String) for i in range(n)
    )

# ----------------------------------------------------------------------
# STEP 3 - PARSE DATES
# ----------------------------------------------------------------------
def parse_dates(df: pl.DataFrame, source_name: str) -> pl.DataFrame:
    for col in DATE_COLS:
        if col not in df.columns:
            continue

        parsed = pl.coalesce([
            pl.col(col).str.strptime(pl.Datetime, format=fmt, strict=False, exact=True)
            for fmt in FLIGHTDATE_FORMATS
        ])

        tmp = df.with_columns(parsed.alias("__parsed"))
        bad = tmp.filter(pl.col(col).is_not_null() & pl.col("__parsed").is_null()).select(col)
        if bad.height:
            print(f"\nWARNING [{source_name}]: {bad.height:,} invalid date value(s) in {col}:")
            print(bad.unique().head(20))
        
        df = tmp.with_columns(pl.col("__parsed").dt.date().alias(col)).drop("__parsed")
    return df

# ----------------------------------------------------------------------
# STEP 4 - INSERT INTO DUCKDB, THEN PREFIX FLIGHT NUMBERS
# ----------------------------------------------------------------------
def insert_into_duckdb(df: pl.DataFrame, db_path: str, table_name: str) -> None:
    validate_table_name(table_name)

    missing = VARCHAR_COLS.difference(df.columns)
    if missing:
        raise ValueError(
            f"Required VARCHAR columns missing from source: {sorted(missing)}\n"
            f"  DataFrame columns:  {df.columns}\n"
            f"  VARCHAR_COLS set:   {sorted(VARCHAR_COLS)}"
        )

    con = duckdb.connect(db_path)
    try:
        con.register("flight_data_df", df)
        table_sql   = ".".join(quote_identifier(p) for p in table_name.split("."))
        columns_sql = ", ".join(quote_identifier(c) for c in df.columns)
        select_sql  = ", ".join(
            f"CAST({quote_identifier(c)} AS VARCHAR) AS {quote_identifier(c)}"
            if c in VARCHAR_COLS else quote_identifier(c)
            for c in df.columns
        )

        con.execute("BEGIN TRANSACTION")
        try:
            if TRUNCATE_BEFORE_LOAD:
                con.execute(f"DROP TABLE IF EXISTS {table_sql}")

            con.execute(
                f"CREATE TABLE IF NOT EXISTS {table_sql} AS "
                f"SELECT {select_sql} FROM flight_data_df WHERE FALSE"
            )

            target_types = {r[0]: r[1].upper()
                            for r in con.execute(f"DESCRIBE SELECT * FROM {table_sql}").fetchall()}
            for col in VARCHAR_COLS:
                if target_types.get(col) != "VARCHAR":
                    raise TypeError(f"{table_name}.{col} must be VARCHAR, "
                                    f"but is {target_types.get(col)!r}.")

            before = con.execute(f"SELECT COUNT(*) FROM {table_sql}").fetchone()[0]
            con.execute(
                f"INSERT INTO {table_sql} ({columns_sql}) SELECT {select_sql} FROM flight_data_df"
            )
            after = con.execute(f"SELECT COUNT(*) FROM {table_sql}").fetchone()[0]
            if after - before != df.height:
                raise RuntimeError(f"Row-count mismatch: source={df.height:,}, "
                                   f"inserted={after - before:,}")

            # ---- FlightNumberN = AirlineCode || FlightNumberN ----
            # Flight numbers were already whitespace-stripped (e.g. "AK585").
            # Only prefix when the value has NO carrier code of its own
            # (pure digits like "733"). A value such as "AK585" under
            # AIRLINE_CODE "FD" (codeshare) is kept as is.
            air = f"TRIM({quote_identifier(AIRLINE_COL)})"
            set_clause = ", ".join(
                f"{quote_identifier(c)} = "
                f"CASE "
                f"  WHEN {air} IS NULL OR {air} = '' "
                f"    OR {quote_identifier(c)} IS NULL OR {quote_identifier(c)} = '' "
                f"  THEN {quote_identifier(c)} "
                f"  WHEN regexp_matches({quote_identifier(c)}, '^([A-Z][A-Z0-9]|[0-9][A-Z])[0-9]') "
                f"  THEN {quote_identifier(c)} "
                f"  ELSE {air} || {quote_identifier(c)} "
                f"END"
                for c in FLIGHT_NUMBER_COLS
            )

            print("Running UPDATE:", set_clause[:160], "...")
            con.execute(f"UPDATE {table_sql} SET {set_clause}")

            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise

        print(f"\nSuccessfully loaded: {table_name}")
        print(f"Source rows:   {df.height:,}")
        print(f"Target rows:   {after:,}")
    finally:
        con.close()

# ----------------------------------------------------------------------
# MAIN
# ----------------------------------------------------------------------
def main() -> None:
    files = list_excel_files(EXCEL_FOLDER, EXCEL_GLOB)
    print(f"Found {len(files)} file(s) in {EXCEL_FOLDER}")

    frames: list[pl.DataFrame] = []
    for path in files:
        name = os.path.basename(path)
        df = read_excel_all_sheets(path)
        if df.height == 0:
            print(f"  {name}: all sheets empty, skipped")
            continue

        df = clean_text_columns(df)
        df = normalize_flight_numbers(df)
        df = split_airports(df, name)
        df = add_uuid_and_source(df, name)
        df = parse_dates(df, name)
        frames.append(df)

    if not frames:
        raise RuntimeError("All files/sheets were empty - nothing to load.")

    combined = pl.concat(frames, how="diagonal")
    print(f"\nProcessed rows (all files/sheets): {combined.height:,}")

    insert_into_duckdb(combined, DB_PATH, TABLE_NAME)

if __name__ == "__main__":
    main()