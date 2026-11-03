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
EXCEL_FOLDER = r"C:\Users\cagri\Desktop\Group2_2026_September\RiyaCanada\filter-0"
EXCEL_GLOB   = "*.xlsx"

DB_PATH      = r"C:\DuckDB\my_db.duckdb"
TABLE_NAME   = "RIYACANADA_RAW"

TRUNCATE_BEFORE_LOAD = True

# ----------------------------------------------------------------------
# IMPORTANT: the Excel loader UPPERCASES every column name.
# So every constant below must be UPPERCASE too.
# ----------------------------------------------------------------------
AIRLINE_COL        = "AIRLINECODE"
FLIGHT_NUMBER_COLS = [f"FLIGHTNUMBER{i}" for i in range(1, 9)]
DATE_COLS          = [f"DEPARTUREDATE{i}" for i in range(1, 9)]

FLIGHTDATE_FORMATS = [
    "%m/%d/%Y %H:%M:%S", "%m/%d/%Y %H:%M", "%m/%d/%Y",
    "%d-%b-%y", "%d-%b-%Y",
    "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d",
    "%d/%m/%Y",
]

VARCHAR_COLS = {
    "DOCUMENTREFERENCENO", "PNRNO", "PAXNAME", AIRLINE_COL,
    *FLIGHT_NUMBER_COLS,
    "AIRPORT1", "AIRPORT2", "AIRPORT3", "AIRPORT4",
    "AIRPORT5", "AIRPORT6", "AIRPORT7", "AIRPORT8", "AIRPORT9",
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
    sheets = pd.read_excel(
        path,
        sheet_name=None,
        dtype=str,
        keep_default_na=False,
        na_values=[""],
        engine="openpyxl",
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

        df = tmp.with_columns(pl.col("__parsed").alias(col)).drop("__parsed")
    return df

# ----------------------------------------------------------------------
# STEP 4 - INSERT INTO DUCKDB, THEN PREFIX FLIGHT NUMBERS
# ----------------------------------------------------------------------
def insert_into_duckdb(df: pl.DataFrame, db_path: str, table_name: str) -> None:
    validate_table_name(table_name)

    missing = VARCHAR_COLS.difference(df.columns)
    if missing:
        # Helpful diagnostic - shows BOTH sides of the case mismatch if it happens.
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
            # Trim both sides first so stray spaces can't break the concat.
            # Explicitly handle NULLs/empty strings and prevent double-prefixing 
            # if the script is executed multiple times on the same data.
            set_clause = ", ".join(
                f"{quote_identifier(c)} = "
                f"CASE "
                f"  WHEN TRIM({quote_identifier(AIRLINE_COL)}) IS NULL OR TRIM({quote_identifier(AIRLINE_COL)}) = '' "
                f"    OR TRIM({quote_identifier(c)}) IS NULL OR TRIM({quote_identifier(c)}) = '' "
                f"  THEN {quote_identifier(c)} "
                f"  WHEN {quote_identifier(c)} LIKE TRIM({quote_identifier(AIRLINE_COL)}) || '%' "
                f"  THEN {quote_identifier(c)} "
                f"  ELSE TRIM({quote_identifier(AIRLINE_COL)}) || TRIM({quote_identifier(c)}) "
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