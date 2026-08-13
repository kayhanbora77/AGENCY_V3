import duckdb
import pandas as pd
from pathlib import Path
import re

# ==================================================
# CONFIG
# ==================================================
DB_PATH = r"C:\DuckDB\my_db.duckdb"
EXCEL_FOLDER = r"C:\Users\cagri\Desktop\Agency_Data\Riya_India\filter0\riya_older"
TABLE_NAME = "RIYA_INDIA_RAW2"

REQUIRED_COLUMNS = [
    "PaxName",
    "PNRNo",
    "AirlineCode",
    "TicketNo",
    "FlightNo",
    "FlightDate",
    "Airport1",
    "Airport2",
]


def normalize_flight_no(flight_no, airline_code):
    if pd.isna(flight_no) or str(flight_no).strip() == "":
        return None

    fn = str(flight_no).strip().upper()
    ac = str(airline_code).strip().upper() if pd.notna(airline_code) else ""

    # If FlightNo is only numbers → prepend AirlineCode
    if re.fullmatch(r"\d+", fn):
        if ac:
            fn = ac + fn
        else:
            return fn

    # Remove spaces and hyphens
    fn = re.sub(r"[\s\-]+", "", fn)

    # Remove leading zeros after airline code
    m = re.match(r"^([A-Z]+)0*(\d+)$", fn)
    if m:
        return m.group(1) + m.group(2)

    return fn


def create_table(con):
    con.execute(f"""
        DROP TABLE IF EXISTS {TABLE_NAME};

        CREATE TABLE {TABLE_NAME} (
            id              UUID DEFAULT gen_random_uuid(),
            SourceFile      VARCHAR,
            SheetName       VARCHAR,
            PaxName         VARCHAR,
            PNRNo           VARCHAR,
            AirlineCode     VARCHAR,
            TicketNo        VARCHAR,
            FlightNo        VARCHAR,
            FlightDate      TIMESTAMP,
            Airport1        VARCHAR,
            Airport2        VARCHAR
        )
    """)
    print(f"✅ Table '{TABLE_NAME}' created successfully.")


def read_excel_safely(file_path):
    """Try different engines to read the Excel file."""
    engines = ["openpyxl", "xlrd", None]  # None = let pandas decide

    for engine in engines:
        try:
            if engine:
                return pd.read_excel(file_path, sheet_name=None, dtype=str, engine=engine)
            else:
                return pd.read_excel(file_path, sheet_name=None, dtype=str)
        except Exception as e:
            last_error = e
            continue

    raise last_error


def load_and_insert(con):
    folder = Path(EXCEL_FOLDER)
    if not folder.exists():
        print(f"❌ Folder not found: {EXCEL_FOLDER}")
        return

    excel_files = list(folder.glob("*.xlsx")) + list(folder.glob("*.xls"))
    if not excel_files:
        print(f"❌ No Excel files found in: {EXCEL_FOLDER}")
        return

    print(f"Found {len(excel_files)} Excel file(s):\n")
    for f in excel_files:
        print(f"  • {f.name}")

    all_dfs = []

    for file_path in excel_files:
        print(f"\n📂 Processing: {file_path.name}")
        try:
            sheets = read_excel_safely(file_path)
        except Exception as e:
            print(f"  ❌ Failed to read {file_path.name}")
            print(f"     Error: {e}")
            print("     → Make sure 'openpyxl' is installed:  pip install openpyxl")
            continue

        for sheet_name, df in sheets.items():
            print(f"   → Sheet: '{sheet_name}'  ({len(df):,} rows)")

            # Clean column names
            df.columns = (
                df.columns
                .astype(str)
                .str.strip()
                .str.replace(" ", "")
                .str.replace("\n", "")
                .str.replace("\r", "")
            )

            available = [c for c in REQUIRED_COLUMNS if c in df.columns]
            missing = set(REQUIRED_COLUMNS) - set(available)
            if missing:
                print(f"      ⚠ Missing columns: {missing}")

            df = df[available].copy()

            for col in REQUIRED_COLUMNS:
                if col not in df.columns:
                    df[col] = None

            df["SourceFile"] = file_path.name
            df["SheetName"] = sheet_name

            all_dfs.append(df)

    if not all_dfs:
        print("\n❌ No usable data found in any file/sheet.")
        return

    df = pd.concat(all_dfs, ignore_index=True)
    print(f"\n✅ Combined total rows from all files/sheets: {len(df):,}")

    # ============================================================
    # FIX: Parse FlightDate in pandas before sending to DuckDB
    # ============================================================
    # pd.to_datetime handles both string dates (1/23/2021) and 
    # datetime objects that slip through despite dtype=str
    df["FlightDate"] = pd.to_datetime(df["FlightDate"], errors="coerce")

    # Optional: Also parse TravelEndDate if you need it later
    # df["TravelEndDate"] = pd.to_datetime(df["TravelEndDate"], errors="coerce")

    # Normalize FlightNo
    df["FlightNo"] = df.apply(
        lambda row: normalize_flight_no(row["FlightNo"], row["AirlineCode"]),
        axis=1
    )

    print("\nSample FlightDate values after parsing:")
    print(df[["SourceFile", "FlightDate"]].head(10).to_string())

    con.execute(f"DELETE FROM {TABLE_NAME}")
    con.register("temp_df", df)

    # ============================================================
    # FIX: Insert FlightDate directly — no TRY_STRPTIME needed
    # ============================================================
    con.execute(f"""
        INSERT INTO {TABLE_NAME} (
            SourceFile, SheetName,
            PaxName, PNRNo, AirlineCode, TicketNo,
            FlightNo, FlightDate, Airport1, Airport2
        )
        SELECT
            SourceFile,
            SheetName,
            PaxName,
            PNRNo,
            AirlineCode,
            TicketNo,
            FlightNo,
            FlightDate,   -- pandas datetime → DuckDB timestamp directly
            Airport1,
            Airport2
        FROM temp_df
    """)
    con.unregister("temp_df")

    print(f"✅ Successfully inserted {len(df):,} rows into '{TABLE_NAME}'.")


def main():
    con = duckdb.connect(DB_PATH)
    try:
        create_table(con)
        load_and_insert(con)

        count = con.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
        print(f"\n🎉 Final row count: {count:,}")

        # Check for NULL FlightDates
        null_count = con.execute(f"""
            SELECT COUNT(*) FROM {TABLE_NAME} WHERE FlightDate IS NULL
        """).fetchone()[0]
        print(f"⚠️  Rows with NULL FlightDate: {null_count:,}")

        print("\n--- Rows per source file ---")
        print(con.execute(f"""
            SELECT SourceFile, COUNT(*) as rows
            FROM {TABLE_NAME}
            GROUP BY 1
            ORDER BY rows DESC
        """).fetchdf().to_string())

        print("\n--- Sample data ---")
        print(con.execute(f"""
            SELECT id, SourceFile, SheetName, PaxName, AirlineCode, FlightNo, FlightDate, Airport1, Airport2
            FROM {TABLE_NAME}
            LIMIT 8
        """).fetchdf().to_string())

    except Exception as e:
        print(f"❌ Error: {e}")
        raise
    finally:
        con.close()


if __name__ == "__main__":
    main()