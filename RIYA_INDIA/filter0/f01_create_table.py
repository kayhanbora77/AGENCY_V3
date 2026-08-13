import duckdb
import pandas as pd
from pathlib import Path
import re

# ==================================================
# CONFIG
# ==================================================
DB_PATH = r"C:\DuckDB\my_db.duckdb"
CSV_PATH = r"C:\Users\cagri\Desktop\Agency_Data\Riya_India\filter0\Riya25-26RawData.csv"
TABLE_NAME = "RIYA_INDIA_RAW"


# ==================================================
# FlightNumber normalizer
# AA-2462 → AA2462
# EK-0542 → EK542
# JL-0061 → JL61
# ==================================================
def normalize_flight_number(value):
    if pd.isna(value) or str(value).strip() == "":
        return None
    s = str(value).strip().replace("-", "").upper()
    # Keep airline code letters + the number without leading zeros
    m = re.match(r"^([A-Z]+)0*(\d+)$", s)
    if m:
        return m.group(1) + m.group(2)
    return s  # fallback if pattern doesn't match


# ==================================================
# CREATE TABLE
# ==================================================
def create_table(con):
    con.execute(f"""
        DROP TABLE IF EXISTS {TABLE_NAME};

        CREATE TABLE {TABLE_NAME} (
            Id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            PaxName         VARCHAR,
            BookingRef      VARCHAR,
            AirlineCode     VARCHAR,
            ETicketNo       VARCHAR,

            FlightNumber1   VARCHAR,
            FlightNumber2   VARCHAR,
            FlightNumber3   VARCHAR,
            FlightNumber4   VARCHAR,
            FlightNumber5   VARCHAR,
            FlightNumber6   VARCHAR,
            FlightNumber7   VARCHAR,
            FlightNumber8   VARCHAR,
            FlightNumber9   VARCHAR,

            FlightDate1     TIMESTAMP,
            FlightDate2     TIMESTAMP,
            FlightDate3     TIMESTAMP,
            FlightDate4     TIMESTAMP,
            FlightDate5     TIMESTAMP,
            FlightDate6     TIMESTAMP,
            FlightDate7     TIMESTAMP,
            FlightDate8     TIMESTAMP,
            FlightDate9     TIMESTAMP,

            Airport1        VARCHAR,
            Airport2        VARCHAR,
            Airport3        VARCHAR,
            Airport4        VARCHAR,
            Airport5        VARCHAR,
            Airport6        VARCHAR,
            Airport7        VARCHAR,
            Airport8        VARCHAR,
            Airport9        VARCHAR,
            Airport10       VARCHAR
        )
    """)
    print(f"✅ Table '{TABLE_NAME}' created successfully.")


# ==================================================
# LOAD + NORMALIZE + INSERT
# ==================================================
def load_and_insert(con):
    path = Path(CSV_PATH)
    if not path.exists():
        print(f"❌ File not found: {CSV_PATH}")
        return

    print(f"Loading: {path.name} ...")

    # Support both CSV and Excel
    if path.suffix.lower() in (".xlsx", ".xls"):
        df = pd.read_excel(path, dtype=str)
    else:
        df = pd.read_csv(path, dtype=str, low_memory=False)

    print(f"✅ Loaded {len(df):,} rows.")

    # Clean column names
    df.columns = df.columns.str.strip().str.replace(" ", "")
    print("Columns:", df.columns.tolist())

    # Normalize all FlightNumber columns
    flight_cols = [f"FlightNumber{i}" for i in range(1, 10)]
    for col in flight_cols:
        if col in df.columns:
            df[col] = df[col].apply(normalize_flight_number)

    # Optional: show a few normalized examples
    print("\nSample normalized FlightNumbers:")
    print(df[[c for c in flight_cols if c in df.columns]].head(8).to_string())

    con.execute(f"DELETE FROM {TABLE_NAME}")
    con.register("temp_df", df)

    con.execute(f"""
        INSERT INTO {TABLE_NAME} (
            PaxName, BookingRef, AirlineCode, ETicketNo,
            FlightNumber1, FlightNumber2, FlightNumber3, FlightNumber4,
            FlightNumber5, FlightNumber6, FlightNumber7, FlightNumber8, FlightNumber9,
            FlightDate1, FlightDate2, FlightDate3, FlightDate4, FlightDate5,
            FlightDate6, FlightDate7, FlightDate8, FlightDate9,
            Airport1, Airport2, Airport3, Airport4, Airport5,
            Airport6, Airport7, Airport8, Airport9, Airport10
        )
        SELECT
            PaxName,
            BookingRef,
            AirlineCode,
            ETicketNo,

            FlightNumber1, FlightNumber2, FlightNumber3, FlightNumber4,
            FlightNumber5, FlightNumber6, FlightNumber7, FlightNumber8, FlightNumber9,

            -- Flexible date parsing (handles m/d/yyyy and m/d/yyyy H:M)
            COALESCE(
                TRY_STRPTIME(FlightDate1, '%m/%d/%Y %H:%M'),
                TRY_STRPTIME(FlightDate1, '%m/%d/%Y'),
                TRY_STRPTIME(FlightDate1, '%Y-%m-%d')
            ),
            COALESCE(
                TRY_STRPTIME(FlightDate2, '%m/%d/%Y %H:%M'),
                TRY_STRPTIME(FlightDate2, '%m/%d/%Y'),
                TRY_STRPTIME(FlightDate2, '%Y-%m-%d')
            ),
            COALESCE(
                TRY_STRPTIME(FlightDate3, '%m/%d/%Y %H:%M'),
                TRY_STRPTIME(FlightDate3, '%m/%d/%Y'),
                TRY_STRPTIME(FlightDate3, '%Y-%m-%d')
            ),
            COALESCE(
                TRY_STRPTIME(FlightDate4, '%m/%d/%Y %H:%M'),
                TRY_STRPTIME(FlightDate4, '%m/%d/%Y'),
                TRY_STRPTIME(FlightDate4, '%Y-%m-%d')
            ),
            COALESCE(
                TRY_STRPTIME(FlightDate5, '%m/%d/%Y %H:%M'),
                TRY_STRPTIME(FlightDate5, '%m/%d/%Y'),
                TRY_STRPTIME(FlightDate5, '%Y-%m-%d')
            ),
            COALESCE(
                TRY_STRPTIME(FlightDate6, '%m/%d/%Y %H:%M'),
                TRY_STRPTIME(FlightDate6, '%m/%d/%Y'),
                TRY_STRPTIME(FlightDate6, '%Y-%m-%d')
            ),
            COALESCE(
                TRY_STRPTIME(FlightDate7, '%m/%d/%Y %H:%M'),
                TRY_STRPTIME(FlightDate7, '%m/%d/%Y'),
                TRY_STRPTIME(FlightDate7, '%Y-%m-%d')
            ),
            COALESCE(
                TRY_STRPTIME(FlightDate8, '%m/%d/%Y %H:%M'),
                TRY_STRPTIME(FlightDate8, '%m/%d/%Y'),
                TRY_STRPTIME(FlightDate8, '%Y-%m-%d')
            ),
            COALESCE(
                TRY_STRPTIME(FlightDate9, '%m/%d/%Y %H:%M'),
                TRY_STRPTIME(FlightDate9, '%m/%d/%Y'),
                TRY_STRPTIME(FlightDate9, '%Y-%m-%d')
            ),

            Airport1, Airport2, Airport3, Airport4, Airport5,
            Airport6, Airport7, Airport8, Airport9, Airport10
        FROM temp_df
    """)
    con.unregister("temp_df")

    print(f"✅ Successfully inserted {len(df):,} rows.")


def main():
    con = duckdb.connect(DB_PATH)
    try:
        create_table(con)
        load_and_insert(con)

        count = con.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
        print(f"\n🎉 Final row count: {count:,}")

        # Quick quality checks
        print("\n--- Quality checks ---")
        print("Sample FlightNumbers after load:")
        print(con.execute(f"""
            SELECT FlightNumber1, FlightNumber2, FlightNumber3, FlightNumber4
            FROM {TABLE_NAME}
            LIMIT 10
        """).fetchdf().to_string())

        print("\nDate parsing success:")
        for i in range(1, 10):
            col = f"FlightDate{i}"
            valid = con.execute(
                f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE {col} IS NOT NULL"
            ).fetchone()[0]
            print(f"  {col}: {valid:,} valid timestamps")

    except Exception as e:
        print(f"❌ Error: {e}")
        raise
    finally:
        con.close()


if __name__ == "__main__":
    main()