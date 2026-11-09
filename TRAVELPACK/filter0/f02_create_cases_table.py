
import duckdb
import pandas as pd
import re

from decimal import Decimal, InvalidOperation

# ============================================================
# CONFIG
# ============================================================
CSV_FILE = (
    r"C:\Users\cagri\Desktop\Group2_2026_September\Travelpack\Cases\TravelpackAllCases.csv"
)
DB_FILE = r"C:\DuckDB\my_db.duckdb"
TABLE_NAME = "TRAVELPACK_CASES"
NULL_VALUES = ["", "NULL", "null", "None", "NaN"]

# ============================================================
# 1. READ CSV
# ============================================================

def read_csv_file(csv_file):
    print("Reading CSV file...")
    df = pd.read_csv(
        csv_file,
        dtype=str,
        keep_default_na=False
    )
    print(f"CSV rows read: {len(df):,}")
    return df

# ============================================================
# 2. CLEAN GENERAL DATA
# ============================================================

def clean_dataframe(df):
    print("Cleaning general data...")
    # Strip whitespace
    for col in df.columns:
        df[col] = df[col].str.strip()
    # Replace empty strings and text NULL with actual NULL
    df = df.replace(NULL_VALUES, None)
    return df


# ============================================================
# 3. CLEAN ETICKET NUMBER
# ============================================================
def clean_ticket_number(value):
    if value is None:
        return None

    value = str(value).strip()
    # Detect scientific notation
    if re.fullmatch(r"\d+(\.\d+)?[Ee][+-]?\d+", value):
        try:
            number = Decimal(value)
            # Convert to exact integer text
            if number == number.to_integral_value():
                return format(
                    number.quantize(Decimal("1")), "f"
                )
        except (InvalidOperation, ValueError):
            pass
    return value

# ============================================================
# 4. CLEAN FLIGHT NUMBER
# ============================================================
def clean_flight_number(value):

    if value is None:
        return None
    value = str(value).strip().upper()
    if not value:
        return None
    # Detect scientific notation
    if re.fullmatch(r"\d+(\.\d+)?E[+-]?\d+", value):
        try:
            number = Decimal(value)
            # Normalize scientific notation
            normalized = format(number.normalize(), "E")

            mantissa, exponent = normalized.split("E")
            # Remove unnecessary decimal zeros
            mantissa = mantissa.rstrip("0").rstrip(".")
            # Remove + sign and leading exponent zeros
            exponent = str(int(exponent))

            return f"{mantissa}E{exponent}"
        except (InvalidOperation, ValueError):
            return value
    return value

# ============================================================
# 5. TRANSFORM DATA
# ============================================================
def transform_dataframe(df):

    print("Transforming data...")
    # Clean ETicketNumber
    df["ETicketNumber"] = df["ETicketNumber"].apply(
        clean_ticket_number
    )
    # Clean FlightNumber
    df["FlightNumber"] = df["FlightNumber"].apply(
        clean_flight_number
    )
    # Convert DepartureDate
    df["DepartureDate"] = pd.to_datetime(
        df["DepartureDate"],
        format="%m/%d/%Y",
        errors="coerce"
    ).dt.date
    return df

# ============================================================
# 6. LOAD DATA INTO DUCKDB
# ============================================================
def load_to_duckdb(df, db_file, table_name):
    print("Connecting to DuckDB...")
    con = duckdb.connect(db_file)
    try:
        con.register("cases_df", df)
        print(f"Creating table: {table_name}")
        con.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT
                TRY_CAST(Id AS UUID) AS Id,
                BookingRef,
                FlightNumber,
                CAST(DepartureDate AS DATE) AS DepartureDate,
                FromAirport,
                ToAirport,
                ETicketNumber,
                PAXFullName
            FROM cases_df
        """)
        print("Data loaded successfully.")
        return con
    except Exception:
        con.close()
        raise
# ============================================================
# 7. VERIFY IMPORT
# ============================================================
def verify_import(con, table_name):

    row_count = con.execute(f"""
        SELECT COUNT(*)
        FROM {table_name}
    """).fetchone()[0]

    invalid_dates = con.execute(f"""
        SELECT COUNT(*)
        FROM {table_name}
        WHERE DepartureDate IS NULL
    """).fetchone()[0]

    null_ids = con.execute(f"""
        SELECT COUNT(*)
        FROM {table_name}
        WHERE Id IS NULL
    """).fetchone()[0]

    print("\n" + "=" * 55)
    print("TRAVELPACK IMPORT COMPLETED")
    print("=" * 55)

    print(f"Total rows loaded: {row_count:,}")
    print(f"Missing/invalid dates: {invalid_dates:,}")
    print(f"Missing/invalid UUIDs: {null_ids:,}")

    print("\nSample records:")

    print(con.execute(f"""
        SELECT *
        FROM {table_name}
        LIMIT 10
    """).df())

# ============================================================
# 8. MAIN
# ============================================================
def main():
    con = None
    try:
        # Step 1: Read CSV
        df = read_csv_file(CSV_FILE)
        # Step 2: General cleaning
        df = clean_dataframe(df)
        # Step 3: Transform data
        df = transform_dataframe(df)
        # Step 4: Load into DuckDB
        con = load_to_duckdb(
            df,
            DB_FILE,
            TABLE_NAME
        )
        # Step 5: Verify import
        verify_import(con, TABLE_NAME)
    except Exception as e:
        print(f"\nERROR: {e}")
        raise
    finally:
        if con is not None:
            con.close()
            print("\nDuckDB connection closed.")
# ============================================================
# ENTRY POINT
# ============================================================
if __name__ == "__main__":
    main()
