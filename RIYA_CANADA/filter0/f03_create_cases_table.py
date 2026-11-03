import duckdb
import pandas as pd

# Files
csv_file = r"C:\Users\cagri\Desktop\Group2_2026_September\RiyaCanada\filter-0\RiyaCanadaCases.csv"
db_file = r"C:\DuckDB\my_db.duckdb"
TABLE_NAME = "RIYACANADA_CASES"

# Read CSV
df = pd.read_csv(
    csv_file,
    dtype=str
)

# Replace text NULL with actual NULL
df = df.replace("NULL", None)

# Convert date column
df["DepartureDate"] = pd.to_datetime(
    df["DepartureDate"],
    errors="coerce"
)

# Connect to DuckDB
con = duckdb.connect(db_file)

# Register dataframe
con.register("cases_df", df)

# Create table
con.execute(f"""
CREATE OR REPLACE TABLE {TABLE_NAME} AS
SELECT
    Id,
    BookingRef,
    FlightNumber,
    DepartureDate,
    PAXFullName
FROM cases_df
""")

# Verify row count
row_count = con.execute(
    f"SELECT COUNT(*) FROM {TABLE_NAME}"
).fetchone()[0]

print(f"{row_count:,} rows loaded.")

con.close()