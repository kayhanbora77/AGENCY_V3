import pandas as pd
import re
import duckdb

CSV_PATH = r"C:\Users\cagri\Desktop\RiyaCanada\RiyaCanadaOperatingFN.csv"
DB_PATH = r"C:\DuckDB\my_db.duckdb"
TABLE_NAME = "RIYACANADA_OPERATINGFN"

# Read source CSV
df = pd.read_csv(CSV_PATH, dtype=str)


def fix_scientific_notation(value):
    if pd.isna(value):
        return value

    value_str = str(value).strip()

    match = re.fullmatch(
        r'(\d+)\.0+E\+?(\d+)',
        value_str,
        re.IGNORECASE
    )

    if match:
        return f"{match.group(1)}E{match.group(2)}"

    return value_str


# Clean columns
df["FlightNumber"] = df["FlightNumber"].apply(fix_scientific_notation)
df["OperatingFlightNo"] = df["OperatingFlightNo"].apply(fix_scientific_notation)

# Insert into DuckDB directly
con = duckdb.connect(DB_PATH)

try:
    con.register("temp_df", df)

    con.execute(f"""
        CREATE OR REPLACE TABLE {TABLE_NAME} AS
        SELECT *
        FROM temp_df
    """)

    print(f"Table {TABLE_NAME} created successfully.")
    con.unregister("temp_df")

finally:
    con.close()