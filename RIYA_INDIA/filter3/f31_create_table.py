import pandas as pd
import re
import duckdb

CSV_PATH = r"C:\Users\cagri\Desktop\RiyaIndia\Cases\AllRiyaIndiaCancelDivertDelayList.csv"
DB_PATH = r"C:\DuckDB\my_db.duckdb"
TABLE_NAME = "RIYAINDIA_CANCELDIVERTDELAY"

# Load CSV into Pandas
df = pd.read_csv(CSV_PATH)

# Function to fix scientific notation
def fix_scientific_notation(value):
    if pd.isna(value):
        return value

    value_str = str(value).strip()

    # Match patterns like 6.00E+11, 6.0E+18, etc.
    match = re.fullmatch(r'(\d+)\.0+E\+?(\d+)', value_str, re.IGNORECASE)

    if match:
        return f"{match.group(1)}E{match.group(2)}"

    return value_str


# Apply to FlightNumber and OperatingFlightNo
df["FlightNumber"] = df["FlightNumber"].apply(fix_scientific_notation)
df["OperatingFlightNo"] = df["OperatingFlightNo"].apply(fix_scientific_notation)

# Save cleaned data back to CSV
CLEANED_CSV_PATH = r"C:\Users\cagri\Desktop\RiyaIndia\Cases\cleaned_flights.csv"
df.to_csv(CLEANED_CSV_PATH, index=False)

# Connect to DuckDB
con = duckdb.connect(database=DB_PATH)

# Import cleaned CSV into DuckDB
con.execute(f"""
    CREATE OR REPLACE TABLE {TABLE_NAME} AS
    SELECT *
    FROM read_csv('{CLEANED_CSV_PATH}', header=true);
""")

# Set EUEligible to NULL for all rows
con.execute(f"""
    UPDATE {TABLE_NAME}
    SET EUEligible = NULL;
""")

print(f"Table {TABLE_NAME} created successfully.")
print("EUEligible set to NULL for all rows.")

con.close()