import os
import shutil
import duckdb

# =====================================================
# CONFIG
# =====================================================

CSV_FILE = r"C:\Users\cagri\Desktop\RiyaGulf\TA_STANDARD_RIYAGULF.csv"
DB_PATH = r"C:\DuckDB\my_db.duckdb"
TABLE_NAME = "TA_STANDARD_RIYAGULF"

# =====================================================
# CSV HEADER
# =====================================================
HEADER = (
    "Id,ConnectionID,PaxName,AgencyRefNumber,ETicketNo,FlightNumber,"
    "DepartureDate,FileName,BookingRef,AirlineCode,FromAirport,ToAirport,"
    "LastLegAirport,GMTDeparture,GMTArrival,EUEligible,EUEligibleDuration,"
    "ExtraNote,FlightFound,LegNo,IsTimeLimitL1,IsTimeLimitL2,"
    "EUFlights_Id,Link_Id,DelayInSecond,Status,IsSingleFlight,"
    "IsMultiSegment,OperatingFlightNo,ScheduledDeparture,ScheduledArrival,"
    "ActualDeparture,ActualArrival,SourceData"
)

# =====================================================
# ADD HEADER IF MISSING
# =====================================================

with open(CSV_FILE, "r", encoding="utf-8-sig", errors="ignore") as f:
    first_line = f.readline().strip()

if not first_line.startswith("Id,ConnectionID"):
    print("Adding CSV header...")
    temp_file = CSV_FILE + ".tmp"
    with open(temp_file, "w", encoding="utf-8", newline="") as outfile:
        outfile.write(HEADER + "\n")
        with open(CSV_FILE, "r", encoding="utf-8-sig", errors="ignore") as infile:
            shutil.copyfileobj(infile, outfile)
    os.replace(temp_file, CSV_FILE)
    print("Header added successfully.")
else:
    print("Header already exists.")

con = duckdb.connect(DB_PATH)
con.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")

print("Loading CSV into DuckDB...")

# Load as VARCHAR, but explicitly cast Booleans and Timestamps
con.execute(f"""
    CREATE TABLE {TABLE_NAME} AS
    SELECT 
        * EXCLUDE (
            EUEligible, IsTimeLimitL1, IsTimeLimitL2, IsSingleFlight, IsMultiSegment,
            DepartureDate, ScheduledDeparture, ScheduledArrival, ActualDeparture, ActualArrival
        ),
        TRY_CAST(EUEligible AS BOOLEAN) AS EUEligible,
        TRY_CAST(IsTimeLimitL1 AS BOOLEAN) AS IsTimeLimitL1,
        TRY_CAST(IsTimeLimitL2 AS BOOLEAN) AS IsTimeLimitL2,
        TRY_CAST(IsSingleFlight AS BOOLEAN) AS IsSingleFlight,
        TRY_CAST(IsMultiSegment AS BOOLEAN) AS IsMultiSegment,
        TRY_CAST(DepartureDate AS TIMESTAMP) AS DepartureDate,
        TRY_CAST(ScheduledDeparture AS TIMESTAMP) AS ScheduledDeparture,
        TRY_CAST(ScheduledArrival AS TIMESTAMP) AS ScheduledArrival,
        TRY_CAST(ActualDeparture AS TIMESTAMP) AS ActualDeparture,
        TRY_CAST(ActualArrival AS TIMESTAMP) AS ActualArrival
    FROM read_csv(
        '{CSV_FILE}',
        header=true,
        delim=',',
        quote='"',
        escape='"',
        all_varchar=true,
        nullstr=['', 'NULL', 'null'],
        ignore_errors=true,
        null_padding=true,
        strict_mode=false,
        sample_size=-1
    )
""")

print("Resetting EUEligible to NULL...")
con.execute(f"UPDATE {TABLE_NAME} SET EUEligible = NULL")
print("EUEligible reset successfully.")

row_count = con.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
eu_null_count = con.execute(f"""
    SELECT COUNT(*) FROM {TABLE_NAME} WHERE EUEligible IS NULL
""").fetchone()[0]

print()
print("=" * 60)
print(f"Table created  : {TABLE_NAME}")
print(f"Rows loaded    : {row_count:,}")
print(f"EUEligible NULL: {eu_null_count:,}")
print("=" * 60)

# =====================================================
# COLUMNS (Showing Types)
# =====================================================
print("\nColumns:")
for row in con.execute(f"DESCRIBE {TABLE_NAME}").fetchall():
    print(f"{row[0]} ({row[1]})")

print("\nSample Rows:")
print(con.execute(f"SELECT * FROM {TABLE_NAME} LIMIT 5").fetchdf())
con.close()
print("\nDone.")