import os
import shutil
import duckdb

# =====================================================
# CONFIG
# =====================================================

CSV_FILE = r"C:\Users\cagri\Desktop\Travelpack\TA_STANDARD_TRAVELPACK_VF.csv"
DB_PATH = r"C:\DuckDB\my_db.duckdb"
TABLE_NAME = "TA_STANDARD_TRAVELPACK_VF"

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

# SQL macro expression equivalent to fix_scientific_notation logic:
# Matches patterns like 6.00E+11, 6.0E+18 and simplifies them to 6E11, 6E18
flight_clean_expr = """
    CASE 
        WHEN FlightNumber IS NULL THEN NULL
        ELSE REGEXP_REPLACE(TRIM(CAST(FlightNumber AS VARCHAR)), '(?i)^(\\d+)\\.0+E\\+?(\\d+)$', '\\1E\\2')
    END
"""

op_flight_clean_expr = """
    CASE 
        WHEN OperatingFlightNo IS NULL THEN NULL
        ELSE REGEXP_REPLACE(TRIM(CAST(OperatingFlightNo AS VARCHAR)), '(?i)^(\\d+)\\.0+E\\+?(\\d+)$', '\\1E\\2')
    END
"""

# Explicitly cast columns while preserving exact header position
con.execute(f"""
    CREATE TABLE {TABLE_NAME} AS
    SELECT 
        CAST(Id AS VARCHAR)                                     AS Id,
        CAST(ConnectionID AS VARCHAR)                           AS ConnectionID,
        CAST(PaxName AS VARCHAR)                                AS PaxName,
        CAST(AgencyRefNumber AS VARCHAR)                        AS AgencyRefNumber,
        CAST(ETicketNo AS VARCHAR)                              AS ETicketNo,
        
        -- Clean FlightNumber
        {flight_clean_expr}                                     AS FlightNumber,
        
        TRY_CAST(DepartureDate AS TIMESTAMP)                    AS DepartureDate,
        CAST(FileName AS VARCHAR)                               AS FileName,
        CAST(BookingRef AS VARCHAR)                             AS BookingRef,
        CAST(AirlineCode AS VARCHAR)                            AS AirlineCode,
        CAST(FromAirport AS VARCHAR)                            AS FromAirport,
        CAST(ToAirport AS VARCHAR)                              AS ToAirport,
        CAST(LastLegAirport AS VARCHAR)                         AS LastLegAirport,
        TRY_CAST(GMTDeparture AS DECIMAL(4,1))                  AS GMTDeparture,
        TRY_CAST(GMTArrival AS DECIMAL(4,1))                    AS GMTArrival,
        TRY_CAST(EUEligible AS BOOLEAN)                         AS EUEligible,
        TRY_CAST(EUEligibleDuration AS INTEGER)                 AS EUEligibleDuration,
        CAST(ExtraNote AS VARCHAR)                              AS ExtraNote,
        TRY_CAST(FlightFound AS BOOLEAN)                        AS FlightFound,
        TRY_CAST(LegNo AS INTEGER)                              AS LegNo,
        TRY_CAST(IsTimeLimitL1 AS BOOLEAN)                      AS IsTimeLimitL1,
        TRY_CAST(IsTimeLimitL2 AS BOOLEAN)                      AS IsTimeLimitL2,
        CAST(EUFlights_Id AS VARCHAR)                           AS EUFlights_Id,
        CAST(Link_Id AS VARCHAR)                                AS Link_Id,
        TRY_CAST(DelayInSecond AS INTEGER)                      AS DelayInSecond,
        CAST(Status AS VARCHAR)                                 AS Status,
        TRY_CAST(IsSingleFlight AS BOOLEAN)                     AS IsSingleFlight,
        TRY_CAST(IsMultiSegment AS BOOLEAN)                     AS IsMultiSegment,
        
        -- Clean OperatingFlightNo
        {op_flight_clean_expr}                                  AS OperatingFlightNo,
        
        TRY_CAST(ScheduledDeparture AS TIMESTAMP)               AS ScheduledDeparture,
        TRY_CAST(ScheduledArrival AS TIMESTAMP)                 AS ScheduledArrival,
        TRY_CAST(ActualDeparture AS TIMESTAMP)                  AS ActualDeparture,
        TRY_CAST(ActualArrival AS TIMESTAMP)                    AS ActualArrival,
        CAST(SourceData AS VARCHAR)                             AS SourceData
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