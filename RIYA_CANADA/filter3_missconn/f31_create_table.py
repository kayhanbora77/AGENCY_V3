import duckdb

# =====================================================
# CONFIG
# =====================================================
CSV_FILE = r"C:\Users\cagri\Desktop\RiyaCanada\TA_STANDARD_RIYACANADA_VF.csv"
DB_PATH = r"C:\DuckDB\my_db.duckdb"
TABLE_NAME = "TA_STANDARD_RIYACANADA_VF"

con = duckdb.connect(str(DB_PATH))

con.execute(f"""
CREATE OR REPLACE TABLE {TABLE_NAME} AS
SELECT
    CAST(src.Id AS VARCHAR)                                 AS Id,
    CAST(src.ConnectionID AS VARCHAR)                       AS ConnectionID,
    CAST(src.PaxName AS VARCHAR)                            AS PaxName,
    CAST(src.AgencyRefNumber AS VARCHAR)                    AS AgencyRefNumber,
    CAST(src.ETicketNo AS VARCHAR)                          AS ETicketNo,

    -- Safe cleaning for scientific notation strings without triggering infinity float overflows
    CASE 
        WHEN TRY_CAST(src.FlightNumber AS DOUBLE) IS NOT NULL 
             AND regexp_matches(src.FlightNumber, '[eE]')
        THEN CAST(TRY_CAST(src.FlightNumber AS DECIMAL(18, 0)) AS VARCHAR)
        ELSE CAST(src.FlightNumber AS VARCHAR)
    END                                                     AS FlightNumber,

    COALESCE(
        TRY_STRPTIME(src.DepartureDate, '%Y-%m-%d %H:%M:%S'),
        TRY_STRPTIME(src.DepartureDate, '%m/%d/%Y %H:%M:%S'),
        TRY_STRPTIME(src.DepartureDate, '%Y-%m-%d'),
        TRY_STRPTIME(src.DepartureDate, '%m/%d/%Y')
    )                                                       AS DepartureDate,

    CAST(src.FileName AS VARCHAR)                           AS FileName,
    CAST(src.BookingRef AS VARCHAR)                         AS BookingRef,
    CAST(src.AirlineCode AS VARCHAR)                        AS AirlineCode,
    CAST(src.FromAirport AS VARCHAR)                        AS FromAirport,
    CAST(src.ToAirport AS VARCHAR)                          AS ToAirport,
    CAST(src.LastLegAirport AS VARCHAR)                     AS LastLegAirport,
    
    TRY_CAST(src.GMTDeparture AS DECIMAL(4,1))              AS GMTDeparture,
    TRY_CAST(src.GMTArrival AS DECIMAL(4,1))                AS GMTArrival,

    TRY_CAST(src.EUEligible AS BOOLEAN)                     AS EUEligible,
    TRY_CAST(src.EUEligibleDuration AS INTEGER)             AS EUEligibleDuration,
    CAST(src.ExtraNote AS VARCHAR)                          AS ExtraNote,
    TRY_CAST(src.FlightFound AS BOOLEAN)                    AS FlightFound,
    TRY_CAST(src.LegNo AS INTEGER)                          AS LegNo,
    TRY_CAST(src.IsTimeLimitL1 AS BOOLEAN)                  AS IsTimeLimitL1,
    TRY_CAST(src.IsTimeLimitL2 AS BOOLEAN)                  AS IsTimeLimitL2,
    CAST(src.EUFlights_Id AS VARCHAR)                       AS EUFlights_Id,
    CAST(src.Link_Id AS VARCHAR)                            AS Link_Id,
    TRY_CAST(src.DelayInSecond AS INTEGER)                  AS DelayInSecond,
    CAST(src.Status AS VARCHAR)                             AS Status,
    TRY_CAST(src.IsSingleFlight AS BOOLEAN)                 AS IsSingleFlight,
    TRY_CAST(src.IsMultiSegment AS BOOLEAN)                 AS IsMultiSegment,
    CAST(src.OperatingFlightNo AS VARCHAR)                  AS OperatingFlightNo,

    COALESCE(
        TRY_STRPTIME(src.ScheduledDeparture, '%Y-%m-%d %H:%M:%S'),
        TRY_STRPTIME(src.ScheduledDeparture, '%m/%d/%Y %H:%M:%S'),
        TRY_STRPTIME(src.ScheduledDeparture, '%m/%d/%Y %H:%M'),
        TRY_STRPTIME(src.ScheduledDeparture, '%Y-%m-%d %H:%M')
    )                                                       AS ScheduledDeparture,

    COALESCE(
        TRY_STRPTIME(src.ScheduledArrival, '%Y-%m-%d %H:%M:%S'),
        TRY_STRPTIME(src.ScheduledArrival, '%m/%d/%Y %H:%M:%S'),
        TRY_STRPTIME(src.ScheduledArrival, '%m/%d/%Y %H:%M'),
        TRY_STRPTIME(src.ScheduledArrival, '%Y-%m-%d %H:%M')
    )                                                       AS ScheduledArrival,

    COALESCE(
        TRY_STRPTIME(src.ActualDeparture, '%Y-%m-%d %H:%M:%S'),
        TRY_STRPTIME(src.ActualDeparture, '%m/%d/%Y %H:%M:%S'),
        TRY_STRPTIME(src.ActualDeparture, '%m/%d/%Y %H:%M'),
        TRY_STRPTIME(src.ActualDeparture, '%Y-%m-%d %H:%M')
    )                                                       AS ActualDeparture,

    COALESCE(
        TRY_STRPTIME(src.ActualArrival, '%Y-%m-%d %H:%M:%S'),
        TRY_STRPTIME(src.ActualArrival, '%m/%d/%Y %H:%M:%S'),
        TRY_STRPTIME(src.ActualArrival, '%m/%d/%Y %H:%M'),
        TRY_STRPTIME(src.ActualArrival, '%Y-%m-%d %H:%M')
    )                                                       AS ActualArrival,

    CAST(src.SourceData AS VARCHAR)                         AS SourceData,

    -- Custom calculated place-holder columns
    CAST(NULL AS BIGINT)                                    AS DelayMissConnection,
    CAST(NULL AS BOOLEAN)                                   AS IsMissConnection

FROM read_csv_auto(
    '{CSV_FILE}',
    delim=',',
    header=true,
    ignore_errors=true,
    nullstr=['NULL', 'null', 'N/A', ''],
    sample_size=-1,
    all_varchar=true
) AS src;
""")

row_count = con.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
print(f"Table created : {TABLE_NAME}")
print(f"Rows loaded   : {row_count:,}")

con.close()