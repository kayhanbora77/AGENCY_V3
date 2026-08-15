import duckdb
import pandas as pd
import numpy as np
import uuid

# ============================================================
# CONFIG
# ============================================================

DB_PATH = r"C:\DuckDB\my_db.duckdb"

SOURCE_TABLE = "TBO26_RAW"
TARGET_TABLE = "TBO26_SPLIT"

DUMMY_FLIGHTS = {"QR000", "000", ""}

STATIC_COLS = [
    "PaxName",
    "BookingRef",
    "ETicketNo",
    "ClientCode",
    "Airline",
    "JourneyType",
]

MAX_LEGS = 7

# ============================================================
# HELPERS
# ============================================================

def is_dummy_flight(flight_number):
    """
    Returns True for dummy/surface-sector flight numbers.
    """
    if pd.isna(flight_number):
        return True

    flight_number = str(flight_number).strip().upper()
    return flight_number in DUMMY_FLIGHTS

# ============================================================
# SPLIT ONE ROW
# ============================================================

def split_open_jaw_row(row):
    """
    Split one TBO26_RAW row into Outbound / Inbound rows.
    ParentId is populated from the original TBO26_RAW.Id.
    """
    parent_id = row.get("Id")

    legs = []

    for i in range(1, MAX_LEGS + 1):
        fn = row.get(f"FlightNumber{i}")
        fd = row.get(f"FlightDate{i}")
        dep_ap = row.get(f"Airport{i}")
        arr_ap = row.get(f"Airport{i + 1}")

        if pd.isna(fn) or str(fn).strip() == "":
            continue

        legs.append({
            "FlightNumber": str(fn).strip(),
            "FlightDate": fd,
            "DepAirport": dep_ap,
            "ArrAirport": arr_ap,
        })

    # --------------------------------------------------------
    # No valid flights
    # --------------------------------------------------------

    if not legs:
        return []

    # --------------------------------------------------------
    # Divide into outbound / inbound
    # --------------------------------------------------------

    outbound_legs = []
    inbound_legs = []

    found_surface_gap = False

    for leg in legs:
        if is_dummy_flight(leg["FlightNumber"]):
            found_surface_gap = True
            continue

        if not found_surface_gap:
            outbound_legs.append(leg)
        else:
            inbound_legs.append(leg)

    # --------------------------------------------------------
    # Create groups only when they contain flights
    # --------------------------------------------------------

    groups = []

    if outbound_legs:
        groups.append(("Outbound", outbound_legs))

    if inbound_legs:
        groups.append(("Inbound", inbound_legs))

    if not groups:
        return []

    split_rows = []
    split_occurred = len(groups) > 1

    for bound_type, bound_legs in groups:
        new_row = {}
        new_row["Id"] = str(uuid.uuid4())
        new_row["ParentId"] = parent_id

        for col in STATIC_COLS:
            new_row[col] = row.get(col)
        if split_occurred:
            new_row["JourneyType"] = f"MultiCity-{bound_type}"

        for idx, leg in enumerate(bound_legs, start=1):
            new_row[f"FlightNumber{idx}"] = leg["FlightNumber"]
            new_row[f"FlightDate{idx}"] = leg["FlightDate"]
            new_row[f"Airport{idx}"] = leg["DepAirport"]

        last_idx = len(bound_legs)
        new_row[f"Airport{last_idx + 1}"] = bound_legs[-1]["ArrAirport"]
        split_rows.append(new_row)

    return split_rows

# ============================================================
# PROCESS COMPLETE DATAFRAME
# ============================================================

def process_tbo26_table(df_source):
    all_split_rows = []
    for _, row in df_source.iterrows():
        split_results = split_open_jaw_row(row)
        if split_results:
            all_split_rows.extend(split_results)
    df_target = pd.DataFrame(all_split_rows)

    # --------------------------------------------------------
    # Target columns in the desired order
    # --------------------------------------------------------
    expected_cols = [
        "Id",
        "PaxName",
        "BookingRef",
        "ETicketNo",
        "ClientCode",
        "Airline",
        "JourneyType",
    ]

    # Add FlightNumber columns
    for i in range(1, MAX_LEGS + 1):
        expected_cols.append(f"FlightNumber{i}")

    # Add FlightDate columns
    for i in range(1, MAX_LEGS + 1):
        expected_cols.append(f"FlightDate{i}")

    # Add Airport columns
    for i in range(1, 8 + 1):  # Airport8 is included
        expected_cols.append(f"Airport{i}")

    # Add ParentId at the end
    expected_cols.append("ParentId")

    # Ensure all expected columns exist in the DataFrame
    for col in expected_cols:
        if col not in df_target.columns:
            df_target[col] = None

    return df_target[expected_cols]

# ============================================================
# DUCKDB ETL
# ============================================================

def run_tbo26_etl():
    print("=" * 70)
    print("TBO26 ETL")
    print("=" * 70)

    con = duckdb.connect(DB_PATH)

    try:
        print(f"\nReading: {SOURCE_TABLE}")
        df_source = con.execute(
            f'SELECT * FROM "{SOURCE_TABLE}"'
        ).df()
        print(f"Source rows: {len(df_source):,}")

        if df_source.empty:
            print("Source table is empty.")
            return
        if "Id" not in df_source.columns:
            raise ValueError(
                f'{SOURCE_TABLE} must contain an "Id" column.'
            )
        print("\nSplitting rows...")
        df_target = process_tbo26_table(df_source)
        print(f"Target rows: {len(df_target):,}")

        con.register("tbo26_split_df", df_target)

        print(f"\nCreating: {TARGET_TABLE}")
        con.execute(f'''
            CREATE OR REPLACE TABLE "{TARGET_TABLE}" AS
            SELECT *
            FROM tbo26_split_df
        ''')

        target_count = con.execute(
            f'SELECT COUNT(*) FROM "{TARGET_TABLE}"'
        ).fetchone()[0]

        parent_count = con.execute(f'''
            SELECT COUNT(DISTINCT ParentId)
            FROM "{TARGET_TABLE}"
        ''').fetchone()[0]

        print("\n" + "=" * 70)
        print("ETL COMPLETED")
        print("=" * 70)

        print(f"Source rows          : {len(df_source):,}")
        print(f"Target rows          : {target_count:,}")
        print(f"Distinct ParentIds   : {parent_count:,}")

        # ----------------------------------------------------
        # Verify parent relationship
        # ----------------------------------------------------

        orphan_count = con.execute(f'''
            SELECT COUNT(*)
            FROM "{TARGET_TABLE}" s
            LEFT JOIN "{SOURCE_TABLE}" r
                ON s.ParentId = r.Id
            WHERE r.Id IS NULL
        ''').fetchone()[0]

        print(f"Orphan ParentIds     : {orphan_count:,}")

        # ----------------------------------------------------
        # Show sample
        # ----------------------------------------------------

        print("\nSample:")

        sample = con.execute(f'''
            SELECT
                Id,
                ParentId,
                PaxName,
                BookingRef,
                JourneyType,
                FlightNumber1,
                Airport1,
                Airport2,
                FlightNumber2,
                Airport3
            FROM "{TARGET_TABLE}"
            LIMIT 10
        ''').df()

        print(sample.to_string(index=False))

    finally:
        con.close()

if __name__ == "__main__":
    run_tbo26_etl()