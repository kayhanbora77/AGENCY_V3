import duckdb
import pandas as pd
from datetime import timedelta
import logging

DB_PATH = r"C:\DuckDB\my_db.duckdb"
SOURCE_TABLE = "TA_STANDARD_RIYAGULF_VF"
MIN_LAYOVER_MINUTES = 45

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def load_connection_data() -> pd.DataFrame:
    """Load multi-leg connection data and parse datetime columns."""
    with duckdb.connect(DB_PATH) as con:
        df = con.execute(f"""
            SELECT Id, ConnectionID, LegNo, ActualArrival, ScheduledDeparture
            FROM {SOURCE_TABLE}
            WHERE IsSingleFlight = FALSE;
        """).df()
    if df.empty:
        logger.info("No multi-leg data found in %s", SOURCE_TABLE)
        return pd.DataFrame()

    logger.info("Loaded %d multi-leg rows", len(df))
    
    # Convert datetime columns that actually exist in our 5-column query
    for col in ["ActualArrival", "ScheduledDeparture"]:
        df[col] = pd.to_datetime(df[col].replace(["NULL", "null", "", "None"], pd.NaT), errors="coerce")

    # Convert LegNo to numeric for correct sorting
    if "LegNo" in df.columns:
        df["LegNo"] = pd.to_numeric(df["LegNo"], errors="coerce")

    return df

def update_missconnection(processed_df: pd.DataFrame) -> None:
    """Batch update using a temp table."""
    # Only update rows that have a calculated delay
    updates = processed_df[processed_df["DelayMissConnectionId"].notna()][
        ["DelayMissConnectionId", "DelayMissConnection", "IsMissConnection"]
    ].copy()

    if updates.empty:
        logger.info("No rows to update")
        return

    updates.columns = ["Id", "DelayMissConnection", "IsMissConnection"]
    logger.info("Updating %d rows in database...", len(updates))

    with duckdb.connect(DB_PATH) as con:
        # 1. Add columns to the source table if they don't exist
        con.execute(f"""
            ALTER TABLE {SOURCE_TABLE} 
            ADD COLUMN IF NOT EXISTS DelayMissConnection BIGINT;
        """)
        con.execute(f"""
            ALTER TABLE {SOURCE_TABLE} 
            ADD COLUMN IF NOT EXISTS IsMissConnection BOOLEAN;
        """)
        # If you also want to save the ID in the database, uncomment these lines:
        # con.execute(f"""
        #     ALTER TABLE {SOURCE_TABLE} 
        #     ADD COLUMN IF NOT EXISTS DelayMissConnectionId VARCHAR;
        # """)

        # 2. Register the dataframe for DuckDB to see it
        con.register("_miss_updates", updates)
        
        # 3. Execute the update
        con.execute(f"""
            UPDATE {SOURCE_TABLE} AS t
            SET 
                DelayMissConnection = u.DelayMissConnection::BIGINT,
                IsMissConnection = u.IsMissConnection::BOOLEAN
            FROM _miss_updates u
            WHERE t.Id = u.Id
        """)
def process_vectorized(df: pd.DataFrame) -> pd.DataFrame:
    # Sort is handled here, so you don't need to do it in main()
    df = df.sort_values(["ConnectionID", "LegNo"]).reset_index(drop=True)

    # Next leg's ScheduledDeparture within each connection
    df["NextScheduledDeparture"] = (
        df.groupby("ConnectionID")["ScheduledDeparture"].shift(-1)
    )

    # Vectorized layover (seconds)
    mask = df["ActualArrival"].notna() & df["NextScheduledDeparture"].notna()
    
    df["DelayMissConnection"] = pd.NA
    df.loc[mask, "DelayMissConnection"] = (
        (df.loc[mask, "NextScheduledDeparture"] - df.loc[mask, "ActualArrival"])
        .dt.total_seconds().astype("Int64")
    )

    df["DelayMissConnectionId"] = pd.NA
    df.loc[mask, "DelayMissConnectionId"] = df.loc[mask, "Id"].astype(str)

    df["IsMissConnection"] = False
    miss_mask = mask & (
        (df["NextScheduledDeparture"] - df["ActualArrival"])
        <= timedelta(minutes=MIN_LAYOVER_MINUTES)
    )
    df.loc[miss_mask, "IsMissConnection"] = True

    return df

def main():
    df = load_connection_data()

    if df.empty:
        logger.info("No data to process")
        return

    # process_vectorized handles the sorting automatically
    processed_df = process_vectorized(df)

    missed_count = processed_df["IsMissConnection"].sum()
    logger.info("Missed connections detected: %d", missed_count)

    update_missconnection(processed_df)
    logger.info("Processing complete")

if __name__ == "__main__":
    main()