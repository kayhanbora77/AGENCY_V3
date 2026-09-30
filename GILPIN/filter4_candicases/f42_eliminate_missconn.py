import logging

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)

# Tables
SOURCE_TABLE = "TA_STANDARD_GILPIN_VF_EU"
TARGET_TABLE = "GILPIN_MISSCONN"

DB_PATH = r"C:\DuckDB\my_db.duckdb"

# Rule thresholds
MIN_DELAY_SECONDS = 120        # Rule 2: DelayInSecond < 120 -> not eligible
MIN_CONNECTION_SECONDS = 2700  # Rule 3: layover < 45 min -> not eligible

# EUEligible is a connection-level flag in the upstream pipeline
# (see _enforce_connection_level_consistency in the RIYAUSA script).
# True  -> a triggered rule flips EUEligible to False for the WHOLE ConnectionID.
# False -> only the triggering rows are flipped.
APPLY_AT_CONNECTION_LEVEL = True


def _fetch_rows(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Rule 1: every row of each ConnectionID that has a leg with
    EUEligible = TRUE AND IsMissConnection = TRUE."""
    return con.execute(f"""
        SELECT * FROM {SOURCE_TABLE}
        WHERE ConnectionID IN (
            SELECT ConnectionID FROM {SOURCE_TABLE}
            WHERE EUEligible IS TRUE AND IsMissConnection IS TRUE
        )
        ORDER BY ConnectionID, LegNo
    """).fetchdf()


def _rule2_low_delay(df: pd.DataFrame, is_miss: pd.Series) -> pd.Series:
    """Rule 2: IsMissConnection = True AND DelayInSecond < 120."""
    delay = pd.to_numeric(df["DelayInSecond"], errors="coerce")
    return is_miss & (delay < MIN_DELAY_SECONDS)  # NaN delay -> False


def _rule3_short_layover(df: pd.DataFrame, is_miss: pd.Series) -> pd.Series:
    """Rule 3: IsMissConnection = True at LegNo=X and, for LegNo=X+1 in the
    same connection,
        ScheduledDeparture(X+1) - ScheduledArrival(X) < 2700 seconds.
    """
    sched_arr = pd.to_datetime(df["ScheduledArrival"], errors="coerce")
    sched_dep = pd.to_datetime(df["ScheduledDeparture"], errors="coerce")

    # Look up leg X+1 by key (not by row position) so gaps in LegNo are safe.
    nxt = df[["ConnectionID", "LegNo"]].copy()
    nxt["LegNo"] = nxt["LegNo"] - 1  # leg X+1 lines up with leg X
    nxt["next_dep"] = sched_dep.values
    nxt = nxt.drop_duplicates(["ConnectionID", "LegNo"])

    keys = df[["ConnectionID", "LegNo"]].merge(
        nxt, on=["ConnectionID", "LegNo"], how="left"
    )
    next_dep = pd.Series(keys["next_dep"].values, index=df.index)

    layover_sec = (next_dep - sched_arr).dt.total_seconds()
    return is_miss & (layover_sec < MIN_CONNECTION_SECONDS)  # NaT -> False


def _apply_rules(df: pd.DataFrame) -> pd.DataFrame:
    is_miss = df["IsMissConnection"].fillna(False).astype(bool)

    trigger = _rule2_low_delay(df, is_miss) | _rule3_short_layover(df, is_miss)

    if APPLY_AT_CONNECTION_LEVEL:
        trigger = trigger.groupby(df["ConnectionID"], sort=False).transform("any")

    was_true = df["EUEligible"].eq(True)
    df["EUEligible"] = df["EUEligible"].mask(trigger, False)

    changed = trigger & was_true
    logger.info(
        f"Rows flipped to EUEligible=False: {int(changed.sum()):,} "
        f"({df.loc[changed, 'ConnectionID'].nunique():,} connections)"
    )
    return df


def process_table():
    con = duckdb.connect(DB_PATH)
    try:
        df = _fetch_rows(con)
        logger.info(f"Fetched {len(df):,} rows / {df['ConnectionID'].nunique():,} connections")

        df = _apply_rules(df)

        # Plain Python None/bool for DuckDB (same approach as the RIYAUSA script)
        df["EUEligible"] = pd.Series(
            [None if pd.isna(v) else bool(v) for v in df["EUEligible"]],
            index=df.index,
            dtype=object,
        )

        # Keep the time-limit rule consistent with the upstream pipeline:
        #   EUEligible IS NULL -> L1/L2 NULL
        #   EUEligible = FALSE -> L1/L2 FALSE
        #   EUEligible = TRUE  -> leave as-is
        con.register("res_df", df)
        con.execute(f"DROP TABLE IF EXISTS {TARGET_TABLE}")
        con.execute(f"""
            CREATE TABLE {TARGET_TABLE} AS
            SELECT * REPLACE (
                CAST(EUEligible AS BOOLEAN) AS EUEligible,
                CASE
                    WHEN EUEligible IS NULL THEN NULL
                    WHEN CAST(EUEligible AS BOOLEAN) = FALSE THEN FALSE
                    ELSE TRY_CAST(IsTimeLimitL1 AS BOOLEAN)
                END AS IsTimeLimitL1,
                CASE
                    WHEN EUEligible IS NULL THEN NULL
                    WHEN CAST(EUEligible AS BOOLEAN) = FALSE THEN FALSE
                    ELSE TRY_CAST(IsTimeLimitL2 AS BOOLEAN)
                END AS IsTimeLimitL2
            )
            FROM res_df
        """)
        con.unregister("res_df")
    finally:
        con.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    process_table()