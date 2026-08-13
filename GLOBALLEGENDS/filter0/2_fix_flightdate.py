"""
GLOBALLEGENDS_RAW -> GLOBALLEGENDS_RAW2
----------------------------------------
Reads GLOBALLEGENDS_RAW, left-compacts (shifts) the FLIGHT_NUMBER1-4 and
FLIGHT_DATE1-4 column groups independently so that any NULL/blank value
in the middle is pushed to the right, and writes the result into
GLOBALLEGENDS_RAW2.

Example (FLIGHT_NUMBER columns):
    NULL, AI2018, NULL, NULL      -> AI2018, NULL, NULL, NULL
    NULL, NULL,   QR780, NULL     -> QR780,  NULL, NULL, NULL
    AI111, NULL,  NULL,  AI900    -> AI111,  AI900, NULL, NULL

The same left-compaction logic is applied separately to the
FLIGHT_DATE1-4 group.
"""

import duckdb
import numpy as np
import pandas as pd

# --------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------
DB_PATH = r"C:\DuckDB\my_db.duckdb"
SOURCE_TABLE = "GLOBALLEGENDS_RAW"
TARGET_TABLE = "GLOBALLEGENDS_RAW2"

FLIGHT_NUMBER_COLS = ["FLIGHT_NUMBER1", "FLIGHT_NUMBER2", "FLIGHT_NUMBER3", "FLIGHT_NUMBER4"]
FLIGHT_DATE_COLS = ["FLIGHT_DATE1", "FLIGHT_DATE2", "FLIGHT_DATE3", "FLIGHT_DATE4"]


# --------------------------------------------------------------------------
# Core logic
# --------------------------------------------------------------------------
def left_compact(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """
    For the given list of columns (row-wise), push all non-null / non-blank
    values to the left (keeping their relative order) and fill the
    remaining positions on the right with None.

    Fully vectorized (no per-row Python loop) so it stays fast on
    multi-million-row tables.
    """
    arr = df[cols].to_numpy(dtype=object)

    # A cell counts as "blank" if it's NULL/NaN/NaT/None or an empty/whitespace
    # string. Date columns come back from DuckDB as pandas Timestamps, and a
    # missing date shows up as pandas.NaT -- NOT as a float NaN -- so we must
    # use pd.isna() directly rather than gating on isinstance(v, float) first.
    def is_blank(v):
        if v is None:
            return True
        try:
            if pd.isna(v):
                return True
        except (TypeError, ValueError):
            # pd.isna() can raise on some array-like/object types; treat as not blank
            pass
        if isinstance(v, str) and v.strip() == "":
            return True
        return False

    is_blank_vec = np.vectorize(is_blank, otypes=[bool])
    mask = ~is_blank_vec(arr)  # True where value is present

    # Build a mask that marks the left-most len(row_valid) positions as True
    justified_mask = np.sort(mask, axis=1)[:, ::-1]

    out = np.full(arr.shape, None, dtype=object)
    out[justified_mask] = arr[mask]

    result = df.copy()
    result[cols] = out
    return result


def build_target_dataframe(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    df = con.execute(f"SELECT * FROM {SOURCE_TABLE}").fetchdf()

    df = left_compact(df, FLIGHT_NUMBER_COLS)
    df = left_compact(df, FLIGHT_DATE_COLS)

    return df


def main():
    con = duckdb.connect(DB_PATH)
    try:
        print(f"Reading from {SOURCE_TABLE} ...")
        df = build_target_dataframe(con)
        print(f"Rows processed: {len(df):,}")

        print(f"Writing to {TARGET_TABLE} ...")
        con.register("df_result", df)
        con.execute(f"CREATE OR REPLACE TABLE {TARGET_TABLE} AS SELECT * FROM df_result")
        con.unregister("df_result")

        row_count = con.execute(f"SELECT COUNT(*) FROM {TARGET_TABLE}").fetchone()[0]
        print(f"Done. {TARGET_TABLE} now has {row_count:,} rows.")
    finally:
        con.close()


if __name__ == "__main__":
    main()