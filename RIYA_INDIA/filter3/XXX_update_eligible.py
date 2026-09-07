import pandas as pd
import duckdb
import logging
from typing import FrozenSet

# Configure logging
logger = logging.getLogger(__name__)

# Constants
SOURCE_TABLE = "RIYAINDIA_OPERATING_FLTNO"
TARGET_TABLE = "RIYAINDIA_OPERATING_FLTNO_RESULT"

DB_PATH = r"C:\DuckDB\my_db.duckdb"

# Carrier and Airport Sets
SPECIAL_NON_EU_CARRIERS: FrozenSet[str] = frozenset({"BA", "TK", "PC", "JU", "FH", "VF", "VS", "XQ"})
TR_CARRIERS: FrozenSet[str] = frozenset({"TK", "PC", "FH", "XQ", "VF"})
UK_CARRIERS: FrozenSet[str] = frozenset({"BA", "VS"})
SRB_CARRIERS: FrozenSet[str] = frozenset({"JU"})
SRB_AIRPORTS: FrozenSet[str] = frozenset({"BEG", "INI", "KVO"})
SPECIAL_CARRIERS: FrozenSet[str] = frozenset({"LH", "XQ", "QR"})


class ReferenceData:
    __slots__ = (
        "eu_airports",
        "eu_carriers",
        "tr_airports",
        "uk_airports",
        "airport_tz",
    )

    def __init__(self, con: duckdb.DuckDBPyConnection):
        self.eu_airports: frozenset = self._load_airports(con)
        self.eu_carriers: frozenset = self._load_carriers(con)
        self.tr_airports: frozenset = self._load_tr_airports(con)
        self.uk_airports: frozenset = self._load_uk_airports(con)
        self.airport_tz: dict = self._load_airport_tz(con)
        logger.info(
            f"Loaded {len(self.eu_airports):,} EU airports, {len(self.eu_carriers):,} EU carriers | "
            f"Loaded {len(self.tr_airports):,} TR airports, {len(self.uk_airports):,} UK airports"
        )

    @staticmethod
    def _load_airports(con) -> frozenset:
        rows = con.execute("""
            SELECT CodeIataAirport, timezone
            FROM AIRPORTS
            WHERE CodeIso2Country NOT IN ('TR','MA')
        """).fetchall()
        return frozenset(r[0].strip().upper() for r in rows if r and r[0])

    @staticmethod
    def _load_carriers(con) -> frozenset:
        rows = con.execute("""
            SELECT IataCode
            FROM AIRLINES
            WHERE IsInUnion = 1
        """).fetchall()
        return frozenset(r[0].strip().upper() for r in rows if r and r[0])

    @staticmethod
    def _load_uk_airports(con) -> frozenset:
        rows = con.execute("""
            SELECT CodeIataAirport
            FROM AIRPORTS
            WHERE CodeIso2Country = 'GB'
        """).fetchall()
        return frozenset(r[0].strip().upper() for r in rows if r and r[0])

    @staticmethod
    def _load_tr_airports(con) -> frozenset:
        rows = con.execute("""
            SELECT CodeIataAirport
            FROM AIRPORTS
            WHERE CodeIso2Country = 'TR'
        """).fetchall()
        return frozenset(r[0].strip().upper() for r in rows if r and r[0])

    @staticmethod
    def _load_airport_tz(con) -> dict:
        rows = con.execute(
            "SELECT iata, timezone FROM AIRPORTS_ALL WHERE iata IS NOT NULL AND timezone IS NOT NULL"
        ).fetchall()
        return {code.strip().upper(): tz for code, tz in rows if code and tz}


def _select_representative_airline(df: pd.DataFrame, ref_data: ReferenceData) -> pd.Series:
    """
    Selects a single representative AirlineCode per ConnectionID.

    Priority
    --------
    1. Any AirlineCode belonging to SPECIAL_NON_EU_CARRIERS  (highest)
    2. Any AirlineCode belonging to eu_carriers
    3. AirlineCode from the first leg (minimum LegNo)         (fallback)

    Returns
    -------
    pd.Series
        Same index as *df* containing the selected code for every row.
    """
    uid_col = "ConnectionID"

    # Work on a LegNo-sorted view so ".first()" is deterministic (lowest leg first)
    sorted_df = df.sort_values(by=[uid_col, "LegNo"])

    is_special = sorted_df["AirlineCode"].isin(SPECIAL_NON_EU_CARRIERS)
    is_eu = sorted_df["AirlineCode"].isin(ref_data.eu_carriers)

    # Priority 1: first special non-EU carrier per journey
    special_by_uid = (
        sorted_df[is_special]
        .groupby(uid_col, sort=False)["AirlineCode"]
        .first()
    )

    # Priority 2: first EU carrier per journey
    eu_by_uid = (
        sorted_df[is_eu]
        .groupby(uid_col, sort=False)["AirlineCode"]
        .first()
    )

    # Priority 3: first-leg airline (always exists)
    first_idx = sorted_df.groupby(uid_col, sort=False)["LegNo"].idxmin()
    first_leg_by_uid = sorted_df.loc[first_idx].set_index(uid_col)["AirlineCode"]

    # Merge priorities: special > eu > first leg
    selected_by_uid = special_by_uid.combine_first(eu_by_uid).combine_first(first_leg_by_uid)

    return df[uid_col].map(selected_by_uid)


def _vectorized_eligibility(df: pd.DataFrame, ref_data: ReferenceData) -> pd.Series:
    """
    Determines journey eligibility using a fully vectorized, priority-based rule engine.
    """
    if df.empty:
        return pd.Series(dtype=bool)

    uid_col = "ConnectionID"
    grp_uid = df[uid_col]

    # ============================================================
    # Derive AirlineCode from OperatingFlightNo when present,
    # otherwise keep the existing AirlineCode from the table.
    # ============================================================
    derived_from_op = (
        df["OperatingFlightNo"]
        .astype(str)
        .str.strip()
        .str[:2]
        .replace("", pd.NA)
        .replace("na", pd.NA)
        .replace("None", pd.NA)
    )

    if "AirlineCode" in df.columns:
        df["AirlineCode"] = derived_from_op.fillna(df["AirlineCode"])
    else:
        df["AirlineCode"] = derived_from_op

    # FIX: fill missing instead of dropping rows so index stays aligned
    df["AirlineCode"] = df["AirlineCode"].fillna("").str.upper().str.strip()

    # Select one representative code per ConnectionID
    df["SelectedAirlineCode"] = _select_representative_airline(df, ref_data)

    from_ap = df["FromAirport"]
    last_leg_airport = df["LastLegAirport"]

    # FirstLegAirport = departure airport of the first leg (lowest LegNo)
    first_idx = df.groupby(uid_col, sort=False)["LegNo"].idxmin()
    first_leg_airport_by_uid = from_ap.loc[first_idx]
    first_leg_airport_by_uid.index = df.loc[first_idx, uid_col].values
    first_leg_airport = grp_uid.map(first_leg_airport_by_uid)

    # Journey-level selected airline
    selected_airline = df["SelectedAirlineCode"]

    # Airport flags
    first_is_tr = first_leg_airport.isin(ref_data.tr_airports)
    last_is_tr = last_leg_airport.isin(ref_data.tr_airports)
    first_is_eu = first_leg_airport.isin(ref_data.eu_airports)
    last_is_eu = last_leg_airport.isin(ref_data.eu_airports)
    first_is_srb = first_leg_airport.isin(SRB_AIRPORTS)
    last_is_srb = last_leg_airport.isin(SRB_AIRPORTS)

    # Selected-airline flags
    selected_is_special = selected_airline.isin(SPECIAL_CARRIERS)          # LH/XQ/QR
    selected_is_special_non_eu = selected_airline.isin(SPECIAL_NON_EU_CARRIERS)

    # ---- Rule conditions (journey-level) ----
    # RULE 5: FirstLegAirport is EU -> Eligible True
    rule5_true = first_is_eu

    # RULE 4: bookend Non-EU -> Non-EU -> Eligible False
    rule4_false = (~first_is_eu) & (~last_is_eu)

    # RULE 3: selected airline is a SPECIAL_NON_EU_CARRIER -> True
    rule3_true = selected_is_special_non_eu

    # RULE 2: FirstLegAirport is TR and selected airline is LH/XQ/QR -> Eligible True
    rule2_true = first_is_tr & selected_is_special

    # RULE 1: bookend TR -> TR -> Eligible False
    rule1_false = first_is_tr & last_is_tr

    # Apply in priority order (lowest first, highest last overwrites)
    eligible = pd.Series(False, index=df.index)
    eligible = eligible.mask(rule5_true, True)   # Rule 5
    eligible = eligible.mask(rule4_false, False) # Rule 4
    eligible = eligible.mask(rule3_true, True)   # Rule 3
    eligible = eligible.mask(rule2_true, True)   # Rule 2
    eligible = eligible.mask(rule1_false, False) # Rule 1 (highest priority)

    # RULE 6: if any leg in a journey is eligible, mark every leg eligible
    eligible = eligible.groupby(grp_uid, sort=False).transform("any")

    return eligible.astype(bool)


def process_table():
    # Connect to DuckDB
    con = duckdb.connect(DB_PATH)

    # Load reference data
    ref_data = ReferenceData(con)

    # Load data from SOURCE_TABLE
    query = f"SELECT * FROM {SOURCE_TABLE}"
    df = con.execute(query).fetchdf()

    # Apply eligibility rules
    df["EUEligible"] = _vectorized_eligibility(df, ref_data)

    # Update IsTimeLimitL1 and IsTimeLimitL2 based on EUEligible
    df.loc[~df["EUEligible"], "IsTimeLimitL1"] = 0
    df.loc[~df["EUEligible"], "IsTimeLimitL2"] = 0

    # Save results to TARGET_TABLE
    con.execute(f"DROP TABLE IF EXISTS {TARGET_TABLE}")
    con.execute(f"CREATE TABLE {TARGET_TABLE} AS SELECT * FROM df")

    # Close connection
    con.close()


if __name__ == "__main__":
    process_table()