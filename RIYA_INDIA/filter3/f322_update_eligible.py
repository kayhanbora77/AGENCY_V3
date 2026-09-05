import pandas as pd
import duckdb
import logging
from typing import FrozenSet

# Configure logging
logger = logging.getLogger(__name__)

# Constants
SOURCE_TABLE = "RIYAINDIA_CANCELDIVERTDELAY"
TARGET_TABLE = "RIYAINDIA_CANCELDIVERTDELAY_RESULT"

DB_PATH = r"C:\DuckDB\my_db.duckdb"

# Carrier and Airport Sets
SPECIAL_NON_EU_CARRIERS: FrozenSet[str] = frozenset({"BA", "TK", "PC", "JU", "FH", "VF", "VS", "XQ"})
TR_CARRIERS: FrozenSet[str] = frozenset({"TK", "PC", "FH", "XQ", "VF"})
UK_CARRIERS: FrozenSet[str] = frozenset({"BA", "VS"})
SRB_CARRIERS: FrozenSet[str] = frozenset({"JU"})
SRB_AIRPORTS: FrozenSet[str] = frozenset({"BEG", "INI", "KVO"})
SPECIAL_CARRIERS: FrozenSet[str] = frozenset({"LH", "XQ", "QR"})

# ASSUMPTION: "SpecialCarrier" in the new disruption-veto rules (Rule 7 below) maps to
# SPECIAL_NON_EU_CARRIERS, since these are the carriers that keep a journey eligible
# despite non-EU routing elsewhere in this codebase. If the spec actually meant the
# LH/XQ/QR set, change this one line to SPECIAL_CARRIERS instead.
DISRUPTION_SPECIAL_CARRIERS: FrozenSet[str] = SPECIAL_NON_EU_CARRIERS

# Disrupted-status values, matched case-insensitively against the Status column
# (confirmed from sample data: "cancel", "Delay" — "diversion" assumed to match the pattern).
DISRUPTED_STATUSES: FrozenSet[str] = frozenset({"CANCEL", "DELAY", "DIVERSION"})


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


def _vectorized_eligibility(df: pd.DataFrame, ref_data: ReferenceData) -> pd.Series:
    """
    Determines journey eligibility using a fully vectorized, priority-based rule engine
    (Rules 1-6, unchanged from the original implementation).
    """
    if df.empty:
        return pd.Series(dtype=bool)

    uid_col = "ConnectionID"
    grp_uid = df[uid_col]

    from_ap = df["FromAirport"]
    last_leg_airport = df["LastLegAirport"]

    # FirstLegAirport = departure airport of the first leg (lowest LegNo)
    first_idx = df.groupby(uid_col, sort=False)["LegNo"].idxmin()
    first_leg_airport_by_uid = from_ap.loc[first_idx]
    first_leg_airport_by_uid.index = df.loc[first_idx, uid_col].values
    first_leg_airport = grp_uid.map(first_leg_airport_by_uid)
    
    airline_code = df["AirlineCode"]

    # Airport flags
    first_is_tr = first_leg_airport.isin(ref_data.tr_airports)
    last_is_tr = last_leg_airport.isin(ref_data.tr_airports)
    first_is_eu = first_leg_airport.isin(ref_data.eu_airports)
    last_is_eu = last_leg_airport.isin(ref_data.eu_airports)

    airline_code_is_special = airline_code.isin(SPECIAL_CARRIERS)
    airline_code_is_special_non_eu = airline_code.isin(SPECIAL_NON_EU_CARRIERS)
    airline_code_is_eu_carrier = airline_code.isin(ref_data.eu_carriers)   # NEW

    # ---- Rule conditions (journey-level) ----
    # RULE 5: FirstLegAirport is EU -> Eligible True
    rule5_true = first_is_eu

    # RULE 4: bookend Non-EU -> Non-EU -> Eligible False
    rule4_false = (~first_is_eu) & (~last_is_eu)

    # RULE 3B (new): arriving into the EU on an EU/Union carrier -> Eligible True
    # Covers e.g. IST(TR)->MAD on UX, IST(TR)->OTP on RO, which Rule 3's
    # hardcoded SPECIAL_NON_EU_CARRIERS list never granted.
    rule3b_true = last_is_eu & airline_code_is_eu_carrier

    # RULE 3: selected airline is a SPECIAL_NON_EU_CARRIER -> True
    rule3_true = airline_code_is_special_non_eu

    # RULE 2: FirstLegAirport is TR and selected airline is LH/XQ/QR -> Eligible True
    rule2_true = first_is_tr & airline_code_is_special

    # RULE 1: bookend TR -> TR -> Eligible False
    rule1_false = first_is_tr & last_is_tr

    # Apply in priority order (lowest first, highest last overwrites)
    eligible = pd.Series(False, index=df.index)
    eligible = eligible.mask(rule5_true, True)    # Rule 5
    eligible = eligible.mask(rule4_false, False)  # Rule 4
    eligible = eligible.mask(rule3b_true, True)   # Rule 3B (new)
    eligible = eligible.mask(rule3_true, True)    # Rule 3
    eligible = eligible.mask(rule2_true, True)    # Rule 2
    eligible = eligible.mask(rule1_false, False)  # Rule 1 (highest priority)
    # RULE 6: if any leg in a journey is eligible, mark every leg eligible
    eligible = eligible.groupby(grp_uid, sort=False).transform("any")

    return eligible.astype(bool)


def _apply_disruption_veto(df: pd.DataFrame, ref_data: ReferenceData) -> pd.Series:
    """
    Rule 7 (new): disruption-based veto layered on top of Rules 1-6.

    Only rows with a disrupted Status (cancel/delay/diversion) can trigger this
    veto. Position within the journey changes which carrier sets exempt the leg:

        - First leg (LegNo == 1) or last leg (LegNo == max LegNo):
            Non-EU -> Non-EU and carrier NOT a DISRUPTION_SPECIAL_CARRIER -> veto
            Non-EU -> EU and carrier NOT (EU carrier or DISRUPTION_SPECIAL_CARRIER) -> veto

        - Middle legs (1 < LegNo < max LegNo):
            Non-EU -> Non-EU and carrier NOT a DISRUPTION_SPECIAL_CARRIER
                              AND NOT an EU carrier -> veto
            Non-EU -> EU and carrier NOT (EU carrier or DISRUPTION_SPECIAL_CARRIER) -> veto

    A veto triggered by ANY leg forces EUEligible=False for the WHOLE ConnectionID
    (EUEligible/IsTimeLimitL1/IsTimeLimitL2 are journey-level fields, matching the
    existing Rule 6 propagation behavior).

    Requires df["EUEligible"] to already be populated by _vectorized_eligibility.
    """
    uid_col = "ConnectionID"

    disrupted = df["Status"].astype(str).str.upper().str.strip().isin(DISRUPTED_STATUSES)

    from_is_eu = df["FromAirport"].isin(ref_data.eu_airports)
    to_is_eu = df["ToAirport"].isin(ref_data.eu_airports)

    airline = df["AirlineCode"]
    is_special = airline.isin(DISRUPTION_SPECIAL_CARRIERS)
    is_eu_carrier = airline.isin(ref_data.eu_carriers)

    max_leg = df.groupby(uid_col, sort=False)["LegNo"].transform("max")
    is_first = df["LegNo"] == 1
    is_last = df["LegNo"] == max_leg
    is_middle = (~is_first) & (~is_last)

    non_eu_to_non_eu = (~from_is_eu) & (~to_is_eu)
    non_eu_to_eu = (~from_is_eu) & to_is_eu

    # First/last leg: Non-EU -> Non-EU veto only exempts DISRUPTION_SPECIAL_CARRIERS
    veto_bookend_domestic = disrupted & (is_first | is_last) & non_eu_to_non_eu & (~is_special)

    # Middle leg: Non-EU -> Non-EU veto exempts BOTH special and EU carriers
    veto_middle_domestic = disrupted & is_middle & non_eu_to_non_eu & (~is_special) & (~is_eu_carrier)

    # All legs (any position): Non-EU -> EU veto unless carrier is EU or special
    veto_to_eu = disrupted & non_eu_to_eu & (~is_special) & (~is_eu_carrier)

    row_veto = veto_bookend_domestic | veto_middle_domestic | veto_to_eu

    # A veto on ANY leg forces the WHOLE connection ineligible
    connection_veto = row_veto.groupby(df[uid_col], sort=False).transform("any")

    return df["EUEligible"] & (~connection_veto)


def _enforce_connection_level_consistency(df: pd.DataFrame) -> pd.Series:
    """
    Guarantees EUEligible is identical across every row sharing the same
    ConnectionID, regardless of which upstream rule set it. Uses "any" so a
    single eligible leg makes the whole connection eligible -- consistent with
    Rule 6's existing propagation behavior. Safe to call even when the value
    is already uniform per group (no-op in that case).
    """
    return df.groupby("ConnectionID", sort=False)["EUEligible"].transform("any").astype(bool)

def process_table():
    con = duckdb.connect(DB_PATH)
    ref_data = ReferenceData(con)

    con.execute(f"""
        UPDATE {SOURCE_TABLE}
        SET FlightNumber = OperatingFlightNo,
            AirlineCode = LEFT(OperatingFlightNo, 2)
        WHERE OperatingFlightNo IS NOT NULL;
    """)

    df = con.execute(f"""
        SELECT * FROM {SOURCE_TABLE}
        ORDER BY ConnectionID, LegNo
    """).fetchdf()

    # Rules 1-6 for everyone
    df["EUEligible"] = _vectorized_eligibility(df, ref_data)

    # Rule 7 veto applies only to multi-leg journeys
    multileg_mask = df["IsSingleFlight"] == 0
    df.loc[multileg_mask, "EUEligible"] = _apply_disruption_veto(
        df.loc[multileg_mask], ref_data
    )

    df["EUEligible"] = _enforce_connection_level_consistency(df)

    df.loc[~df["EUEligible"], "IsTimeLimitL1"] = 0
    df.loc[~df["EUEligible"], "IsTimeLimitL2"] = 0

    con.execute(f"DROP TABLE IF EXISTS {TARGET_TABLE}")
    con.execute(f"CREATE TABLE {TARGET_TABLE} AS SELECT * FROM df")
    con.close()


if __name__ == "__main__":
    process_table()