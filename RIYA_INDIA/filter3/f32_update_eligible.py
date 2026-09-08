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

# ASSUMPTION: "Special Airline" in Rule 6 (disruption override, below) maps to
# SPECIAL_NON_EU_CARRIERS, same assumption the old veto made. If the spec
# actually meant the LH/XQ/QR set, change this one line to SPECIAL_CARRIERS.
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


def _compute_eu_eligibility(df: pd.DataFrame, ref_data: ReferenceData) -> pd.Series:
    """
    Single-pass EU261 eligibility engine. Replaces the previous
    _vectorized_eligibility + _apply_disruption_veto split with one function
    that evaluates the priority-ordered rules below and broadcasts each
    verdict to every leg sharing a ConnectionID, so eligibility is always
    uniform within a journey.

    Rules 1-5 (priority lowest -> highest; a later rule overwrites an earlier
    one wherever both conditions hold for the same connection). Any
    connection matched by NONE of these five is left NULL, not defaulted to
    False:

        Rule 2 - FirstLeg (LegNo=1) departs an EU airport               -> True
        Rule 5 - Bookend Non-EU -> Non-EU (FirstLeg from & LastLeg to)  -> False
        Rule 3 - Any leg operated by a SPECIAL_NON_EU_CARRIER           -> True
        Rule 4 - FirstLeg departs TR on LH/XQ/QR                        -> True
        Rule 1 - Bookend TR -> TR (FirstLeg from & LastLeg to)          -> False (absolute)

    Rule 6 - disruption override - only runs on candidate rows: those still
    NULL after Rules 1-5, AND belonging to a multi-leg journey
    (IsSingleFlight == 0). Only the EARLIEST disrupted leg among a
    candidate connection's rows decides the override — later disrupted legs
    in the same journey are ignored. If that leg's From/To combination isn't
    one of the cases below, Rule 6 makes no determination and the row stays
    NULL.

        Disrupted FirstLeg (LegNo=1):
            NonEU -> NonEU                      => False
            NonEU -> EU, carrier EU/Special     => True   else False

        Disrupted LastLeg:
            EU -> EU                            => True
            NonEU -> EU, carrier EU/Special     => True   else False

        Disrupted middle leg (FirstLeg < LegNo < LastLeg):
            From EU (any To)                    => True
            NonEU -> EU, carrier EU/Special     => True   else False
            NonEU -> NonEU, carrier EU/Special  => True   else False

    Anything still NULL after Rule 6 (not disrupted at all, or disrupted but
    an uncovered From/To combination) is finalized to False.

    ASSUMPTION: Rule 1 (TR->TR) is folded into the Rules 1-5 pass as an
    absolute override, since a domestic-Turkey bookend is structurally
    outside EU261 regardless of disruption, so Rule 6 never runs on those
    rows (they're already False, not NULL, going into Rule 6).
    """
    if df.empty:
        return pd.Series(dtype=bool)

    uid_col = "ConnectionID"
    uid = df[uid_col]
    grp = df.groupby(uid_col, sort=False)

    # ---- FirstLeg / LastLeg attributes, broadcast to every row in the connection ----
    first_idx = grp["LegNo"].idxmin()
    last_idx = grp["LegNo"].idxmax()

    first_from_by_uid = df.loc[first_idx, "FromAirport"]
    first_from_by_uid.index = df.loc[first_idx, uid_col].values
    last_to_by_uid = df.loc[last_idx, "ToAirport"]
    last_to_by_uid.index = df.loc[last_idx, uid_col].values

    first_from = uid.map(first_from_by_uid)
    last_to = uid.map(last_to_by_uid)

    first_from_is_tr = first_from.isin(ref_data.tr_airports)
    last_to_is_tr = last_to.isin(ref_data.tr_airports)
    first_from_is_eu = first_from.isin(ref_data.eu_airports)
    last_to_is_eu = last_to.isin(ref_data.eu_airports)

    max_leg = grp["LegNo"].transform("max")
    is_first_row = df["LegNo"] == 1
    is_last_row = df["LegNo"] == max_leg
    is_middle_row = (~is_first_row) & (~is_last_row)

    # ---- Row-level airline flags, needed for Rule 3 / Rule 4 ("any leg") ----
    airline = df["AirlineCode"]
    is_special_non_eu = airline.isin(SPECIAL_NON_EU_CARRIERS)
    is_special_lh_xq_qr = airline.isin(SPECIAL_CARRIERS)

    rule3_any_leg = is_special_non_eu.groupby(uid, sort=False).transform("any")
    rule4_any_leg = (
        (is_first_row & first_from_is_tr & is_special_lh_xq_qr)
        .groupby(uid, sort=False)
        .transform("any")
    )

    # ---- Rules 2, 5, 3, 4, 1 in increasing priority. Nullable boolean: a
    # connection matched by none of these five stays <NA>, not False. ----
    base = pd.Series(pd.NA, index=df.index, dtype="boolean")
    base = base.mask(first_from_is_eu, True)                          # Rule 2
    base = base.mask((~first_from_is_eu) & (~last_to_is_eu), False)   # Rule 5
    base = base.mask(rule3_any_leg, True)                             # Rule 3
    base = base.mask(rule4_any_leg, True)                             # Rule 4
    base = base.mask(first_from_is_tr & last_to_is_tr, False)         # Rule 1 (absolute)

    # ---- Rule 6: disruption override, candidate rows only (base still NULL) ----
    disrupted = df["Status"].astype(str).str.upper().str.strip().isin(DISRUPTED_STATUSES)
    from_is_eu_row = df["FromAirport"].isin(ref_data.eu_airports)
    to_is_eu_row = df["ToAirport"].isin(ref_data.eu_airports)
    carrier_ok_row = airline.isin(ref_data.eu_carriers) | airline.isin(DISRUPTION_SPECIAL_CARRIERS)

    verdict = pd.Series(pd.NA, index=df.index, dtype="boolean")

    m = disrupted & is_first_row
    verdict = verdict.mask(m & (~from_is_eu_row) & (~to_is_eu_row), False)
    verdict = verdict.mask(m & (~from_is_eu_row) & to_is_eu_row, carrier_ok_row)

    m = disrupted & is_last_row
    verdict = verdict.mask(m & from_is_eu_row & to_is_eu_row, True)
    verdict = verdict.mask(m & (~from_is_eu_row) & to_is_eu_row, carrier_ok_row)

    m = disrupted & is_middle_row
    verdict = verdict.mask(m & from_is_eu_row, True)
    verdict = verdict.mask(m & (~from_is_eu_row) & to_is_eu_row, carrier_ok_row)
    verdict = verdict.mask(m & (~from_is_eu_row) & (~to_is_eu_row), carrier_ok_row)

    # Candidate rows: EUEligible still NULL after Rules 1-5, on a multi-leg journey.
    is_multileg = df["IsSingleFlight"] == 0
    candidate_mask = base.isna() & is_multileg

    # Only the earliest disrupted leg among a candidate connection's rows
    # decides the Rule 6 verdict for that whole connection.
    disrupted_idx = df.index[disrupted & candidate_mask]
    if len(disrupted_idx):
        first_disrupted_idx_by_uid = (
            df.loc[disrupted_idx].groupby(uid_col, sort=False)["LegNo"].idxmin()
        )
        earliest_verdict = verdict.loc[first_disrupted_idx_by_uid.values]
        earliest_verdict.index = first_disrupted_idx_by_uid.index  # -> ConnectionID
        connection_verdict = uid.map(earliest_verdict)
    else:
        connection_verdict = pd.Series(pd.NA, index=df.index, dtype="boolean")

    apply_rule6 = candidate_mask & connection_verdict.notna()
    eligible = base.mask(apply_rule6, connection_verdict.fillna(False).astype(bool))

    # Anything still NULL after Rule 6 (not disrupted, or an uncovered
    # disrupted From/To combination) is finalized to False.
    return eligible.fillna(False).astype(bool)


def _enforce_connection_level_consistency(df: pd.DataFrame) -> pd.Series:
    """
    Guarantees EUEligible is identical across every row sharing the same
    ConnectionID, regardless of which upstream rule set it. Uses "any" so a
    single eligible leg makes the whole connection eligible. Safe to call
    even when the value is already uniform per group (no-op in that case).
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

    df["EUEligible"] = _compute_eu_eligibility(df, ref_data)
    df["EUEligible"] = _enforce_connection_level_consistency(df)

    df.loc[~df["EUEligible"], "IsTimeLimitL1"] = 0
    df.loc[~df["EUEligible"], "IsTimeLimitL2"] = 0

    con.execute(f"DROP TABLE IF EXISTS {TARGET_TABLE}")
    con.execute(f"CREATE TABLE {TARGET_TABLE} AS SELECT * FROM df")
    con.close()


if __name__ == "__main__":
    process_table()