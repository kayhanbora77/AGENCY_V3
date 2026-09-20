import pandas as pd
import duckdb
import logging
from typing import FrozenSet

# Configure logging
logger = logging.getLogger(__name__)

# Constants
SOURCE_TABLE = "TA_STANDARD_RIYAINDIA_VF"
TARGET_TABLE = "TA_STANDARD_RIYAINDIA_VF_RESULT"

DB_PATH = r"C:\DuckDB\my_db.duckdb"

# "Special Airlines" carve-out used in Priority2 and in Priority5's
# connection-level carrier_ok checks.
SPECIAL_NON_EU_CARRIERS: FrozenSet[str] = frozenset({"BA", "TK", "PC", "JU", "FH", "VF", "VS", "XQ"})
SPECIAL_AIRLINES: FrozenSet[str] = SPECIAL_NON_EU_CARRIERS

# Priority3's narrower "Turkey departure" carve-out. Distinct from
# SPECIAL_AIRLINES above -- only these three carriers qualify for Priority3.
SPECIAL_TR_CARRIERS: FrozenSet[str] = frozenset({"LH", "XQ", "QR"})

# Leg-level statuses that (together with IsMissConnection) make up the
# "DisruptedLeg" predicate.
CANCEL_DIVERT_STATUSES: FrozenSet[str] = frozenset({"CANCEL", "DIVERSION"})
DELAY_STATUS = "DELAY"


class ReferenceData:
    __slots__ = ("eu_airports", "eu_carriers", "tr_airports")

    def __init__(self, con: duckdb.DuckDBPyConnection):
        self.eu_airports: frozenset = self._load_airports(con)
        self.eu_carriers: frozenset = self._load_carriers(con)
        self.tr_airports: frozenset = self._load_country_airports(con, "TR")
        logger.info(
            f"Loaded {len(self.eu_airports):,} EU airports, "
            f"{len(self.eu_carriers):,} EU carriers, "
            f"{len(self.tr_airports):,} TR airports"
        )

    @staticmethod
    def _load_airports(con) -> frozenset:
        rows = con.execute("""
            SELECT CodeIataAirport
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
    def _load_country_airports(con, iso2: str) -> frozenset:
        """Airports whose country is `iso2` -- used for Priority3's
        'FromAirport=TR' departure check."""
        rows = con.execute(
            "SELECT CodeIataAirport FROM AIRPORTS WHERE CodeIso2Country = ?",
            [iso2],
        ).fetchall()
        return frozenset(r[0].strip().upper() for r in rows if r and r[0])


def _fetch_candidate_rows(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    Coarse SQL-level pre-filter: pull every row belonging to a ConnectionID
    that has AT LEAST ONE leg flagged IsMissConnection, or with Status in
    (cancel, diversion, Delay) -- anywhere in the connection. This is a
    superset of the true "Data" definition used for eligibility (which only
    counts Delay when it's on the LastLeg); the extra rows it brings in
    resolve to EUEligible=False downstream since they don't meet the
    stricter DisruptedLeg definition.
    """
    return con.execute(f"""
        SELECT * FROM {SOURCE_TABLE}
        WHERE ConnectionID IN (
            SELECT ConnectionID FROM {SOURCE_TABLE}
            WHERE IsMissConnection IS TRUE
               OR UPPER(TRIM(Status)) = 'CANCEL'
               OR UPPER(TRIM(Status)) = 'DIVERSION'
               OR UPPER(TRIM(Status)) = 'DELAY'
        )
        ORDER BY ConnectionID, LegNo
    """).fetchdf()


def _broadcast(uid: pd.Series, source: pd.Series, idx_by_uid: pd.Series) -> pd.Series:
    """Pick `source`'s value at each connection's selected row (idx_by_uid,
    indexed by ConnectionID) and broadcast it to every row sharing that
    ConnectionID."""
    picked = source.loc[idx_by_uid.values]
    picked.index = idx_by_uid.index
    return uid.map(picked)


def _compute_eu_eligibility(df: pd.DataFrame, ref_data: ReferenceData) -> pd.Series:
    """
    EU261 eligibility per the Priority1(High)..Priority5(Low) rule set.
    Priorities are evaluated in order; the highest-priority rule whose
    condition holds for a connection (or single-flight row) decides
    EUEligible. Anything not resolved True/False by Priority1-5 finalizes
    to False (rows outside the "Data" filter included).

    DisruptedLeg (per leg): Status in (CANCEL, DIVERSION), OR
    (Status == DELAY AND that leg is the connection's LastLeg), OR
    IsMissConnection == True.

    Data / candidacy:
        IsSingleFlight=1 -> that (only) leg is Disrupted
        IsSingleFlight=0 -> ANY leg in the connection is Disrupted
    A ConnectionID group of size 1 makes "any leg" and "the leg itself"
    identical, so most rules below are computed with one expression that
    covers both IsSingleFlight cases; FirstLeg == LastLeg == the only leg
    for a single flight.

    Priority1 (High): FirstLeg.FromAirport = EU AND (that connection has a
        Disrupted leg) -> True.

    Priority2: ANY leg is Disrupted AND that SAME leg's AirlineCode is in
        SPECIAL_AIRLINES -> True. (Checked per-leg, not scoped to FirstLeg;
        for a single flight this reduces to LegNo=1.)

    Priority3: FirstLeg.FromAirport = TR AND FirstLeg is Disrupted AND
        FirstLeg.AirlineCode in SPECIAL_TR_CARRIERS (LH/XQ/QR) -> True.
        (Same condition for single flights and connections -- always keyed
        off FirstLeg / LegNo=1.)

    Priority4: FirstLeg.FromAirport = NonEU AND LastLeg.ToAirport = NonEU
        -> False (whole itinerary outside the EU).

    Priority5 (Low): only reached once P1-P4 didn't fire, which means
        FirstLeg.FromAirport = NonEU and LastLeg.ToAirport = EU.
        - IsSingleFlight=1: EUEligible = True if AirlineCode is in
          (EU carriers + Special carriers) combined, else False.
        - IsSingleFlight=0: take the EARLIEST Disrupted leg and branch on
          its position (first/last/middle) and its own From/To EU status:
            DisruptedLegNo == 1 (guaranteed From=NonEU here):
                ToAirport=NonEU, carrier Special           -> True, else False
                ToAirport=EU, carrier EU or Special        -> True, else False
            DisruptedLegNo == LastLeg:
                FromAirport=EU                             -> True
                FromAirport=NonEU, ToAirport=EU,
                    carrier EU or Special                  -> True, else False
            1 < DisruptedLegNo < LastLeg:
                FromAirport=EU                             -> True
                FromAirport=NonEU, ToAirport=EU,
                    carrier EU or Special                  -> True, else False
                FromAirport=NonEU, ToAirport=NonEU,
                    carrier EU or Special                  -> True, else False
    """
    if df.empty:
        return pd.Series(dtype=bool)

    uid_col = "ConnectionID"
    uid = df[uid_col]
    grp = df.groupby(uid_col, sort=False)

    max_leg = grp["LegNo"].transform("max")
    is_last_row = df["LegNo"] == max_leg

    status_upper = df["Status"].astype(str).str.upper().str.strip()
    is_cancel_or_divert = status_upper.isin(CANCEL_DIVERT_STATUSES)
    is_delay_last = status_upper.eq(DELAY_STATUS) & is_last_row
    is_miss = df["IsMissConnection"].fillna(False).astype(bool)

    is_disrupted_leg = is_cancel_or_divert | is_delay_last | is_miss

    # ---- Data / candidacy ----
    is_candidate = is_disrupted_leg.groupby(uid, sort=False).transform("any")

    if "IsSingleFlight" in df.columns:
        is_single = df["IsSingleFlight"].fillna(0).astype(int).astype(bool)
    else:
        # ASSUMPTION: no IsSingleFlight column on the source table -- derive
        # it from leg count (a "connection" of exactly one leg is a single
        # flight). Change this if IsSingleFlight is actually a real column.
        is_single = max_leg.eq(1)

    # ---- FirstLeg / LastLeg attributes, broadcast to every row ----
    first_idx = grp["LegNo"].idxmin()
    last_idx = grp["LegNo"].idxmax()

    first_from = _broadcast(uid, df["FromAirport"], first_idx)
    first_airline = _broadcast(uid, df["AirlineCode"], first_idx)
    first_disrupted = _broadcast(uid, is_disrupted_leg, first_idx)
    last_to = _broadcast(uid, df["ToAirport"], last_idx)

    first_from_is_eu = first_from.isin(ref_data.eu_airports)
    first_from_is_tr = first_from.isin(ref_data.tr_airports)
    last_to_is_eu = last_to.isin(ref_data.eu_airports)

    eligible = pd.Series(pd.NA, index=df.index, dtype="boolean")

    # ================= Priority5 (Low) -- computed first so every ================
    # ================= higher priority below can overwrite it     ================

    # Single flight: only remaining scenario after P1-P4 is
    # FromAirport=NonEU, ToAirport=EU (P4 already vetoes NonEU->NonEU).
    # UPDATED: carrier check is now EU carriers + Special carriers combined
    # (previously EU carriers only).
    single_carrier_ok = df["AirlineCode"].isin(ref_data.eu_carriers) | df["AirlineCode"].isin(SPECIAL_AIRLINES)
    p5_single = is_single & is_candidate & (~first_from_is_eu)
    eligible = eligible.mask(p5_single, single_carrier_ok)

    # Connections: earliest Disrupted leg, branch on position + From/To EU.
    needs_lookup = (~is_single) & is_candidate & eligible.isna()
    disrupted_idx = df.index[is_disrupted_leg & needs_lookup]

    if len(disrupted_idx):
        first_disrupted_idx_by_uid = (
            df.loc[disrupted_idx].groupby(uid_col, sort=False)["LegNo"].idxmin()
        )

        d_from = df.loc[first_disrupted_idx_by_uid.values, "FromAirport"]
        d_from.index = first_disrupted_idx_by_uid.index
        d_to = df.loc[first_disrupted_idx_by_uid.values, "ToAirport"]
        d_to.index = first_disrupted_idx_by_uid.index
        d_airline = df.loc[first_disrupted_idx_by_uid.values, "AirlineCode"]
        d_airline.index = first_disrupted_idx_by_uid.index
        d_legno = df.loc[first_disrupted_idx_by_uid.values, "LegNo"]
        d_legno.index = first_disrupted_idx_by_uid.index
        d_maxleg = max_leg.loc[first_disrupted_idx_by_uid.values]
        d_maxleg.index = first_disrupted_idx_by_uid.index

        from_eu = d_from.isin(ref_data.eu_airports)
        to_eu = d_to.isin(ref_data.eu_airports)
        carrier_ok = d_airline.isin(ref_data.eu_carriers) | d_airline.isin(SPECIAL_AIRLINES)
        special_ok = d_airline.isin(SPECIAL_AIRLINES)

        leg_first = d_legno == 1
        leg_last = d_legno == d_maxleg
        leg_middle = (~leg_first) & (~leg_last)

        verdict = pd.Series(pd.NA, index=d_legno.index, dtype="boolean")

        # DisruptedLegNo == 1 (From is guaranteed NonEU here)
        m = leg_first
        verdict = verdict.mask(m & (~to_eu), special_ok)
        verdict = verdict.mask(m & to_eu, carrier_ok)

        # DisruptedLegNo == LastLeg
        # FromAirport=EU is sufficient on its own (ToAirport=EU is not
        # required for this branch to resolve True).
        m = leg_last
        verdict = verdict.mask(m & from_eu, True)
        verdict = verdict.mask(m & (~from_eu) & to_eu, carrier_ok)

        # 1 < DisruptedLegNo < LastLeg
        m = leg_middle
        verdict = verdict.mask(m & from_eu, True)
        verdict = verdict.mask(m & (~from_eu) & to_eu, carrier_ok)
        verdict = verdict.mask(m & (~from_eu) & (~to_eu), carrier_ok)

        connection_verdict = uid.map(verdict)
        eligible = eligible.mask(needs_lookup & connection_verdict.notna(), connection_verdict)

    # ================= Priority4 =================
    # Whole itinerary outside the EU. Unified: for a single flight,
    # FirstLeg == LastLeg == the only leg.
    p4_cond = is_candidate & (~first_from_is_eu) & (~last_to_is_eu)
    eligible = eligible.mask(p4_cond, False)

    # ================= Priority3 =================
    # Turkey-departure LH/XQ/QR override. Unified: for a single flight
    # "the leg" == FirstLeg.
    p3_cond = is_candidate & first_from_is_tr & first_disrupted & first_airline.isin(SPECIAL_TR_CARRIERS)
    eligible = eligible.mask(p3_cond, True)

    # ================= Priority2 =================
    # Any Disrupted leg whose OWN AirlineCode is a Special Airline
    # -> True. This is a per-leg check across the whole connection,
    # not scoped to FirstLeg only. For a single flight it reduces to the
    # only leg (LegNo=1), matching the old behavior in that case.
    leg_is_special_disrupted = is_disrupted_leg & df["AirlineCode"].isin(SPECIAL_AIRLINES)
    p2_cond = leg_is_special_disrupted.groupby(uid, sort=False).transform("any")
    eligible = eligible.mask(p2_cond, True)

    # ================= Priority1 (High) =================
    # FirstLeg departs EU. Unified across single/connection.
    p1_cond = is_candidate & first_from_is_eu
    eligible = eligible.mask(p1_cond, True)

    return eligible.fillna(False).astype(bool)

def _enforce_connection_level_consistency(df: pd.DataFrame) -> pd.Series:
    """
    Guarantees EUEligible is identical across every row sharing the same
    ConnectionID. No-op given the broadcast/group logic above (every rule
    resolves to the same value for every row in a connection); kept as a
    safety net the same way the CANCELDIVERTDELAY script does.
    """
    return df.groupby("ConnectionID", sort=False)["EUEligible"].transform("any").astype(bool)


def process_table():
    con = duckdb.connect(DB_PATH)
    ref_data = ReferenceData(con)

    df = _fetch_candidate_rows(con)

    df["EUEligible"] = _compute_eu_eligibility(df, ref_data)
    df["EUEligible"] = _enforce_connection_level_consistency(df)

    # FIX: Changed 0 to False because these columns are now strict BOOLEAN types
    df.loc[~df["EUEligible"], "IsTimeLimitL1"] = False
    df.loc[~df["EUEligible"], "IsTimeLimitL2"] = False

    # Source table is left untouched -- only the result table is written.
    con.execute(f"DROP TABLE IF EXISTS {TARGET_TABLE}")
    con.execute(f"CREATE TABLE {TARGET_TABLE} AS SELECT * FROM df")
    con.close()

if __name__ == "__main__":
    process_table()