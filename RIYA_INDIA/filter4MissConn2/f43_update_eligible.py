import pandas as pd
import duckdb
import logging
from typing import FrozenSet

# Configure logging
logger = logging.getLogger(__name__)

# Constants
SOURCE_TABLE = "RIYAINDIA_MISSCONNECTION"
TARGET_TABLE = "RIYAINDIA_MISSCONNECTION_RESULT"

DB_PATH = r"C:\DuckDB\my_db.duckdb"

# "Special Airlines" per the spec's "(EU Carrier or Special Airlines)" clause.
# ASSUMPTION: same carrier set used as DISRUPTION_SPECIAL_CARRIERS in the
# CANCELDIVERTDELAY script (the other "similar task"). Change this one line
# if the spec actually means the LH/XQ/QR set instead.
SPECIAL_NON_EU_CARRIERS: FrozenSet[str] = frozenset({"BA", "TK", "PC", "JU", "FH", "VF", "VS", "XQ"})
SPECIAL_AIRLINES: FrozenSet[str] = SPECIAL_NON_EU_CARRIERS

# Statuses that trigger a leg to be considered for eligibility purposes.
CANCEL_DIVERT_STATUSES: FrozenSet[str] = frozenset({"CANCEL", "DIVERSION"})
DELAY_STATUS = "DELAY"
# ASSUMPTION: the connection-level *trigger* (step 2's outer "if") only
# counts a Delay when it's on the LAST leg, per the spec: "any leg status
# in (cancel,diver) or last leg status=Delay or any leg IsMissConnection".
# But the *lookup* for "the first disrupted leg" (used once a connection has
# already qualified) counts Delay on ANY leg, per: "find first leg which
# one status in (cancel,diver,delay) or IsMissConnection." These two are
# taken literally from the spec even though they're inconsistent with each
# other -- flagging in case that was a spec typo rather than intentional.
LOOKUP_DISRUPTED_STATUSES: FrozenSet[str] = frozenset({"CANCEL", "DIVERSION", "DELAY"})


class ReferenceData:
    __slots__ = ("eu_airports", "eu_carriers")

    def __init__(self, con: duckdb.DuckDBPyConnection):
        self.eu_airports: frozenset = self._load_airports(con)
        self.eu_carriers: frozenset = self._load_carriers(con)
        logger.info(
            f"Loaded {len(self.eu_airports):,} EU airports, {len(self.eu_carriers):,} EU carriers"
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


def _fetch_candidate_rows(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    Step 1 of the spec: pull every row belonging to a ConnectionID that has
    AT LEAST ONE leg flagged IsMissConnection, or with Status in
    (cancel, diversion, Delay) -- anywhere in the connection. This is a
    superset of the true "candidate" definition used in eligibility (which
    restricts Delay to the last leg); the extra rows it brings in are
    resolved to EUEligible=False downstream since they don't meet the
    stricter trigger.
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


def _compute_eu_eligibility(df: pd.DataFrame, ref_data: ReferenceData) -> pd.Series:
    """
    EU261 eligibility for RIYAINDIA_MISSCONNECTION, per spec step 2.

    Only rows in a "candidate" connection get a real verdict; everything
    else finalizes to False. A connection is a candidate when:
        any leg Status in (CANCEL, DIVERSION)
        OR last-leg Status == DELAY
        OR any leg IsMissConnection == True

    Within a candidate connection:

        Rule A - FirstLeg (LegNo=1) departs an EU airport -> True,
                 broadcast to every leg in the connection.

        Rule B - FirstLeg departs a Non-EU airport -> find the EARLIEST
                 disrupted leg (Status in CANCEL/DIVERSION/DELAY, or
                 IsMissConnection True -- any leg, any position) and apply,
                 based on that leg's position:

            Disrupted FirstLeg (LegNo=1) [guaranteed From=NonEU here]:
                ToAirport=NonEU                        => False
                ToAirport=EU, carrier EU/Special       => True   else False

            Disrupted LastLeg (LegNo=Max):
                FromAirport=EU                         => True
                FromAirport=NonEU, ToAirport=EU,
                    carrier EU/Special                 => True   else False
                (FromAirport=NonEU, ToAirport=NonEU -> uncovered, stays NULL
                 -> finalizes False)

            Disrupted middle leg (1 < LegNo < Max):
                FromAirport=EU                         => True
                FromAirport=NonEU, ToAirport=EU,
                    carrier EU/Special                 => True   else False
                FromAirport=NonEU, ToAirport=NonEU,
                    carrier EU/Special                 => True   else False

        If a candidate connection has no leg matching the disrupted-leg
        lookup (e.g. it only qualified via a non-last-leg Delay under the
        stricter definition below) or the earliest disrupted leg's From/To
        combination isn't covered above, it stays NULL and finalizes False.
    """
    if df.empty:
        return pd.Series(dtype=bool)

    uid_col = "ConnectionID"
    uid = df[uid_col]
    grp = df.groupby(uid_col, sort=False)

    max_leg = grp["LegNo"].transform("max")
    is_first_row = df["LegNo"] == 1
    is_last_row = df["LegNo"] == max_leg

    status_upper = df["Status"].astype(str).str.upper().str.strip()
    is_cancel_or_divert = status_upper.isin(CANCEL_DIVERT_STATUSES)
    is_delay = status_upper.eq(DELAY_STATUS)
    is_miss = df["IsMissConnection"].fillna(False).astype(bool)

    # ---- Connection-level trigger ----
    trigger_any_leg = (is_cancel_or_divert | is_miss).groupby(uid, sort=False).transform("any")
    trigger_last_delay = (is_delay & is_last_row).groupby(uid, sort=False).transform("any")
    is_candidate_conn = trigger_any_leg | trigger_last_delay

    # ---- FirstLeg FromAirport, broadcast to every row in the connection ----
    first_idx = grp["LegNo"].idxmin()
    first_from_by_uid = df.loc[first_idx, "FromAirport"]
    first_from_by_uid.index = df.loc[first_idx, uid_col].values
    first_from = uid.map(first_from_by_uid)
    first_from_is_eu = first_from.isin(ref_data.eu_airports)

    eligible = pd.Series(pd.NA, index=df.index, dtype="boolean")

    # Rule A
    eligible = eligible.mask(is_candidate_conn & first_from_is_eu, True)

    # Rule B: candidate connections whose FirstLeg is Non-EU and not yet decided
    needs_lookup = is_candidate_conn & (~first_from_is_eu) & eligible.isna()
    is_disrupted_leg = is_cancel_or_divert | is_delay | is_miss
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

        leg_first = d_legno == 1
        leg_last = d_legno == d_maxleg
        leg_middle = (~leg_first) & (~leg_last)

        verdict = pd.Series(pd.NA, index=d_legno.index, dtype="boolean")

        # Disrupted first leg (From is guaranteed Non-EU in this branch)
        m = leg_first
        verdict = verdict.mask(m & (~to_eu), False)
        verdict = verdict.mask(m & to_eu, carrier_ok)

        # Disrupted last leg
        m = leg_last
        verdict = verdict.mask(m & from_eu, True)
        verdict = verdict.mask(m & (~from_eu) & to_eu, carrier_ok)

        # Disrupted middle leg
        m = leg_middle
        verdict = verdict.mask(m & from_eu, True)
        verdict = verdict.mask(m & (~from_eu) & to_eu, carrier_ok)
        verdict = verdict.mask(m & (~from_eu) & (~to_eu), carrier_ok)

        connection_verdict = uid.map(verdict)
        eligible = eligible.mask(needs_lookup & connection_verdict.notna(), connection_verdict)

    return eligible.fillna(False).astype(bool)


def _enforce_connection_level_consistency(df: pd.DataFrame) -> pd.Series:
    """
    Guarantees EUEligible is identical across every row sharing the same
    ConnectionID. No-op given the broadcast logic above, kept as a safety
    net the same way the CANCELDIVERTDELAY script does.
    """
    return df.groupby("ConnectionID", sort=False)["EUEligible"].transform("any").astype(bool)


def process_table():
    con = duckdb.connect(DB_PATH)
    ref_data = ReferenceData(con)

    df = _fetch_candidate_rows(con)

    df["EUEligible"] = _compute_eu_eligibility(df, ref_data)
    df["EUEligible"] = _enforce_connection_level_consistency(df)

    df.loc[~df["EUEligible"], "IsTimeLimitL1"] = 0
    df.loc[~df["EUEligible"], "IsTimeLimitL2"] = 0

    # Source table is left untouched -- only the result table is written.
    con.execute(f"DROP TABLE IF EXISTS {TARGET_TABLE}")
    con.execute(f"CREATE TABLE {TARGET_TABLE} AS SELECT * FROM df")
    con.close()


if __name__ == "__main__":
    process_table()