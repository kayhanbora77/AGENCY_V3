import pandas as pd
import duckdb
import logging
from typing import FrozenSet

# Configure logging
logger = logging.getLogger(__name__)

# Constants
SOURCE_TABLE = "RIYACANADA_OPERATINGFN"
TARGET_TABLE = "RIYACANADA_OPERATINGFN_RESULT"

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
    
        # ---- Rule 7: Re-evaluate Rule 3 (special carriers) ----
    has_special_carrier = is_special_non_eu.groupby(uid, sort=False).transform("any")
    rule7_candidates = eligible.fillna(False) & has_special_carrier

    if rule7_candidates.any():
        disrupted_in_candidates = disrupted & rule7_candidates

        # Pre-compute connection-level bookend traits indexed by ConnectionID
        first_from_eu_conn = first_from_is_eu.groupby(uid, sort=False).first()
        last_to_eu_conn = last_to_is_eu.groupby(uid, sort=False).first()
        pure_non_eu_conn = (~first_from_eu_conn) & (~last_to_eu_conn)

        # ----------------------------------------------------------
        # Case A: There is at least one disruption in the connection
        # ----------------------------------------------------------
        if disrupted_in_candidates.any():
            first_disrupted_idx_by_uid = (
                df.loc[disrupted_in_candidates]
                  .groupby(uid_col, sort=False)["LegNo"]
                  .idxmin()
            )

            # Remap everything to ConnectionID index to guarantee alignment
            d_airline = df.loc[first_disrupted_idx_by_uid.values, "AirlineCode"]
            d_airline.index = first_disrupted_idx_by_uid.index

            d_from = df.loc[first_disrupted_idx_by_uid.values, "FromAirport"]
            d_from.index = first_disrupted_idx_by_uid.index

            d_to = df.loc[first_disrupted_idx_by_uid.values, "ToAirport"]
            d_to.index = first_disrupted_idx_by_uid.index

            d_legno = df.loc[first_disrupted_idx_by_uid.values, "LegNo"]
            d_legno.index = first_disrupted_idx_by_uid.index

            total_legs = max_leg.loc[first_disrupted_idx_by_uid.values]
            total_legs.index = first_disrupted_idx_by_uid.index

            is_disrupted_special = d_airline.isin(SPECIAL_NON_EU_CARRIERS)

            # ---- Decision tree (if-elif chain) ----
            # 1. If disrupted leg is special carrier -> True (and stop)
            keep_true = is_disrupted_special.copy()

            # Explicitly reindex connection-level traits to the disrupted subset
            ffeu = first_from_eu_conn.reindex(keep_true.index).fillna(False)
            pneu = pure_non_eu_conn.reindex(keep_true.index).fillna(False)

            # 2. elif First leg departs EU -> True (and stop)
            mask = ~keep_true
            keep_true.loc[mask & ffeu] = True

            # 3. elif Pure Non-EU -> Non-EU bookend -> False (and stop)
            mask = ~keep_true
            keep_true.loc[mask & pneu] = False

            # 4. elif Non-EU -> EU bookend -> apply Rule-6 style logic
            # Only evaluate if not resolved by 1-3
            remaining = (~keep_true) & ~pneu
            if remaining.any():
                from_eu = d_from.isin(ref_data.eu_airports)
                to_eu   = d_to.isin(ref_data.eu_airports)
                carrier_ok = (
                    d_airline.isin(ref_data.eu_carriers) |
                    d_airline.isin(DISRUPTION_SPECIAL_CARRIERS)
                )

                leg_first  = d_legno == 1
                leg_last   = d_legno == total_legs
                leg_middle = (~leg_first) & (~leg_last)

                # Initialize with False, indexed by the full keep_true index to avoid alignment errors
                rule6_style = pd.Series(False, index=keep_true.index)

                # First leg
                m = leg_first & remaining
                rule6_style.loc[m & (~from_eu) & to_eu] = carrier_ok.loc[m & (~from_eu) & to_eu]

                # Last leg
                m = leg_last & remaining
                rule6_style.loc[m & from_eu & to_eu] = True
                rule6_style.loc[m & (~from_eu) & to_eu] = carrier_ok.loc[m & (~from_eu) & to_eu]

                # Middle leg
                m = leg_middle & remaining
                rule6_style.loc[m & from_eu] = True
                rule6_style.loc[m & (~from_eu) & to_eu] = carrier_ok.loc[m & (~from_eu) & to_eu]
                rule6_style.loc[m & (~from_eu) & (~to_eu)] = carrier_ok.loc[m & (~from_eu) & (~to_eu)]

                keep_true = keep_true | rule6_style

            # Apply the final decision to the whole connection
            failing = keep_true.index[~keep_true]
            if len(failing):
                eligible = eligible.mask(uid.isin(failing), False)

        # ----------------------------------------------------------
        # Case B: Special-carrier connection with NO disruption
        # ----------------------------------------------------------
        else:
            # Only force False on pure Non-EU → Non-EU bookends
            # (Note: Rule 5 already catches this upstream, but keeping it for strict spec adherence)
            pure_non_eu_row = (~first_from_is_eu) & (~last_to_is_eu)
            pure_non_eu_conn_mask = pure_non_eu_row & rule7_candidates
            if pure_non_eu_conn_mask.any():
                eligible = eligible.mask(pure_non_eu_conn_mask, False)

    # Finalise any remaining NULLs
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

    try:
        # ========================================================
        # Load reference data
        # ========================================================
        ref_data = ReferenceData(con)

        # ========================================================
        # SOURCE COUNTS BEFORE PROCESSING
        # SOURCE TABLE IS NEVER MODIFIED
        # ========================================================
        source_total = con.execute(f"""
            SELECT COUNT(*)
            FROM {SOURCE_TABLE}
        """).fetchone()[0]

        source_eligible_true = con.execute(f"""
            SELECT COUNT(*)
            FROM {SOURCE_TABLE}
            WHERE EUEligible = TRUE
        """).fetchone()[0]

        source_eligible_false = con.execute(f"""
            SELECT COUNT(*)
            FROM {SOURCE_TABLE}
            WHERE EUEligible = FALSE
        """).fetchone()[0]

        source_eligible_null = con.execute(f"""
            SELECT COUNT(*)
            FROM {SOURCE_TABLE}
            WHERE EUEligible IS NULL
        """).fetchone()[0]

        # ========================================================
        # Print SOURCE status
        # ========================================================
        print("\n" + "=" * 70)
        print("SOURCE TABLE - BEFORE PROCESSING")
        print("=" * 70)
        print(f"Table               : {SOURCE_TABLE}")
        print(f"Total Rows          : {source_total:,}")
        print(f"EUEligible TRUE     : {source_eligible_true:,}")
        print(f"EUEligible FALSE    : {source_eligible_false:,}")
        print(f"EUEligible NULL     : {source_eligible_null:,}")
        print("=" * 70)

        # ========================================================
        # Read SOURCE TABLE
        # ========================================================
        df = con.execute(f"""
            SELECT *
            FROM {SOURCE_TABLE}
            ORDER BY ConnectionID, LegNo
        """).fetchdf()

        print(f"\nLoaded {len(df):,} rows for processing.")
        df["IsTimeLimitL1"] = pd.to_numeric(df["IsTimeLimitL1"], errors="coerce").astype("Int64")
        df["IsTimeLimitL2"] = pd.to_numeric(df["IsTimeLimitL2"], errors="coerce").astype("Int64")
        # ========================================================
        # Reset EUEligible before recalculation
        # ========================================================
        df["EUEligible"] = pd.NA

        # ========================================================
        # Update FlightNumber and AirlineCode from OperatingFlightNo
        # ========================================================
        mask = (df["OperatingFlightNo"].notna() & (df["OperatingFlightNo"].astype(str).str.strip() != ""))

        df.loc[mask, "FlightNumber"] = (df.loc[mask, "OperatingFlightNo"])

        df.loc[mask, "AirlineCode"] = (
            df.loc[mask, "OperatingFlightNo"]
            .astype(str)
            .str.strip()
            .str[:2]
        )

        print(f"OperatingFlightNo updates: {mask.sum():,} rows")

        df["EUEligible"] = _compute_eu_eligibility(df, ref_data)
        df["EUEligible"] = _enforce_connection_level_consistency(df)

        df.loc[~df["EUEligible"], "IsTimeLimitL1"] = 0
        df.loc[~df["EUEligible"], "IsTimeLimitL2"] = 0

        # ========================================================
        # Create TARGET TABLE
        # ========================================================
        con.register("processed_df", df)

        con.execute(f"""
            CREATE OR REPLACE TABLE {TARGET_TABLE} AS
            SELECT *
            FROM processed_df
        """)

        con.unregister("processed_df")
        del df

        # ========================================================
        # TARGET COUNTS AFTER PROCESSING
        # ========================================================
        target_total = con.execute(f"""
            SELECT COUNT(*)
            FROM {TARGET_TABLE}
        """).fetchone()[0]

        target_eligible_true = con.execute(f"""
            SELECT COUNT(*)
            FROM {TARGET_TABLE}
            WHERE EUEligible = TRUE
        """).fetchone()[0]

        target_eligible_false = con.execute(f"""
            SELECT COUNT(*)
            FROM {TARGET_TABLE}
            WHERE EUEligible = FALSE
        """).fetchone()[0]

        target_eligible_null = con.execute(f"""
            SELECT COUNT(*)
            FROM {TARGET_TABLE}
            WHERE EUEligible IS NULL
        """).fetchone()[0]

        # ========================================================
        # SOURCE vs TARGET ROW COUNT COMPARISON
        # ========================================================
        row_difference = target_total - source_total

        # ========================================================
        # EUEligible BEFORE vs AFTER COMPARISON
        # ========================================================
        print("\n" + "=" * 70)
        print("SOURCE vs TARGET - EUEligible COMPARISON")
        print("=" * 70)

        print(
            f"{'Status':<25}"
            f"{'SOURCE (Before)':>20}"
            f"{'TARGET (After)':>20}"
            f"{'Difference':>15}"
        )

        print("-" * 80)

        print(
            f"{'Total Rows':<25}"
            f"{source_total:>20,}"
            f"{target_total:>20,}"
            f"{row_difference:>+15,}"
        )

        print(
            f"{'EUEligible = TRUE':<25}"
            f"{source_eligible_true:>20,}"
            f"{target_eligible_true:>20,}"
            f"{target_eligible_true - source_eligible_true:>+15,}"
        )

        print(
            f"{'EUEligible = FALSE':<25}"
            f"{source_eligible_false:>20,}"
            f"{target_eligible_false:>20,}"
            f"{target_eligible_false - source_eligible_false:>+15,}"
        )

        print(
            f"{'EUEligible = NULL':<25}"
            f"{source_eligible_null:>20,}"
            f"{target_eligible_null:>20,}"
            f"{target_eligible_null - source_eligible_null:>+15,}"
        )

        print("=" * 80)

        # ========================================================
        # Row count validation
        # ========================================================
        if source_total == target_total:
            print("\nSUCCESS: SOURCE and TARGET row counts MATCH.")
        else:
            print("\nWARNING: SOURCE and TARGET row counts DO NOT MATCH!")

        print(f"\nTarget table created successfully: {TARGET_TABLE}")

    except Exception as e:
        print(f"\nERROR: {e}")
        raise

    finally:
        con.close()

if __name__ == "__main__":
    process_table()