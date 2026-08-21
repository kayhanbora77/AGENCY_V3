import duckdb

DB_PATH = r"C:\DuckDB\my_db.duckdb"

SOURCE_TABLE = "RIYA_USA_SPLIT9"
TARGET_TABLE = "RIYA_USA_SPLIT10"
REJECT_TABLE = "RIYA_USA_REJECT"

REJECTION_REASON = "AP1=AP3"

con = duckdb.connect(DB_PATH)

# ------------------------------------------------------------
# Reject condition
# ------------------------------------------------------------
reject_condition = """
    AIRPORT1 = AIRPORT3
"""

# ------------------------------------------------------------
# Target condition
#
# Keep the row when:
# - AIRPORT1 is NULL
# - OR AIRPORT3 is NULL
# - OR AIRPORT1 and AIRPORT3 are different
#
# Reject ONLY when AIRPORT1 = AIRPORT3
# ------------------------------------------------------------
target_condition = """
    AIRPORT1 IS NULL
    OR AIRPORT3 IS NULL
    OR AIRPORT1 <> AIRPORT3
"""


# ------------------------------------------------------------
# 1. Get source count
# ------------------------------------------------------------
source_count = con.execute(f"""
    SELECT COUNT(*)
    FROM {SOURCE_TABLE}
""").fetchone()[0]

print(f"Source rows                 : {source_count:,}")


# ------------------------------------------------------------
# 2. Count rows that will be rejected
# ------------------------------------------------------------
reject_count = con.execute(f"""
    SELECT COUNT(*)
    FROM {SOURCE_TABLE}
    WHERE {reject_condition}
""").fetchone()[0]

print(f"Rows to reject              : {reject_count:,}")


# ------------------------------------------------------------
# 3. Insert rejected records
# ------------------------------------------------------------
con.execute(f"""
    INSERT INTO {REJECT_TABLE}
    SELECT
        *,
        '{REJECTION_REASON}' AS REJECTIONREASON
    FROM {SOURCE_TABLE}
    WHERE {reject_condition}
""")

print(f"Rejected records inserted   : {reject_count:,}")


# ------------------------------------------------------------
# 4. Recreate target table
# ------------------------------------------------------------
con.execute(f"""
    DROP TABLE IF EXISTS {TARGET_TABLE}
""")

con.execute(f"""
    CREATE TABLE {TARGET_TABLE} AS
    SELECT *
    FROM {SOURCE_TABLE}
    WHERE {target_condition}
""")


# ------------------------------------------------------------
# 5. Get target count
# ------------------------------------------------------------
target_count = con.execute(f"""
    SELECT COUNT(*)
    FROM {TARGET_TABLE}
""").fetchone()[0]

print(f"Valid target rows           : {target_count:,}")


# ------------------------------------------------------------
# 6. Count rejected rows currently in reject table
# ------------------------------------------------------------
rejected_total_count = con.execute(f"""
    SELECT COUNT(*)
    FROM {REJECT_TABLE}
    WHERE REJECTIONREASON = '{REJECTION_REASON}'
""").fetchone()[0]

print(f"Rejected rows in table      : {rejected_total_count:,}")


# ------------------------------------------------------------
# 7. Expected target count
# ------------------------------------------------------------
expected_target_count = source_count - reject_count

print(f"Expected target rows        : {expected_target_count:,}")


# ------------------------------------------------------------
# 8. Validation
# ------------------------------------------------------------
print()
print("--------------------------------------------------")
print("VALIDATION")
print("--------------------------------------------------")

print(f"Source                      : {source_count:,}")
print(f"Rejected this run           : {reject_count:,}")
print(f"Expected target             : {expected_target_count:,}")
print(f"Target                      : {target_count:,}")
print(f"Rejected + Target           : {reject_count + target_count:,}")
print(f"Difference                  : {source_count - (reject_count + target_count):,}")

if source_count == reject_count + target_count:
    print("STATUS                      : OK - Counts match")
else:
    print("STATUS                      : WARNING - Counts do NOT match")


# ------------------------------------------------------------
# 9. Additional reject validation
# ------------------------------------------------------------
print()
print("--------------------------------------------------")
print("REJECT VALIDATION")
print("--------------------------------------------------")

if rejected_total_count >= reject_count:
    print("STATUS                      : OK - Reject records exist")
else:
    print("STATUS                      : WARNING - Reject count is lower than expected")


# ------------------------------------------------------------
# 10. Direct source validation
# ------------------------------------------------------------
direct_reject_count = con.execute(f"""
    SELECT COUNT(*)
    FROM {SOURCE_TABLE}
    WHERE {reject_condition}
""").fetchone()[0]

direct_target_count = con.execute(f"""
    SELECT COUNT(*)
    FROM {SOURCE_TABLE}
    WHERE {target_condition}
""").fetchone()[0]

print()
print("--------------------------------------------------")
print("DIRECT SOURCE VALIDATION")
print("--------------------------------------------------")

print(f"Source rows                 : {source_count:,}")
print(f"Reject rows                 : {direct_reject_count:,}")
print(f"Target rows                 : {direct_target_count:,}")
print(f"Reject + Target             : {direct_reject_count + direct_target_count:,}")

if source_count == direct_reject_count + direct_target_count:
    print("STATUS                      : OK - COMPLETE 1:1 SPLIT")
else:
    print("STATUS                      : WARNING - SPLIT DOES NOT MATCH")


con.close()