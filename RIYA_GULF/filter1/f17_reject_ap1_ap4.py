import duckdb

DB_PATH = r"C:\DuckDB\my_db.duckdb"

SOURCE_TABLE = "RIYA_GULF_SPLIT6"
TARGET_TABLE = "RIYA_GULF_SPLIT7"
REJECT_TABLE = "RIYA_GULF_REJECT"

REJECTION_REASON = "AP1=AP4 AND AP4 IS NOT NULL"

con = duckdb.connect(DB_PATH)

reject_condition = """
    AIRPORT1 = AIRPORT4
    AND AIRPORT4 IS NOT NULL
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
    WHERE NOT ({reject_condition})
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
# 7. Validation
# ------------------------------------------------------------
print()
print("--------------------------------------------------")
print("VALIDATION")
print("--------------------------------------------------")

print(f"Source                       : {source_count:,}")
print(f"Rejected this run            : {reject_count:,}")
print(f"Target                       : {target_count:,}")
print(f"Rejected + Target            : {reject_count + target_count:,}")
print(f"Difference                   : {source_count - (reject_count + target_count):,}")

if source_count == reject_count + target_count:
    print("STATUS                       : OK - Counts match")
else:
    print("STATUS                       : WARNING - Counts do NOT match")


# ------------------------------------------------------------
# 8. Additional validation
# ------------------------------------------------------------
print()
print("--------------------------------------------------")
print("REJECT VALIDATION")
print("--------------------------------------------------")

if rejected_total_count >= reject_count:
    print("STATUS                       : OK - Reject records exist")
else:
    print("STATUS                       : WARNING - Reject count is lower than expected")


con.close()