import duckdb

DB_PATH = r"C:\DuckDB\my_db.duckdb"

SOURCE_TABLE = "RIYA_CANADA_SPLIT5"
TARGET_TABLE = "RIYA_CANADA_CLEANED"
REJECT_TABLE = "RIYA_CANADA_REJECT"

REJECTION_REASON = "AP1=AP4 AND AP5 IS NULL"

con = duckdb.connect(DB_PATH)

reject_condition = """
    AIRPORT1 = AIRPORT4
    AND AIRPORT5 IS NULL
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
    WHERE AIRPORT1 IS DISTINCT FROM AIRPORT4
       OR AIRPORT5 IS NOT NULL
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
# 6. Validation
# ------------------------------------------------------------
print()
print("--------------------------------------------------")
print("VALIDATION")
print("--------------------------------------------------")

difference = source_count - (reject_count + target_count)

print(f"Source                       : {source_count:,}")
print(f"Rejected                     : {reject_count:,}")
print(f"Target                       : {target_count:,}")
print(f"Rejected + Target            : {reject_count + target_count:,}")
print(f"Difference                   : {difference:,}")

if difference == 0:
    print("STATUS                       : OK - Counts match")
else:
    print("STATUS                       : WARNING - Counts do NOT match")


con.close()