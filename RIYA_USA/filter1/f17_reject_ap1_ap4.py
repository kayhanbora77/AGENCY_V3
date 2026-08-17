import duckdb

DB_PATH = r"C:\DuckDB\my_db.duckdb"

SOURCE_TABLE = "RIYA_USA_SPLIT6"
TARGET_TABLE = "RIYA_USA_SPLIT7"
REJECT_TABLE = "RIYA_USA_REJECT"

con = duckdb.connect(DB_PATH)

reject_condition = """
    AIRPORT1 = AIRPORT4
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

print(f"Rows to reject (AP1=AP4)    : {reject_count:,}")

# ------------------------------------------------------------
# 3. Insert rejected records
# ------------------------------------------------------------
con.execute(f"""
    INSERT INTO {REJECT_TABLE}
    SELECT
        *,
        'AP1=AP4' AS REJECTIONREASON
    FROM {SOURCE_TABLE}
    WHERE {reject_condition}
""")

# Count how many were actually inserted for this reason
rejected_inserted_count = con.execute(f"""
    SELECT COUNT(*)
    FROM {REJECT_TABLE}
    WHERE REJECTIONREASON = 'AP1=AP4'
""").fetchone()[0]

print(f"Rejected records in table   : {rejected_inserted_count:,}")

# ------------------------------------------------------------
# 4. Recreate target table
#
# IS DISTINCT FROM keeps NULL rows as valid.
# ------------------------------------------------------------
con.execute(f"""
    DROP TABLE IF EXISTS {TARGET_TABLE}
""")

con.execute(f"""
    CREATE TABLE {TARGET_TABLE} AS
    SELECT *
    FROM {SOURCE_TABLE}
    WHERE AIRPORT1 IS DISTINCT FROM AIRPORT4
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

print(f"Source                       : {source_count:,}")
print(f"Rejected                     : {reject_count:,}")
print(f"Target                       : {target_count:,}")
print(f"Rejected + Target           : {rejected_inserted_count + target_count:,}")
print(f"Difference                   : {source_count - (rejected_inserted_count + target_count):,}")

if source_count == rejected_inserted_count + target_count:
    print("STATUS                       : OK - Counts match")
else:
    print("STATUS                       : WARNING - Counts do NOT match")

con.close()