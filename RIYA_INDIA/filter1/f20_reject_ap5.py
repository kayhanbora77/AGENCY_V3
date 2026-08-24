import duckdb

DB_PATH = r"C:\DuckDB\my_db.duckdb"

SOURCE_TABLE = "RIYA_INDIA_SPLIT9"
TARGET_TABLE = "RIYA_INDIA_SPLIT10"
REJECT_TABLE = "RIYA_INDIA_REJECT"

REJECTION_REASON = "AP5 IS NOT NULL"

con = duckdb.connect(DB_PATH)

# ------------------------------------------------------------
# Reject condition
#
# Reject ONLY when:
# Airport5 IS NOT NULL
# ------------------------------------------------------------

reject_condition = """
    Airport5 IS NOT NULL
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
""")

# Remove rejected rows from target
con.execute(f"""
    DELETE FROM {TARGET_TABLE}
    WHERE {reject_condition}
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
print(
    f"Difference                  : "
    f"{source_count - (reject_count + target_count):,}"
)

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
    print(
        "STATUS                      : WARNING - "
        "Reject count is lower than expected"
    )


# ------------------------------------------------------------
# 10. Direct TARGET table validation
#
# No target_condition needed.
# The target was created from SOURCE and then the same
# reject_condition was deleted.
# ------------------------------------------------------------

direct_reject_count = con.execute(f"""
    SELECT COUNT(*)
    FROM {SOURCE_TABLE}
    WHERE {reject_condition}
""").fetchone()[0]

direct_target_count = con.execute(f"""
    SELECT COUNT(*)
    FROM {TARGET_TABLE}
""").fetchone()[0]

print()
print("--------------------------------------------------")
print("DIRECT SOURCE/TARGET VALIDATION")
print("--------------------------------------------------")

print(f"Source rows                 : {source_count:,}")
print(f"Reject rows                 : {direct_reject_count:,}")
print(f"Target rows                 : {direct_target_count:,}")
print(
    f"Reject + Target             : "
    f"{direct_reject_count + direct_target_count:,}"
)

if source_count == direct_reject_count + direct_target_count:
    print("STATUS                      : OK - COMPLETE 1:1 SPLIT")
else:
    print("STATUS                      : WARNING - SPLIT DOES NOT MATCH")


# ------------------------------------------------------------
# 11. Final exact target validation
# ------------------------------------------------------------

print()
print("--------------------------------------------------")
print("FINAL VALIDATION")
print("--------------------------------------------------")

if target_count == expected_target_count:
    print("TARGET STATUS               : OK")
else:
    print("TARGET STATUS               : WARNING")

if rejected_total_count >= reject_count:
    print("REJECT STATUS               : OK")
else:
    print("REJECT STATUS               : WARNING")

if source_count == reject_count + target_count:
    print("SPLIT STATUS                : OK - 1:1 COMPLETE")
else:
    print("SPLIT STATUS                : WARNING")

print("--------------------------------------------------")

con.close()