import duckdb

DB_PATH = r"C:\DuckDB\my_db.duckdb"

SOURCE_TABLE = "RIYA_USA_SPLIT2"
TARGET_TABLE = "RIYA_USA_SPLIT3"
REJECT_TABLE = "RIYA_USA_REJECT"

con = duckdb.connect(DB_PATH)

reject_condition = """
(
    AIRPORT6 IS NOT NULL OR
    AIRPORT7 IS NOT NULL OR
    AIRPORT8 IS NOT NULL
)
"""

# Insert rejected records into reject table
con.execute(f"""
INSERT INTO {REJECT_TABLE}
SELECT
    *,
    'AP6 OR AP7 OR AP8' AS REJECTIONREASON
FROM {SOURCE_TABLE}
WHERE {reject_condition}
""")

# Recreate target table with only valid records
con.execute(f"DROP TABLE IF EXISTS {TARGET_TABLE}")

con.execute(f"""
CREATE TABLE {TARGET_TABLE} AS
SELECT *
FROM {SOURCE_TABLE}
WHERE NOT {reject_condition}
""")

print(f"Rows with AP6/AP7/AP8 moved to {REJECT_TABLE}")
print(f"Valid rows copied to {TARGET_TABLE}")

con.close()