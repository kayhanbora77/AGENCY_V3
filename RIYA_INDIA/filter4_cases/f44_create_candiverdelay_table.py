import duckdb

# =====================================================
# CONFIG
# =====================================================
DB_PATH = r"C:\DuckDB\my_db.duckdb"
SOURCE_TABLE = "TA_STANDARD_RIYAINDIA_VF_RESULT"
EXCLUDE_TABLE = "RIYAINDIA_MISSCONN_CHECKED"
NEW_TABLE_NAME = "RIYAINDIA_CANDIVERDELAY_CHECKED"

con = duckdb.connect(DB_PATH)

print(f"Creating table {NEW_TABLE_NAME}...")

con.execute(f"DROP TABLE IF EXISTS {NEW_TABLE_NAME}")

con.execute(f"""
    CREATE TABLE {NEW_TABLE_NAME} AS
    WITH Src AS (
        SELECT *
        FROM {SOURCE_TABLE} AS vf_result
        WHERE vf_result.EUEligible IS TRUE 
          AND vf_result.Status IN ('cancel', 'diversion', 'Delay') 
          AND NOT EXISTS (
                SELECT 1
                FROM {EXCLUDE_TABLE} AS mc_checked
                WHERE mc_checked.Id = vf_result.Id
          )
    )
    SELECT *
    FROM {SOURCE_TABLE}
    WHERE ConnectionID IN (
        SELECT DISTINCT ConnectionID
        FROM Src
    )
    ORDER BY ConnectionID, LegNo;
""")

print("=" * 60)
print(f"Table created successfully: {NEW_TABLE_NAME}")
print("=" * 60)

# =====================================================
# DISPLAY RESULTS
# =====================================================
row_count = con.execute(f"SELECT COUNT(*) FROM {NEW_TABLE_NAME}").fetchone()[0]
print(f"Total Rows inserted: {row_count}")

print("\nColumns:")
for row in con.execute(f"DESCRIBE {NEW_TABLE_NAME}").fetchall():
    print(f"{row[0]} ({row[1]})")

print("\nSample Rows:")
print(con.execute(f"SELECT * FROM {NEW_TABLE_NAME} LIMIT 5").fetchdf())

con.close()
print("\nDone.")