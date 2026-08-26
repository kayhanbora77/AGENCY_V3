import duckdb

# File paths
CSV_FILE = r"C:\Users\cagri\Desktop\Agency_Data\BabylonTravels\filter0\2025Sales.csv"
DB_PATH = r"C:\DuckDB\my_db.duckdb"
TABLE_NAME = "BABYLONTRAVELS_RAW"


def insert_csv_to_duckdb(csv_path, db_path, table_name):
    # Connect to DuckDB
    con = duckdb.connect(db_path)

    try:
        print(f"Reading CSV and creating table '{table_name}'...")

        # Read ALL CSV columns as VARCHAR.
        # This prevents values such as 6.19E+08 from being
        # interpreted as numbers.
        query = f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT
                uuid() AS Id,
                *
            FROM read_csv(
                ?,
                all_varchar = true,
                header = true,
                ignore_errors = false
            );
        """

        # Execute
        con.execute(query, [csv_path])

        print("Data successfully inserted!")

        # --------------------------------------------------
        # Verification
        # --------------------------------------------------
        print("\n--- Sample Data from DuckDB ---")

        sample_df = con.execute(
            f"""
            SELECT
                Id,
                AirlineCode,
                RBDsClass,
                CabinClass,
                PassangerType,
                PNR,
                FlightNumber1,
                FlightNumber2,
                DepartureDate1,
                DepartureDate2,
                Airport1,
                Airport2
            FROM {table_name}
            LIMIT 10
            """
        ).fetchdf()

        print(sample_df.to_string(index=False))

        # --------------------------------------------------
        # Row count
        # --------------------------------------------------
        row_count = con.execute(
            f"SELECT COUNT(*) FROM {table_name}"
        ).fetchone()[0]

        print(f"\nTotal rows: {row_count:,}")

        # --------------------------------------------------
        # Schema
        # --------------------------------------------------
        print("\n--- Table Schema ---")

        schema_df = con.execute(
            f"DESCRIBE {table_name}"
        ).fetchdf()

        print(schema_df.to_string(index=False))

    finally:
        con.close()
        print("\nDuckDB connection closed.")


if __name__ == "__main__":
    insert_csv_to_duckdb(
        CSV_FILE,
        DB_PATH,
        TABLE_NAME
    )