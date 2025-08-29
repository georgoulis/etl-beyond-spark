from utils import load_csv_to_duckdb, write_duckdb_to_delta
import os
import duckdb


CONTAINER = os.getenv("BLOB_CONTAINER_NAME")

def main():
    con = duckdb.connect()
    # Define column names manually, according to your data schema
    column_names = [
        "IN_ID", "IN_NAME", "IN_SC_ID",
    ]

    industry_df = load_csv_to_duckdb(
            container_name=CONTAINER,
            blob_path="sf50/raw/Batch1/Industry.txt",
            column_names=column_names,
            connection=con
        )

    # Write Delta Lake table
    write_duckdb_to_delta(
        container_name=CONTAINER,
        blob_path="sf50/bronze/duckdb/industry",
        df=industry_df,
        )
    print("the industry updated")

if __name__ == "__main__":
    main()