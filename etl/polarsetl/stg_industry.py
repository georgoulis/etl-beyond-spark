from utils import load_csv_to_polars, write_polars_to_delta
import os


CONTAINER = os.getenv("BLOB_CONTAINER_NAME")

def main():
    # Define column names manually, according to your data schema
    column_names = [
        "IN_ID", "IN_NAME", "IN_SC_ID",
    ]

    industry_df = load_csv_to_polars(
            container_name=CONTAINER,
            blob_path="sf500/Batch1/Industry.txt",
            column_names=column_names
        )

    # Write Delta Lake table
    write_polars_to_delta(
        container_name=CONTAINER,
        blob_path="sf500/bronze/polars/industry",
        df=industry_df,
        )
    print("the industry updated")

if __name__ == "__main__":
    main()