from utils import load_csv_to_spark, write_spark_to_delta


BLOB_CONTAINER_NAME='benchmarks'


def main():
    # Define column names manually, according to your data schema
    column_names = [
        "IN_ID", "IN_NAME", "IN_SC_ID",
    ]

    industry_df = load_csv_to_spark(
            container_name=BLOB_CONTAINER_NAME,
            blob_path="sf10/raw/Batch1/Industry.txt",
            column_names=column_names
        )

    # Write Delta Lake table
    write_spark_to_delta(
        container_name=BLOB_CONTAINER_NAME,
        blob_path="sf10/bronze/pyspark/industry",
        df=industry_df,
        )
    print("the industry updated")

if __name__ == "__main__":
    main()