from pyspark.sql.types import StructType, StructField, StringType


BLOB_CONTAINER_NAME='benchmarks'
STORAGE_ACCOUNT='stetlbenchmarks'


def get_uri(
    blob_path: str,
    container_name: str,
    account: str = STORAGE_ACCOUNT
    ):
    return  f"abfss://{container_name}@{account}.dfs.core.windows.net/{blob_path}"


def load_csv_to_spark(
    container_name: str,
    blob_path: str,
    column_names: list[str],
    delimiter: str = "|",
):
    file_path = get_uri(blob_path=blob_path, container_name=container_name)
    schema = StructType([StructField(c, StringType(), True) for c in column_names])
    df = (
        spark.read
        .option("delimiter", delimiter)
        .option("header", False)
        .schema(schema)
        .csv(file_path)
    )
    return df


def load_delta_to_spark(
    container_name: str,
    blob_path: str
):
    uri = get_uri(
        container_name=container_name,
        blob_path=blob_path
    )        
    return spark.read.format("delta").load(uri)

    
def write_spark_to_delta(
    container_name: str,
    blob_path: str,
    df: spark.DataFrame,
    partition: list = [],
    mode: str = 'overwrite'
    ) -> None:
    fact_markethistory_uri = get_uri(
        container_name=container_name,
        blob_path=blob_path
    )
    df.write.mode(mode).save(fact_markethistory_uri, partitionBy=partition)