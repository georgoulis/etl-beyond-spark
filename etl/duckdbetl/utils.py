from deltalake import DeltaTable, write_deltalake
import fsspec
import duckdb
import os

CONN_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
STORAGE_ACCOUNT = os.getenv("STORAGE_ACCOUNT")
AZURE_STORAGE_KEY = os.getenv("AZURE_STORAGE_KEY")
TOKEN = os.getenv("TOKEN")


FS = fsspec.filesystem(
        "abfss",
        account_name=STORAGE_ACCOUNT,
        account_key=AZURE_STORAGE_KEY,
    )

def load_csv_to_duckdb(
    container_name: str,
    blob_path: str,
    column_names: list[str],
    delimiter: str = "|",
    connection: duckdb.DuckDBPyConnection = None
) -> duckdb.DuckDBPyRelation:
    file_path = get_https(blob_path=blob_path, container_name=container_name)
    cols = {col_name: 'VARCHAR' for col_name in column_names}
    rel = connection.read_csv(file_path, delimiter=delimiter, header=False, columns=cols)
    return rel

def load_delta_to_duckdb(
    container_name: str,
    blob_path: str,
    connection: duckdb.DuckDBPyConnection = None
):
    uri = get_uri(
        container_name=container_name,
        blob_path=blob_path
    )
    
    # If no existing DuckDB connection is provided, create one in memory
    if connection is None:
        connection = duckdb.connect()
        
    arrow_table = DeltaTable(
            uri, storage_options=FS.storage_options
            ).to_pyarrow_table()
    return connection.from_arrow(arrow_table)

def write_duckdb_to_delta(
    container_name: str,
    blob_path: str,
    df: duckdb.DuckDBPyRelation,
    partition: list = [],
    mode: str = 'overwrite'
    ):
    fact_markethistory_uri = get_uri(
        container_name=container_name,
        blob_path=blob_path
    )
    write_deltalake(
        fact_markethistory_uri,
        df.arrow(), 
        mode=mode,
        partition_by=partition,
        storage_options=FS.storage_options
        )
    
def get_uri(
    blob_path: str,
    container_name: str,
    account: str = STORAGE_ACCOUNT
    ):
    return  f"abfss://{container_name}@{account}.dfs.core.windows.net/{blob_path}"

def get_https(
    blob_path: str,
    container_name: str,
    account: str = STORAGE_ACCOUNT, 
    token: str = TOKEN
    ):
    return f'https://{account}.blob.core.windows.net/{container_name}/{blob_path}?{token}'