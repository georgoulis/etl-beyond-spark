from azure.storage.blob import BlobClient
from pyarrow import BufferReader
from deltalake import DeltaTable, write_deltalake
import fsspec
import pyarrow.csv as pv
import polars as pl
import os

CONN_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
STORAGE_ACCOUNT = os.getenv("STORAGE_ACCOUNT")
AZURE_STORAGE_KEY = os.getenv("AZURE_STORAGE_KEY")
FS = fsspec.filesystem(
        "abfss",
        account_name=STORAGE_ACCOUNT,
        account_key=AZURE_STORAGE_KEY,
    )

def load_csv_to_polars(
    container_name: str,
    blob_path: str,
    column_names: list[str],
    delimiter: str = "|",
) -> pl.DataFrame:
    fs_options = {
        "account_name": STORAGE_ACCOUNT,
        "account_key": AZURE_STORAGE_KEY,
    }
    file_path = get_uri(blob_path=blob_path, container_name=container_name)
    return pl.read_csv(
        source=file_path,
        storage_options=fs_options,
        separator=delimiter,
        has_header=False,
        new_columns=column_names,
    )

def load_delta_to_polars(
    container_name: str,
    blob_path: str,
    ):
    uri = get_uri(
        container_name=container_name,
        blob_path=blob_path
    )

    return pl.from_arrow(
        DeltaTable(
            uri, storage_options=FS.storage_options
            ).to_pyarrow_table()
        )

def write_polars_to_delta(
    container_name: str,
    blob_path: str,
    df: pl.DataFrame,
    partition: list = [],
    mode: str = 'overwrite'
    ):
    fact_markethistory_uri = get_uri(
        container_name=container_name,
        blob_path=blob_path
    )
    write_deltalake(
        fact_markethistory_uri,
        df.to_arrow(), 
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
