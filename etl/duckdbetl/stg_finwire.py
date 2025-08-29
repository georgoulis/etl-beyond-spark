import re
import os
import duckdb
from utils import write_duckdb_to_delta, load_csv_to_duckdb, get_https
from azure.storage.blob import BlobServiceClient


CONTAINER = os.getenv("BLOB_CONTAINER_NAME")
CONN_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")


FIN_FIELDS = [
    (0, 15, "PTS", "VARCHAR"),
    (15, 18, "RECTYPE", "VARCHAR"),
    (18, 22, "YEAR", "VARCHAR"),
    (22, 23, "QUARTER", "VARCHAR"),
    (23, 31, "QTR_START_DATE", "VARCHAR"),
    (31, 39, "POSTING_DATE", "VARCHAR"),
    (39, 56, "REVENUE", "VARCHAR"),
    (56, 73, "EARNINGS", "VARCHAR"),
    (73, 85, "EPS", "VARCHAR"),
    (85, 97, "DILUTED_EPS", "VARCHAR"),
    (97, 109, "MARGIN", "VARCHAR"),
    (109, 126, "INVENTORY", "VARCHAR"),
    (126, 143, "ASSETS", "VARCHAR"),
    (143, 160, "LIABILITIES", "VARCHAR"),
    (160, 173, "SH_OUT", "VARCHAR"),
    (173, 186, "DILUTED_SH_OUT", "VARCHAR"),
    (186, None, "CO_NAME_OR_CIK", "VARCHAR"),  # variable width end
]

SEC_FIELDS = [
    (0, 15, "PTS", "VARCHAR"),
    (15, 18, "RECTYPE", "VARCHAR"),
    (18, 33, "SYMBOL", "VARCHAR"),
    (33, 39, "ISSUE_TYPE", "VARCHAR"),
    (39, 43, "STATUS", "VARCHAR"),
    (43, 113, "NAME", "VARCHAR"),
    (113, 119, "EX_ID", "VARCHAR"),
    (119, 132, "SH_OUT", "VARCHAR"),
    (132, 140, "FIRST_TRADE_DATE", "VARCHAR"),
    (140, 148, "FIRST_TRADE_EXCHG", "VARCHAR"),
    (148, 160, "DIVIDEND", "VARCHAR"),
    (160, None, "CO_NAME_OR_CIK", "VARCHAR"),
]

CMP_FIELDS = [
    (0, 15, "PTS", "VARCHAR"),
    (15, 18, "RECTYPE", "VARCHAR"),
    (18, 78, "COMPANY_NAME", "VARCHAR"),
    (78, 88, "CIK", "VARCHAR"),
    (88, 92, "STATUS", "VARCHAR"),
    (92, 94, "INDUSTRY_ID", "VARCHAR"),
    (94, 98, "SP_RATING", "VARCHAR"),
    (98, 106, "FOUNDING_DATE", "VARCHAR"),
    (106, 186, "ADDR1", "VARCHAR"),
    (186, 266, "ADDR2", "VARCHAR"),
    (266, 278, "POSTAL_CODE", "VARCHAR"),
    (278, 303, "CITY", "VARCHAR"),
    (303, 323, "STATE_PROVINCE", "VARCHAR"),
    (323, 347, "COUNTRY", "VARCHAR"),
    (347, 393, "CEO_NAME", "VARCHAR"),
    (393, 543, "DESCRIPTION", "VARCHAR"),
]


def main():
    con = duckdb.connect()
    con.execute("INSTALL azure;")
    con.execute("LOAD azure;")
    con.execute(
        f"""
        CREATE SECRET secret1 (
            TYPE azure,
            CONNECTION_STRING '{CONN_STRING}'
        );
        """)
    # Paths
    folder_path = "sf50/raw/Batch1"  # folder path inside the container, if any

    blob_service_client = BlobServiceClient.from_connection_string(CONN_STRING)
    container_client = blob_service_client.get_container_client(CONTAINER)

    lines = []

    # List blobs under your folder path
    blob_list = container_client.list_blobs(name_starts_with=folder_path)
    pattern = re.compile(r'FINWIRE\d{4}Q[1-4]\b')
    paths = []
    for blob in blob_list:
        blob_name = blob.name
        filename = blob_name.split('/')[-1]  # get just the filename part
        if pattern.search(filename):
            # Download blob content as text
            paths.append('sf50/raw/Batch1/' + filename)
    
    file_path = [get_https(blob_path=blob_path, container_name=CONTAINER) for blob_path in paths]
    cols = {col_name: 'VARCHAR' for col_name in ['line']}
    finwire_df = con.read_csv(file_path, delimiter='|', header=False, columns=cols)
    con.register('finwire_df', finwire_df)

    cmp_cols = [f'line[{start}:{end}] AS {name}' for start, end, name, _ in CMP_FIELDS]
    cmp_df = con.execute(f"SELECT {',  '.join(cmp_cols)} FROM finwire_df WHERE line[15:18] = 'CMP';")
    write_duckdb_to_delta(
        container_name=CONTAINER,
        blob_path="sf50/bronze/duckdb/finwire_cmp",
        df=cmp_df,
        )

    sec_cols = [f'line[{start}:{end}] AS {name}' for start, end, name, _ in SEC_FIELDS[:-1]]
    sec_cols.append(f'line[{SEC_FIELDS[-1][0]}:] AS {SEC_FIELDS[-1][2]}')
    sec_df = con.execute(f"SELECT {',  '.join(sec_cols)} FROM finwire_df WHERE line[15:18] = 'SEC';")
    write_duckdb_to_delta(
        container_name=CONTAINER,
        blob_path="sf50/bronze/duckdb/finwire_sec",
        df=sec_df,
        )

    fin_cols = [f'line[{start}:{end}] AS {name}' for start, end, name, _ in FIN_FIELDS[:-1]]
    fin_cols.append(f'line[{FIN_FIELDS[-1][0]}:] AS {FIN_FIELDS[-1][2]}')
    fin_df = con.execute(f"SELECT {',  '.join(fin_cols)} FROM finwire_df WHERE line[15:18] = 'FIN';")
    write_duckdb_to_delta(
        container_name=CONTAINER,
        blob_path="sf50/bronze/duckdb/finwire_fin",
        df=fin_df,
        )

if __name__ == "__main__":
    main()