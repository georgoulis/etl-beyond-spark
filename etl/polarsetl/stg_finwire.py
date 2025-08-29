import re
import os
import logging
import polars as pl
from utils import write_polars_to_delta
from azure.storage.blob import BlobServiceClient


CONTAINER = os.getenv("BLOB_CONTAINER_NAME")
CONN_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")


FIN_FIELDS = [
    (0, 15, "PTS", pl.Utf8),
    (15, 18, "RECTYPE", pl.Utf8),
    (18, 22, "YEAR", pl.Int32),
    (22, 23, "QUARTER", pl.Int8),
    (23, 31, "QTR_START_DATE", pl.Utf8),
    (31, 39, "POSTING_DATE", pl.Utf8),
    (39, 56, "REVENUE", pl.Float64),
    (56, 73, "EARNINGS", pl.Float64),
    (73, 85, "EPS", pl.Float64),
    (85, 97, "DILUTED_EPS", pl.Float64),
    (97, 109, "MARGIN", pl.Float64),
    (109, 126, "INVENTORY", pl.Float64),
    (126, 143, "ASSETS", pl.Float64),
    (143, 160, "LIABILITIES", pl.Float64),
    (160, 173, "SH_OUT", pl.Int64),
    (173, 186, "DILUTED_SH_OUT", pl.Int64),
    (186, None, "CO_NAME_OR_CIK", pl.Utf8),  # variable width end
]

SEC_FIELDS = [
    (0, 15, "PTS", pl.Utf8),
    (15, 18, "RECTYPE", pl.Utf8),
    (18, 33, "SYMBOL", pl.Utf8),
    (33, 39, "ISSUE_TYPE", pl.Utf8),
    (39, 43, "STATUS", pl.Utf8),
    (43, 113, "NAME", pl.Utf8),
    (113, 119, "EX_ID", pl.Utf8),
    (119, 132, "SH_OUT", pl.Int64),
    (132, 140, "FIRST_TRADE_DATE", pl.Utf8),
    (140, 148, "FIRST_TRADE_EXCHG", pl.Utf8),
    (148, 160, "DIVIDEND", pl.Float64),
    (160, None, "CO_NAME_OR_CIK", pl.Utf8),
]

CMP_FIELDS = [
    (0, 15, "PTS", pl.Utf8),
    (15, 18, "RECTYPE", pl.Utf8),
    (18, 78, "COMPANY_NAME", pl.Utf8),
    (78, 88, "CIK", pl.Utf8),
    (88, 92, "STATUS", pl.Utf8),
    (92, 94, "INDUSTRY_ID", pl.Utf8),
    (94, 98, "SP_RATING", pl.Utf8),
    (98, 106, "FOUNDING_DATE", pl.Utf8),
    (106, 186, "ADDR1", pl.Utf8),
    (186, 266, "ADDR2", pl.Utf8),
    (266, 278, "POSTAL_CODE", pl.Utf8),
    (278, 303, "CITY", pl.Utf8),
    (303, 323, "STATE_PROVINCE", pl.Utf8),
    (323, 347, "COUNTRY", pl.Utf8),
    (347, 393, "CEO_NAME", pl.Utf8),
    (393, 543, "DESCRIPTION", pl.Utf8),
]

def parse_fixed_width(lines, fields):
    records = []
    for line in lines:
        record = {}
        for start, end, name, dtype in fields:
            value = line[start:end].strip() if end else line[start:].strip()
            try:
                if dtype in (pl.Int64, pl.Int32, pl.Int8):
                    record[name] = int(value) if value else None
                elif dtype == pl.Float64:
                    record[name] = float(value) if value else None
                else:
                    record[name] = value
            except ValueError:
                record[name] = None
        records.append(record)
    return pl.DataFrame(records)

def main():
    # Paths
    folder_path = "sf500/Batch1"  # folder path inside the container, if any

    blob_service_client = BlobServiceClient.from_connection_string(CONN_STRING)
    container_client = blob_service_client.get_container_client(CONTAINER)

    lines = []

    # List blobs under your folder path
    blob_list = container_client.list_blobs(name_starts_with=folder_path)
    pattern = re.compile(r'FINWIRE\d{4}Q[1-4]\b')

    for blob in blob_list:
        blob_name = blob.name
        filename = blob_name.split('/')[-1]  # get just the filename part
        if pattern.search(filename):
            # Download blob content as text
            blob_client = container_client.get_blob_client(blob_name)
            stream = blob_client.download_blob()
            content = stream.readall().decode('utf-8')
            
            lines.extend(content.splitlines())

    # Separate records by type
    cmp_lines = [l for l in lines if l[15:18] == "CMP"]
    cmp_df = parse_fixed_width(cmp_lines, CMP_FIELDS).shrink_to_fit()
    write_polars_to_delta(
        container_name=CONTAINER,
        blob_path="sf500/bronze/polars/finwire_cmp",
        df=cmp_df,
        )
    
    sec_lines = [l for l in lines if l[15:18] == "SEC"]
    sec_df = parse_fixed_width(sec_lines, SEC_FIELDS).shrink_to_fit()
    write_polars_to_delta(
        container_name=CONTAINER,
        blob_path="sf500/bronze/polars/finwire_sec",
        df=sec_df,
        )
    
    fin_lines = [l for l in lines if l[15:18] == "FIN"]
    fin_df = parse_fixed_width(fin_lines, FIN_FIELDS).shrink_to_fit()
    write_polars_to_delta(
        container_name=CONTAINER,
        blob_path="sf500/bronze/polars/finwire_fin",
        df=fin_df,
        )
     
if __name__ == "__main__":
    main()