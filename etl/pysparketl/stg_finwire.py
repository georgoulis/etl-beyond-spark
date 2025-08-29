from utils import load_csv_to_spark, write_spark_to_delta
from pyspark.sql import functions as F

BLOB_CONTAINER_NAME='benchmarks'


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


def fixed_width_df(df, fields, record_type):
    df = df.filter(F.substring("line", 16, 3) == record_type)
    for start, end, name, dtype in fields:
        length = (end - start) if end else None
        if length:
            df = df.withColumn(name, F.trim(F.substring("line", start+1, length)))
        else:
            df = df.withColumn(name, F.trim(F.expr(f"substring(line, {start+1}, length(line))")))
    return df.drop("line")


def main():
    # Paths
    finwire_records_df = load_csv_to_spark(
        container_name=BLOB_CONTAINER_NAME,
        blob_path=r"sf10/raw/Batch*/FINWIRE*Q[1-4]",
        column_names=['line']
    )
    # Separate records by type
    cmp_df = fixed_width_df(finwire_records_df, fields=CMP_FIELDS, record_type='CMP')
    write_spark_to_delta(
        container_name=BLOB_CONTAINER_NAME,
        blob_path="sf10/bronze/pyspark/finwire_cmp",
        df=cmp_df,
        )

    sec_df = fixed_width_df(finwire_records_df, fields=SEC_FIELDS, record_type='SEC')
    write_spark_to_delta(
        container_name=BLOB_CONTAINER_NAME,
        blob_path="sf10/bronze/pyspark/finwire_sec",
        df=sec_df,
        )

    fin_df = fixed_width_df(finwire_records_df, fields=FIN_FIELDS, record_type='FIN')
    write_spark_to_delta(
        container_name=BLOB_CONTAINER_NAME,
        blob_path="sf10/bronze/pyspark/finwire_fin",
        df=fin_df,
        )

if __name__ == "__main__":
    main()