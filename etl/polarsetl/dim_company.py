#!/usr/bin/env python

import polars as pl
import os
import pyarrow as pa
import polars.selectors as cs
from deltalake import DeltaTable, write_deltalake
from utils import load_csv_to_polars, load_delta_to_polars, write_polars_to_delta


CONTAINER = os.getenv("BLOB_CONTAINER_NAME")

def main():
    # Read bronze delta tables
    finwire_cmp = load_delta_to_polars(
        container_name=CONTAINER,
        blob_path="sf500/bronze/polars/finwire_cmp"
        )
    
    industry = load_delta_to_polars(
        container_name=CONTAINER,
        blob_path="sf500/bronze/polars/industry"
        )
    # companyid = cik as bigint
    finwire_cmp = finwire_cmp.join(
        industry.select(
            [
                pl.col("IN_ID").alias("INDUSTRY_ID"), 
                pl.col("IN_NAME")
            ]
        ), 
        on="INDUSTRY_ID"
        )

    dim_company = finwire_cmp\
        .select(
            [
                pl.col("CIK").alias("Companyid"),
                pl.col("STATUS").alias("Status"),
                pl.col("COMPANY_NAME").alias("Name"),
                pl.col("IN_NAME").alias("Industry"),
                pl.col("SP_RATING").alias("SPrating"),
                pl.col("CEO_NAME").alias("ceo"),
                pl.col("ADDR1").alias("AddressLine1"),
                pl.col("ADDR2").alias("AddressLine2"),
                pl.col("POSTAL_CODE").alias("PostalCode"),
                pl.col("CITY"),
                pl.col("STATE_PROVINCE").alias("StateProv"),
                pl.col("COUNTRY"),
                pl.col("DESCRIPTION"),
                pl.col("FOUNDING_DATE"),
                pl.col("PTS").str.strptime(pl.Datetime, format="%Y%m%d-%H%M%S").alias("effective_timestamp"),
                pl.concat_str(pl.col("PTS").str.replace("-",""), pl.col("CIK")).alias("sku_company_id")
            ]
        )\
        .with_columns(
            [
                pl.when(
                    pl.col("SPrating")\
                    .is_in(['AAA','AA','A','AA+','A+','AA-','A-','BBB','BBB+','BBB-'])
                        )\
                    .then(False)\
                    .otherwise(True)\
                    .alias("isLowgrade"),
                pl.col("effective_timestamp")
                    .shift(-1)
                    .over("Companyid")
                    .fill_null(pl.lit("9999-12-31 00:00:00").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S"))
                    .alias("end_timestamp")
                
            ]
        )\
        .with_columns(
            (pl.col("end_timestamp") == pl.lit("9999-12-31 00:00:00").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S")).alias("iscurrent")
        )

    # decode status
    write_polars_to_delta(
        container_name=CONTAINER,
        blob_path="sf500/silver/polars/dimcompany",
        df=dim_company,
        )

if __name__ == "__main__":
    main()