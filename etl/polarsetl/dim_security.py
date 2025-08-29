import polars as pl
import os
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from utils import load_csv_to_polars, load_delta_to_polars, write_polars_to_delta


CONTAINER = os.getenv("BLOB_CONTAINER_NAME")

def main():
    # Read bronze delta tables
    finwire_sec = load_delta_to_polars(
        container_name=CONTAINER,
        blob_path="sf500/bronze/polars/finwire_sec"
        )
    
    dimcompany = load_delta_to_polars(
        container_name=CONTAINER,
        blob_path="sf500/silver/polars/dimcompany"
        )

    finwire_sec = finwire_sec.join(
        dimcompany.select(
            [
                pl.col("Companyid").alias("id_based_sku_company_id"), 
                pl.col("Companyid").alias("CO_NAME_OR_CIK")
                
            ]
        ),                        
        on="CO_NAME_OR_CIK",
        how='left'
    ).join(
        dimcompany.select(
            [
                pl.col("Companyid").alias("name_based_sku_company_id"), 
                pl.col("Name").alias("CO_NAME_OR_CIK")
                
            ]
        ),                        
        on="CO_NAME_OR_CIK",
        how='left'
    ).with_columns(
        pl.when(
            pl.col("id_based_sku_company_id").is_null()
            )\
        .then(pl.col("name_based_sku_company_id"))\
        .otherwise(pl.col("id_based_sku_company_id"))\
        .alias("sku_company_id")
    )

    # companyid = cik as bigint

    dim_security = finwire_sec\
        .select(
            [   
                pl.concat_str([pl.col("SYMBOL"), pl.col("EX_ID"), pl.col("sku_company_id")]).alias("SK_SecurityID"),
                pl.col("SYMBOL"),
                pl.col("ISSUE_TYPE").alias("Issue"),
                pl.col("STATUS"),
                pl.col("NAME"),
                pl.col("EX_ID").alias("ExchangeID"),
                pl.col("sku_company_id").alias("SK_CompanyID"),
                pl.col("SH_OUT").alias("SharesOutstanding"),
                pl.col("FIRST_TRADE_DATE").alias("FirstTrade"),
                pl.col("FIRST_TRADE_EXCHG").alias("FirstTradeOnExchange"),
                pl.col("DIVIDEND"),
                pl.lit(1).alias("BatchID"),
                pl.col("PTS").str.strptime(pl.Datetime, format="%Y%m%d-%H%M%S").alias("effective_timestamp"),
            ]
        )\
        .with_columns(
            [
                pl.col("effective_timestamp")
                    .shift(-1)
                    .over("SYMBOL")
                    .fill_null(pl.lit("9999-12-31 00:00:00").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S"))
                    .alias("end_timestamp")
                
            ]
        )\
        .with_columns(
            (pl.col("end_timestamp") == pl.lit("9999-12-31 00:00:00").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S")).alias("iscurrent")
        )
    
    write_polars_to_delta(
        container_name=CONTAINER,
        blob_path="sf500/silver/polars/dimsecurity",
        df=dim_security,
        )


if __name__ == "__main__":
    main()
