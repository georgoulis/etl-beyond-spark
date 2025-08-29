import os
import polars as pl
from utils import load_delta_to_polars, write_polars_to_delta



CONTAINER = os.getenv("BLOB_CONTAINER_NAME")

def main():
    # Read bronze delta tables
    finware_sec = load_delta_to_polars(
        container_name=CONTAINER,
        blob_path="sf500/bronze/polars/finwire_fin"
        )

    dimcompany = load_delta_to_polars(
        container_name=CONTAINER,
        blob_path="sf500/silver/polars/dimcompany"
        )
    
    financial = finware_sec.join(
        dimcompany.select(
            [
                pl.col("Companyid").alias("id_based_sku_company_id"), 
                pl.col("Companyid").alias("CO_NAME_OR_CIK")
                
            ]
        ),                        
        on="CO_NAME_OR_CIK",
        how='left'
    )\
    .join(
        dimcompany.select(
            [
                pl.col("Companyid").alias("name_based_sku_company_id"), 
                pl.col("Name").alias("CO_NAME_OR_CIK")
                
            ]
        ),                        
        on="CO_NAME_OR_CIK",
        how='left'
    )\
    .with_columns(
        pl.when(
            pl.col("id_based_sku_company_id").is_null()
            )\
        .then(pl.col("name_based_sku_company_id"))\
        .otherwise(pl.col("id_based_sku_company_id"))\
        .alias("sku_company_id")
    )\
    .select(
        [
            pl.col("sku_company_id").alias("SK_CompanyID"),
            pl.col("YEAR").alias("FI_YEAR"),
            pl.col("QUARTER").alias("FI_QTR"),
            pl.col("QTR_START_DATE").alias("FI_QTR_START_DATE"),
            pl.col("REVENUE").alias("FI_REVENUE"),
            pl.col("EARNINGS").alias("FI_NET_EARN"),
            pl.col("EPS").alias("FI_BASIC_EPS"),
            pl.col("DILUTED_EPS").alias("FI_DILUT_EPS"),
            pl.col("MARGIN").alias("FI_MARGIN"),
            pl.col("INVENTORY").alias("FI_INVENTORY"),
            pl.col("ASSETS").alias("FI_ASSETS"),
            pl.col("LIABILITIES").alias("FI_LIABILITY"),
            pl.col("SH_OUT").alias("FI_OUT_BASIC"),
            pl.col("DILUTED_SH_OUT").alias("FI_OUT_DILUT"),
        ]
    )

    write_polars_to_delta(
        container_name=CONTAINER,
        blob_path="sf500/silver/polars/financial",
        df=financial
        )

if __name__ == "__main__":
    main()