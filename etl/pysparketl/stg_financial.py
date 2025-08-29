from pyspark.sql import functions as F
from utils import load_delta_to_spark, write_spark_to_delta



CONTAINER='benchmarks'

def main():
    # Read bronze delta tables
    finwire_fin = load_delta_to_spark(
        container_name=CONTAINER,
        blob_path="sf10/bronze/pyspark/finwire_fin"
        )
    finwire_fin.display()
    dimcompany = load_delta_to_spark(
        container_name=CONTAINER,
        blob_path="sf10/silver/pyspark/dimcompany"
        )

    dimcompany_id = dimcompany.select(
        F.col("Companyid").alias("id_based_sku_company_id"),
        F.col("Companyid").alias("join_id")
    )

    dimcompany_name = dimcompany.select(
        F.col("Companyid").alias("name_based_sku_company_id"),
        F.col("Name").alias("join_name")
    )

    financial = finwire_fin \
        .join(dimcompany_id, finwire_fin.CO_NAME_OR_CIK == dimcompany_id.join_id, how='left') \
        .join(dimcompany_name, finwire_fin.CO_NAME_OR_CIK == dimcompany_name.join_name, how='left') \
        .withColumn(
            "sku_company_id",
            F.coalesce(F.col("id_based_sku_company_id"), F.col("name_based_sku_company_id"))
        ).select(
        F.col("sku_company_id").alias("SK_CompanyID"),
        F.col("YEAR").alias("FI_YEAR"),
        F.col("QUARTER").alias("FI_QTR"),
        F.col("QTR_START_DATE").alias("FI_QTR_START_DATE"),
        F.col("REVENUE").alias("FI_REVENUE"),
        F.col("EARNINGS").alias("FI_NET_EARN"),
        F.col("EPS").alias("FI_BASIC_EPS"),
        F.col("DILUTED_EPS").alias("FI_DILUT_EPS"),
        F.col("MARGIN").alias("FI_MARGIN"),
        F.col("INVENTORY").alias("FI_INVENTORY"),
        F.col("ASSETS").alias("FI_ASSETS"),
        F.col("LIABILITIES").alias("FI_LIABILITY"),
        F.col("SH_OUT").alias("FI_OUT_BASIC"),
        F.col("DILUTED_SH_OUT").alias("FI_OUT_DILUT"),
    )

    write_spark_to_delta(
        container_name=CONTAINER,
        blob_path="sf10/silver/pyspark/financial",
        df=financial
        )

if __name__ == "__main__":
    main()