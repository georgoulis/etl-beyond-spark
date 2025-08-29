from pyspark.sql import functions as F
from pyspark.sql.window import Window
from utils import load_delta_to_spark, write_spark_to_delta


CONTAINER = 'benchmarks'

def main():
    # Read bronze delta tables
    w = Window.partitionBy("SYMBOL").orderBy("effective_timestamp")

    finwire_sec = load_delta_to_spark(
        container_name=CONTAINER,
        blob_path="sf10/bronze/pyspark/finwire_sec"
        )

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

    dim_security = finwire_sec \
        .join(dimcompany_id, finwire_sec.CO_NAME_OR_CIK == dimcompany_id.join_id, how='left') \
        .join(dimcompany_name, finwire_sec.CO_NAME_OR_CIK == dimcompany_name.join_name, how='left') \
        .withColumn(
            "sku_company_id",
            F.coalesce(F.col("id_based_sku_company_id"), F.col("name_based_sku_company_id"))
        )\
        .select(
            [
                F.concat(F.col("SYMBOL"), F.col("EX_ID"), F.col("sku_company_id")).alias("SK_SecurityID"),
                F.col("SYMBOL"),
                F.col("ISSUE_TYPE").alias("Issue"),
                F.col("STATUS"),
                F.col("NAME"),
                F.col("EX_ID").alias("ExchangeID"),
                F.col("sku_company_id").alias("SK_CompanyID"),
                F.col("SH_OUT").alias("SharesOutstanding"),
                F.col("FIRST_TRADE_DATE").alias("FirstTrade"),
                F.col("FIRST_TRADE_EXCHG").alias("FirstTradeOnExchange"),
                F.col("DIVIDEND"),
                F.lit(1).alias("BatchID"),
                F.to_timestamp(F.col("PTS"), format="yyyyMMdd-HHmmss").alias("effective_timestamp")
            ]
        )\
        .withColumn(
            "end_timestamp",
            F.coalesce(
                F.lead("effective_timestamp", 1).over(w),
                F.lit("9999-12-31 00:00:00").cast("timestamp")
            )
        )\
        .withColumn(
            "iscurrent",
            (F.col("end_timestamp") == F.lit("9999-12-31 00:00:00"))
        )

    write_spark_to_delta(
        container_name=CONTAINER,
        blob_path="sf10/silver/pyspark/dimsecurity",
        df=dim_security,
        )


if __name__ == "__main__":
    main()
