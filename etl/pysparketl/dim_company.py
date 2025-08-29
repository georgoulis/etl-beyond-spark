#!/usr/bin/env python

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from utils import load_delta_to_spark, write_spark_to_delta


CONTAINER = 'benchmarks'

def main():
    w = Window.partitionBy("Companyid").orderBy("effective_timestamp")

    # Read bronze delta tables
    finwire_cmp = load_delta_to_spark(
        container_name=CONTAINER,
        blob_path="sf10/bronze/pyspark/finwire_cmp"
        )

    industry = load_delta_to_spark(
        container_name=CONTAINER,
        blob_path="sf10/bronze/pyspark/industry"
        )
    # companyid = cik as bigint
    finwire_cmp = finwire_cmp.join(
        industry.select(
            [
                F.col("IN_ID").alias("INDUSTRY_ID"), 
                F.col("IN_NAME")
            ]
        ), 
        on="INDUSTRY_ID"
        )

    dim_company = finwire_cmp\
        .select(
            [
                F.col("CIK").alias("Companyid"),
                F.col("STATUS").alias("Status"),
                F.col("COMPANY_NAME").alias("Name"),
                F.col("IN_NAME").alias("Industry"),
                F.col("SP_RATING").alias("SPrating"),
                F.col("CEO_NAME").alias("ceo"),
                F.col("ADDR1").alias("AddressLine1"),
                F.col("ADDR2").alias("AddressLine2"),
                F.col("POSTAL_CODE").alias("PostalCode"),
                F.col("CITY"),
                F.col("STATE_PROVINCE").alias("StateProv"),
                F.col("COUNTRY"),
                F.col("DESCRIPTION"),
                F.col("FOUNDING_DATE"),
                F.to_timestamp(F.col("PTS"), format="yyyyMMdd-HHmmss").alias("effective_timestamp"),
                F.concat(F.regexp_replace(F.col("PTS"),"-",""), F.col("CIK")).alias("sku_company_id")
            ]
        )\
        .withColumn(
            "isLowgrade",
            F.when(
                F.col("SPrating").isin(['AAA','AA','A','AA+','A+','AA-','A-','BBB','BBB+','BBB-']),
                False
            ).otherwise(True)
        ).withColumn(
            "end_timestamp",
            F.coalesce(
                F.lead("effective_timestamp", 1).over(w),
                F.lit("9999-12-31 00:00:00").cast("timestamp")
            )
        ).withColumn(
            "iscurrent",
            F.col("end_timestamp") == F.lit("9999-12-31 00:00:00").cast("timestamp")
        )

    # decode status
    write_spark_to_delta(
        container_name=CONTAINER,
        blob_path="sf10/silver/pyspark/dimcompany",
        df=dim_company,
        )

if __name__ == "__main__":
    main()