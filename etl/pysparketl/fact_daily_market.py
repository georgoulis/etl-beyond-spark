from utils import load_csv_to_spark, load_delta_to_spark, write_spark_to_delta
from pyspark.sql import functions as F

CONTAINER = 'benchmarks'


def main():
    # Read the Daily Market data
    CONTAINER = 'benchmarks'

    # Read the Daily Market data
    column_names = [
        "DM_DATE", "DM_S_SYMB", "DM_CLOSE", "DM_HIGH", "DM_LOW", "DM_VOL"
    ]
    dailyMarket_batch1_df = load_csv_to_spark(
        container_name=CONTAINER,
        blob_path="sf10/raw/Batch1/DailyMarket.txt",
        column_names=column_names
    )\
    .withColumn("BatchID", F.lit("1"))


    incremental_column_names = [
        "CDC_FLAG", "CDC_DSN", "DM_DATE", "DM_S_SYMB", "DM_CLOSE", "DM_HIGH", "DM_LOW", "DM_VOL"
    ]

    dailyMarket_batch2_df = load_csv_to_spark(
        container_name=CONTAINER,
        blob_path="sf10/raw/Batch2/DailyMarket.txt",
        column_names=incremental_column_names
    )\
    .withColumn("BatchID", F.lit("2"))
        
    dailyMarket_batch2_df = dailyMarket_batch2_df\
        .select([c for c in dailyMarket_batch2_df.columns if c not in ["CDC_FLAG", "CDC_DSN"]])


    dailyMarket_batch3_df = load_csv_to_spark(
        container_name=CONTAINER,
        blob_path="sf10/raw/Batch2/DailyMarket.txt",
        column_names=incremental_column_names
    )\
    .withColumn("BatchID", F.lit("3"))


    dailyMarket_batch3_df= dailyMarket_batch3_df\
        .select([c for c in dailyMarket_batch2_df.columns if c not in ["CDC_FLAG", "CDC_DSN"]])


    daily_market = dailyMarket_batch1_df\
        .union(dailyMarket_batch2_df)\
        .union(dailyMarket_batch3_df)

    # Read the DIM_SECURITY
    dimsecurity = load_delta_to_spark(
        container_name=CONTAINER,
        blob_path="sf10/silver/pyspark/dimsecurity"
        )

    # Calculate the daily market full load
    fact_daily_market = daily_market.join(
        dimsecurity.select([F.col("SK_SecurityID"), F.col("SK_CompanyID"), F.col("SYMBOL").alias("DM_S_SYMB"), F.col("effective_timestamp"), F.col("end_timestamp")]),
        on="DM_S_SYMB",
        how="left"
    ).filter(
        (F.to_date(F.col("DM_DATE")) >= F.col("effective_timestamp"))
        & (F.to_date(F.col("DM_DATE")) < F.col("end_timestamp"))
    ).select(
        [
            F.col("SK_SecurityID"),
            F.col("SK_CompanyID"),
            F.col("DM_DATE").alias("SK_DateID"),
            F.year(F.to_date(F.col("DM_DATE"))).alias("YearID"),
            F.lit(1).alias("PERatio"),
            F.lit(1).alias("Yield"),
            F.lit(1).alias("FiftyTwoWeekHigh"),
            F.lit(1).alias("SK_FiftyTwoWeekHighDate"),
            F.lit(1).alias("FiftyTwoWeekLow"),
            F.lit(1).alias("SK_FiftyTwoWeekLowDate"),
            F.col("DM_CLOSE").alias("ClosePrice"),
            F.col("DM_HIGH").alias("DayHigh"),
            F.col("DM_LOW").alias("DayLow"),
            F.col("DM_VOL").alias("Volume"),
            F.col("BatchID"),
        ]
    )

    # Write Delta Lake table
    write_spark_to_delta(
        container_name=CONTAINER,
        blob_path="sf10/silver/pyspark/FactMarketHistory",
        df=fact_daily_market,
        partition=["YearID"],
        )


    print(f"✅ Industry Delta table written to lake")


if __name__ == "__main__":
    main()