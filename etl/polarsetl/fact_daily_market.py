import os
import polars as pl
from utils import load_csv_to_polars, load_delta_to_polars, write_polars_to_delta


CONTAINER = os.getenv("BLOB_CONTAINER_NAME")


def main():
    # Read the Daily Market data
    column_names = [
        "DM_DATE", "DM_S_SYMB", "DM_CLOSE", "DM_HIGH", "DM_LOW", "DM_VOL"
    ]
    dailyMarket_batch1_df = load_csv_to_polars(
        container_name=CONTAINER,
        blob_path="sf500/Batch1/DailyMarket.txt",
        column_names=column_names
    )\
    .with_columns([pl.lit("1").alias("BatchID")])


    incremental_column_names = [
        "CDC_FLAG", "CDC_DSN", "DM_DATE", "DM_S_SYMB", "DM_CLOSE", "DM_HIGH", "DM_LOW", "DM_VOL"
    ]
    
    dailyMarket_batch2_df = load_csv_to_polars(
        container_name=CONTAINER,
        blob_path="sf500/Batch2/DailyMarket.txt",
        column_names=incremental_column_names
    )\
    .select(pl.exclude(["CDC_FLAG", "CDC_DSN"]))\
    .with_columns([pl.lit("2").alias("BatchID")])


    dailyMarket_batch3_df = load_csv_to_polars(
        container_name=CONTAINER,
        blob_path="sf500/Batch2/DailyMarket.txt",
        column_names=incremental_column_names
    )\
    .select(pl.exclude(["CDC_FLAG", "CDC_DSN"]))\
    .with_columns([pl.lit("3").alias("BatchID")])


    daily_market = pl.concat(
        [
            dailyMarket_batch1_df,
            dailyMarket_batch2_df,
            dailyMarket_batch3_df
        ]
    )

    # Read the DIM_SECURITY
    dimsecurity = load_delta_to_polars(
        container_name=CONTAINER,
        blob_path="sf500/silver/polars/dimsecurity"
        )

    # Calculate the daily market full load
    fact_daily_market = daily_market.join(
        dimsecurity.select([pl.col("SK_SecurityID"), pl.col("SK_CompanyID"), pl.col("SYMBOL").alias("DM_S_SYMB"), pl.col("effective_timestamp"), pl.col("end_timestamp")]),
        on="DM_S_SYMB",
        how="left"
    ).filter(
        pl.col("DM_DATE").cast(pl.Date) >= pl.col("effective_timestamp"),
        pl.col("DM_DATE").cast(pl.Date) < pl.col("end_timestamp")
    ).select(
        [
            pl.col("SK_SecurityID"),
            pl.col("SK_CompanyID"),
            pl.col("DM_DATE").alias("SK_DateID"),
            pl.col("DM_DATE").cast(pl.Date).dt.year().alias("YearID"),
            pl.lit(1).alias("PERatio"),
            pl.lit(1).alias("Yield"),
            pl.lit(1).alias("FiftyTwoWeekHigh"),
            pl.lit(1).alias("SK_FiftyTwoWeekHighDate"),
            pl.lit(1).alias("FiftyTwoWeekLow"),
            pl.lit(1).alias("SK_FiftyTwoWeekLowDate"),
            pl.col("DM_CLOSE").alias("ClosePrice"),
            pl.col("DM_HIGH").alias("DayHigh"),
            pl.col("DM_LOW").alias("DayLow"),
            pl.col("DM_VOL").alias("Volume"),
            pl.col("BatchID"),
        ]
    )
    print(fact_daily_market)
    # Write Delta Lake table
    write_polars_to_delta(
        container_name=CONTAINER,
        blob_path="sf500/silver/polars/FactMarketHistory",
        df=fact_daily_market,
        partition=["YearID"],
        )

    print(f"✅ Industry Delta table written to lake")


if __name__ == "__main__":
    main()