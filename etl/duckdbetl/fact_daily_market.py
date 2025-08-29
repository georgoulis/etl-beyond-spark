import os
import duckdb
from utils import load_csv_to_duckdb, load_delta_to_duckdb, write_duckdb_to_delta


CONTAINER = os.getenv("BLOB_CONTAINER_NAME")
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

def main():
    con = duckdb.connect()
    con.execute("INSTALL azure;")
    con.execute("LOAD azure;")
    con.execute(
        f"""
        CREATE SECRET secret1 (
            TYPE azure,
            CONNECTION_STRING '{AZURE_STORAGE_CONNECTION_STRING}'
        );
        """)
    con.execute("SET memory_limit='60GB';")
    # Read the Daily Market data
    #df = con.query(f"SELECT * FROM read_csv_auto('https://stetlbenchmarks.blob.core.windows.net/benchmarks/sf500/Batch1/DailyMarket.txt?sp=r&st=2025-08-20T13:52:46Z&se=2025-08-29T22:07:46Z&spr=https&sv=2024-11-04&sr=c&sig=1klHrCMhKAlDROEVf%2BJpkG8%2FeoRJc8mTCTexY2axaII%3D')")
    #print(df)
    
    #df2 = con.query(f"SELECT * FROM read_csv_auto('https://stetlbenchmarks.blob.core.windows.net/benchmarks/sf500/Batch2/DailyMarket.txt?sp=r&st=2025-08-20T13:52:46Z&se=2025-08-29T22:07:46Z&spr=https&sv=2024-11-04&sr=c&sig=1klHrCMhKAlDROEVf%2BJpkG8%2FeoRJc8mTCTexY2axaII%3D')")
    #print(df2)
    
    column_names = [
        "DM_DATE", "DM_S_SYMB", "DM_CLOSE", "DM_HIGH", "DM_LOW", "DM_VOL"
    ]
    dailyMarket_batch1_df = load_csv_to_duckdb(
        container_name=CONTAINER,
        blob_path="sf50/raw/Batch1/DailyMarket.txt",
        column_names=column_names,
        connection=con
    )
    con.register('dailyMarket_batch1_df', dailyMarket_batch1_df)

    incremental_column_names = [
        "CDC_FLAG", "CDC_DSN", "DM_DATE", "DM_S_SYMB", "DM_CLOSE", "DM_HIGH", "DM_LOW", "DM_VOL"
    ]
    
    dailyMarket_batch2_df = load_csv_to_duckdb(
        container_name=CONTAINER,
        blob_path="sf50/raw/Batch2/DailyMarket.txt",
        column_names=incremental_column_names,
        connection=con
    )
    con.register('dailyMarket_batch2_df', dailyMarket_batch2_df)
    

    dailyMarket_batch3_df = load_csv_to_duckdb(
        container_name=CONTAINER,
        blob_path="sf50/raw/Batch2/DailyMarket.txt",
        column_names=incremental_column_names,
        connection=con
    )
    con.register('dailyMarket_batch3_df', dailyMarket_batch3_df)
    

    # Read the DIM_SECURITY
    dimsecurity = load_delta_to_duckdb(
        container_name=CONTAINER,
        blob_path="sf50/silver/duckdb/dimsecurity",
        connection=con
        )
    con.register('dimsecurity', dimsecurity)
    
    fact_daily_market = con.query(
        """WITH daily_market as (
            SELECT *, '1' as BatchID
            FROM dailyMarket_batch1_df
            UNION
            SELECT * EXCLUDE (CDC_FLAG, CDC_DSN), '2' as BatchID
            FROM dailyMarket_batch2_df
            UNION
            SELECT * EXCLUDE (CDC_FLAG, CDC_DSN), '3' as BatchID
            FROM dailyMarket_batch3_df 
            )
            select 
                ds.SK_SecurityID,
                ds.SK_CompanyID,
                dm.DM_DATE as SK_DateID,
                EXTRACT(YEAR FROM CAST(dm.DM_DATE AS DATE)) AS YearID,
                '1' as PERatio,
                '1' as Yield,
                '1' as FiftyTwoWeekHigh,
                '1' as SK_FiftyTwoWeekHighDate,
                '1' as FiftyTwoWeekLow,
                '1' as SK_FiftyTwoWeekLowDate,
                dm.DM_CLOSE as ClosePrice,
                dm.DM_HIGH as DayHigh,
                dm.DM_LOW as DayLow,
                dm.DM_VOL as Volume,
                dm.BatchID
            from daily_market as dm
            left join dimsecurity as ds
                on dm.DM_S_SYMB = ds.SYMBOL
            WHERE CAST(dm.DM_DATE AS DATE) >= ds.effective_timestamp
            and CAST(dm.DM_DATE AS DATE) < ds.end_timestamp
        """
    )

    # Calculate the daily market full load
    fact_daily_market 
    print(fact_daily_market)
    # Write Delta Lake table
    write_duckdb_to_delta(
        container_name=CONTAINER,
        blob_path="sf50/silver/duckdb/FactMarketHistory",
        df=fact_daily_market,
        partition=["YearID"],
        )

    print(f"✅ Industry Delta table written to lake")


if __name__ == "__main__":
    main()