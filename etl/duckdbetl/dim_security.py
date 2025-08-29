import duckdb
import os
from utils import load_delta_to_duckdb, write_duckdb_to_delta


CONTAINER = os.getenv("BLOB_CONTAINER_NAME")

def main():
    con = duckdb.connect()
    # Read bronze delta tables
    finwire_sec = load_delta_to_duckdb(
        container_name=CONTAINER,
        blob_path="sf50/bronze/duckdb/finwire_sec",
        connection=con
        )
    
    dimcompany = load_delta_to_duckdb(
        container_name=CONTAINER,
        blob_path="sf50/silver/duckdb/dimcompany",
        connection=con
        )
    
    con.register('finwire_sec', finwire_sec)
    con.register('dimcompany', dimcompany)

    dim_security = con.query(
        """SELECT
                CONCAT(fs.SYMBOL, fs.EX_ID, COALESCE(id_based.Companyid, name_based.Companyid)) AS SK_SecurityID,
                fs.SYMBOL,
                fs.ISSUE_TYPE AS Issue,
                fs.STATUS,
                fs.NAME,
                fs.EX_ID AS ExchangeID,
                COALESCE(id_based.Companyid, name_based.Companyid) AS SK_CompanyID,
                fs.SH_OUT AS SharesOutstanding,
                fs.FIRST_TRADE_DATE AS FirstTrade,
                fs.FIRST_TRADE_EXCHG AS FirstTradeOnExchange,
                fs.DIVIDEND,
                1 AS BatchID,
                STRPTIME(fs.PTS, '%Y%m%d-%H%M%S') AS effective_timestamp,
                COALESCE(
                    LEAD(STRPTIME(fs.PTS, '%Y%m%d-%H%M%S')) OVER (PARTITION BY fs.SYMBOL ORDER BY STRPTIME(fs.PTS, '%Y%m%d-%H%M%S')),
                    STRPTIME('9999-12-31 00:00:00', '%Y-%m-%d %H:%M:%S')
                ) AS end_timestamp,
                (
                    COALESCE(
                        LEAD(STRPTIME(fs.PTS, '%Y%m%d-%H%M%S')) OVER (PARTITION BY fs.SYMBOL ORDER BY STRPTIME(fs.PTS, '%Y%m%d-%H%M%S')),
                        STRPTIME('9999-12-31 00:00:00', '%Y-%m-%d %H:%M:%S')
                    ) = STRPTIME('9999-12-31 00:00:00', '%Y-%m-%d %H:%M:%S')
                ) AS iscurrent
            FROM finwire_sec fs
            LEFT JOIN dimcompany id_based
                ON fs.CO_NAME_OR_CIK = id_based.Companyid
            LEFT JOIN dimcompany name_based
                ON fs.CO_NAME_OR_CIK = name_based.Name
        """
    )
    
    write_duckdb_to_delta(
        container_name=CONTAINER,
        blob_path="sf50/silver/duckdb/dimsecurity",
        df=dim_security,
        )


if __name__ == "__main__":
    main()
