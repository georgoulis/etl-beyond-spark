#!/usr/bin/env python

import duckdb
import os
from utils import load_delta_to_duckdb, write_duckdb_to_delta


CONTAINER = os.getenv("BLOB_CONTAINER_NAME")

def main():
    con = duckdb.connect()
    # Read bronze delta tables
    finwire_cmp = load_delta_to_duckdb(
        container_name=CONTAINER,
        blob_path="sf50/bronze/duckdb/finwire_cmp",
        connection=con
        )
    
    industry = load_delta_to_duckdb(
        container_name=CONTAINER,
        blob_path="sf50/bronze/duckdb/industry",
        connection=con
        )
    con.register('finwire_cmp', finwire_cmp)
    con.register('industry', industry)
    dim_company =  con.execute(
        """SELECT 
            CIK as Companyid,
            STATUS as Status,
            COMPANY_NAME as Name,
            IN_NAME as Industry,
            SP_RATING as SPrating,
            CEO_NAME as ceo,
            ADDR1 as AddressLine1,
            ADDR2 as AddressLine2,
            POSTAL_CODE as PostalCode,
            CITY,
            STATE_PROVINCE as StateProv,
            COUNTRY,
            DESCRIPTION,
            FOUNDING_DATE,
            CAST(strptime(PTS, '%Y%m%d-%H%M%S') AS TIMESTAMP WITH TIME ZONE) as effective_timestamp,
            concat(replace(PTS, '-', ''), CIK) as sku_company_id,
            case 
                when SPrating in('AAA','AA','A','AA+','A+','AA-','A-','BBB','BBB+','BBB-') then False
                    else True
                end as isLowgrade,
            COALESCE(
                LEAD(effective_timestamp) OVER (PARTITION BY Companyid ORDER BY effective_timestamp),
                STRPTIME('9999-12-31 00:00:00', '%Y-%m-%d %H:%M:%S')
            ) AS end_timestamp,
            (
                COALESCE(
                    LEAD(effective_timestamp) OVER (PARTITION BY Companyid ORDER BY effective_timestamp),
                    STRPTIME('9999-12-31 00:00:00', '%Y-%m-%d %H:%M:%S')
                ) = STRPTIME('9999-12-31 00:00:00', '%Y-%m-%d %H:%M:%S')
            ) AS iscurrent
        FROM finwire_cmp
        LEFT JOIN industry
            ON finwire_cmp.INDUSTRY_ID = industry.IN_ID"""
        )
    
    # decode status
    write_duckdb_to_delta(
        container_name=CONTAINER,
        blob_path="sf50/silver/duckdb/dimcompany",
        df=dim_company,
        )

if __name__ == "__main__":
    main()