import os
import duckdb
from utils import load_delta_to_duckdb, write_duckdb_to_delta



CONTAINER = os.getenv("BLOB_CONTAINER_NAME")

def main():
    con = duckdb.connect()
    # Read bronze delta tables
    finware_fin = load_delta_to_duckdb(
        container_name=CONTAINER,
        blob_path="sf50/bronze/duckdb/finwire_fin",
        connection=con
        )

    dimcompany = load_delta_to_duckdb(
        container_name=CONTAINER,
        blob_path="sf50/silver/duckdb/dimcompany",
        connection=con
        )
    
    con.register('finware_fin', finware_fin)
    con.register('dimcompany', dimcompany)

    financial =  con.execute(
        """SELECT 
            case 
                when company_with_id.Companyid is null then company_with_name.Companyid
                else company_with_id.Companyid end as SK_CompanyID,
            YEAR as FI_YEAR,
            QUARTER as FI_QTR,
            QTR_START_DATE as FI_QTR_START_DATE,
            REVENUE as FI_REVENUE,
            EARNINGS as FI_NET_EARN,
            EPS as FI_BASIC_EPS,
            DILUTED_EPS as FI_DILUT_EPS,
            MARGIN as FI_MARGIN,
            INVENTORY as FI_INVENTORY,
            ASSETS as FI_ASSETS,
            LIABILITIES as FI_LIABILITY,
            SH_OUT as FI_OUT_BASIC,
            DILUTED_SH_OUT as FI_OUT_DILUT,
        FROM finware_fin
        LEFT JOIN dimcompany as company_with_id
            ON finware_fin.CO_NAME_OR_CIK = company_with_id.Companyid
        LEFT JOIN dimcompany as company_with_name
            ON finware_fin.CO_NAME_OR_CIK = company_with_name.Name"""
        )
    
    write_duckdb_to_delta(
        container_name=CONTAINER,
        blob_path="sf50/silver/duckdb/financial",
        df=financial
        )

if __name__ == "__main__":
    main()