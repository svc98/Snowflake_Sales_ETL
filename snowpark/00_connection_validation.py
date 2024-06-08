import sys
import logging
from snowflake.snowpark import Session

# Initiate logging at info level
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s",
                    datefmt='%I:%M:%S')


def get_snowpark_session() -> Session:
    connection_parameters = {
        "ACCOUNT": "gasylia-eob07677",
        "USER": "snowpark_user",
        "PASSWORD": "Password123",
        "ROLE": "SYSADMIN",
        "DATABASE": "SALES_DWH",
        "SCHEMA": "BRONZE",
        "WAREHOUSE": "SNOWPARK_ETL_WH"
    }
    return Session.builder.configs(connection_parameters).create()


def main():
    session = get_snowpark_session()

    context_df = session.sql("select current_role(), current_database(), current_schema(), current_warehouse()")
    context_df.show(2)

    customer_df = session.sql("select c_custkey,c_name,c_phone,c_mktsegment from snowflake_sample_data.tpch_sf1.customer limit 10")
    customer_df.show(5)


if __name__ == '__main__':
    main()
