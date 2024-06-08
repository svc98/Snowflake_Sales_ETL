import sys
import logging
from snowflake.snowpark import Session, DataFrame
from snowflake.snowpark.types import StructType, StringType, StructField, StringType, LongType, DecimalType, DateType, TimestampType
from snowflake.snowpark.functions import col, lit, row_number, rank
from snowflake.snowpark import Window

# logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%I:%M:%S')


def get_snowpark_session():
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

def ingest_china_sales(session):
    print(f"Before China Copy count: {session.sql('select count(*) from sales_dwh.bronze.china_sales_orders').collect()[0]['COUNT(*)']}")
    session.sql("""
            copy into sales_dwh.bronze.china_sales_orders from (
            select
                sales_dwh.bronze.china_sales_orders_seq.nextval,
                t.$1::text as order_id,
                t.$2::text as customer_name,
                t.$3::text as mobile_key,
                t.$4::number as order_quantity,
                t.$5::number as unit_price,
                t.$6::number as order_value,
                t.$7::text as promotion_code ,
                t.$8::number(10,2)  as final_order_amount,
                t.$9::number(10,2) as tax_amount,
                t.$10::date as order_date,
                t.$11::text as payment_status,
                t.$12::text as shipping_status,
                t.$13::text as payment_method,
                t.$14::text as payment_provider,
                t.$15::text as phone_number,
                t.$16::text as shipping_address,
                metadata$filename as stg_file_name,
                metadata$file_row_number as stg_row_numer,
                metadata$file_last_modified as stg_last_modified
            from
                @sales_dwh.bronze.internal_loading_stage/source=CHN/format=csv/
                ( file_format => 'sales_dwh.common.csv_format') t  )  on_error = 'Continue'
                """).collect()
    print(f"After China Copy count: {session.sql('select count(*) from sales_dwh.bronze.china_sales_orders').collect()[0]['COUNT(*)']}")
    print("----")

def ingest_france_sales(session):
    print(f"Before France Copy count: {session.sql('select count(*) from sales_dwh.bronze.france_sales_orders').collect()[0]['COUNT(*)']}")
    session.sql("""
        copy into sales_dwh.bronze.france_sales_orders from (                                                  
        select                                         
            sales_dwh.bronze.france_sales_orders_seq.nextval,    
            $1:"Order ID"::text as order_id,              
            $1:"Customer Name"::text as customer_name,     
            $1:"Mobile Model"::text as mobile_key,         
            to_number($1:"Quantity") as quantity,          
            to_number($1:"Price per Unit") as unit_price,  
            to_decimal($1:"Total Price") as total_price,   
            $1:"Promotion Code"::text as promotion_code,   
            $1:"Order Amount"::number(10,2) as order_amount,    
            to_decimal($1:"GST") as tax_amount,                   
            $1:"Order Date"::date as order_date,             
            $1:"Payment Status"::text as payment_status,   
            $1:"Shipping Status"::text as shipping_status, 
            $1:"Payment Method"::text as payment_method,   
            $1:"Payment Provider"::text as payment_provider,    
            $1:"Mobile"::text as phone_number,                     
            $1:"Delivery Address"::text as shipping_address,    
            metadata$filename as stg_file_name,
            metadata$file_row_number as stg_row_numer,
            metadata$file_last_modified as stg_last_modified
        from                                           
            @sales_dwh.bronze.internal_loading_stage/source=FR/format=json/
            (file_format => sales_dwh.common.json_format)
            ) on_error=continue
        """).collect()
    print(f"After France Copy count: {session.sql('select count(*) from sales_dwh.bronze.france_sales_orders').collect()[0]['COUNT(*)']}")
    print("----")

def ingest_us_sales(session):
    print(f"Before US Copy count: {session.sql('select count(*) from sales_dwh.bronze.us_sales_orders').collect()[0]['COUNT(*)']}")
    session.sql("""
            copy into sales_dwh.bronze.us_sales_orders from (                                  
            select                         
                sales_dwh.bronze.us_sales_orders_seq.nextval, 
                $1:"Order ID"::text as order_id,   
                $1:"Customer Name"::text as customer_name,
                $1:"Mobile Model"::text as mobile_key,
                to_number($1:"Quantity") as quantity,
                to_number($1:"Price per Unit") as unit_price,
                to_decimal($1:"Total Price") as total_price,
                $1:"Promotion Code"::text as promotion_code,
                $1:"Order Amount"::number(15,2) as final_order_amount,
                $1:"GST"::number(15,2) as tax_amount,
                $1:"Order Date"::date as order_date,
                $1:"Payment Status"::text as payment_status,
                $1:"Shipping Status"::text as shipping_status,
                $1:"Payment Method"::text as payment_method,
                $1:"Payment Provider"::text as payment_provider,
                $1:"Mobile"::text as phone_number,
                $1:"Delivery Address"::text as shipping_address,
                metadata$filename as stg_file_name,
                metadata$file_row_number as stg_row_numer,
                metadata$file_last_modified as stg_last_modified
            from                           
                @sales_dwh.bronze.internal_loading_stage/source=US/format=parquet/
                (file_format => sales_dwh.common.parquet_format)
                ) on_error = continue 
            """).collect()
    print(f"After US Copy count: {session.sql('select count(*) from sales_dwh.bronze.us_sales_orders').collect()[0]['COUNT(*)']}")


def main():
    # Get Snowflake Session
    session = get_snowpark_session()

    # Load Staging data into Bronze tables
    ingest_china_sales(session)
    ingest_france_sales(session)
    ingest_us_sales(session)


if __name__ == '__main__':
    main()
