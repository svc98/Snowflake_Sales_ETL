import sys
import logging
from snowflake.snowpark import Session, DataFrame
from snowflake.snowpark.functions import col, lit, row_number, rank, round, cast
from snowflake.snowpark.types import DecimalType
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

def load_bronze_into_silver(session, table):
    # Set Country Variables
    if table == 'china':
        country = "CHN"
        region = "East Asia"
        local_currency = "YEN"
        exchange_rate = "USD2JPY"
    elif table == 'france':
        country = "FR"
        region = "Western Europe"
        local_currency = "EUR"
        exchange_rate = "USD2EUR"
    else:
        country = "US"
        region = "North America"
        local_currency = "USD"
        exchange_rate = "USD2USD"

    # Select the Bronze China
    sales_df = session.sql("select * from "+table+"_sales_orders")

    # Filter: Paid & Delivered sales orders ONLY
    paid_and_shipped_sales_df = sales_df.filter((col('PAYMENT_STATUS') == 'Paid') & (col('SHIPPING_STATUS') == 'Delivered'))

    # Insert: Country & Region columns to the DF
    country_sales_df = (paid_and_shipped_sales_df.with_column('Country', lit(country))
                                                 .with_column('Region', lit(region)))

    # Join: Add Exchange Rate calculations
    exchange_rate_df = session.sql("select * from sales_dwh.common.exchange_rate")
    country_sales_with_exchange_rate_df = country_sales_df.join(exchange_rate_df, country_sales_df['order_date'] == exchange_rate_df['DATE'], join_type='left')

    # De-Duplication:
    unique_orders = (country_sales_with_exchange_rate_df
                        .with_column('order_rank', rank().over(Window.partitionBy(col("order_id")).order_by(col('_metadata_last_modified').desc())))
                        .filter(col("order_rank") == 1)
                        .select(col('SALES_ORDER_KEY').alias('unique_sales_order_key')))

    print(f"{table.capitalize()} Sales Record Count: {country_sales_with_exchange_rate_df.count()}")
    print(f"{table.capitalize()} Sales Unique Count: {unique_orders.count()}")

    final_sales_df = unique_orders.join(
                        country_sales_with_exchange_rate_df, unique_orders['unique_sales_order_key'] == country_sales_with_exchange_rate_df['SALES_ORDER_KEY'], join_type='inner')

    # Final DF Select
    final_sales_df = final_sales_df.select(
        col('SALES_ORDER_KEY'),
        col('ORDER_ID'),
        col('ORDER_DATE'),
        col('CUSTOMER_NAME'),
        col('phone_number'),
        col('MOBILE_KEY'),
        col('Country'),
        col('Region'),
        col('ORDER_QUANTITY'),
        lit(local_currency).alias('LOCAL_CURRENCY'),
        col('UNIT_PRICE').alias('LOCAL_UNIT_PRICE'),
        col('PROMOTION_CODE').alias('PROMOTION_CODE'),
        col('FINAL_ORDER_AMOUNT').alias('LOCAL_TOTAL_ORDER_AMOUNT'),
        col('TAX_AMOUNT').alias('LOCAL_TAX_AMOUNT'),
        col(exchange_rate).alias("EXCHANGE_RATE"),
        round(col('FINAL_ORDER_AMOUNT') / col(exchange_rate), 2).alias('US_TOTAL_ORDER_AMOUNT'),
        round(col('TAX_AMOUNT') / col(exchange_rate), 2).alias('US_TAX_AMOUNT'),
        col('payment_status'),
        col('shipping_status'),
        col('payment_method'),
        col('payment_provider'),
        col('shipping_address')
    )

    final_sales_df.write.save_as_table("sales_dwh.silver."+table+"_sales_orders", mode="append")
    print(f"{table.capitalize()} Data Loaded!")


def main():
    # Get Snowflake Session Object
    session = get_snowpark_session()

    # Load Bronze tables into Silver tables
    load_bronze_into_silver(session, 'china')
    load_bronze_into_silver(session, 'france')
    load_bronze_into_silver(session, 'us')


if __name__ == '__main__':
    main()
