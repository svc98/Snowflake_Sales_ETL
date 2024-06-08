import sys
import logging
import pandas as pd
from snowflake.snowpark import Session, DataFrame, CaseExpr
from snowflake.snowpark.functions import col, lit, row_number, rank, split, cast, when, expr, min, max
from snowflake.snowpark.types import StructType, StringType, StructField, StringType, LongType, DecimalType, DateType, TimestampType, IntegerType
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

def create_region_dim(df, session):
    region_dim_df = df.groupBy(col("country"), col("region")).count()
    region_dim_df = region_dim_df.with_column("isActive", lit('Y'))
    region_dim_df = region_dim_df.selectExpr("sales_dwh.gold.region_dim_seq.nextval as region_id_pk", "country", "region", "isActive")

    # Delta Data Processing:
    existing_region_dim_df = session.sql("select country, region from sales_dwh.gold.region_dim")
    region_dim_df = region_dim_df.join(existing_region_dim_df, region_dim_df['country'] == existing_region_dim_df['country'], join_type='leftanti')

    # Data Writing
    row_count = int(region_dim_df.count())
    if row_count > 0:
        region_dim_df.write.save_as_table("sales_dwh.gold.region_dim", mode="append")
        print("New data inserted in Region table!")
    else:
        print("No change in Region table!")

def create_product_dim(df, session):
    product_dim_df = (df.with_column("brand", split(col('mobile_key'), lit('/'))[0])
                        .with_column("model", split(col('mobile_key'), lit('/'))[1])
                        .with_column("color", split(col('mobile_key'), lit('/'))[2])
                        .with_column("memory", split(col('mobile_key'), lit('/'))[3])
                        .select(col('mobile_key'), col('brand'), col('model'), col('color'), col('memory')))

    product_dim_df = (product_dim_df.select(col('mobile_key'),
                                            cast(col('brand'), StringType()).as_("brand"),
                                            cast(col('model'), StringType()).as_("model"),
                                            cast(col('color'), StringType()).as_("color"),
                                            cast(col('memory'), StringType()).as_("memory")
                                            ))

    product_dim_df = product_dim_df.groupBy(col('mobile_key'), col("brand"), col("model"), col("color"), col("memory")).count()
    product_dim_df = product_dim_df.with_column("isActive", lit('Y'))

    # Delta Data Processing:
    existing_product_dim_df = session.sql("select mobile_key, brand, model, color, memory from sales_dwh.gold.product_dim")
    existing_product_dim_df.count()

    product_dim_df = product_dim_df.join(existing_product_dim_df, ["mobile_key", "brand", "model", "color", "memory"], join_type='leftanti')
    product_dim_df = product_dim_df.selectExpr("sales_dwh.gold.product_dim_seq.nextval as product_id_pk", "mobile_key", "brand", "model", "color", "memory", "isActive")

    # Data Writing
    row_count = int(product_dim_df.count())
    if row_count > 0:
        product_dim_df.write.save_as_table("sales_dwh.gold.product_dim", mode="append")
        print("New data inserted in Product table!")
    else:
        print("No change in Product table!")


def create_promo_code_dim(df, session):
    promo_code_dim_df = df.with_column("promotion_code", expr("case when promotion_code is null then 'NA' else promotion_code end"))
    promo_code_dim_df = promo_code_dim_df.groupBy(col("promotion_code"), col("country"), col("region")).count()
    promo_code_dim_df = promo_code_dim_df.with_column("isActive", lit('Y'))

    # Delta Data Processing:
    existing_promo_code_dim_df = session.sql("select promotion_code, country, region from sales_dwh.gold.promo_code_dim")
    promo_code_dim_df = promo_code_dim_df.join(existing_promo_code_dim_df, ["promotion_code", "country", "region"], join_type='leftanti')
    promo_code_dim_df = promo_code_dim_df.selectExpr("sales_dwh.gold.promo_code_dim_seq.nextval as promo_code_id_pk", "promotion_code", "country", "region", "isActive")

    # Data Writing
    row_count = int(promo_code_dim_df.count())
    if row_count > 0:
        promo_code_dim_df.write.save_as_table("sales_dwh.gold.promo_code_dim", mode="append")
        print("New data inserted in Promo Code table!")
    else:
        print("No change in Promo Code table!")


def create_customer_dim(df, session):
    customer_dim_df = df.groupBy(col("country"), col("region"), col("customer_name"), col("phone_number"), col("shipping_address")).count()
    customer_dim_df = customer_dim_df.with_column("isActive", lit('Y'))
    customer_dim_df = customer_dim_df.selectExpr("customer_name", "phone_number", "shipping_address", "country", "region", "isActive")

    # Delta Data Processing:
    existing_customer_dim_df = session.sql("select customer_name, phone_number, shipping_address, country, region from sales_dwh.gold.customer_dim")
    customer_dim_df = customer_dim_df.join(existing_customer_dim_df, ["customer_name", "phone_number", "shipping_address", "country", "region"], join_type='leftanti')
    customer_dim_df = customer_dim_df.selectExpr("sales_dwh.gold.customer_dim_seq.nextval as customer_id_pk", "customer_name", "phone_number", "shipping_address", "country",
                                                 "region", "isActive")

    # Data Writing
    row_count = int(customer_dim_df.count())
    if row_count > 0:
        customer_dim_df.write.save_as_table("sales_dwh.gold.customer_dim", mode="append")
        print("New data inserted in Customer table!")
    else:
        print("No change in Customer table!")


def create_payment_dim(df, session):
    payment_dim_df = df.groupBy(col("country"), col("region"), col("payment_method"), col("payment_provider")).count()
    payment_dim_df = payment_dim_df.with_column("isActive", lit('Y'))

    # Delta Data Processing:
    existing_payment_dim_df = session.sql("select payment_method,payment_provider,country, region from sales_dwh.gold.payment_dim")
    payment_dim_df = payment_dim_df.join(existing_payment_dim_df, ["payment_method", "payment_provider", "country", "region"], join_type='leftanti')
    payment_dim_df = payment_dim_df.selectExpr("sales_dwh.gold.payment_dim_seq.nextval as payment_id_pk", "payment_method", "payment_provider", "country", "region", "isActive")

    # Data Writing
    row_count = int(payment_dim_df.count())
    if row_count > 0:
        payment_dim_df.write.save_as_table("sales_dwh.gold.payment_dim", mode="append")
        print("New data inserted in Payment table!")
    else:
        print("No change in Payment table!")


def create_date_dim(df, session):
    # Collecting start and end dates
    start_date = df.select(min(col("order_date")).alias("min_order_date")).collect()[0]['MIN_ORDER_DATE']
    end_date = df.select(max(col("order_date")).alias("max_order_date")).collect()[0]['MAX_ORDER_DATE']
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')

    # Creating a pandas DataFrame from the date range
    date_dim_df = pd.DataFrame({
        "order_date": date_range.date,
        "Year": date_range.year,
        "Month": date_range.month,
        "Quarter": date_range.quarter,
        "Day": date_range.day,
        "DayOfWeek": date_range.dayofweek,
        "DayCounter": date_range.dayofyear,
        "DayName": date_range.strftime("%A"),
        "DayOfMonth": date_range.day,
        "Weekday": ["Weekday" if day < 5 else "Weekend" for day in date_range.dayofweek]
    })

    # Convert the pandas DataFrame to a list of dictionaries (Was throwing TypeError Issues)
    data_list = date_dim_df.to_dict(orient='records')

    # Delta Data Processing
    date_dim_snow_df = session.create_dataframe(data=data_list)
    existing_date_dim_df = session.sql("select order_date from sales_dwh.gold.date_dim")
    date_dim_snow_df = date_dim_snow_df.join(existing_date_dim_df, existing_date_dim_df['ORDER_DATE'] == date_dim_snow_df['ORDER_DATE'], join_type='leftanti')

    # Final Selection
    date_dim_snow_df = date_dim_snow_df.selectExpr("""
                                   sales_dwh.gold.date_dim_seq.nextval as date_id_pk,
                                   order_date,
                                   Year as order_year,
                                   Month as order_month,
                                   Quarter as order_quarter,
                                   Day as order_day,
                                   DayOfWeek as order_dayofweek,
                                   DayCounter as order_daycounter,
                                   DayName as order_dayname,
                                   DayOfMonth as order_dayofmonth,
                                   Weekday as order_weekday
                                """)

    # Data Writing
    row_count = int(date_dim_snow_df.count())
    if row_count > 0:
        date_dim_snow_df.write.save_as_table("sales_dwh.gold.date_dim", mode="append")
        print("New data inserted in Date table!")
    else:
        print("No change in Date table!")


def main():
    # Get Snowflake Session Object
    session = get_snowpark_session()

    # Load Silver Tables into DF's
    china_sales_df = session.sql("select * from sales_dwh.silver.china_sales_orders")
    us_sales_df = session.sql("select * from sales_dwh.silver.us_sales_orders")
    france_sales_df = session.sql("select * from sales_dwh.silver.france_sales_orders")

    # Combine Silver DF's into 1 DF
    combined_sales_df = china_sales_df.union(us_sales_df).union(france_sales_df)

    # Create Dimension Tables
    create_region_dim(combined_sales_df, session)
    create_product_dim(combined_sales_df, session)
    create_promo_code_dim(combined_sales_df, session)
    create_customer_dim(combined_sales_df, session)
    create_payment_dim(combined_sales_df, session)
    create_date_dim(combined_sales_df, session)

    # Grab Primary Keys from Dimension Tables
    date_dim_df = session.sql("select date_id_pk, order_date from sales_dwh.gold.date_dim")
    customer_dim_df = session.sql("select customer_id_pk, customer_name, shipping_address, country, region from sales_dwh.gold.customer_dim")
    payment_dim_df = session.sql("select payment_id_pk, payment_method, payment_provider, country, region from sales_dwh.gold.payment_dim")
    product_dim_df = session.sql("select product_id_pk, mobile_key from sales_dwh.gold.product_dim")
    promo_code_dim_df = session.sql("select promo_code_id_pk, promotion_code, country, region from sales_dwh.gold.promo_code_dim")
    region_dim_df = session.sql("select region_id_pk, country, region from sales_dwh.gold.region_dim")

    # Join dimension tables with combined sales DataFrame
    combined_sales_df = combined_sales_df.join(date_dim_df, ["order_date"], join_type='inner')
    combined_sales_df = combined_sales_df.join(customer_dim_df, ["customer_name", "shipping_address", "country", "region"], join_type='inner')
    combined_sales_df = combined_sales_df.join(payment_dim_df, ["payment_method", "payment_provider", "country", "region"], join_type='inner')
    combined_sales_df = combined_sales_df.join(product_dim_df, ["mobile_key"], join_type='inner')
    combined_sales_df = combined_sales_df.join(promo_code_dim_df, ["promotion_code", "country", "region"], join_type='inner')
    combined_sales_df = combined_sales_df.join(region_dim_df, ["country", "region"], join_type='inner')

    # Deduplicate combined sales DataFrame
    combined_sales_df = combined_sales_df.distinct()
    combined_sales_df.show(5)

    combined_sales_df = combined_sales_df.selectExpr("""
                                   sales_dwh.gold.sales_fact_seq.nextval as order_id_pk,
                                   order_id as order_code,
                                   date_id_pk as date_id_fk,
                                   region_id_pk as region_id_fk,
                                   customer_id_pk as customer_id_fk,
                                   payment_id_pk as payment_id_fk,
                                   product_id_pk as product_id_fk,
                                   promo_code_id_pk as promo_code_id_fk,
                                   order_quantity,
                                   local_total_order_amount,
                                   local_tax_amount,
                                   exchange_rate,
                                   us_total_order_amount,
                                   us_tax_amt as us_tax_amount
                               """)

    # Final Write
    combined_sales_df.write.save_as_table("sales_dwh.gold.sales_fact", mode="append")


if __name__ == '__main__':
    main()
