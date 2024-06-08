use role sysadmin;
use database sales_dwh;
use schema silver;

-- Create Sequences: will help to de-duplicate the data
create or replace sequence china_sales_order_seq
    start = 1
    increment = 1
    comment='This is sequence for China sales order table';

create or replace sequence us_sales_order_seq
    start = 1
    increment = 1
    comment='This is sequence for USA sales order table';

create or replace sequence france_sales_order_seq
    start = 1
    increment = 1
    comment='This is sequence for France sales order table';


-- Create Silver Tables
-- China Sales Table in Silver Schema (CSV File)
create or replace table china_sales_orders (
    sales_order_key number(38,0),
    order_id varchar(),
    order_date date,
    customer_name varchar(),
    phone_number varchar(),
    mobile_key varchar(),
    country varchar(),
    region varchar(),
    order_quantity number(38,0),
    local_currency varchar(),
    local_unit_price number(38,0),
    promotion_code varchar(),
    local_total_order_amount number(10,2),
    local_tax_amount number(10,2),
    exchange_rate number(15,7),
    us_total_order_amount number(10,2),
    us_tax_amt number(10,2),
    payment_status varchar(),
    shipping_status varchar(),
    payment_method varchar(),
    payment_provider varchar(),
    shipping_address varchar()
);

-- US Sales Table in Silver Schema (Parquet File)
create or replace table us_sales_orders (
    sales_order_key number(38,0),
    order_id varchar(),
    order_date date,
    customer_name varchar(),
    phone_number varchar(),
    mobile_key varchar(),
    country varchar(),
    region varchar(),
    order_quantity number(38,0),
    local_currency varchar(),
    local_unit_price number(38,0),
    promotion_code varchar(),
    local_total_order_amount number(10,2),
    local_tax_amount number(10,2),
    exchange_rate number(15,7),
    us_total_order_amount number(10,2),
    us_tax_amt number(10,2),
    payment_status varchar(),
    shipping_status varchar(),
    payment_method varchar(),
    payment_provider varchar(),
    shipping_address varchar()
);

-- France Sales Table in Silver Schema (JSON File)
create or replace table france_sales_orders (
    sales_order_key number(38,0),
    order_id varchar(),
    order_date date,
    customer_name varchar(),
    phone_number varchar(),
    mobile_key varchar(),
    country varchar(),
    region varchar(),
    order_quantity number(38,0),
    local_currency varchar(),
    local_unit_price number(38,0),
    promotion_code varchar(),
    local_total_order_amount number(10,2),
    local_tax_amount number(10,2),
    exchange_rate number(15,7),
    us_total_order_amount number(10,2),
    us_tax_amt number(10,2),
    payment_status varchar(),
    shipping_status varchar(),
    payment_method varchar(),
    payment_provider varchar(),
    shipping_address varchar()
);

-- View Sequences / Tables
show sequences;
show tables;
select * from china_sales_orders;
select * from france_sales_orders;
select * from us_sales_orders;