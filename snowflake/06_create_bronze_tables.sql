use role sysadmin;
use database sales_dwh;
use schema bronze;


-- Create Sequences: will help to de-duplicate the data
create or replace sequence china_sales_orders_seq
    start = 1
    increment = 1
    comment='This is sequence for China sales order table';

create or replace sequence us_sales_orders_seq
    start = 1
    increment = 1
    comment='This is sequence for USA sales order table';

create or replace sequence france_sales_orders_seq
    start = 1
    increment = 1
    comment='This is sequence for France sales order table';


-- Create Bronze Tables
-- China Sales Table in Bronze Schema (CSV File)
create or replace transient table china_sales_orders (
 sales_order_key number(38,0),
 order_id varchar(),
 customer_name varchar(),
 mobile_key varchar(),
 order_quantity number(38,0),
 unit_price number(38,0),
 order_value number(38,0),
 promotion_code varchar(),
 final_order_amount number(10,2),
 tax_amount number(10,2),
 order_date date,
 payment_status varchar(),
 shipping_status varchar(),
 payment_method varchar(),
 payment_provider varchar(),
 phone_number varchar(),
 shipping_address varchar(),
 _metadata_file_name varchar(),
 _metadata_row_numer number(38,0),
 _metadata_last_modified timestamp_ntz(9)
);

-- US Sales Table in Bronze Schema (Parquet File)
create or replace transient table us_sales_orders (
 sales_order_key number(38,0),
 order_id varchar(),
 customer_name varchar(),
 mobile_key varchar(),
 order_quantity number(38,0),
 unit_price number(38,0),
 order_value number(38,0),
 promotion_code varchar(),
 final_order_amount number(10,2),
 tax_amount number(10,2),
 order_date date,
 payment_status varchar(),
 shipping_status varchar(),
 payment_method varchar(),
 payment_provider varchar(),
 phone_number varchar(),
 shipping_address varchar(),
 _metadata_file_name varchar(),
 _metadata_row_numer number(38,0),
 _metadata_last_modified timestamp_ntz(9)
);

-- France Sales Table in Bronze Schema (JSON File)
create or replace transient table france_sales_orders (
 sales_order_key number(38,0),
 order_id varchar(),
 customer_name varchar(),
 mobile_key varchar(),
 order_quantity number(38,0),
 unit_price number(38,0),
 order_value number(38,0),
 promotion_code varchar(),
 final_order_amount number(10,2),
 tax_amount number(10,2),
 order_date date,
 payment_status varchar(),
 shipping_status varchar(),
 payment_method varchar(),
 payment_provider varchar(),
 phone_number varchar(),
 shipping_address varchar(),
 _metadata_file_name varchar(),
 _metadata_row_numer number(38,0),
 _metadata_last_modified timestamp_ntz(9)
);

-- View Sequences / Tables
show sequences;
show tables;
select * from china_sales_orders;
select * from france_sales_orders;
select * from us_sales_orders;

-- drop sequence china_sales_orders_seq;
-- drop sequence france_sales_orders_seq;
-- drop sequence us_sales_orders_seq;
-- drop table china_sales_orders;
-- drop table france_sales_orders;
-- drop table us_sales_orders;


-- Add in Variability to the loaded data
insert into china_sales_orders (order_id, PAYMENT_STATUS, SHIPPING_STATUS, _METADATA_LAST_MODIFIED) values ('VTNN3OHRXE0000000000', 'Paid', 'Delivered', '2024-05-31 17:22:20.000');
insert into china_sales_orders (order_id, PAYMENT_STATUS, SHIPPING_STATUS, _METADATA_LAST_MODIFIED) values ('VTNN3OHRXE0000000000', 'Paid', 'Delivered', '2024-05-31 17:22:23.000');
insert into china_sales_orders (order_id, PAYMENT_STATUS, SHIPPING_STATUS, _METADATA_LAST_MODIFIED) values ('VTNN3OHRXE0000000000', 'Paid', 'Delivered', '2024-05-31 17:22:29.000');

select * from china_sales_orders where payment_status = 'Paid' and shipping_status = 'Delivered';
