use role sysadmin;
use database sales_dwh;
use schema gold;

-- Create Sequences:
create or replace sequence region_dim_seq start = 1 increment = 1;
create or replace sequence product_dim_seq start = 1 increment = 1;
create or replace sequence promo_code_dim_seq start = 1 increment = 1;
create or replace sequence customer_dim_seq start = 1 increment = 1;
create or replace sequence payment_dim_seq start = 1 increment = 1;
create or replace sequence date_dim_seq start = 1 increment = 1;


-- Create Dimension Tables:
create or replace transient table region_dim(
    region_id_pk number primary key,
    country text,
    region text,
    isActive text(1)
);

create or replace transient table product_dim(
    product_id_pk number primary key,
    mobile_key text,
    brand text,
    model text,
    color text,
    memory text,
    isActive text(1)
);

create or replace transient table promo_code_dim(
    promo_code_id_pk number primary key,
    promo_code text,
    isActive text(1)
);

create or replace transient table customer_dim(
    customer_id_pk number primary key,
    customer_name text,
    phone_number text,
    shipping_address text,
    country text,
    region text,
    isActive text(1)
);

create or replace transient table payment_dim(
    payment_id_pk number primary key,
    payment_method text,
    payment_provider text,
    country text,
    region text,
    isActive text(1)
);

create or replace transient table date_dim(
    date_id_pk int primary key,
    order_date date,
    order_year int,
    oder_month int,
    order_quarter int,
    order_day int,
    order_dayofweek int,
    order_dayname text,
    order_dayofmonth int,
    order_weekday text
);


-- Sales Fact Table:
create or replace table sales_fact (
 order_id_pk number(38,0),
 order_code varchar(),
 date_id_fk number(38,0),
 region_id_fk number(38,0),
 customer_id_fk number(38,0),
 payment_id_fk number(38,0),
 product_id_fk number(38,0),
 promo_code_id_fk number(38,0),
 order_quantity number(38,0),
 local_total_order_amount number(10,2),
 local_tax_amount number(10,2),
 exchange_rate number(15,7),
 us_total_order_amount number(23,8),
 usd_tax_amount number(23,8)
);


-- Sales Fact Table Constraints:
alter table sales_fact add
    constraint fk_sales_region FOREIGN KEY (REGION_ID_FK) REFERENCES region_dim (REGION_ID_PK) NOT ENFORCED;

alter table sales_fact add
    constraint fk_sales_date FOREIGN KEY (DATE_ID_FK) REFERENCES date_dim (DATE_ID_PK) NOT ENFORCED;

alter table sales_fact add
    constraint fk_sales_customer FOREIGN KEY (CUSTOMER_ID_FK) REFERENCES customer_dim (CUSTOMER_ID_PK) NOT ENFORCED;
--
alter table sales_fact add
    constraint fk_sales_payment FOREIGN KEY (PAYMENT_ID_FK) REFERENCES payment_dim (PAYMENT_ID_PK) NOT ENFORCED;

alter table sales_fact add
    constraint fk_sales_product FOREIGN KEY (PRODUCT_ID_FK) REFERENCES product_dim (PRODUCT_ID_PK) NOT ENFORCED;

alter table sales_fact add
    constraint fk_sales_promo FOREIGN KEY (PROMO_CODE_ID_FK) REFERENCES promo_code_dim (PROMO_CODE_ID_PK) NOT ENFORCED;