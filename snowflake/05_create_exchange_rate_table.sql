-- From Internal Stage: Load the data into Table
use role sysadmin;
use database sales_dwh;
use schema common;

-- Create Exchange Rate table
create or replace transient table exchange_rate(
    date date,
    usd2usd int,
    usd2eur decimal(10,4),
    usd2jpy decimal(10,4)
);

-- Copy from Internal Stage into Table
copy into sales_dwh.common.exchange_rate
from
(
select
    t.$1::date as exchange_date,
    to_number(t.$2) as usd2usd,
    to_decimal(t.$3,10,4) as usd2eur,
    to_decimal(t.$4,10,4) as usd2jpy
from
     @sales_dwh.bronze.internal_loading_stage/exchange_rate.csv
     (file_format => 'sales_dwh.common.csv_format') t
);