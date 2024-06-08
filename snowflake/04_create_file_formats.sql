-- Create file formats CSV (China), JSON (France), Parquet (USA)
use role sysadmin;
use database sales_dwh;
use schema common;

-- CSV file format
create or replace file format csv_format
  type = csv
  field_delimiter = ','
  skip_header = 1
  null_if = ('null', 'null')
  empty_field_as_null = true
  field_optionally_enclosed_by = '\042'
  compression = auto;

-- JSON file format with strip outer array true
create or replace file format json_format
  type = json
  strip_outer_array = true
  compression = auto;

-- Parquet file format
create or replace file format parquet_format
  type = parquet
  compression = snappy;


-- View File Formats
show file formats;