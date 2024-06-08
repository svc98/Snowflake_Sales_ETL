-- Creating internal stage within sales_dwb > bronze.
use database sales_dwh;
use schema bronze;
use role sysadmin;
create or replace stage internal_loading_stage;


-- View Stages and Content
show stages;
list @internal_loading_stage;