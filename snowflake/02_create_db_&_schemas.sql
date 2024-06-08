-- Create database
create database if not exists sales_dwh;
use database sales_dwh;


-- Create Schemas
create schema if not exists bronze; -- will have source stage etc
create schema if not exists silver; -- data curation and de-duplication
create schema if not exists gold; -- fact & dimension
create schema if not exists audit; -- to capture all audit records
create schema if not exists common; -- for file formats sequence object etc