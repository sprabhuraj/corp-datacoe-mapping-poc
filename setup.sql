/*
USE ROLE THAT WILL ALLOWS YOU TO CREATE A DATABASE AND/OR SCHEMA FOR THIS PROTOTYPE
*/

--this is the role in my sandbox where I can create a db
use role prd_sysadmin; 

-- ################################################################################
-- DATABASES AND SCHEMAS FOR MAPPING PROTOTYPE
-- ################################################################################
create database if not exists jci;
create schema if not exists mapping;
create schema if not exists commercial;
create schema if not exists hvac;


-- ################################################################################
-- DDL FOR MAPPING TABLE AMD SEED
-- ################################################################################

-- this is used to id records that are inserted
create or replace sequence mapping.dd_seq;

-- this will be used by streamlit to get a distinct list of business units and available tables to edit
create or replace transient table mapping.jci_region_bu_mapping (
    id int default mapping.dd_seq.nextval,
    bu string,
    table_name string ,
    load_ts timestamp default current_timestamp(),
    constraint pk primary key (id)
);


insert into mapping.jci_region_bu_mapping (bu, table_name)
values('COMMERCIAL', 'BUSINESS_MAPPING_TABLE');
insert into mapping.jci_region_bu_mapping (bu, table_name)
values('HVAC', 'BUSINESS_MAPPING_TABLE');

select *
from mapping.jci_region_bu_mapping;


create or replace transient table HVAC.BUSINESS_MAPPING_TABLE (
  id int default mapping.dd_seq.nextval
  , branch_type string 
  , case_owner_branch string
  , comments string
  , load_ts timestamp default current_timestamp()
  , updated_by string default current_user()
  , constraint pk primary key (id)
);

insert into HVAC.BUSINESS_MAPPING_TABLE (branch_type, case_owner_branch, comments)
values ('DIRECT', 'CASE OWNER 1 - CARLOS', 'this is the end attribute');
insert into HVAC.BUSINESS_MAPPING_TABLE (branch_type, case_owner_branch, comments)
values ('INDIRECT', 'OTHER CASE OWNERS', 'HVAC');
insert into HVAC.BUSINESS_MAPPING_TABLE (branch_type, case_owner_branch, comments)
values ('DIRECT', 'SF IT CASE OWNER', 'COMMERCIAL');

select *
from hvac.business_mapping_table;

create or replace transient table COMMERCIAL.BUSINESS_MAPPING_TABLE (
  id int default mapping.dd_seq.nextval
  , branch_type string 
  , case_owner_branch string
  , comments string
  , load_ts timestamp default current_timestamp()
  , updated_by string default current_user()
  , constraint pk primary key (id)
);


insert into COMMERCIAL.BUSINESS_MAPPING_TABLE (branch_type, case_owner_branch, comments)
values ('DIRECT', 'CASE OWNER 1 - CARLOS', 'N/A');
insert into COMMERCIAL.BUSINESS_MAPPING_TABLE (branch_type, case_owner_branch, comments)
values ('INDIRECT', 'OTHER CASE OWNERS', 'N/A');
insert into COMMERCIAL.BUSINESS_MAPPING_TABLE (branch_type, case_owner_branch, comments)
values ('DIRECT', 'SF IT CASE OWNER', 'N/A');

select *
from COMMERCIAL.BUSINESS_MAPPING_TABLE
;


