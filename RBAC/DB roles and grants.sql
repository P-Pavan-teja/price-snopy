-- =========================================
-- READ ONLY ROLES FOR ALL DBS
-- Common schema-level readable objects
-- Includes:
--   database usage
--   schema usage
--   select on tables, views, materialized views
--   usage on sequences
--   usage on functions
-- Excludes:
--   stages, file formats, streams, pipes, tasks, procedures
-- =========================================

use role securityadmin;

-- =========================================
-- SALES RO
-- =========================================

create or replace role sales_ro;


grant usage on database SALES to role sales_ro;
grant usage on all schemas in database SALES to role sales_ro;
grant usage on future schemas in database SALES to role sales_ro;

grant select on all tables in database SALES to role sales_ro;
grant select on future tables in database SALES to role sales_ro;

grant select on all views in database SALES to role sales_ro;
grant select on future views in database SALES to role sales_ro;

grant select on all materialized views in database SALES to role sales_ro;
grant select on future materialized views in database SALES to role sales_ro;

grant usage on all sequences in database SALES to role sales_ro;
grant usage on future sequences in database SALES to role sales_ro;

grant usage on all functions in database SALES to role sales_ro;
grant usage on future functions in database SALES to role sales_ro;


-- =========================================
-- FINANCE RO
-- =========================================

create or replace role finance_ro;


grant usage on database FINANCE to role finance_ro;
grant usage on all schemas in database FINANCE to role finance_ro;
grant usage on future schemas in database FINANCE to role finance_ro;

grant select on all tables in database FINANCE to role finance_ro;
grant select on future tables in database FINANCE to role finance_ro;

grant select on all views in database FINANCE to role finance_ro;
grant select on future views in database FINANCE to role finance_ro;

grant select on all materialized views in database FINANCE to role finance_ro;
grant select on future materialized views in database FINANCE to role finance_ro;

grant usage on all sequences in database FINANCE to role finance_ro;
grant usage on future sequences in database FINANCE to role finance_ro;

grant usage on all functions in database FINANCE to role finance_ro;
grant usage on future functions in database FINANCE to role finance_ro;


-- =========================================
-- HR RO
-- =========================================

create or replace role hr_ro;


grant usage on database HR to role hr_ro;
grant usage on all schemas in database HR to role hr_ro;
grant usage on future schemas in database HR to role hr_ro;

grant select on all tables in database HR to role hr_ro;
grant select on future tables in database HR to role hr_ro;

grant select on all views in database HR to role hr_ro;
grant select on future views in database HR to role hr_ro;

grant select on all materialized views in database HR to role hr_ro;
grant select on future materialized views in database HR to role hr_ro;

grant usage on all sequences in database HR to role hr_ro;
grant usage on future sequences in database HR to role hr_ro;

grant usage on all functions in database HR to role hr_ro;
grant usage on future functions in database HR to role hr_ro;

-- =======================================================================================================================================
-- =======================================================================================================================================
-- =======================================================================================================================================
-- =========================================
-- SALES RW
-- Read/write on business data and read/execute on helpers
-- No create/alter/drop on any objects
-- No explicit grants on streams, pipes, tasks
-- =========================================

create or replace role sales_rw;

grant usage on database SALES to role sales_rw;
grant usage on all schemas in database SALES to role sales_rw;
grant usage on future schemas in database SALES to role sales_rw;

grant select, insert, update, delete, truncate on all tables in database SALES to role sales_rw;
grant select, insert, update, delete, truncate on future tables in database SALES to role sales_rw;

grant select on all views in database SALES to role sales_rw;
grant select on future views in database SALES to role sales_rw;

grant select on all materialized views in database SALES to role sales_rw;
grant select on future materialized views in database SALES to role sales_rw;

grant usage on all sequences in database SALES to role sales_rw;
grant usage on future sequences in database SALES to role sales_rw;

grant usage on all stages in database SALES to role sales_rw;
grant usage on future stages in database SALES to role sales_rw;

grant usage on all file formats in database SALES to role sales_rw;
grant usage on future file formats in database SALES to role sales_rw;

grant usage on all functions in database SALES to role sales_rw;
grant usage on future functions in database SALES to role sales_rw;

grant usage on all procedures in database SALES to role sales_rw;
grant usage on future procedures in database SALES to role sales_rw;



-- =========================================
-- FINANCE RW
-- =========================================

create or replace role finance_rw;

grant usage on database FINANCE to role finance_rw;
grant usage on all schemas in database FINANCE to role finance_rw;
grant usage on future schemas in database FINANCE to role finance_rw;

grant select, insert, update, delete, truncate on all tables in database FINANCE to role finance_rw;
grant select, insert, update, delete, truncate on future tables in database FINANCE to role finance_rw;

grant select on all views in database FINANCE to role finance_rw;
grant select on future views in database FINANCE to role finance_rw;

grant select on all materialized views in database FINANCE to role finance_rw;
grant select on future materialized views in database FINANCE to role finance_rw;

grant usage on all sequences in database FINANCE to role finance_rw;
grant usage on future sequences in database FINANCE to role finance_rw;

grant usage on all stages in database FINANCE to role finance_rw;
grant usage on future stages in database FINANCE to role finance_rw;

grant usage on all file formats in database FINANCE to role finance_rw;
grant usage on future file formats in database FINANCE to role finance_rw;

grant usage on all functions in database FINANCE to role finance_rw;
grant usage on future functions in database FINANCE to role finance_rw;

grant usage on all procedures in database FINANCE to role finance_rw;
grant usage on future procedures in database FINANCE to role finance_rw;



-- =========================================
-- HR RW
-- =========================================

create or replace role hr_rw;

grant usage on database HR to role hr_rw;
grant usage on all schemas in database HR to role hr_rw;
grant usage on future schemas in database HR to role hr_rw;

grant select, insert, update, delete, truncate on all tables in database HR to role hr_rw;
grant select, insert, update, delete, truncate on future tables in database HR to role hr_rw;

grant select on all views in database HR to role hr_rw;
grant select on future views in database HR to role hr_rw;

grant select on all materialized views in database HR to role hr_rw;
grant select on future materialized views in database HR to role hr_rw;

grant usage on all sequences in database HR to role hr_rw;
grant usage on future sequences in database HR to role hr_rw;

grant usage on all stages in database HR to role hr_rw;
grant usage on future stages in database HR to role hr_rw;

grant usage on all file formats in database HR to role hr_rw;
grant usage on future file formats in database HR to role hr_rw;

grant usage on all functions in database HR to role hr_rw;
grant usage on future functions in database HR to role hr_rw;

grant usage on all procedures in database HR to role hr_rw;
grant usage on future procedures in database HR to role hr_rw;


-- =========================================
-- SALES FULL
-- Full schema-level design inside SALES
-- Can create/alter/drop schema objects (tables, views, stages, etc.)
-- No create/drop database, schema, warehouse, external integration
-- =========================================

-- =======================================================================================================================================
-- =======================================================================================================================================
-- =======================================================================================================================================

create or replace role sales_full;

grant usage on database SALES to role sales_full;
grant usage on all schemas in database SALES to role sales_full;
grant usage on future schemas in database SALES to role sales_full;

grant select, insert, update, delete, truncate on all tables in database SALES to role sales_full;
grant select, insert, update, delete, truncate on future tables in database SALES to role sales_full;

grant select on all views in database SALES to role sales_full;
grant select on future views in database SALES to role sales_full;

grant select on all materialized views in database SALES to role sales_full;
grant select on future materialized views in database SALES to role sales_full;

grant usage on all sequences in database SALES to role sales_full;
grant usage on future sequences in database SALES to role sales_full;

grant usage on all stages in database SALES to role sales_full;
grant usage on future stages in database SALES to role sales_full;

grant usage on all file formats in database SALES to role sales_full;
grant usage on future file formats in database SALES to role sales_full;

grant usage on all functions in database SALES to role sales_full;
grant usage on future functions in database SALES to role sales_full;

grant usage on all procedures in database SALES to role sales_full;
grant usage on future procedures in database SALES to role sales_full;

grant create table on all schemas in database SALES to role sales_full;
grant create view on all schemas in database SALES to role sales_full;
grant create materialized view on all schemas in database SALES to role sales_full;
grant create sequence on all schemas in database SALES to role sales_full;
grant create stage on all schemas in database SALES to role sales_full;
grant create file format on all schemas in database SALES to role sales_full;
grant create function on all schemas in database SALES to role sales_full;
grant create procedure on all schemas in database SALES to role sales_full;
grant create stream on all schemas in database SALES to role sales_full;
grant create pipe on all schemas in database SALES to role sales_full;
grant create task on all schemas in database SALES to role sales_full;



-- =========================================
-- FINANCE FULL
-- Full schema-level design inside FINANCE
-- =========================================

create or replace role finance_full;

grant usage on database FINANCE to role finance_full;
grant usage on all schemas in database FINANCE to role finance_full;
grant usage on future schemas in database FINANCE to role finance_full;

grant select, insert, update, delete, truncate on all tables in database FINANCE to role finance_full;
grant select, insert, update, delete, truncate on future tables in database FINANCE to role finance_full;

grant select on all views in database FINANCE to role finance_full;
grant select on future views in database FINANCE to role finance_full;

grant select on all materialized views in database FINANCE to role finance_full;
grant select on future materialized views in database FINANCE to role finance_full;

grant usage on all sequences in database FINANCE to role finance_full;
grant usage on future sequences in database FINANCE to role finance_full;

grant usage on all stages in database FINANCE to role finance_full;
grant usage on future stages in database FINANCE to role finance_full;

grant usage on all file formats in database FINANCE to role finance_full;
grant usage on future file formats in database FINANCE to role finance_full;

grant usage on all functions in database FINANCE to role finance_full;
grant usage on future functions in database FINANCE to role finance_full;

grant usage on all procedures in database FINANCE to role finance_full;
grant usage on future procedures in database FINANCE to role finance_full;

grant create table on all schemas in database FINANCE to role finance_full;
grant create view on all schemas in database FINANCE to role finance_full;
grant create materialized view on all schemas in database FINANCE to role finance_full;
grant create sequence on all schemas in database FINANCE to role finance_full;
grant create stage on all schemas in database FINANCE to role finance_full;
grant create file format on all schemas in database FINANCE to role finance_full;
grant create function on all schemas in database FINANCE to role finance_full;
grant create procedure on all schemas in database FINANCE to role finance_full;
grant create stream on all schemas in database FINANCE to role finance_full;
grant create pipe on all schemas in database FINANCE to role finance_full;
grant create task on all schemas in database FINANCE to role finance_full;



-- =========================================
-- HR FULL
-- Full schema-level design inside HR
-- =========================================

create or replace role hr_full;

grant usage on database HR to role hr_full;
grant usage on all schemas in database HR to role hr_full;
grant usage on future schemas in database HR to role hr_full;

grant select, insert, update, delete, truncate on all tables in database HR to role hr_full;
grant select, insert, update, delete, truncate on future tables in database HR to role hr_full;

grant select on all views in database HR to role hr_full;
grant select on future views in database HR to role hr_full;

grant select on all materialized views in database HR to role hr_full;
grant select on future materialized views in database HR to role hr_full;

grant usage on all sequences in database HR to role hr_full;
grant usage on future sequences in database HR to role hr_full;

grant usage on all stages in database HR to role hr_full;
grant usage on future stages in database HR to role hr_full;

grant usage on all file formats in database HR to role hr_full;
grant usage on future file formats in database HR to role hr_full;

grant usage on all functions in database HR to role hr_full;
grant usage on future functions in database HR to role hr_full;

grant usage on all procedures in database HR to role hr_full;
grant usage on future procedures in database HR to role hr_full;

grant create table on all schemas in database HR to role hr_full;
grant create view on all schemas in database HR to role hr_full;
grant create materialized view on all schemas in database HR to role hr_full;
grant create sequence on all schemas in database HR to role hr_full;
grant create stage on all schemas in database HR to role hr_full;
grant create file format on all schemas in database HR to role hr_full;
grant create function on all schemas in database HR to role hr_full;
grant create procedure on all schemas in database HR to role hr_full;
grant create stream on all schemas in database HR to role hr_full;
grant create pipe on all schemas in database HR to role hr_full;
grant create task on all schemas in database HR to role hr_full;