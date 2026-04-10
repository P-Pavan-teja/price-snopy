-- =========================================
-- DEV ORG ROLES
-- =========================================

create or replace role dev_data_engineer;
create or replace role dev_architect_lead;
create or replace role dev_support;
create or replace role dev_viewer;
create or replace role dev_devops_infra;

-- =========================================
-- DEV DATA ENGINEER
-- Builders: UI/UX, developers, data engineers
-- Full schema-level design in DEV for all business DBs
-- =========================================

grant role sales_full   to role dev_data_engineer;
grant role finance_full to role dev_data_engineer;
grant role hr_full      to role dev_data_engineer;



-- =========================================
-- DEV ARCHITECT LEAD
-- Architect and technical lead
-- Same access as dev_data_engineer in DEV
-- =========================================

grant role sales_full   to role dev_architect_lead;
grant role finance_full to role dev_architect_lead;
grant role hr_full      to role dev_architect_lead;



-- =========================================
-- DEV SUPPORT
-- HR, PM, BA, testers, support in DEV
-- Read/write on data, no schema design
-- =========================================

grant role sales_rw     to role dev_support;
grant role finance_rw   to role dev_support;
grant role hr_rw        to role dev_support;



-- =========================================
-- DEV VIEWER
-- Read-only on DEV data
-- =========================================

grant role sales_ro     to role dev_viewer;
grant role finance_ro   to role dev_viewer;
grant role hr_ro        to role dev_viewer;



-- =========================================
-- DEV DEVOPS INFRA
-- DevOps: infra-focused, data read-only in DEV
-- (Warehouse / integration roles will be added separately)
-- =========================================

grant role sales_ro     to role dev_devops_infra;
grant role finance_ro   to role dev_devops_infra;
grant role hr_ro        to role dev_devops_infra;