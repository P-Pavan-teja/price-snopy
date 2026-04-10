-- =========================================
-- DEV WAREHOUSES: CREATION
-- =========================================

use role ACCOUNTADMIN;

-- DEV_ENGINEERING_WH
create or replace warehouse DEV_ENGINEERING_WH
    warehouse_size = 'XSMALL'
    warehouse_type = 'STANDARD'
    auto_suspend   = 120          -- 2 minutes
    auto_resume    = true
    initially_suspended = true
    min_cluster_count   = 1
    max_cluster_count   = 1
    scaling_policy      = 'STANDARD'
    comment = 'DEV warehouse for data engineers, architects, DevOps';

-- DEV_COMPUTE_WH
create or replace warehouse DEV_COMPUTE_WH
    warehouse_size = 'XSMALL'
    warehouse_type = 'STANDARD'
    auto_suspend   = 60           -- 1 minute
    auto_resume    = true
    initially_suspended = true
    min_cluster_count   = 1
    max_cluster_count   = 1
    scaling_policy      = 'STANDARD'
    comment = 'DEV warehouse for BA, DA, QA, support, viewers';

-- DEV_MAINTENANCE_WH
create or replace warehouse DEV_MAINTENANCE_WH
    warehouse_size = 'XSMALL'
    warehouse_type = 'STANDARD'
    auto_suspend   = 300          -- 5 minutes
    auto_resume    = true
    initially_suspended = true
    min_cluster_count   = 1
    max_cluster_count   = 1
    scaling_policy      = 'STANDARD'
    comment = 'DEV maintenance/admin warehouse for platform/admin roles';



-- =========================================
-- DEV WAREHOUSE GRANTS BY ORG ROLE
-- Org roles assumed to already exist:
--   dev_data_engineer
--   dev_architect_lead
--   dev_support
--   dev_viewer
--   dev_devops_infra
--   snf_admin   (platform/account admins)
-- =========================================

use role SECURITYADMIN;


-- =========================================
-- DEV_ENGINEERING_WH
-- For: dev_data_engineer, dev_architect_lead, dev_devops_infra, snf_admin
-- =========================================

-- dev_data_engineer: heavy dev / ETL testing
grant usage   on warehouse DEV_ENGINEERING_WH to role dev_data_engineer;
grant monitor on warehouse DEV_ENGINEERING_WH to role dev_data_engineer;

-- dev_architect_lead: design owner, same visibility as engineers
grant usage   on warehouse DEV_ENGINEERING_WH to role dev_architect_lead;
grant monitor on warehouse DEV_ENGINEERING_WH to role dev_architect_lead;

-- dev_devops_infra: infra-focused, monitors engineering workload
grant usage   on warehouse DEV_ENGINEERING_WH to role dev_devops_infra;
grant monitor on warehouse DEV_ENGINEERING_WH to role dev_devops_infra;

-- snf_admin: full control over DEV_ENGINEERING_WH
grant usage   on warehouse DEV_ENGINEERING_WH to role snf_admin;
grant monitor on warehouse DEV_ENGINEERING_WH to role snf_admin;
grant operate on warehouse DEV_ENGINEERING_WH to role snf_admin;
grant modify  on warehouse DEV_ENGINEERING_WH to role snf_admin;



-- =========================================
-- DEV_COMPUTE_WH
-- For: dev_support, dev_viewer, dev_data_engineer, dev_architect_lead, dev_devops_infra, snf_admin
-- =========================================

-- dev_support (BA, DA, QA, support): business queries only
grant usage   on warehouse DEV_COMPUTE_WH to role dev_support;

-- dev_viewer: read-only consumers
grant usage   on warehouse DEV_COMPUTE_WH to role dev_viewer;

-- dev_data_engineer: can run and monitor business-facing queries if needed
grant usage   on warehouse DEV_COMPUTE_WH to role dev_data_engineer;
grant monitor on warehouse DEV_COMPUTE_WH to role dev_data_engineer;

-- dev_architect_lead: same as data engineers
grant usage   on warehouse DEV_COMPUTE_WH to role dev_architect_lead;
grant monitor on warehouse DEV_COMPUTE_WH to role dev_architect_lead;

-- dev_devops_infra: monitor this shared warehouse as well
grant usage   on warehouse DEV_COMPUTE_WH to role dev_devops_infra;
grant monitor on warehouse DEV_COMPUTE_WH to role dev_devops_infra;

-- snf_admin: full control over DEV_COMPUTE_WH
grant usage   on warehouse DEV_COMPUTE_WH to role snf_admin;
grant monitor on warehouse DEV_COMPUTE_WH to role snf_admin;
grant operate on warehouse DEV_COMPUTE_WH to role snf_admin;
grant modify  on warehouse DEV_COMPUTE_WH to role snf_admin;



-- =========================================
-- DEV_MAINTENANCE_WH
-- For: snf_admin only (maintenance, governance, admin queries)
-- =========================================

grant usage   on warehouse DEV_MAINTENANCE_WH to role snf_admin;
grant monitor on warehouse DEV_MAINTENANCE_WH to role snf_admin;
grant operate on warehouse DEV_MAINTENANCE_WH to role snf_admin;
grant modify  on warehouse DEV_MAINTENANCE_WH to role snf_admin;

USE ROLE SECURITYADMIN;


-- If user already exists, just grant roles
GRANT ROLE dev_data_engineer   TO USER PAVANTEJA;
GRANT ROLE dev_architect_lead  TO USER PAVANTEJA;
GRANT ROLE dev_support         TO USER PAVANTEJA;
GRANT ROLE dev_viewer          TO USER PAVANTEJA;
GRANT ROLE dev_devops_infra    TO USER PAVANTEJA;
GRANT ROLE snf_admin           TO USER PAVANTEJA;