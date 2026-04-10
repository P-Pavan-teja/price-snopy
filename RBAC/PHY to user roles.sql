USE ROLE SECURITYADMIN;

-- =========================================================
-- PHYSICAL / ORG ROLE -> USER ASSIGNMENTS
-- DB roles are already attached to org roles
-- Warehouse grants are already attached to org roles
-- So users should get only these org roles
-- =========================================================

-- President
GRANT ROLE dev_viewer TO USER peter;

-- HR
GRANT ROLE dev_support TO USER anithA;
GRANT ROLE dev_support TO USER dinakar;

-- Architect
GRANT ROLE dev_architect_lead TO USER prashanth_kumar;

-- Data Engineer
GRANT ROLE dev_data_engineer TO USER poorna_prasadh;

-- DevOps
GRANT ROLE dev_devops_infra TO USER Sunny;

-- Developers
GRANT ROLE dev_data_engineer TO USER barath;
GRANT ROLE dev_data_engineer TO USER vishal;

-- Testers
GRANT ROLE dev_support TO USER manikanth;
GRANT ROLE dev_support TO USER ajay;

-- Lead
GRANT ROLE dev_architect_lead TO USER gagan;

-- Project Manager
GRANT ROLE dev_support TO USER venkat;

-- BA
GRANT ROLE dev_support TO USER AKSHAY;

-- Scrum Master
GRANT ROLE dev_support TO USER Anirudh;

-- Admins
GRANT ROLE snf_admin TO USER sree;
GRANT ROLE snf_admin TO USER Vijay;

-- Support
GRANT ROLE dev_support TO USER Daniel;
GRANT ROLE dev_support TO USER Gopal;

-- UI/UX Designer
GRANT ROLE dev_data_engineer TO USER Vineeth;