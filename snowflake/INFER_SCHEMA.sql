-- Example 2: Direct query without variables
SELECT ARRAY_SIZE(PARSE_JSON(
    INFER_SCHEMA(
        LOCATION => '@COMMON_DB.UTIL_SCH.MY_EXTERNAL_STAGE/raw/orders/',
        FILE_FORMAT => 'COMMON_DB.UTIL_SCH.MY_CSV_FORMAT',
        FILES => 'orders_2026-05-14.csv'
    )
)) AS actual_column_count;

CREATE OR REPLACE STAGE COMMON_DB.UTIL_SCH.INTERNAL_USE;

-- Enable directory on your internal stage
ALTER STAGE COMMON_DB.UTIL_SCH.INTERNAL_USE 
SET DIRECTORY = (ENABLE = TRUE);

-- Refresh the directory metadata
ALTER STAGE COMMON_DB.UTIL_SCH.INTERNAL_USE REFRESH;

CREATE OR REPLACE FILE FORMAT COMMON_DB.UTIL_SCH.CSV_FORMAT
    TYPE = CSV  -- Must specify TYPE for CSV
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'  -- Use single quote around double quote
    NULL_IF = ('NULL', 'null')
    -- PARSE_HEADER = TRUE  -- For INFER_SCHEMA to detect column names
    COMPRESSION = NONE;
create or replace table COMMON_DB.UTIL_SCH.STG_customer AS
select * from snowflake_sample_data.tpch_sf1.customer
limit 0;

COPY INTO @COMMON_DB.UTIL_SCH.INTERNAL_USE/TPCH_SF1/Source-Files/CUSTOMER-1.csv
from (
SELECT 'ABC' AS RECORD_TYPE, *
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER
ORDER BY C_CUSTKEY ASC
LIMIT 10
OFFSET 0
)
FILE_FORMAT = 'COMMON_DB.UTIL_SCH.CSV_FORMAT'
SINGLE = TRUE
HEADER = TRUE;

COPY INTO @COMMON_DB.UTIL_SCH.INTERNAL_USE/TPCH_SF1/Source-Files/CUSTOMER-2.csv
from (
SELECT  *
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER
ORDER BY C_CUSTKEY ASC
LIMIT 10
OFFSET 10
)
FILE_FORMAT = 'COMMON_DB.UTIL_SCH.CSV_FORMAT'
SINGLE = TRUE
HEADER = TRUE;

COPY INTO @COMMON_DB.UTIL_SCH.INTERNAL_USE/TPCH_SF1/Source-Files/ORDERS-2.csv
from (
SELECT 'ABC' AS RECORD_TYPE, *
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS
ORDER BY O_ORDERKEY ASC
LIMIT 10
OFFSET 0
)
FILE_FORMAT = 'COMMON_DB.UTIL_SCH.CSV_FORMAT'
SINGLE = TRUE
HEADER = TRUE;

remove @COMMON_DB.UTIL_SCH.INTERNAL_USE/TPCH_SF1/Source-Files/CUSTOMER-2.csv;

LIST @COMMON_DB.UTIL_SCH.INTERNAL_USE/TPCH_SF1/Source-Files/;


SELECT *
from table(   INFER_SCHEMA(
        LOCATION => '@COMMON_DB.UTIL_SCH.INTERNAL_USE/TPCH_SF1/Source-Files/CUSTOMER',
        FILE_FORMAT => 'COMMON_DB.UTIL_SCH.CSV_FORMAT'
        -- FILES => 'orders_2026-05-14.csv'
    ));



-- Count files by counting commas + 1
SELECT 
    COLUMN_NAME,
    FILENAMES,
    ARRAY_SIZE(SPLIT(FILENAMES, ',')) AS file_count
FROM TABLE(
    INFER_SCHEMA(
        LOCATION => '@COMMON_DB.UTIL_SCH.INTERNAL_USE/TPCH_SF1/Source-Files/CUSTOMER',
        FILE_FORMAT => 'COMMON_DB.UTIL_SCH.CSV_FORMAT'
    )
)
ORDER BY file_count DESC, COLUMN_NAME;

-- See what files you have
SELECT 
    SPLIT_PART(RELATIVE_PATH, 'TPCH_SF1/Source-Files/', 2) AS file_name,
    SIZE,
    LAST_MODIFIED
FROM DIRECTORY('@COMMON_DB.UTIL_SCH.INTERNAL_USE')
WHERE STARTSWITH(UPPER(RELATIVE_PATH), UPPER('TPCH_SF1/Source-Files/')) 
  AND CONTAINS(UPPER(RELATIVE_PATH), UPPER('CUSTOMER'))
ORDER BY file_name;

-- Check CUSTOMER-1.csv
SELECT COUNT(*) AS column_count
FROM TABLE(
    INFER_SCHEMA(
        LOCATION => '@COMMON_DB.UTIL_SCH.INTERNAL_USE/TPCH_SF1/Source-Files/',
        FILE_FORMAT => 'COMMON_DB.UTIL_SCH.CSV_FORMAT',
        FILES => ('CUSTOMER-1.csv')
    )
);

-- Check CUSTOMER-2.csv
SELECT COUNT(*) AS column_count
FROM TABLE(
    INFER_SCHEMA(
        LOCATION => '@COMMON_DB.UTIL_SCH.INTERNAL_USE/TPCH_SF1/Source-Files/',
        FILE_FORMAT => 'COMMON_DB.UTIL_SCH.CSV_FORMAT',
        FILES => ('CUSTOMER-2.csv')
    )
);

-----------------------------------------------

-- See all your CUSTOMER files
SELECT 
    SPLIT_PART(RELATIVE_PATH, 'TPCH_SF1/Source-Files/', 2) AS file_name,
    SIZE,
    LAST_MODIFIED,
    MD5,
    RELATIVE_PATH
FROM DIRECTORY('@COMMON_DB.UTIL_SCH.INTERNAL_USE')
WHERE STARTSWITH(UPPER(RELATIVE_PATH), UPPER('TPCH_SF1/Source-Files/')) 
  AND CONTAINS(UPPER(RELATIVE_PATH), 'CUSTOMER')
ORDER BY file_name;

WITH 
-- Get file list from DIRECTORY
directory_files AS (
    SELECT 
        SPLIT_PART(RELATIVE_PATH, 'TPCH_SF1/Source-Files/', 2) AS file_name,
        RELATIVE_PATH,
        SIZE,
        LAST_MODIFIED
    FROM DIRECTORY('@COMMON_DB.UTIL_SCH.INTERNAL_USE')
    WHERE STARTSWITH(UPPER(RELATIVE_PATH), UPPER('TPCH_SF1/Source-Files/CUSTOMER'))
),
-- Count total files
file_count AS (
    SELECT COUNT(*) AS total_files
    FROM directory_files
),
-- Get schema info from INFER_SCHEMA
schema_info AS (
    SELECT 
        COLUMN_NAME,
        TYPE,
        FILENAMES,
        ARRAY_SIZE(SPLIT(FILENAMES, ',')) AS num_files_with_column
    FROM TABLE(
        INFER_SCHEMA(
            LOCATION => '@COMMON_DB.UTIL_SCH.INTERNAL_USE/TPCH_SF1/Source-Files/CUSTOMER',
            FILE_FORMAT => 'COMMON_DB.UTIL_SCH.CSV_FORMAT'
        )
    )
),
-- Validation summary
validation AS (
    SELECT 
        COUNT(*) AS total_columns,
        MIN(num_files_with_column) AS min_file_count,
        MAX(num_files_with_column) AS max_file_count,
        (SELECT total_files FROM file_count) AS expected_file_count
    FROM schema_info
)
-- select * from validation;
SELECT 
    v.*,
    CASE 
        WHEN v.min_file_count = v.max_file_count 
         AND v.min_file_count = v.expected_file_count 
        THEN '✅ VALID: All ' || v.expected_file_count || ' files have all ' || v.total_columns || ' columns'
        WHEN v.min_file_count != v.max_file_count
        THEN '❌ INVALID: Columns appear in different number of files (inconsistent schema)'
        WHEN v.max_file_count != v.expected_file_count
        THEN '❌ INVALID: Expected ' || v.expected_file_count || ' files but some columns only appear in ' || v.max_file_count
        ELSE '❌ INVALID: Unknown issue'
    END AS validation_status
FROM validation v;

WITH 
-- Get all files from DIRECTORY
all_files AS (
    SELECT 
        SPLIT_PART(RELATIVE_PATH, 'TPCH_SF1/Source-Files/CUSTOMER/', 2) AS file_name,
        RELATIVE_PATH
    FROM DIRECTORY('@COMMON_DB.UTIL_SCH.INTERNAL_USE')
    WHERE STARTSWITH(UPPER(RELATIVE_PATH), UPPER('TPCH_SF1/Source-Files/CUSTOMER'))
),
-- Get schema with file mappings
schema_info AS (
    SELECT 
        COLUMN_NAME,
        SPLIT(FILENAMES, ',') AS file_array,
        ARRAY_SIZE(SPLIT(FILENAMES, ',')) AS num_files
    FROM TABLE(
        INFER_SCHEMA(
            LOCATION => '@COMMON_DB.UTIL_SCH.INTERNAL_USE/TPCH_SF1/Source-Files/CUSTOMER',
            FILE_FORMAT => 'COMMON_DB.UTIL_SCH.CSV_FORMAT'
        )
    )
),
-- Max files (what we expect)
expected_file_count AS (
    SELECT COUNT(*) AS total_files FROM all_files
),
-- Columns that don't appear in ALL files
problematic_columns AS (
    SELECT 
        s.COLUMN_NAME,
        s.file_array,
        s.num_files,
        e.total_files,
        e.total_files - s.num_files AS missing_from_count
    FROM schema_info s
    CROSS JOIN expected_file_count e
    WHERE s.num_files < e.total_files
)
SELECT 
    COLUMN_NAME,
    num_files AS appears_in_files,
    total_files AS expected_files,
    missing_from_count AS missing_from,
    file_array AS files_containing_column
FROM problematic_columns
ORDER BY missing_from_count DESC, COLUMN_NAME;

WITH 
-- All files from DIRECTORY
all_files AS (
    SELECT DISTINCT
        TRIM(SPLIT_PART(RELATIVE_PATH, 'TPCH_SF1/Source-Files/', 2)) AS file_name
    FROM DIRECTORY('@COMMON_DB.UTIL_SCH.INTERNAL_USE')
    WHERE STARTSWITH(UPPER(RELATIVE_PATH), UPPER('TPCH_SF1/Source-Files/CUSTOMER'))
),
-- All columns from schema
all_columns AS (
    SELECT DISTINCT COLUMN_NAME
    FROM TABLE(
        INFER_SCHEMA(
            LOCATION => '@COMMON_DB.UTIL_SCH.INTERNAL_USE/TPCH_SF1/Source-Files/CUSTOMER',
            FILE_FORMAT => 'COMMON_DB.UTIL_SCH.CSV_FORMAT'
        )
    )
),
-- All expected file-column combinations
expected_combinations AS (
    SELECT 
        af.file_name,
        ac.COLUMN_NAME
    FROM all_files af
    CROSS JOIN all_columns ac
),
-- Actual file-column combinations from INFER_SCHEMA
schema_with_files AS (
    SELECT 
        COLUMN_NAME,
        SPLIT(FILENAMES, ',') AS file_array
    FROM TABLE(
        INFER_SCHEMA(
            LOCATION => '@COMMON_DB.UTIL_SCH.INTERNAL_USE/TPCH_SF1/Source-Files/CUSTOMER',
            FILE_FORMAT => 'COMMON_DB.UTIL_SCH.CSV_FORMAT'
        )
    )
),
actual_combinations AS (
    SELECT DISTINCT
        TRIM(f.value::STRING) AS file_name,
        s.COLUMN_NAME
    FROM schema_with_files s,
    LATERAL FLATTEN(input => s.file_array) f
),
-- Find missing combinations
missing_columns AS (
    SELECT 
        ec.file_name,
        ec.COLUMN_NAME AS missing_column
    FROM expected_combinations ec
    LEFT JOIN actual_combinations ac
        ON CONTAINS(ec.file_name, SPLIT_PART(ac.file_name, '/', -1))
        AND ec.COLUMN_NAME = ac.COLUMN_NAME
    WHERE ac.COLUMN_NAME IS NULL
)
-- Show files with missing columns
SELECT 
    file_name,
    COUNT(*) AS num_missing_columns,
    LISTAGG(missing_column, ', ') WITHIN GROUP (ORDER BY missing_column) AS missing_columns
FROM missing_columns
GROUP BY file_name
HAVING COUNT(*) > 0
ORDER BY num_missing_columns DESC;

-- COMMON_DB.UTIL_SCH.STG_customer

WITH 
-- Expected columns from target table
expected_schema AS (
    SELECT COUNT(*) AS expected_columns
    FROM COMMON_DB.INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'UTIL_SCH'
      AND TABLE_NAME = 'STG_customer'
      AND COLUMN_NAME NOT IN (
          'SOURCE_SYSTEM_NAME', 'SOURCE_FILE_INSERT_TIMESTAMP',
          'SOURCE_FILE_NAME', 'SOURCE_FILE_ROW_NUMBER',
          'ETL_INSERT_TIMESTAMP', 'ETL_USER'
      )
),
-- Actual columns from files
actual_schema AS (
    SELECT COUNT(*) AS actual_columns
    FROM TABLE(
        INFER_SCHEMA(
            LOCATION => '@COMMON_DB.UTIL_SCH.INTERNAL_USE/TPCH_SF1/Source-Files/CUSTOMER',
            FILE_FORMAT => 'COMMON_DB.UTIL_SCH.CSV_FORMAT'
        )
    )
)
SELECT 
    e.expected_columns,
    a.actual_columns,
    CASE 
        WHEN e.expected_columns = a.actual_columns THEN '✅ Column count matches'
        ELSE '❌ Column count mismatch'
    END AS validation_status
FROM expected_schema e
CROSS JOIN actual_schema a;


-----------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------

WITH schema_info AS (
    SELECT 
        COLUMN_NAME,
        ARRAY_SIZE(SPLIT(FILENAMES, ',')) AS num_files
    FROM TABLE(
        INFER_SCHEMA(
            LOCATION => '@COMMON_DB.UTIL_SCH.INTERNAL_USE/TPCH_SF1/Source-Files/ORDERS',
            -- LOCATION => '@COMMON_DB.UTIL_SCH.INTERNAL_USE/TPCH_SF1/Source-Files/CUSTOMER',ORDERS
            FILE_FORMAT => 'COMMON_DB.UTIL_SCH.CSV_FORMAT'
        )
    )
),
validation AS (
    SELECT 
        COUNT(*) AS total_columns,
        MIN(num_files) AS min_files,
        MAX(num_files) AS max_files
    FROM schema_info
)
SELECT 
    CASE 
        WHEN min_files = max_files THEN total_columns::VARCHAR
        ELSE (
            SELECT LISTAGG(COLUMN_NAME, ', ') WITHIN GROUP (ORDER BY COLUMN_NAME)
            FROM schema_info
            WHERE num_files != (SELECT MAX(num_files) FROM schema_info)
        )
    END AS result
FROM validation;

-----------------------------------------------------------------------------------------------------------
-- COMMON_DB.UTIL_SCH.STG_customer

WITH 
-- Expected columns from table
expected_columns AS (
    SELECT 
        COLUMN_NAME,
        COUNT(*) OVER () AS total_expected
    FROM COMMON_DB.INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'UTIL_SCH'  -- Change to your schema
      AND TABLE_NAME = 'STG_customer'
      AND COLUMN_NAME NOT IN (
          'SOURCE_SYSTEM_NAME', 'SOURCE_FILE_INSERT_TIMESTAMP',
          'SOURCE_FILE_NAME', 'SOURCE_FILE_ROW_NUMBER',
          'ETL_INSERT_TIMESTAMP', 'ETL_USER'
      )
),
-- Actual columns from files
actual_columns AS (
    SELECT 
        COLUMN_NAME,
        COUNT(*) OVER () AS total_actual
    FROM TABLE(
        INFER_SCHEMA(
            LOCATION => '@COMMON_DB.UTIL_SCH.INTERNAL_USE/TPCH_SF1/Source-Files/CUSTOMER',
            FILE_FORMAT => 'COMMON_DB.UTIL_SCH.CSV_FORMAT'
        )
    )
),
-- Compare counts
validation AS (
    SELECT 
        (SELECT MAX(total_expected) FROM expected_columns) AS expected_count,
        (SELECT MAX(total_actual) FROM actual_columns) AS actual_count
)
SELECT 
    CASE 
        WHEN expected_count = actual_count THEN 'TRUE'
        WHEN expected_count > actual_count THEN (
            SELECT LISTAGG(COLUMN_NAME, ', ') WITHIN GROUP (ORDER BY COLUMN_NAME)
            FROM expected_columns
            WHERE COLUMN_NAME NOT IN (SELECT COLUMN_NAME FROM actual_columns)
        )
        WHEN actual_count > expected_count THEN (
            SELECT LISTAGG(COLUMN_NAME, ', ') WITHIN GROUP (ORDER BY COLUMN_NAME)
            FROM actual_columns
            WHERE COLUMN_NAME NOT IN (SELECT COLUMN_NAME FROM expected_columns)
        )
    END AS result
FROM validation;

-----------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------

WITH 
-- Get total files from DIRECTORY
directory_files AS (
    SELECT COUNT(*) AS total_files
    FROM DIRECTORY('@COMMON_DB.UTIL_SCH.INTERNAL_USE')
    WHERE STARTSWITH(UPPER(RELATIVE_PATH), UPPER('TPCH_SF1/Source-Files/CUSTOMER'))
),
-- Get schema info
schema_info AS (
    SELECT 
        COLUMN_NAME,
        ARRAY_SIZE(SPLIT(FILENAMES, ',')) AS num_files
    FROM TABLE(
        INFER_SCHEMA(
            LOCATION => '@COMMON_DB.UTIL_SCH.INTERNAL_USE/TPCH_SF1/Source-Files/CUSTOMER',
            FILE_FORMAT => 'COMMON_DB.UTIL_SCH.CSV_FORMAT'
        )
    )
),
-- Check if all columns appear in all files
consistency_check AS (
    SELECT 
        MIN(num_files) AS min_files,
        MAX(num_files) AS max_files,
        (SELECT total_files FROM directory_files) AS expected_files
    FROM schema_info
)
-- Return summary if consistent, else show problematic columns
SELECT 
    CASE 
        WHEN min_files = max_files AND min_files = expected_files 
        THEN '✅ All files (' || expected_files || ') have all columns (' || (SELECT COUNT(*) FROM schema_info) || ')'
        ELSE NULL
    END AS validation_result,
    CASE 
        WHEN min_files != max_files OR min_files != expected_files 
        THEN s.COLUMN_NAME
        ELSE NULL
    END AS missing_column,
    CASE 
        WHEN min_files != max_files OR min_files != expected_files 
        THEN s.num_files || ' / ' || c.expected_files || ' files'
        ELSE NULL
    END AS file_count_mismatch
FROM consistency_check c
LEFT JOIN schema_info s 
    ON (c.min_files != c.max_files OR c.min_files != c.expected_files)
    AND s.num_files < c.expected_files
WHERE validation_result IS NOT NULL 
   OR missing_column IS NOT NULL;

WITH 
directory_files AS (
    SELECT COUNT(*) AS total_files
    FROM DIRECTORY('@COMMON_DB.UTIL_SCH.INTERNAL_USE')
    WHERE STARTSWITH(UPPER(RELATIVE_PATH), UPPER('TPCH_SF1/Source-Files/CUSTOMER'))
),
schema_info AS (
    SELECT 
        COLUMN_NAME,
        ARRAY_SIZE(SPLIT(FILENAMES, ',')) AS num_files
    FROM TABLE(
        INFER_SCHEMA(
            LOCATION => '@COMMON_DB.UTIL_SCH.INTERNAL_USE/TPCH_SF1/Source-Files/CUSTOMER',
            FILE_FORMAT => 'COMMON_DB.UTIL_SCH.CSV_FORMAT'
        )
    )
),
validation AS (
    SELECT 
        (SELECT COUNT(*) FROM schema_info) AS total_columns,
        MIN(num_files) = MAX(num_files) AND MIN(num_files) = (SELECT total_files FROM directory_files) AS is_consistent
    FROM schema_info
)
SELECT 
    CASE WHEN v.is_consistent THEN total_columns || ' columns in all files' ELSE NULL END AS result,
    CASE WHEN NOT v.is_consistent THEN s.COLUMN_NAME ELSE NULL END AS column_name,
    CASE WHEN NOT v.is_consistent THEN s.num_files ELSE NULL END AS file_count
FROM validation v
LEFT JOIN schema_info s ON NOT v.is_consistent AND s.num_files < (SELECT total_files FROM directory_files)
WHERE result IS NOT NULL OR column_name IS NOT NULL;

WITH 
directory_files AS (
    SELECT COUNT(*) AS total_files
    FROM DIRECTORY('@COMMON_DB.UTIL_SCH.INTERNAL_USE')
    WHERE STARTSWITH(UPPER(RELATIVE_PATH), UPPER('TPCH_SF1/Source-Files/CUSTOMER'))
),
schema_info AS (
    SELECT 
        COLUMN_NAME,
        FILENAMES,
        ARRAY_SIZE(SPLIT(FILENAMES, ',')) AS num_files
    FROM TABLE(
        INFER_SCHEMA(
            LOCATION => '@COMMON_DB.UTIL_SCH.INTERNAL_USE/TPCH_SF1/Source-Files/CUSTOMER',
            FILE_FORMAT => 'COMMON_DB.UTIL_SCH.CSV_FORMAT'
        )
    )
),
-- Get all unique files from schema
all_files_from_schema AS (
    SELECT DISTINCT TRIM(f.value::STRING) AS file_name
    FROM schema_info,
    LATERAL FLATTEN(input => SPLIT(FILENAMES, ',')) f
),
validation AS (
    SELECT 
        (SELECT COUNT(*) FROM schema_info) AS total_columns,
        MIN(num_files) = MAX(num_files) AND MIN(num_files) = (SELECT total_files FROM directory_files) AS is_consistent
    FROM schema_info
),
-- Find which files are missing which columns
missing_columns AS (
    SELECT 
        af.file_name,
        s.COLUMN_NAME,
        s.num_files,
        (SELECT total_files FROM directory_files) AS expected_files
    FROM all_files_from_schema af
    CROSS JOIN schema_info s
    LEFT JOIN LATERAL FLATTEN(input => SPLIT(s.FILENAMES, ',')) f
        ON TRIM(f.value::STRING) = af.file_name
    WHERE f.value IS NULL  -- Column is missing from this file
)
SELECT 
    CASE WHEN v.is_consistent THEN v.total_columns || ' columns in all files' ELSE NULL END AS result,
    CASE WHEN NOT v.is_consistent THEN mc.file_name ELSE NULL END AS file_name,
    CASE WHEN NOT v.is_consistent THEN mc.COLUMN_NAME ELSE NULL END AS missing_column
FROM validation v
LEFT JOIN missing_columns mc ON NOT v.is_consistent
WHERE result IS NOT NULL OR file_name IS NOT NULL
ORDER BY file_name, missing_column;

WITH 
directory_files AS (
    SELECT COUNT(*) AS total_files
    FROM DIRECTORY('@COMMON_DB.UTIL_SCH.INTERNAL_USE')
    WHERE STARTSWITH(UPPER(RELATIVE_PATH), UPPER('TPCH_SF1/Source-Files/CUSTOMER'))
),
schema_info AS (
    SELECT 
        COLUMN_NAME,
        FILENAMES,
        ARRAY_SIZE(SPLIT(FILENAMES, ',')) AS num_files
    FROM TABLE(
        INFER_SCHEMA(
            LOCATION => '@COMMON_DB.UTIL_SCH.INTERNAL_USE/TPCH_SF1/Source-Files/CUSTOMER',
            FILE_FORMAT => 'COMMON_DB.UTIL_SCH.CSV_FORMAT'
        )
    )
),
-- Get all unique files
all_files AS (
    SELECT DISTINCT TRIM(f.value::STRING) AS file_name
    FROM schema_info,
    LATERAL FLATTEN(input => SPLIT(FILENAMES, ',')) f
),
validation AS (
    SELECT 
        (SELECT COUNT(*) FROM schema_info) AS total_columns,
        MIN(num_files) = MAX(num_files) AND MIN(num_files) = (SELECT total_files FROM directory_files) AS is_consistent
    FROM schema_info
),
-- Find missing columns: cross join all files with all columns, then subtract actual combinations
all_combinations AS (
    SELECT 
        af.file_name,
        s.COLUMN_NAME
    FROM all_files af
    CROSS JOIN schema_info s
),
-- Get actual file-column combinations
actual_combinations AS (
    SELECT DISTINCT
        TRIM(f.value::STRING) AS file_name,
        s.COLUMN_NAME
    FROM schema_info s,
    LATERAL FLATTEN(input => SPLIT(s.FILENAMES, ',')) f
),
-- Find what's missing
missing_columns AS (
    SELECT 
        ac.file_name,
        ac.COLUMN_NAME
    FROM all_combinations ac
    LEFT JOIN actual_combinations actual
        ON ac.file_name = actual.file_name 
        AND ac.COLUMN_NAME = actual.COLUMN_NAME
    WHERE actual.COLUMN_NAME IS NULL  -- This column is missing from this file
)
SELECT 
    CASE WHEN v.is_consistent THEN v.total_columns || ' columns in all files' ELSE NULL END AS result,
    CASE WHEN NOT v.is_consistent THEN mc.file_name ELSE NULL END AS file_name,
    CASE WHEN NOT v.is_consistent THEN mc.COLUMN_NAME ELSE NULL END AS missing_column
FROM validation v
LEFT JOIN missing_columns mc ON NOT v.is_consistent
WHERE result IS NOT NULL OR file_name IS NOT NULL
ORDER BY file_name, missing_column;

WITH 
directory_files AS (
    SELECT COUNT(*) AS total_files
    FROM DIRECTORY('@COMMON_DB.UTIL_SCH.INTERNAL_USE')
    WHERE STARTSWITH(UPPER(RELATIVE_PATH), UPPER('TPCH_SF1/Source-Files/CUSTOMER'))
),
schema_info AS (
    SELECT 
        FILENAMES,
        ARRAY_SIZE(SPLIT(FILENAMES, ',')) AS num_files
    FROM TABLE(
        INFER_SCHEMA(
            LOCATION => '@COMMON_DB.UTIL_SCH.INTERNAL_USE/TPCH_SF1/Source-Files/CUSTOMER',
            FILE_FORMAT => 'COMMON_DB.UTIL_SCH.CSV_FORMAT'
        )
    )
    -- )
),
-- Files that have missing columns (appear in num_files < total_files)
files_with_issues AS (
    SELECT DISTINCT TRIM(f.value::STRING) AS file_name
    FROM schema_info s,
    LATERAL FLATTEN(input => SPLIT(s.FILENAMES, ',')) f
    WHERE s.num_files < (SELECT total_files FROM directory_files)
)
SELECT file_name FROM files_with_issues;

    SELECT 
    FROM DIRECTORY('@COMMON_DB.UTIL_SCH.INTERNAL_USE')
    WHERE STARTSWITH(UPPER(RELATIVE_PATH), UPPER('TPCH_SF1/Source-Files/CUSTOMER'));

    SELECT 
        COLUMN_NAME,
        FILENAMES,
        ARRAY_SIZE(SPLIT(FILENAMES, ',')) AS num_files
    FROM TABLE(
        INFER_SCHEMA(
            LOCATION => '@COMMON_DB.UTIL_SCH.INTERNAL_USE/TPCH_SF1/Source-Files/CUSTOMER',
            FILE_FORMAT => 'COMMON_DB.UTIL_SCH.CSV_FORMAT'
        )
    );
WITH INFER_SCHEMA_DATA AS (
SELECT 
    COLUMN_NAME,
    TRIM(f.value::STRING) AS file_name
FROM TABLE(
    INFER_SCHEMA(
        LOCATION => '@COMMON_DB.UTIL_SCH.INTERNAL_USE/TPCH_SF1/Source-Files/CUSTOMER',
        FILE_FORMAT => 'COMMON_DB.UTIL_SCH.CSV_FORMAT'
    )
),
LATERAL FLATTEN(input => SPLIT(FILENAMES, ',')) f
ORDER BY COLUMN_NAME, file_name
), DIRECTORY_DATA AS(
    SELECT *
    FROM DIRECTORY('@COMMON_DB.UTIL_SCH.INTERNAL_USE')
    WHERE STARTSWITH(UPPER(RELATIVE_PATH), UPPER('TPCH_SF1/Source-Files/CUSTOMER')))

SELECT * 
FROM INFER_SCHEMA_DATA ISD
LEFT JOIN DIRECTORY_DATA DD ON DD.RELATIVE_PATH = ISD.file_name;


WITH 
infer_schema_data AS (
    SELECT 
        COLUMN_NAME,
        TRIM(f.value::STRING) AS file_name
    FROM TABLE(
        INFER_SCHEMA(
            LOCATION => '@COMMON_DB.UTIL_SCH.INTERNAL_USE/TPCH_SF1/Source-Files/CUSTOMER',
            FILE_FORMAT => 'COMMON_DB.UTIL_SCH.CSV_FORMAT'
        )
    ),
    LATERAL FLATTEN(input => SPLIT(FILENAMES, ',')) f
),
expected_columns AS (
    SELECT COUNT(DISTINCT COLUMN_NAME) AS total_columns
    FROM infer_schema_data
),
file_column_count AS (
    SELECT 
        file_name,
        COUNT(DISTINCT COLUMN_NAME) AS column_count
    FROM infer_schema_data
    GROUP BY file_name
)
SELECT file_name
FROM file_column_count fc
CROSS JOIN expected_columns ec
WHERE fc.column_count < ec.total_columns;

WITH schema_info AS (
    SELECT 
        TRIM(f.value::STRING) AS file_name,
        COUNT(DISTINCT COLUMN_NAME) AS column_count
    FROM TABLE(
        INFER_SCHEMA(
            LOCATION => '@COMMON_DB.UTIL_SCH.INTERNAL_USE/TPCH_SF1/Source-Files/CUSTOMER',
            FILE_FORMAT => 'COMMON_DB.UTIL_SCH.CSV_FORMAT'
        )
    ),
    LATERAL FLATTEN(input => SPLIT(FILENAMES, ',')) f
    GROUP BY file_name
)
SELECT file_name
FROM schema_info
WHERE column_count < (SELECT MAX(column_count) FROM schema_info);
