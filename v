CREATE OR REPLACE PROCEDURE AFSVISION_RAW_DB.AFSVISION_CLV_STG_SCH.AFSVISION_EMAIL_SUMMARY_LOAD_1(
    CURRENT_LAYER VARCHAR, 
    EMAILS VARCHAR, 
    JOB_ID VARCHAR, 
    TASKFLOW_NAME VARCHAR
)
RETURNS TABLE()
LANGUAGE SQL
EXECUTE AS CALLER
AS 
DECLARE 
    SQL_STATEMENT_RESULT VARCHAR;
    STATEMENT_TABLE_RESULT VARCHAR;
    v_table_data VARCHAR;
    v_email_subject STRING;
    v_email_body VARCHAR;
    V_EMAIL_RESULT VARCHAR;
    V_SQL_STATEMENT_RESULT VARCHAR;
    V_FILE_NAME VARCHAR;
    V_LOAD_DATE VARCHAR;
    V_ACCOUNTING_DATE VARCHAR;
    ABC RESULTSET;
BEGIN

    -- Main query with CTEs
    WITH CURRENT_LAYER_CTE AS (
        SELECT :CURRENT_LAYER AS CURRENT_LAYER  -- Use parameter instead of hardcoded 'INT'
    ),
    allowed_sources AS (
        SELECT
            TRIM(SOURCE_TABLE_NAME) AS SOURCE_TABLE_NAME,
            REGEXP_REPLACE(TRIM(SOURCE_TABLE_NAME), '^(RAW_|STG_|CUR_)', '') AS BASE_NAME,
            GROUP_NAME
        FROM COMMON_DB.UTIL_SCH.DATA_CONVERSION_MASTER
        WHERE GROUP_NAME IN ('GRP1', 'GRP2', 'GRP3')
    ),
    latest_job AS (
        SELECT JOB_ID
        FROM COMMON_DB.UTIL_SCH.GDR_AUDIT_LOG
        WHERE TASKFLOW_NAME = :TASKFLOW_NAME  -- Use parameter
        ORDER BY LOAD_DATE DESC
        LIMIT 1
    ),
    latest_jobs AS (
        SELECT
            JOB_ID,
            LOAD_DATE,
            ACCOUNTING_DATE,
            TRIM(SOURCE_NAME) AS SOURCE_NAME,
            TRIM(TARGET_NAME) AS TARGET_NAME,
            TOTAL_RECORDS_SOURCE,
            TOTAL_RECORDS_TARGET,
            ERROR_MSG,
            TASKFLOW_STATUS,
            ETL_USER,
            STARTTIME_EST,
            ENDTIME_EST,
            ROW_NUMBER() OVER (
                PARTITION BY SOURCE_NAME
                ORDER BY LOAD_DATE DESC
            ) AS rn,
            REGEXP_REPLACE(TRIM(SOURCE_NAME), '^(RAW_|STG_|CUR_|INT_)', '') AS BASE_NAME
        FROM COMMON_DB.UTIL_SCH.GDR_AUDIT_LOG
        WHERE JOB_ID = (SELECT JOB_ID FROM latest_job)
          AND (
            SOURCE_NAME LIKE 'RAW_%' OR SOURCE_NAME LIKE 'STG_%'
            OR SOURCE_NAME LIKE 'CUR_%' OR SOURCE_NAME LIKE 'INT_%'
          )
    ),
    layer_counts AS (
        SELECT
            raw.JOB_ID,
            raw.LOAD_DATE,
            raw.ACCOUNTING_DATE,
            raw.BASE_NAME,
            REPLACE(raw.SOURCE_NAME, 'RAW_', '') AS FILE_NAME,
            raw.TOTAL_RECORDS_SOURCE AS RAW_COUNT,
            CASE WHEN cte.CURRENT_LAYER IN ('STG', 'CUR', 'INT') THEN stg.TOTAL_RECORDS_TARGET ELSE NULL END AS STG_COUNT,
            CASE WHEN cte.CURRENT_LAYER IN ('CUR', 'INT') THEN cur.TOTAL_RECORDS_TARGET ELSE NULL END AS CUR_COUNT,
            CASE WHEN cte.CURRENT_LAYER = 'INT' THEN intg.TOTAL_RECORDS_TARGET ELSE NULL END AS INT_COUNT,
            CASE
                WHEN cte.CURRENT_LAYER = 'INT' THEN COALESCE(intg.ERROR_MSG, cur.ERROR_MSG, stg.ERROR_MSG, raw.ERROR_MSG)
                WHEN cte.CURRENT_LAYER = 'CUR' THEN COALESCE(cur.ERROR_MSG, stg.ERROR_MSG, raw.ERROR_MSG)
                WHEN cte.CURRENT_LAYER = 'STG' THEN COALESCE(stg.ERROR_MSG, raw.ERROR_MSG)
                ELSE raw.ERROR_MSG
            END AS ERROR_MSG,
            CASE
                WHEN cte.CURRENT_LAYER = 'INT' THEN COALESCE(intg.TASKFLOW_STATUS, cur.TASKFLOW_STATUS, stg.TASKFLOW_STATUS, raw.TASKFLOW_STATUS)
                WHEN cte.CURRENT_LAYER = 'CUR' THEN COALESCE(cur.TASKFLOW_STATUS, stg.TASKFLOW_STATUS, raw.TASKFLOW_STATUS)
                WHEN cte.CURRENT_LAYER = 'STG' THEN COALESCE(stg.TASKFLOW_STATUS, raw.TASKFLOW_STATUS)
                ELSE raw.TASKFLOW_STATUS
            END AS TASKFLOW_STATUS
        FROM latest_jobs raw
        JOIN CURRENT_LAYER_CTE cte ON 1=1  -- Cross join syntax fix
        LEFT JOIN latest_jobs stg ON stg.TARGET_NAME = 'STG_' || raw.BASE_NAME AND stg.rn = 1
        LEFT JOIN latest_jobs cur ON cur.TARGET_NAME = 'CUR_' || raw.BASE_NAME AND cur.rn = 1
        LEFT JOIN latest_jobs intg ON intg.TARGET_NAME = 'INT_' || raw.BASE_NAME AND intg.rn = 1
        WHERE raw.SOURCE_NAME LIKE 'RAW_%'
          AND raw.rn = 1
    ),
    final_result AS (
        SELECT
            lc.FILE_NAME,
            a.GROUP_NAME,
            lc.LOAD_DATE,
            lc.ACCOUNTING_DATE,
            lc.RAW_COUNT,
            lc.STG_COUNT,
            lc.CUR_COUNT,
            lc.INT_COUNT,
            CASE 
                WHEN cte.CURRENT_LAYER IN ('STG', 'CUR', 'INT') 
                THEN COALESCE(lc.RAW_COUNT, 0) - COALESCE(lc.STG_COUNT, 0) 
                ELSE NULL 
            END AS RAW_TO_STG_REJECTED,
            CASE 
                WHEN cte.CURRENT_LAYER IN ('CUR', 'INT') 
                THEN COALESCE(lc.STG_COUNT, 0) - COALESCE(lc.CUR_COUNT, 0) 
                ELSE NULL 
            END AS STG_TO_CUR_REJECTED,
            CASE 
                WHEN cte.CURRENT_LAYER = 'INT' AND a.GROUP_NAME IN ('GRP1', 'GRP2') 
                THEN COALESCE(lc.CUR_COUNT, 0) - COALESCE(lc.INT_COUNT, 0)
                ELSE NULL 
            END AS CUR_TO_INT_REJECTED,
            lc.ERROR_MSG,
            lc.TASKFLOW_STATUS,
            CASE WHEN cte.CURRENT_LAYER = 'RAW' THEN
                    CASE
                        WHEN lc.RAW_COUNT IS NOT NULL AND (lc.TASKFLOW_STATUS = '1' OR lc.TASKFLOW_STATUS = '3')
                             AND (lc.ERROR_MSG IS NULL OR UPPER(TRIM(lc.ERROR_MSG)) IN ('NA', 'N/A','FILE SKIPED', 'NO ERRORS ENCOUNTERED', ''))
                        THEN 'Succeeded' ELSE 'Failed'
                    END
                WHEN cte.CURRENT_LAYER = 'STG' THEN
                    CASE
                        WHEN lc.RAW_COUNT IS NOT NULL AND lc.STG_COUNT IS NOT NULL
                             AND (lc.TASKFLOW_STATUS = '1' OR lc.TASKFLOW_STATUS = '3')
                             AND (lc.ERROR_MSG IS NULL OR UPPER(TRIM(lc.ERROR_MSG)) IN ('NA', 'N/A','FILE SKIPED', 'NO ERRORS ENCOUNTERED', ''))
                        THEN 'Succeeded' ELSE 'Failed'
                    END
                WHEN cte.CURRENT_LAYER = 'CUR' THEN
                    CASE
                        WHEN lc.RAW_COUNT IS NOT NULL AND lc.STG_COUNT IS NOT NULL AND lc.CUR_COUNT IS NOT NULL
                             AND lc.TASKFLOW_STATUS = '1'
                             AND (lc.ERROR_MSG IS NULL OR UPPER(TRIM(lc.ERROR_MSG)) IN ('NA', 'N/A', 'NO ERRORS ENCOUNTERED', ''))
                        THEN 'Succeeded' ELSE 'Failed'
                    END
                WHEN cte.CURRENT_LAYER = 'INT' THEN
                    CASE
                        WHEN a.GROUP_NAME IN ('GRP1', 'GRP2')
                             AND lc.RAW_COUNT IS NOT NULL AND lc.STG_COUNT IS NOT NULL
                             AND lc.CUR_COUNT IS NOT NULL AND lc.INT_COUNT IS NOT NULL
                             AND lc.TASKFLOW_STATUS = '1'
                             AND (lc.ERROR_MSG IS NULL OR UPPER(TRIM(lc.ERROR_MSG)) IN ('NA', 'N/A', 'NO ERRORS ENCOUNTERED', ''))
                        THEN 'Succeeded' 
                        WHEN a.GROUP_NAME NOT IN ('GRP1', 'GRP2')
                             AND lc.RAW_COUNT IS NOT NULL AND lc.STG_COUNT IS NOT NULL
                             AND lc.CUR_COUNT IS NOT NULL  -- INT_COUNT can be NULL
                             AND lc.TASKFLOW_STATUS = '1'
                             AND (lc.ERROR_MSG IS NULL OR UPPER(TRIM(lc.ERROR_MSG)) IN ('NA', 'N/A', 'NO ERRORS ENCOUNTERED', ''))
                        THEN 'Succeeded'
                        ELSE 'Failed'
                    END            
            END AS JOB_STATUS
        FROM layer_counts lc
        JOIN allowed_sources a ON lc.BASE_NAME = a.BASE_NAME
        JOIN CURRENT_LAYER_CTE cte ON 1=1  -- Cross join syntax fix
        ORDER BY lc.FILE_NAME
    )
    
    -- Assign the result to a variable first
    SELECT * FROM final_result;
    
    -- Capture the SQL ID for RESULT_SCAN
    V_SQL_STATEMENT_RESULT := SQLID;

    -- Create the resultset using RESULT_SCAN
    ABC := (SELECT * FROM TABLE(RESULT_SCAN(:V_SQL_STATEMENT_RESULT)));
       
    RETURN TABLE(ABC);
END;