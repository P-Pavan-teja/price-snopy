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

    -- Execute the main query and capture results
    ABC := (
        WITH allowed_sources AS (
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
            WHERE TASKFLOW_NAME = :TASKFLOW_NAME
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
                stg.TOTAL_RECORDS_TARGET AS STG_COUNT,
                cur.TOTAL_RECORDS_TARGET AS CUR_COUNT,
                intg.TOTAL_RECORDS_TARGET AS INT_COUNT,
                COALESCE(intg.ERROR_MSG, cur.ERROR_MSG, stg.ERROR_MSG, raw.ERROR_MSG) AS ERROR_MSG,
                COALESCE(intg.TASKFLOW_STATUS, cur.TASKFLOW_STATUS, stg.TASKFLOW_STATUS, raw.TASKFLOW_STATUS) AS TASKFLOW_STATUS
            FROM latest_jobs raw
            LEFT JOIN latest_jobs stg ON stg.TARGET_NAME = 'STG_' || raw.BASE_NAME AND stg.rn = 1
            LEFT JOIN latest_jobs cur ON cur.TARGET_NAME = 'CUR_' || raw.BASE_NAME AND cur.rn = 1
            LEFT JOIN latest_jobs intg ON intg.TARGET_NAME = 'INT_' || raw.BASE_NAME AND intg.rn = 1
            WHERE raw.SOURCE_NAME LIKE 'RAW_%'
              AND raw.rn = 1
        )
        SELECT
            lc.FILE_NAME,
            a.GROUP_NAME,
            lc.LOAD_DATE,
            lc.ACCOUNTING_DATE,
            lc.RAW_COUNT,
            lc.STG_COUNT,
            lc.CUR_COUNT,
            lc.INT_COUNT,
            (COALESCE(lc.RAW_COUNT, 0) - COALESCE(lc.STG_COUNT, 0)) AS RAW_TO_STG_REJECTED,
            (COALESCE(lc.STG_COUNT, 0) - COALESCE(lc.CUR_COUNT, 0)) AS STG_TO_CUR_REJECTED,
            (COALESCE(lc.CUR_COUNT, 0) - COALESCE(lc.INT_COUNT, 0)) AS CUR_TO_INT_REJECTED,
            lc.ERROR_MSG,
            lc.TASKFLOW_STATUS,
            CASE 
                WHEN :CURRENT_LAYER = 'RAW' THEN
                    CASE
                        WHEN lc.RAW_COUNT IS NOT NULL 
                             AND (lc.TASKFLOW_STATUS = '1' OR lc.TASKFLOW_STATUS = '3')
                             AND (lc.ERROR_MSG IS NULL OR UPPER(TRIM(lc.ERROR_MSG)) IN ('NA', 'N/A','FILE SKIPED', 'NO ERRORS ENCOUNTERED', ''))
                        THEN 'Succeeded' 
                        ELSE 'Failed'
                    END
                WHEN :CURRENT_LAYER = 'STG' THEN
                    CASE
                        WHEN lc.RAW_COUNT IS NOT NULL AND lc.STG_COUNT IS NOT NULL
                             AND (lc.TASKFLOW_STATUS = '1' OR lc.TASKFLOW_STATUS = '3')
                             AND (lc.ERROR_MSG IS NULL OR UPPER(TRIM(lc.ERROR_MSG)) IN ('NA', 'N/A','FILE SKIPED', 'NO ERRORS ENCOUNTERED', ''))
                        THEN 'Succeeded' 
                        ELSE 'Failed'
                    END
                WHEN :CURRENT_LAYER = 'CUR' THEN
                    CASE
                        WHEN lc.RAW_COUNT IS NOT NULL AND lc.STG_COUNT IS NOT NULL AND lc.CUR_COUNT IS NOT NULL
                             AND lc.TASKFLOW_STATUS = '1'
                             AND (lc.ERROR_MSG IS NULL OR UPPER(TRIM(lc.ERROR_MSG)) IN ('NA', 'N/A', 'NO ERRORS ENCOUNTERED', ''))
                        THEN 'Succeeded' 
                        ELSE 'Failed'
                    END
                WHEN :CURRENT_LAYER = 'INT' THEN
                    CASE
                        WHEN a.GROUP_NAME IN ('GRP1', 'GRP2')
                             AND lc.RAW_COUNT IS NOT NULL AND lc.STG_COUNT IS NOT NULL
                             AND lc.CUR_COUNT IS NOT NULL AND lc.INT_COUNT IS NOT NULL
                             AND lc.TASKFLOW_STATUS = '1'
                             AND (lc.ERROR_MSG IS NULL OR UPPER(TRIM(lc.ERROR_MSG)) IN ('NA', 'N/A', 'NO ERRORS ENCOUNTERED', ''))
                        THEN 'Succeeded' 
                        WHEN a.GROUP_NAME NOT IN ('GRP1', 'GRP2')
                             AND lc.RAW_COUNT IS NOT NULL AND lc.STG_COUNT IS NOT NULL
                             AND lc.CUR_COUNT IS NOT NULL
                             AND lc.TASKFLOW_STATUS = '1'
                             AND (lc.ERROR_MSG IS NULL OR UPPER(TRIM(lc.ERROR_MSG)) IN ('NA', 'N/A', 'NO ERRORS ENCOUNTERED', ''))
                        THEN 'Succeeded'
                        ELSE 'Failed'
                    END
                ELSE 'Unknown'
            END AS JOB_STATUS
        FROM layer_counts lc
        JOIN allowed_sources a ON lc.BASE_NAME = a.BASE_NAME
        ORDER BY lc.FILE_NAME
    );
       
    RETURN TABLE(ABC);
END;