USE DATABASE ADMIN_DB;
USE SCHEMA UTIL_SCH;

CREATE OR REPLACE TABLE LINEAGE_EDGES (
  QUERY_ID         STRING,
  QUERY_START_TIME TIMESTAMP_LTZ,
  USER_NAME        STRING,
  SOURCE_OBJECT    STRING,
  TARGET_OBJECT    STRING,
  EDGE_TYPE        STRING,   -- 'READ_TO_WRITE'
  LOAD_TS          TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Helpful for lookups
CREATE OR REPLACE VIEW LINEAGE_EDGES_LATEST AS
SELECT *
FROM LINEAGE_EDGES
QUALIFY ROW_NUMBER() OVER (PARTITION BY QUERY_ID, SOURCE_OBJECT, TARGET_OBJECT ORDER BY QUERY_START_TIME DESC) = 1;

CREATE OR REPLACE PROCEDURE ADMIN_DB.UTIL_SCH.SP_REFRESH_LINEAGE_EDGES(DAYS_BACK INTEGER)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS OWNER
AS

BEGIN

MERGE INTO ADMIN_DB.UTIL_SCH.LINEAGE_EDGES t
USING (
  WITH ah AS (
    SELECT
      query_id,
      query_start_time,
      user_name,
      direct_objects_accessed,
      objects_modified
    FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY
    WHERE query_start_time >= DATEADD('day', -:DAYS_BACK, CURRENT_TIMESTAMP())
  ),
  src AS (
    SELECT
      a.query_id,
      a.query_start_time,
      a.user_name,
      s.value:objectName::string AS source_object
    FROM ah a,
         LATERAL FLATTEN(input => a.direct_objects_accessed) s
    WHERE s.value:objectDomain::string IN ('Table','View')
  ),
  tgt AS (
    SELECT
      a.query_id,
      a.query_start_time,
      a.user_name,
      m.value:objectName::string AS target_object
    FROM ah a,
         LATERAL FLATTEN(input => a.objects_modified) m
    WHERE m.value:objectDomain::string IN ('Table','View')
  )
  SELECT
    s.query_id,
    s.query_start_time,
    s.user_name,
    s.source_object,
    t.target_object,
    'READ_TO_WRITE' AS edge_type
  FROM src s
  JOIN tgt t
    ON s.query_id = t.query_id
  WHERE s.source_object IS NOT NULL
    AND t.target_object IS NOT NULL
) s
ON  t.query_id = s.query_id
AND t.source_object = s.source_object
AND t.target_object = s.target_object
WHEN NOT MATCHED THEN
  INSERT (
    QUERY_ID,
    QUERY_START_TIME,
    USER_NAME,
    SOURCE_OBJECT,
    TARGET_OBJECT,
    EDGE_TYPE
  )
  VALUES (
    s.query_id,
    s.query_start_time,
    s.user_name,
    s.source_object,
    s.target_object,
    s.edge_type
  );

RETURN 'OK';

END;


CALL ADMIN_DB.UTIL_SCH.SP_REFRESH_LINEAGE_EDGES(30);

SELECT * FROM LINEAGE_EDGES;

SELECT DISTINCT SOURCE_OBJECT
FROM ADMIN_DB.UTIL_SCH.LINEAGE_EDGES_LATEST
-- WHERE TARGET_OBJECT = 'DG_DB.DATA.CUSTOMER'
ORDER BY 1;

SELECT DISTINCT TARGET_OBJECT
FROM ADMIN_DB.UTIL_SCH.LINEAGE_EDGES_LATEST
-- WHERE SOURCE_OBJECT = 'DG_DB.DATA.CUSTOMER'
ORDER BY 1;
