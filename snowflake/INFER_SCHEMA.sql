-- Example 2: Direct query without variables
SELECT ARRAY_SIZE(PARSE_JSON(
    INFER_SCHEMA(
        LOCATION => '@COMMON_DB.UTIL_SCH.MY_EXTERNAL_STAGE/raw/orders/',
        FILE_FORMAT => 'COMMON_DB.UTIL_SCH.MY_CSV_FORMAT',
        FILES => 'orders_2026-05-14.csv'
    )
)) AS actual_column_count;
