DECLARE
  v_copy_qid STRING;
BEGIN
  -- 1) Run COPY
  COPY INTO mydb.public.t1
    FROM @mystage
    FILE_FORMAT = (FORMAT_NAME = my_csv_ff)
    ON_ERROR = 'SKIP_FILE';

  -- 2) Get the latest COPY query id for this table in this session
  SELECT QUERY_ID
    INTO :v_copy_qid
  FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
         TABLE_NAME => 'MYDB.PUBLIC.T1',
         START_TIME => DATEADD('hour', -1, CURRENT_TIMESTAMP())  -- or tighter window
       ))
  WHERE QUERY_ID = LAST_QUERY_ID()  -- or filter by SESSION_ID / LOAD_STATUS as needed
  ORDER BY START_TIME DESC
  LIMIT 1;
  -- Alternative: just pick the most recent for that table if you know only one COPY ran for it in this proc call.[web:76][web:83]

  -- 3) Use it in VALIDATE
  INSERT INTO load_error_log ( ... )
  SELECT ...
  FROM TABLE(VALIDATE(MYDB.PUBLIC.T1, JOB_ID => :v_copy_qid));
END;
