CREATE OR REPLACE PROCEDURE BOOKS.SPOT.MERGE_LISTENER_WEEKLY_REPORT_GENERAL(TARGET_TABLE_NAME VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_raw_table       VARCHAR;
    v_target_col_list VARCHAR;
    v_source_select   VARCHAR;
    v_source_file_name_col VARCHAR;
    v_sql             VARCHAR;
    v_deleted         INT;
    v_inserted        INT;
    v_unparsed        INT;
BEGIN

    SELECT RAW_TABLE_NAME
    INTO   :v_raw_table
    FROM   BOOKS.CONFIG.SOURCE_TABLE_INFO
    WHERE  TARGET_TABLE_NAME = :TARGET_TABLE_NAME;

    v_source_file_name_col := 'DP_SOURCE_FILE_NAME';


    SELECT LISTAGG(TARGET_COLUMN_NAME, ', ')    WITHIN GROUP (ORDER BY RAW_COLUMN_NUMBER),
           LISTAGG(
               CASE
                   WHEN TARGET_DATA_TYPE = 'DATE'        THEN 'TRY_TO_DATE(' || RAW_COLUMN_NAME || ', ''YYYY-MM-DD'')'
                   WHEN TARGET_DATA_TYPE LIKE 'NUMBER%'  THEN 'TRY_TO_NUMBER(' || RAW_COLUMN_NAME || ', ' || REGEXP_SUBSTR(TARGET_DATA_TYPE, '\\d+', 1, 1) || ', ' || REGEXP_SUBSTR(TARGET_DATA_TYPE, '\\d+', 1, 2) || ')'
                   ELSE RAW_COLUMN_NAME
               END, ', '
           )                                    WITHIN GROUP (ORDER BY RAW_COLUMN_NUMBER)
    INTO   :v_target_col_list, :v_source_select
    FROM   BOOKS.CONFIG.CROSSWALK
    WHERE  TARGET_TABLE_NAME = :TARGET_TABLE_NAME;

    -- Create temporary table that includes DP_SOURCE_FILE_BEGIN_DATE and DP_SOURCE_FILE_END_DATE, parsed from the file name.
    -- File names look like: books/spot/weekly_listeners/csv_stage/weekly-20261116-00000-of-00001.csv
    -- The 8-digit YYYYMMDD after "weekly-" is the Monday AFTER the reporting week (Mon-Sun), so:
    --   BEGIN_DATE = file date - 7 days (Monday), END_DATE = file date - 1 day (Sunday)
    --   e.g. 20261116 -> 2026-11-09 through 2026-11-15
    v_sql :=
'CREATE OR REPLACE TEMPORARY TABLE TEMP_RAW AS
SELECT t.*,
       DATEADD(DAY, -7, t.FILE_DATE) AS DP_SOURCE_FILE_BEGIN_DATE,
       DATEADD(DAY, -1, t.FILE_DATE) AS DP_SOURCE_FILE_END_DATE
FROM (SELECT *,
             TRY_TO_DATE(
                 REGEXP_SUBSTR(' || :v_source_file_name_col || ', ''weekly-([0-9]{8})-'', 1, 1, ''e'', 1),
                 ''YYYYMMDD''
             ) AS FILE_DATE
      FROM BOOKS.' || :v_raw_table || ') t';
    EXECUTE IMMEDIATE :v_sql;

    -- Fail loudly if any file name could not be parsed (otherwise the delete below would miss them and reruns would duplicate rows)
    SELECT COUNT(*) INTO :v_unparsed FROM TEMP_RAW WHERE DP_SOURCE_FILE_BEGIN_DATE IS NULL;
    IF (v_unparsed > 0) THEN
        RETURN 'ERROR: Could not derive DP_SOURCE_FILE_BEGIN_DATE/END_DATE for ' || :v_unparsed::VARCHAR || ' row(s) in BOOKS.' || :v_raw_table || '. No changes made.';
    END IF;

    -- Delete existing rows in the target table for the given file week(s)
    -- NOTE: reruns replace rows for each week present in the raw table.
    v_sql :=
    'DELETE FROM BOOKS.' || :TARGET_TABLE_NAME || '
WHERE DP_SOURCE_FILE_BEGIN_DATE IN (
    SELECT DISTINCT DP_SOURCE_FILE_BEGIN_DATE
    FROM TEMP_RAW
)';
    EXECUTE IMMEDIATE :v_sql;
    v_deleted := SQLROWCOUNT;

    -- Insert new rows from the raw table into the target table
    v_sql :=
'INSERT INTO BOOKS.' || :TARGET_TABLE_NAME || ' (
    ' || :v_target_col_list || ',
    DP_RUN_ID, DP_SOURCE_FILE_NAME, DP_SOURCE_FILE_ROW_NUMBER, DP_LOAD_TIMESTAMP,
    DP_SOURCE_FILE_BEGIN_DATE, DP_SOURCE_FILE_END_DATE
)
SELECT
    ' || :v_source_select || ',
    DP_RUN_ID, DP_SOURCE_FILE_NAME, DP_SOURCE_FILE_ROW_NUMBER, DP_LOAD_TIMESTAMP,
    DP_SOURCE_FILE_BEGIN_DATE, DP_SOURCE_FILE_END_DATE
FROM TEMP_RAW';
    EXECUTE IMMEDIATE :v_sql;
    v_inserted := SQLROWCOUNT;

    RETURN 'SUCCESS: Deleted ' || :v_deleted::VARCHAR || ' row(s), inserted ' || :v_inserted::VARCHAR || ' row(s) into BOOKS.' || :TARGET_TABLE_NAME || '.';
END;
$$;
