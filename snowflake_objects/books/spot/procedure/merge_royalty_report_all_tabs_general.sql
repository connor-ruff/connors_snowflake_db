CREATE OR REPLACE PROCEDURE BOOKS.SPOT.MERGE_ROYALTY_REPORT_ALL_TABS_GENERAL(TARGET_TABLE_NAME VARCHAR)
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

    -- Create temporary table that includes DP_SOURCE_FILE_MONTH, parsed from the file name.
    -- File names look like: .../retail/Jenifer Ruff - Digital Royalty - March 2026-retail.csv
    --                       .../retail/Jenifer Ruff - Digital Royalty - August 2026-retail.csv
    -- The month is the "<Month name> <YYYY>" after "Royalty - " (e.g. "March 2026"). It is parsed
    -- as '01 March 2026' with the 'DD MMMM YYYY' format, so DP_SOURCE_FILE_MONTH is the 1st of that month.
    v_sql :=
'CREATE OR REPLACE TEMPORARY TABLE TEMP_RAW AS
SELECT *,
       TRY_TO_DATE(
           ''01 '' || REGEXP_SUBSTR(' || :v_source_file_name_col || ', ''Royalty - ([A-Za-z]+ [0-9]{4})-'', 1, 1, ''e'', 1),
           ''DD MMMM YYYY''
       ) AS DP_SOURCE_FILE_MONTH
FROM BOOKS.' || :v_raw_table;
    EXECUTE IMMEDIATE :v_sql;

    -- Fail loudly if any file name could not be parsed (otherwise the delete below would miss them and reruns would duplicate rows)
    SELECT COUNT(*) INTO :v_unparsed FROM TEMP_RAW WHERE DP_SOURCE_FILE_MONTH IS NULL;
    IF (v_unparsed > 0) THEN
        RETURN 'ERROR: Could not derive DP_SOURCE_FILE_MONTH for ' || :v_unparsed::VARCHAR || ' row(s) in BOOKS.' || :v_raw_table || '. No changes made.';
    END IF;

    -- Delete existing rows in the target table for the given file month date
    -- NOTE: all files for a month land in the same table, so stage them together.
    v_sql :=
    'DELETE FROM BOOKS.' || :TARGET_TABLE_NAME || '
WHERE DP_SOURCE_FILE_MONTH IN (
    SELECT DISTINCT DP_SOURCE_FILE_MONTH
    FROM TEMP_RAW
)';
    EXECUTE IMMEDIATE :v_sql;
    v_deleted := SQLROWCOUNT;

    -- Insert new rows from the raw table into the target table
    v_sql :=
'INSERT INTO BOOKS.' || :TARGET_TABLE_NAME || ' (
    ' || :v_target_col_list || ',
    DP_RUN_ID, DP_SOURCE_FILE_NAME, DP_SOURCE_FILE_ROW_NUMBER, DP_LOAD_TIMESTAMP,
    DP_SOURCE_FILE_MONTH
)
SELECT
    ' || :v_source_select || ',
    DP_RUN_ID, DP_SOURCE_FILE_NAME, DP_SOURCE_FILE_ROW_NUMBER, DP_LOAD_TIMESTAMP,
    DP_SOURCE_FILE_MONTH
FROM TEMP_RAW';
    EXECUTE IMMEDIATE :v_sql;
    v_inserted := SQLROWCOUNT;

    RETURN 'SUCCESS: Deleted ' || :v_deleted::VARCHAR || ' row(s), inserted ' || :v_inserted::VARCHAR || ' row(s) into BOOKS.' || :TARGET_TABLE_NAME || '.';
END;
$$;
