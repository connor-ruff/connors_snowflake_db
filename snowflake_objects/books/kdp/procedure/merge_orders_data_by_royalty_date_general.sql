CREATE OR REPLACE PROCEDURE BOOKS.KDP.MERGE_ORDERS_DATA_BY_ROYALTY_DATE_GENERAL(TARGET_TABLE_NAME VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_raw_table       VARCHAR;
    v_date_col_raw    VARCHAR;
    v_date_col_target VARCHAR;
    v_target_col_list VARCHAR;
    v_source_select   VARCHAR;
    v_sql             VARCHAR;
    v_deleted         INT;
    v_inserted        INT;
BEGIN

    SELECT RAW_TABLE_NAME
    INTO   :v_raw_table
    FROM   BOOKS.CONFIG.SOURCE_TABLE_INFO
    WHERE  TARGET_TABLE_NAME = :TARGET_TABLE_NAME;

    SELECT RAW_COLUMN_NAME, TARGET_COLUMN_NAME
    INTO   :v_date_col_raw, :v_date_col_target
    FROM   BOOKS.CONFIG.CROSSWALK
    WHERE  TARGET_TABLE_NAME = :TARGET_TABLE_NAME
      AND  TARGET_DATA_TYPE = 'DATE';

    SELECT LISTAGG(TARGET_COLUMN_NAME, ', ')    WITHIN GROUP (ORDER BY RAW_COLUMN_NUMBER),
           LISTAGG(
               CASE
                   WHEN TARGET_DATA_TYPE = 'DATE'        THEN 'TRY_TO_DATE(' || RAW_COLUMN_NAME || ', ''YYYY-MM-DD'')'
                   WHEN TARGET_DATA_TYPE LIKE 'NUMBER%'  THEN 'TRY_TO_NUMBER(' || RAW_COLUMN_NAME || ')'
                   ELSE RAW_COLUMN_NAME
               END, ', '
           )                                    WITHIN GROUP (ORDER BY RAW_COLUMN_NUMBER)
    INTO   :v_target_col_list, :v_source_select
    FROM   BOOKS.CONFIG.CROSSWALK
    WHERE  TARGET_TABLE_NAME = :TARGET_TABLE_NAME;

    v_sql :=
'DELETE FROM BOOKS.' || :TARGET_TABLE_NAME || '
WHERE ' || :v_date_col_target || ' IN (
    SELECT DISTINCT TRY_TO_DATE(' || :v_date_col_raw || ', ''YYYY-MM-DD'')
    FROM BOOKS.' || :v_raw_table || '
)';
    EXECUTE IMMEDIATE :v_sql;
    v_deleted := SQLROWCOUNT;

    v_sql :=
'INSERT INTO BOOKS.' || :TARGET_TABLE_NAME || ' (
    ' || :v_target_col_list || ',
    DP_RUN_ID, DP_SOURCE_FILE_NAME, DP_SOURCE_FILE_ROW_NUMBER, DP_LOAD_TIMESTAMP
)
SELECT
    ' || :v_source_select || ',
    DP_RUN_ID, DP_SOURCE_FILE_NAME, DP_SOURCE_FILE_ROW_NUMBER, DP_LOAD_TIMESTAMP
FROM BOOKS.' || :v_raw_table;
    EXECUTE IMMEDIATE :v_sql;
    v_inserted := SQLROWCOUNT;

    RETURN 'SUCCESS: Deleted ' || :v_deleted::VARCHAR || ' row(s), inserted ' || :v_inserted::VARCHAR || ' row(s) into BOOKS.' || :TARGET_TABLE_NAME || '.';
END;
$$;



