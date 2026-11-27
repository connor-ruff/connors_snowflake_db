CREATE OR REPLACE PROCEDURE BOOKS.CONFIG.MERGE_RAW_TO_FINAL_GENERAL(TARGET_TABLE_NAME VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_raw_table       VARCHAR;
    v_target_table_name VARCHAR;
    v_raw_table_name VARCHAR;
    v_delete_key_column VARCHAR;
    v_insert_col_list   VARCHAR;
    v_select_col_list   VARCHAR;
    v_sql             VARCHAR;
    v_deleted         INT;
    v_inserted        INT;
BEGIN


    SELECT 
        RAW_TABLE_NAME,
        TARGET_TABLE_NAME,
        DELETE_KEY_COLUMN
    INTO  :v_raw_table_name, :v_target_table_name, :v_delete_key_column
    FROM CONFIG.SOURCE_TABLE_INFO
    WHERE TARGET_TABLE_NAME = :TARGET_TABLE_NAME;


    -- Extract target column list and source select statement based on the crosswalk configuration
    SELECT 

        -- INSERT (<list>)
        LISTAGG(TARGET_COLUMN_NAME, ',\n') WITHIN GROUP (ORDER BY RAW_COLUMN_NUMBER, TARGET_COLUMN_NAME) AS TARGET_COLUMNS,

        -- SELECT (<list>) FROM RAW_TABLE
        LISTAGG(

            CASE 
                -- Column has some custom transformation logic applied
            WHEN CUSTOM_TRANSFORMATION_LOGIC IS NOT NULL THEN CUSTOM_TRANSFORMATION_LOGIC

            -- Column is a standard DP column name that is set in the s3-to-raw process, so can just be carried over as-is from the RAW table
            WHEN IS_DP_COLUMN AND TARGET_COLUMN_NAME IN ('DP_RUN_ID', 'DP_SOURCE_FILE_NAME', 'DP_SOURCE_FILE_ROW_NUMBER') THEN TARGET_COLUMN_NAME

                -- Column is the DP load timestamp, which should be set to the current timestamp
            WHEN IS_DP_COLUMN AND TARGET_COLUMN_NAME = 'DP_LOAD_TIMESTAMP' THEN 'CURRENT_TIMESTAMP()'

            -- Column is a DP column that doesn't match any of the known cases and doesn't have custom logic, so raise an error
            WHEN IS_DP_COLUMN THEN '!!!ERROR!!!'

            -- Standard type checks 
            WHEN TARGET_DATA_TYPE = 'DATE' THEN 'TO_DATE(' || RAW_COLUMN_NAME || ', \'YYYY-MM-DD\')'

            WHEN TARGET_DATA_TYPE ILIKE 'NUMBER%' THEN 'TO_NUMBER(' || RAW_COLUMN_NAME || ',  ' || REGEXP_SUBSTR(TARGET_DATA_TYPE, '\\d+', 1, 1) || ', ' || REGEXP_SUBSTR(TARGET_DATA_TYPE, '\\d+', 1, 2)    || ')'

            WHEN TARGET_DATA_TYPE ILIKE 'VARCHAR%' THEN TRIM(RAW_COLUMN_NAME)

            ELSE '!!!ERROR!!!'

            END, ',\n'

        ) WITHIN GROUP (ORDER BY RAW_COLUMN_NUMBER, TARGET_COLUMN_NAME) AS INSERT_STATEMENT


    INTO :v_insert_col_list, :v_select_col_list
    FROM CONFIG.CROSSWALK
    WHERE TARGET_TABLE_NAME = :v_target_table_name
    ;


    -- Check for any '!!!ERROR!!!' in the select list (i.e. a column with no valid mapping)
    IF (POSITION('!!!ERROR!!!', :v_select_col_list) > 0) THEN
        RETURN 'ERROR: Invalid column mapping found in crosswalk for table ' || :v_target_table_name || '. No changes made.';
    END IF;

    -- 1) Stage the transformed rows in a temp table with the exact structure of the target.
    --    Any failing transformation (e.g. TO_DATE on an unparseable file name) errors here, BEFORE the target is touched.
    v_sql := 'CREATE OR REPLACE TEMPORARY TABLE TEMP_STAGED LIKE BOOKS.' || :v_target_table_name;
    EXECUTE IMMEDIATE :v_sql;

    v_sql := 'INSERT INTO TEMP_STAGED (' || :v_insert_col_list || ') SELECT ' || :v_select_col_list || ' FROM BOOKS.' || :v_raw_table_name;
    EXECUTE IMMEDIATE :v_sql;
    v_inserted := SQLROWCOUNT;

    -- 2) Replace the target rows for every key value (file month, royalty date, ...) present in the staged data.
    BEGIN TRANSACTION;

    v_sql := 'DELETE FROM BOOKS.' || :v_target_table_name || ' WHERE ' || :v_delete_key_column ||
             ' IN (SELECT DISTINCT ' || :v_delete_key_column || ' FROM TEMP_STAGED)';
    EXECUTE IMMEDIATE :v_sql;
    v_deleted := SQLROWCOUNT;

    v_sql := 'INSERT INTO BOOKS.' || :v_target_table_name || ' SELECT * FROM TEMP_STAGED';
    EXECUTE IMMEDIATE :v_sql;

    COMMIT;

    RETURN 'SUCCESS: Deleted ' || :v_deleted::VARCHAR || ' row(s), inserted ' || :v_inserted::VARCHAR || ' row(s) into BOOKS.' || :v_target_table_name || '.';

EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;
        RETURN 'ERROR: ' || SQLERRM || ' (no changes made to BOOKS.' || :v_target_table_name || ')';
END;
$$;
