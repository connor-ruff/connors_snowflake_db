CREATE OR REPLACE PROCEDURE BOOKS.CONFIG.COPY_S3_TO_RAW_GENERAL(RAW_TABLE_NAME VARCHAR, DP_RUN_ID VARCHAR DEFAULT NULL)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_stage_name   VARCHAR;
    v_file_format  VARCHAR;
    v_col_list     VARCHAR;
    v_pos_list     VARCHAR;
    v_run_id       VARCHAR;
    v_sql          VARCHAR;
BEGIN

    v_run_id := COALESCE(:DP_RUN_ID, 'manual-' || CURRENT_TIMESTAMP()::VARCHAR);
    SELECT S3_STAGE_NAME, FILE_FORMAT_NAME
    INTO   :v_stage_name, :v_file_format
    FROM   BOOKS.CONFIG.SOURCE_TABLE_INFO
    WHERE  RAW_TABLE_NAME = :RAW_TABLE_NAME;

    SELECT LISTAGG(RAW_COLUMN_NAME, ', ')              WITHIN GROUP (ORDER BY RAW_COLUMN_NUMBER),
           LISTAGG('$' || RAW_COLUMN_NUMBER::VARCHAR, ', ') WITHIN GROUP (ORDER BY RAW_COLUMN_NUMBER)
    INTO   :v_col_list, :v_pos_list
    FROM   BOOKS.CONFIG.CROSSWALK
    WHERE  RAW_TABLE_NAME = :RAW_TABLE_NAME;

    v_sql := 'TRUNCATE TABLE BOOKS.' || :RAW_TABLE_NAME;
    EXECUTE IMMEDIATE :v_sql;

    v_sql :=
'COPY INTO BOOKS.' || :RAW_TABLE_NAME || ' (
    ' || :v_col_list || ',
    DP_RUN_ID,
    DP_SOURCE_FILE_NAME,
    DP_SOURCE_FILE_ROW_NUMBER,
    DP_LOAD_TIMESTAMP
)
FROM (
    SELECT
        ' || :v_pos_list || ',
        ''' || :v_run_id || ''',
        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER,
        CURRENT_TIMESTAMP()
    FROM @BOOKS.' || :v_stage_name || '
)
FILE_FORMAT = (FORMAT_NAME = ''BOOKS.' || :v_file_format || ''')
PURGE = TRUE
';

    EXECUTE IMMEDIATE :v_sql;


    RETURN 'SUCCESS: COPY INTO BOOKS.' || :RAW_TABLE_NAME || ' complete.';
END;
$$;
