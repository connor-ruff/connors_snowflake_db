-- Returns (does NOT run) a CREATE OR REPLACE TABLE statement for a RAW table, built from BOOKS.CONFIG.CROSSWALK.
--   * One VARCHAR column per crosswalk row for the raw table where RAW_COLUMN_NUMBER IS NOT NULL (in column-number order),
--     named exactly as RAW_COLUMN_NAME (which already carries its double quotes in the crosswalk)
--   * Plus the 4 standard DP_ columns that appear on every raw table
-- Returns NULL if the crosswalk has no source-file columns for the given raw table.
--
-- Usage:
--   SELECT BOOKS.CONFIG.CREATE_RAW_DDL_FROM_CROSSWALK('RAW.KDP_KENP_STAGE');
CREATE OR REPLACE FUNCTION BOOKS.CONFIG.CREATE_RAW_DDL_FROM_CROSSWALK(P_RAW_TABLE_NAME VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
    SELECT
        'CREATE OR REPLACE TABLE BOOKS.' || P_RAW_TABLE_NAME || ' (\n'
        || LISTAGG('    ' || RAW_COLUMN_NAME || ' VARCHAR', ',\n') WITHIN GROUP (ORDER BY RAW_COLUMN_NUMBER)
        || ',\n    DP_RUN_ID                 VARCHAR'
        || ',\n    DP_SOURCE_FILE_NAME       VARCHAR'
        || ',\n    DP_SOURCE_FILE_ROW_NUMBER INT'
        || ',\n    DP_LOAD_TIMESTAMP         TIMESTAMP_NTZ(9)'
        || '\n);'
    FROM BOOKS.CONFIG.CROSSWALK
    WHERE RAW_TABLE_NAME = P_RAW_TABLE_NAME
      AND RAW_COLUMN_NUMBER IS NOT NULL
$$;
