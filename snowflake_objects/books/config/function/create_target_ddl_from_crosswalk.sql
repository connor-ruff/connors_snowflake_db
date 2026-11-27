-- Returns (does NOT run) a CREATE TABLE IF NOT EXISTS statement for a final/target table, built from BOOKS.CONFIG.CROSSWALK.
--   * Source-file columns (IS_DP_COLUMN = FALSE) first, in RAW_COLUMN_NUMBER order, typed with TARGET_DATA_TYPE
--   * Then the DP_ columns defined in the crosswalk for that table: any non-standard ones first
--     (e.g. DP_SOURCE_FILE_MONTH, DP_SOURCE_FILE_BEGIN_DATE), then the 4 standard ones in the usual order
--     (DP_RUN_ID, DP_SOURCE_FILE_NAME, DP_SOURCE_FILE_ROW_NUMBER, DP_LOAD_TIMESTAMP)
-- Uses IF NOT EXISTS (like the existing target table DDLs) so that running it can never wipe loaded data.
-- Returns NULL if the crosswalk has no rows for the given target table.
--
-- Usage:
--   SELECT BOOKS.CONFIG.CREATE_TARGET_DDL_FROM_CROSSWALK('IG.GLOBAL_CONNECT');
CREATE OR REPLACE FUNCTION BOOKS.CONFIG.CREATE_TARGET_DDL_FROM_CROSSWALK(P_TARGET_TABLE_NAME VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
    SELECT
        'CREATE TABLE IF NOT EXISTS BOOKS.' || P_TARGET_TABLE_NAME || ' (\n'
        || LISTAGG(
               IFF(NOT COALESCE(IS_DP_COLUMN, FALSE), '    ' || TARGET_COLUMN_NAME || ' ' || TARGET_DATA_TYPE, NULL),
               ',\n'
           ) WITHIN GROUP (ORDER BY COALESCE(RAW_COLUMN_NUMBER, 0), TARGET_COLUMN_NAME)
        || ',\n\n'
        || LISTAGG(
               IFF(COALESCE(IS_DP_COLUMN, FALSE), '    ' || TARGET_COLUMN_NAME || ' ' || TARGET_DATA_TYPE, NULL),
               ',\n'
           ) WITHIN GROUP (ORDER BY
               CASE TARGET_COLUMN_NAME
                   WHEN 'DP_RUN_ID'                 THEN 1
                   WHEN 'DP_SOURCE_FILE_NAME'       THEN 2
                   WHEN 'DP_SOURCE_FILE_ROW_NUMBER' THEN 3
                   WHEN 'DP_LOAD_TIMESTAMP'         THEN 4
                   ELSE 0
               END,
               TARGET_COLUMN_NAME)
        || '\n);'
    FROM BOOKS.CONFIG.CROSSWALK
    WHERE TARGET_TABLE_NAME = P_TARGET_TABLE_NAME
$$;
