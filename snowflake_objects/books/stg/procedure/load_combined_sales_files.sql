CREATE PROCEDURE IF NOT EXISTS BOOKS.STG.LOAD_COMBINED_SALES_FILES()
RETURNS VARCHAR 
LANGUAGE JAVASCRIPT 
EXECUTE AS OWNER 
AS 
$$

    var prod_table_name = 'DATA.COMBINED_SALES'
    var stg_table_name = `STG.COMBINED_SALES`;
    var stg_stage_name = `STG.COMBINED_SALES_STAGE`;
    var file_format_name = `STG.CSV_SKIP_HEADER_QUOTES`;
    var audit_table_name = `DATA.AUDIT`;
    var sql_var;
    var sql_result_set;
    var audit_sid;

    // Add entry to audit table
    sql_var = `
        SELECT NVL(MAX(AUDIT_SID), 0) + 1 AS AUDIT_SID
        FROM ${audit_table_name}
    `;
    sql_result_set = snowflake.createStatement({sqlText: sql_var}).execute();
    sql_result_set.next();
    audit_sid = sql_result_set.getColumnValue('AUDIT_SID');


    sql_var = `
        INSERT INTO ${audit_table_name} 
        (AUDIT_SID, START_DT, SUCCESS_FLAG)
        VALUES 
        (${audit_sid}, CURRENT_TIMESTAMP(), FALSE)
    `;
    snowflake.createStatement({sqlText: sql_var}).execute();

    // Count rows in stage
    sql_var = `
        SELECT COUNT(*) AS ROWS_IN_SRC
        FROM @${stg_stage_name}
        (FILE_FORMAT => '${file_format_name}')
    `;
    sql_result_set = snowflake.createStatement({sqlText: sql_var}).execute();
    sql_result_set.next();
    var rows_in_src = sql_result_set.getColumnValue('ROWS_IN_SRC');

    // Truncate table
    sql_var = `
        TRUNCATE TABLE ${stg_table_name}
    `;
    snowflake.createStatement({sqlText: sql_var}).execute();

    // Copy into stage
    sql_var = `
        COPY INTO ${stg_table_name}
        FROM (
            SELECT 
                $1, $2, $3, $4, $5, 
                $6, $7, $8, $9, $10, 
                $11, $12, $13, $14, $15,
                METADATA$FILENAME,
                CURRENT_TIMESTAMP()
            FROM @${stg_stage_name}
            (FILE_FORMAT => '${file_format_name}')
        )
    `;
    snowflake.createStatement({sqlText: sql_var}).execute();

    // Count rows added in stage
    sql_var = `
        SELECT COUNT(*) AS ROWS_ADDED_TO_STG
        FROM ${stg_table_name}
    `;
    sql_result_set = snowflake.createStatement({sqlText: sql_var}).execute();
    sql_result_set.next();
    var rows_added_to_stg = sql_result_set.getColumnValue('ROWS_ADDED_TO_STG');

    // Delete from prod
    sql_var = `
        DELETE FROM ${prod_table_name} 
        WHERE ROYALTY_DATE IN (
            SELECT ROYALTY_DATE 
            FROM ${stg_table_name}
        )
    `;
    result_set = snowflake.createStatement({sqlText: sql_var}).execute();
    result_set.next();
    var rows_deleted_in_prod = result_set.getColumnValue(1);

    // Insert into prod
    sql_var = `
        INSERT INTO ${prod_table_name}
        SELECT * FROM ${stg_table_name}
    `;
    result_set = snowflake.createStatement({sqlText: sql_var}).execute();
    result_set.next();
    var rows_added_to_prod = result_set.getColumnValue(1);

    // Update audit table
    sql_var = `
        UPDATE ${audit_table_name}
        SET 
            SUCCESS_FLAG = TRUE,
            END_DT = CURRENT_TIMESTAMP(),
            ROWS_IN_SRC = ${rows_in_src},
            ROWS_ADDED_TO_STG = ${rows_added_to_stg},
            ROWS_DELETED_IN_PROD = ${rows_deleted_in_prod},
            ROWS_ADDED_TO_PROD = ${rows_added_to_prod}
        WHERE AUDIT_SID = ${audit_sid}
    `;
    snowflake.createStatement({sqlText: sql_var}).execute();


    return audit_sid;

$$
;




