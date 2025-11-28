CREATE OR REPLACE PROCEDURE STG.BIRD_TAXONOMY_REFRESH()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
try {


    var sql_command = `
        BEGIN TRANSACTION;
    `;
    var stmt = snowflake.createStatement({sqlText: sql_command});
    var result = stmt.execute();

    sql_command = `
        SELECT STG.BIRD_GENERAL_REFRESHER('REFRESH_TAXONOMY_REF', NULL, NULL, NULL);
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    result = stmt.execute();

    sql_command = `
        TRUNCATE TABLE STG.BIRD_SIMPLE_VARIANT_STAGE;
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    result = stmt.execute();

    sql_command = `
        COPY INTO STG.BIRD_SIMPLE_VARIANT_STAGE (MAIN_OBJ)
        FROM 
        (
            SELECT $1::VARIANT 
            FROM @STG.BIRD_STAGE/taxonomy_ref/taxonomy_ref.json
            (FILE_FORMAT => 'STG.JSON_STRIP_OUTER_FF')
        )
        PURGE = TRUE
        ;
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    result = stmt.execute();
 
    sql_command = `
        MERGE INTO BIRD.TAXONOMY_REFERENCE AS TARGET
        USING
        (
            SELECT 
                $1:speciesCode::VARCHAR AS SPECIES_CODE,
                $1:comName::VARCHAR AS SPECIES,
                $1:familyCode::VARCHAR AS FAMILY_CODE,
                $1:familyComName::VARCHAR AS FAMILY_COMMON_NAME,
                $1:familySciName::VARCHAR AS FAMILY_SCI_NAME,
                $1:order::VARCHAR AS ORDER_NAME,
                $1:sciName::VARCHAR AS SCIENTIFIC_NAME,
                $1:sciNameCodes::ARRAY AS SCI_NAME_CODES,
            FROM STG.BIRD_SIMPLE_VARIANT_STAGE
        ) AS SOURCE
        ON TARGET.SPECIES_CODE = SOURCE.SPECIES_CODE
        WHEN MATCHED AND 
            (
                TARGET.SPECIES <> SOURCE.SPECIES OR
                TARGET.SCIENTIFIC_NAME <> SOURCE.SCIENTIFIC_NAME OR
                TARGET.SCI_NAME_CODES <> SOURCE.SCI_NAME_CODES OR
                TARGET.ORDER_NAME <> SOURCE.ORDER_NAME OR
                TARGET.FAMILY_COMMON_NAME <> SOURCE.FAMILY_COMMON_NAME OR
                TARGET.FAMILY_SCI_NAME <> SOURCE.FAMILY_SCI_NAME OR
                TARGET.FAMILY_CODE <> SOURCE.FAMILY_CODE
            )
        THEN
            UPDATE SET
                SPECIES = SOURCE.SPECIES,
                SCIENTIFIC_NAME = SOURCE.SCIENTIFIC_NAME,
                SCI_NAME_CODES = SOURCE.SCI_NAME_CODES,
                ORDER_NAME = SOURCE.ORDER_NAME,
                FAMILY_COMMON_NAME = SOURCE.FAMILY_COMMON_NAME,
                FAMILY_SCI_NAME = SOURCE.FAMILY_SCI_NAME,
                FAMILY_CODE = SOURCE.FAMILY_CODE,
                UPDATE_DT = CURRENT_TIMESTAMP(9)
        WHEN NOT MATCHED
        THEN
            INSERT (
                SPECIES_CODE,
                SPECIES,
                SCIENTIFIC_NAME,
                SCI_NAME_CODES,
                ORDER_NAME,
                FAMILY_COMMON_NAME,
                FAMILY_SCI_NAME,
                FAMILY_CODE
            )
            VALUES (
                SOURCE.SPECIES_CODE,
                SOURCE.SPECIES,
                SOURCE.SCIENTIFIC_NAME,
                SOURCE.SCI_NAME_CODES,
                SOURCE.ORDER_NAME,
                SOURCE.FAMILY_COMMON_NAME,
                SOURCE.FAMILY_SCI_NAME,
                SOURCE.FAMILY_CODE
            )
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    result = stmt.execute();
    
    sql_command =`
        COMMIT;
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    result = stmt.execute();


    return "SUCCESS";


} catch (err) {
    var sql_error_command = `
        ROLLBACK;
    `;
    var error_stmt = snowflake.createStatement({sqlText: sql_error_command});
    error_stmt.execute();

    // Catch and return error message
    return "Error: " + err.message;
}
$$;