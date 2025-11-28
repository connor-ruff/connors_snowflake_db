CREATE OR REPLACE PROCEDURE STG.BIRD_GEO_REGION_REFRESH()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
try {

    var STATES_OF_INTEREST = ['NJ', 'NY', 'NC', 'SC'];

    var sql_command = `
        BEGIN TRANSACTION;
    `;
    var stmt = snowflake.createStatement({sqlText: sql_command});
    var result = stmt.execute();

    /////////////////////////////////////////////////
    // REFRESH COUNTRY - WORLD GEO REGIONS
    /////////////////////////////////////////////////

    sql_command = `
        SELECT STG.BIRD_GEO_REGION_REFRESH('country', 'world');
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    result = stmt.execute();

    sql_command = `
        TRUNCATE TABLE STG.BIRD_GEO_REGIONS;
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    result = stmt.execute();

    sql_command = `
        COPY INTO STG.BIRD_GEO_REGIONS (MAIN_OBJ)
        FROM 
        (
            SELECT $1::VARIANT 
            FROM @STG.BIRD_STAGE/geo_regions/country/world.json
            (FILE_FORMAT => 'STG.JSON_FF')
        )
        PURGE = TRUE
        ;
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    result = stmt.execute();

    sql_command = `
        DELETE FROM BIRD.GEO_REGIONS WHERE GEO_TYPE = 'COUNTRY';
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    result = stmt.execute();

    sql_command = `
        INSERT INTO BIRD.GEO_REGIONS (GEO_TYPE, GEO_CODE, GEO_NAME)
        SELECT 
            'COUNTRY' AS GEO_TYPE,
            F.VALUE:code::STRING AS REGION_CODE,
            F.VALUE:name::STRING AS REGION_NAME
        FROM STG.BIRD_GEO_REGIONS M, 
        LATERAL FLATTEN(INPUT => MAIN_OBJ) AS F
        ;
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    result = stmt.execute();


    /////////////////////////////////////////////////
    // REFRESH US STATES
    /////////////////////////////////////////////////
    sql_command = `
        SELECT STG.BIRD_GEO_REGION_REFRESH('subnational1', 'US');
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    result = stmt.execute();

    sql_command = `
        TRUNCATE TABLE STG.BIRD_GEO_REGIONS;
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    result = stmt.execute();

    sql_command = `
        COPY INTO STG.BIRD_GEO_REGIONS (MAIN_OBJ)
        FROM 
        (
            SELECT $1::VARIANT 
            FROM @STG.BIRD_STAGE/geo_regions/subnational1/US.json
            (FILE_FORMAT => 'STG.JSON_FF')
        )
        PURGE = TRUE
        ;
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    result = stmt.execute();

    sql_command = `
        DELETE FROM BIRD.GEO_REGIONS WHERE GEO_TYPE = 'SUBNATIONAL1';
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    result = stmt.execute();

    sql_command = `
        INSERT INTO BIRD.GEO_REGIONS (GEO_TYPE, GEO_CODE, GEO_NAME)
        SELECT 
            'SUBNATIONAL1' AS GEO_TYPE,
            F.VALUE:code::STRING AS REGION_CODE,
            F.VALUE:name::STRING AS REGION_NAME
        FROM STG.BIRD_GEO_REGIONS M, 
        LATERAL FLATTEN(INPUT => MAIN_OBJ) AS F
        ;
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    result = stmt.execute();

    /////////////////////////////////////////////////
    // REFRESH COUNTIES IN STATES OF INTEREST
    /////////////////////////////////////////////////

    var state;
    for (var i = 0; i < STATES_OF_INTEREST.length ; i++) {
        state = STATES_OF_INTEREST[i];
        
        sql_command = `
            SELECT STG.BIRD_GEO_REGION_REFRESH('subnational2', 'US-${state}');
        `;
        stmt = snowflake.createStatement({sqlText: sql_command});
        result = stmt.execute();
    }

    sql_command = `
        TRUNCATE TABLE STG.BIRD_GEO_REGIONS;
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    result = stmt.execute();

    sql_command = `
        COPY INTO STG.BIRD_GEO_REGIONS (MAIN_OBJ)
        FROM 
        (
            SELECT $1::VARIANT 
            FROM @STG.BIRD_STAGE/geo_regions/subnational2/
            (FILE_FORMAT => 'STG.JSON_FF')
        )
        PURGE = TRUE
        ;
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    result = stmt.execute();

    sql_command = `
        DELETE FROM BIRD.GEO_REGIONS WHERE GEO_TYPE = 'SUBNATIONAL2'
        ;
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    result = stmt.execute();

    sql_command = `
        INSERT INTO BIRD.GEO_REGIONS (GEO_TYPE, GEO_CODE, GEO_NAME)
        SELECT 
            'SUBNATIONAL2' AS GEO_TYPE,
            F.VALUE:code::STRING AS REGION_CODE,
            F.VALUE:name::STRING AS REGION_NAME
        FROM STG.BIRD_GEO_REGIONS M, 
        LATERAL FLATTEN(INPUT => MAIN_OBJ) AS F
        ;
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