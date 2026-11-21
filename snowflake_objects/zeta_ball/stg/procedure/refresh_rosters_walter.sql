CREATE OR REPLACE PROCEDURE STG.REFRESH_ROSTERS_WALTER()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
try {

    sql_command = `
        INSERT INTO STG.AUDIT (RESULT_MSG)
        VALUES ('STARTING ROSTERS REFRESH PROCESS - WALTER API');
    `;
    var stmt = snowflake.createStatement({sqlText: sql_command});
    stmt.execute();

    sql_command = `
        SELECT AUDIT_SID FROM STG.AUDIT
        ORDER BY START_DT DESC 
        LIMIT 1;
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    var result_set = stmt.execute();
    result_set.next();
    var audit_sid = result_set.getColumnValue('AUDIT_SID');

    // Get current season
    sql_command = `
        SELECT YEAR(MAX(WEEK_1_START_DATE)) AS CURRENT_SEASON
        FROM DWH.LEAGUES;
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    result_set = stmt.execute();
    result_set.next();
    var current_season = result_set.getColumnValue('CURRENT_SEASON');

    // Call external function: REFRESH_TYPE_ID 3 = rosters (no inputs, always current season)
    var REFRESH_TYPE_ROSTERS = 3;
    sql_command = `
        SELECT STG.ZETA_BALL_WALTER_REFRESH_LAMBDA_TRIGGER(
            ${REFRESH_TYPE_ROSTERS}, NULL, NULL, NULL, NULL, NULL
        ) AS RESPONSE;
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    result_set = stmt.execute();
    result_set.next();
    var response = result_set.getColumnValue('RESPONSE');

    if (response !== 'SUCCESS') {
        throw new Error('Lambda function failed for rosters, season ' + current_season + ' with response: ' + response);
    }

    sql_command = `
        BEGIN TRANSACTION;
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    stmt.execute();

    sql_command = `
        TRUNCATE TABLE STG.ROSTERS_WALTER_RAW;
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    stmt.execute();

    sql_command = `
        COPY INTO STG.ROSTERS_WALTER_RAW (
            FULL_OBJECT,
            SOURCE_FILE_NAME,
            LOAD_DT
        )
        FROM (
            SELECT
                $1 AS FULL_OBJECT,

                METADATA$FILENAME AS SOURCE_FILE_NAME,
                CURRENT_TIMESTAMP() AS LOAD_DT

            FROM @STG.ZETA_BALL_WALTER_STAGE/
            (FILE_FORMAT => 'STG.JSON_FF',
            PATTERN => '.*rosters/current[.]json$')
        )
        FORCE = TRUE;
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    stmt.execute(); 

    sql_command = `
        TRUNCATE TABLE DWH.CURRENT_ROSTERS;
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    stmt.execute();

    sql_command = `
        INSERT INTO DWH.CURRENT_ROSTERS (
            YAHOO_TEAM_KEY,
            PLAYER_KEY,
            PLAYER_NAME,
            PLAYER_TEAM,
            PLAYER_POSITION,
            RECENT_STATS_OBJ
        )
        WITH MANAGER_OBJS AS (
        SELECT 
                SOURCE_FILE_NAME, F.VALUE AS MANAGER_OBJ, 
                F.VALUE:team_key AS YAHOO_TEAM_KEY
            FROM STG.ROSTERS_WALTER_RAW,
            LATERAL FLATTEN(input => FULL_OBJECT:data) AS F
        )
        SELECT 
            YAHOO_TEAM_KEY,
            F.VALUE:player_key::varchar AS PLAYER_KEY,
            F.VALUE:name::varchar AS PLAYER_NAME,
            F.VALUE:pro_team::varchar AS PLAYER_TEAM,
            F.VALUE:position::varchar AS PLAYER_POSITION,
            F.VALUE:recent::object AS RECENT_STATS_OBJ
        FROM MANAGER_OBJS, 
        LATERAL FLATTEN(input => MANAGER_OBJ:players) AS F
        ; 
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    stmt.execute();


    sql_command = `
        COMMIT;
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    stmt.execute();


    sql_command = `
        UPDATE STG.AUDIT
        SET 
            SUCCESS_FLG = TRUE,
            RESULT_MSG = ?,
            END_DT = CURRENT_TIMESTAMP()
        WHERE AUDIT_SID = ?;
    `;
    stmt = snowflake.createStatement({
        sqlText: sql_command,
        binds: ['ROSTERS REFRESH COMPLETED SUCCESSFULLY - WALTER API', audit_sid]
    });
    stmt.execute();

    var sql_command = `
         CALL SYSTEM$SEND_EMAIL(
                'BASIC_EMAIL_INTEGRATION',
                'connorruff99@gmail.com',
                'SUCCESS: Zeta Ball Rosters Refresh',
                'The rosters refresh process has completed successfully.'
        );
    `;
    var email_stmt = snowflake.createStatement({sqlText: sql_command});
    email_stmt.execute();


    // Return success message or result
    return "SUCCESS";

} catch (err) {
    var sql_error_command = `
        ROLLBACK;
    `;
    var error_stmt = snowflake.createStatement({sqlText: sql_error_command});
    error_stmt.execute();

    var log_error_command = `
        UPDATE STG.AUDIT
        SET 
            SUCCESS_FLG = FALSE,
            RESULT_MSG = ?,
            END_DT = CURRENT_TIMESTAMP()
        WHERE AUDIT_SID = ?;
    `;
    var log_error_stmt = snowflake.createStatement({
        sqlText: log_error_command,
        binds: ['FAILED: ' + err.message, audit_sid]
    });
    log_error_stmt.execute();

    var sql_command = `
         CALL SYSTEM$SEND_EMAIL(
                'BASIC_EMAIL_INTEGRATION',
                'connorruff99@gmail.com',
                'FAILURE: Zeta Ball Rosters Refresh',
                'The rosters refresh process has failed.'
        );
    `;
    var email_stmt = snowflake.createStatement({sqlText: sql_command});
    email_stmt.execute();

    // Catch and return error message
    return "Error: " + err.message;
}
$$;
