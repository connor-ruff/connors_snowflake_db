CREATE OR REPLACE PROCEDURE STG.REFRESH_TEAMS_WALTER()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
try {

    sql_command = `
        INSERT INTO STG.AUDIT (RESULT_MSG)
        VALUES ('STARTING TEAM NAMES REFRESH PROCESS - WALTER API');
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

    // Call external function: REFRESH_TYPE_ID 2 = teams (input 1 = season)
    var REFRESH_TYPE_TEAMS = 2;
    sql_command = `
        SELECT STG.ZETA_BALL_WALTER_REFRESH_LAMBDA_TRIGGER(
            ${REFRESH_TYPE_TEAMS}, '${current_season}', NULL, NULL, NULL, NULL
        ) AS RESPONSE;
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    result_set = stmt.execute();
    result_set.next();
    var response = result_set.getColumnValue('RESPONSE');

    if (response !== 'SUCCESS') {
        throw new Error('Lambda function failed for teams, season ' + current_season + ' with response: ' + response);
    }

    sql_command = `
        BEGIN TRANSACTION;
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    stmt.execute();

    sql_command = `
        TRUNCATE TABLE STG.TEAMS_WALTER_RAW;
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    stmt.execute();

    sql_command = `
        COPY INTO STG.TEAMS_WALTER_RAW (
            SEASON,
            FULL_OBJECT,
            SOURCE_FILE_NAME,
            LOAD_DT
        )
        FROM (
            SELECT
                ${current_season} AS SEASON,

                $1 AS FULL_OBJECT,

                METADATA$FILENAME AS SOURCE_FILE_NAME,
                CURRENT_TIMESTAMP() AS LOAD_DT

            FROM @STG.ZETA_BALL_WALTER_STAGE/
            (FILE_FORMAT => 'STG.JSON_FF',
            PATTERN => '.*teams/season_${current_season}[.]json$')
        )
        FORCE = TRUE;
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    stmt.execute();

    // Walter's TEAM_ID is the Yahoo team id, so it joins to DWH.TEAMS on season + YAHOO_TEAM_ID.
    // Every team for the season should match; otherwise an id failed to map.
    sql_command = `
        SELECT
            (SELECT COUNT(*) FROM DWH.TEAMS WHERE LEAGUE_START_YEAR = ${current_season}) AS EXPECTED_TEAMS,
            (SELECT COUNT(*)
                FROM STG.TEAMS_WALTER_RAW R,
                LATERAL FLATTEN(INPUT => R.FULL_OBJECT:data) F,
                DWH.TEAMS T
                WHERE T.LEAGUE_START_YEAR = R.SEASON
                    AND T.YAHOO_TEAM_ID = F.VALUE:team_id::INT
            ) AS MATCHED_TEAMS
        ;
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    result_set = stmt.execute();
    result_set.next();
    var expected_teams = result_set.getColumnValue('EXPECTED_TEAMS');
    var matched_teams = result_set.getColumnValue('MATCHED_TEAMS');

    if (matched_teams !== expected_teams) {
        throw new Error('Expected ' + expected_teams + ' teams in DWH.TEAMS but matched ' + matched_teams + ' from the API');
    }

    sql_command = `
        MERGE INTO DWH.TEAMS T
        USING (
            SELECT
                R.SEASON,
                F.VALUE:team_id::INT AS TEAM_ID,
                F.VALUE:team::VARCHAR AS TEAM_NAME
            FROM STG.TEAMS_WALTER_RAW R,
            LATERAL FLATTEN(INPUT => R.FULL_OBJECT:data) F
        ) S
            ON T.LEAGUE_START_YEAR = S.SEASON
            AND T.YAHOO_TEAM_ID = S.TEAM_ID
        WHEN MATCHED AND T.YAHOO_TEAM_NAME IS DISTINCT FROM S.TEAM_NAME THEN UPDATE SET
            T.YAHOO_TEAM_NAME = S.TEAM_NAME,
            T.LAST_UPDATE_DT = CURRENT_TIMESTAMP()
        ;
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    result_set = stmt.execute();
    result_set.next();
    var names_updated = result_set.getColumnValue(1);

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
        binds: ['TEAM NAMES REFRESH COMPLETED SUCCESSFULLY - WALTER API (' + names_updated + ' names updated)', audit_sid]
    });
    stmt.execute();

    var sql_command = `
         CALL SYSTEM$SEND_EMAIL(
                'BASIC_EMAIL_INTEGRATION',
                'connorruff99@gmail.com',
                'SUCCESS: Zeta Ball Team Names Refresh',
                'The team names refresh process has completed successfully.'
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
                'FAILURE: Zeta Ball Team Names Refresh',
                'The team names refresh process has failed.'
        );
    `;
    var email_stmt = snowflake.createStatement({sqlText: sql_command});
    email_stmt.execute();

    // Catch and return error message
    return "Error: " + err.message;
}
$$;
