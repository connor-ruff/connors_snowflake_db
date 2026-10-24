CREATE OR REPLACE PROCEDURE STG.REFRESH_WEEKLY_WALTER()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
try {

    sql_command = `
        INSERT INTO STG.AUDIT (RESULT_MSG)
        VALUES ('STARTING WEEKLY REFRESH PROCESS - WALTER API');
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

    // Get current season and week number
    var current_season;
    var current_week_number;
    sql_command = `
        SELECT 
            YEAR(MAX(WEEK_1_START_DATE)) AS CURRENT_SEASON, 
            DATEDIFF('WEEK', MAX(WEEK_1_START_DATE), CURRENT_DATE()) AS WEEK_DIFF
        FROM DWH.LEAGUES;
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    result_set = stmt.execute();
    result_set.next();
    current_season = result_set.getColumnValue('CURRENT_SEASON');
    current_week_number = result_set.getColumnValue('WEEK_DIFF');

    if (current_week_number < 1) {
        throw new Error('No completed week to pull yet (week ' + current_week_number + ')');
    }

    // Call external function: REFRESH_TYPE_ID 1 = matchups (input 1 = season, input 2 = week)
    var REFRESH_TYPE_MATCHUPS = 1;
    sql_command = `
        SELECT STG.ZETA_BALL_WALTER_REFRESH_LAMBDA_TRIGGER(
            ${REFRESH_TYPE_MATCHUPS}, '${current_season}', '${current_week_number}', NULL, NULL, NULL
        ) AS RESPONSE;
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    result_set = stmt.execute();
    result_set.next();
    var response = result_set.getColumnValue('RESPONSE');

    if (response !== 'SUCCESS') {
        throw new Error('Lambda function failed for matchups, season ' + current_season + ' week ' + current_week_number + ' with response: ' + response);
    }

    sql_command = `
        BEGIN TRANSACTION;
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    stmt.execute();

    sql_command = `
        TRUNCATE TABLE STG.WEEKLY_MATCHUPS_WALTER_RAW;
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    stmt.execute();

    sql_command = `
        COPY INTO STG.WEEKLY_MATCHUPS_WALTER_RAW (
            SEASON,
            WEEK_NUMBER,
            FULL_OBJECT,
            SOURCE_FILE_NAME,
            LOAD_DT
        )
        FROM (
            SELECT
                ${current_season} AS SEASON,
                ${current_week_number} AS WEEK_NUMBER,

                $1 AS FULL_OBJECT,

                METADATA$FILENAME AS SOURCE_FILE_NAME,
                CURRENT_TIMESTAMP() AS LOAD_DT

            FROM @STG.ZETA_BALL_WALTER_STAGE/
            (FILE_FORMAT => 'STG.JSON_FF',
            PATTERN => '.*matchups/season_${current_season}/week_${current_week_number}[.]json$')
        )
        FORCE = TRUE;
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    stmt.execute();

    sql_command = `
        TRUNCATE TABLE STG.WEEKLY_MATCHUP_CATEGORY_STATS_WALTER;
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    stmt.execute();

    // One row per team per category: matchup -> its 2 teams -> its 9 categories (values[i] belongs to teams[i])
    sql_command = `
        INSERT INTO STG.WEEKLY_MATCHUP_CATEGORY_STATS_WALTER (
            SEASON,
            WEEK_NUMBER,
            MANAGER,
            TEAM_NAME,
            TEAM_ID,
            CATEGORY,
            CATEGORY_VALUE,
            SOURCE_FILE_NAME,
            LOAD_DT
        )
        SELECT
            R.SEASON,
            R.WEEK_NUMBER,

            T.VALUE:manager::VARCHAR AS MANAGER,
            T.VALUE:team::VARCHAR AS TEAM_NAME,
            T.VALUE:team_id::INT AS TEAM_ID,

            C.VALUE:category::VARCHAR AS CATEGORY,
            GET(C.VALUE:values, T.INDEX)::FLOAT AS CATEGORY_VALUE,

            R.SOURCE_FILE_NAME,
            R.LOAD_DT
        FROM STG.WEEKLY_MATCHUPS_WALTER_RAW R,
        LATERAL FLATTEN(INPUT => R.FULL_OBJECT:data) M,
        LATERAL FLATTEN(INPUT => M.VALUE:teams) T,
        LATERAL FLATTEN(INPUT => M.VALUE:categories) C
        ;
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    stmt.execute();

    sql_command = `
        DELETE FROM DWH.WEEKLY_TEAM_TOTALS
        WHERE LEAGUE_START_YEAR = ${current_season}
        AND WEEK_NUMBER = ${current_week_number}
        ;
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    stmt.execute();

    // Walter's TEAM_ID is the Yahoo team id, so it joins to DWH.TEAMS on season + YAHOO_TEAM_ID
    sql_command = `
        INSERT INTO DWH.WEEKLY_TEAM_TOTALS
        (
            LEAGUE_START_YEAR,
            WEEK_NUMBER,
            TEAM_SID,
            TOTAL_PTS,
            TOTAL_REB,
            TOTAL_AST,
            TOTAL_BLK,
            TOTAL_STL,
            TOTAL_THREE_PM,
            FG_PCT,
            FT_PCT,
            TOTAL_TOS,
            LOAD_DT
        )
        SELECT
            T.LEAGUE_START_YEAR,
            S.WEEK_NUMBER,
            T.TEAM_SID,

            MAX(CASE WHEN S.CATEGORY = 'pts' THEN S.CATEGORY_VALUE END)::NUMBER(38,0) AS TOTAL_PTS,
            MAX(CASE WHEN S.CATEGORY = 'reb' THEN S.CATEGORY_VALUE END)::NUMBER(38,0) AS TOTAL_REB,
            MAX(CASE WHEN S.CATEGORY = 'ast' THEN S.CATEGORY_VALUE END)::NUMBER(38,0) AS TOTAL_AST,
            MAX(CASE WHEN S.CATEGORY = 'blk' THEN S.CATEGORY_VALUE END)::NUMBER(38,0) AS TOTAL_BLK,
            MAX(CASE WHEN S.CATEGORY = 'stl' THEN S.CATEGORY_VALUE END)::NUMBER(38,0) AS TOTAL_STL,
            MAX(CASE WHEN S.CATEGORY = 'tpm' THEN S.CATEGORY_VALUE END)::NUMBER(38,0) AS TOTAL_THREE_PM,
            ROUND(MAX(CASE WHEN S.CATEGORY = 'fg_pct' THEN S.CATEGORY_VALUE END), 6) AS FG_PCT,
            ROUND(MAX(CASE WHEN S.CATEGORY = 'ft_pct' THEN S.CATEGORY_VALUE END), 6) AS FT_PCT,
            MAX(CASE WHEN S.CATEGORY = 'tov' THEN S.CATEGORY_VALUE END)::NUMBER(38,0) AS TOTAL_TOS,

            CURRENT_TIMESTAMP() AS LOAD_DT
        FROM STG.WEEKLY_MATCHUP_CATEGORY_STATS_WALTER S
        INNER JOIN DWH.TEAMS T
            ON T.LEAGUE_START_YEAR = S.SEASON
            AND T.YAHOO_TEAM_ID = S.TEAM_ID
        GROUP BY T.LEAGUE_START_YEAR, S.WEEK_NUMBER, T.TEAM_SID
        ;
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    stmt.execute();

    // Every team in DWH.TEAMS for the season should have a row; otherwise an id failed to map
    sql_command = `
        SELECT
            (SELECT COUNT(*) FROM DWH.TEAMS WHERE LEAGUE_START_YEAR = ${current_season}) AS EXPECTED_TEAMS,
            (SELECT COUNT(*) FROM DWH.WEEKLY_TEAM_TOTALS
                WHERE LEAGUE_START_YEAR = ${current_season} AND WEEK_NUMBER = ${current_week_number}) AS LOADED_TEAMS
        ;
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    result_set = stmt.execute();
    result_set.next();
    var expected_teams = result_set.getColumnValue('EXPECTED_TEAMS');
    var loaded_teams = result_set.getColumnValue('LOADED_TEAMS');

    if (loaded_teams !== expected_teams) {
        throw new Error('Expected ' + expected_teams + ' teams in WEEKLY_TEAM_TOTALS but loaded ' + loaded_teams);
    }

    sql_command = `
        DELETE FROM DWH.MATCHUPS_CROSSJOIN
        WHERE LEAGUE_START_YEAR = ${current_season}
        AND WEEK_NUMBER = ${current_week_number}
        ;
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    stmt.execute();

    sql_command = `
        INSERT INTO DWH.MATCHUPS_CROSSJOIN
        (
            LEAGUE_START_YEAR,
            WEEK_NUMBER,
            MATCHUP_ID,
            TEAM_1_SID,
            TEAM_2_SID,
            CATEGORIES_WON_BY_TEAM_1,
            CATEGORIES_WON_BY_TEAM_2,
            TEAM_1_RESULT,
            LOAD_DT
        )
        SELECT
            P1.LEAGUE_START_YEAR,
            P1.WEEK_NUMBER,
            CONCAT(P1.LEAGUE_START_YEAR::VARCHAR, '^', P1.WEEK_NUMBER::VARCHAR, '^', LEAST(P1.TEAM_SID, P2.TEAM_SID), '--', GREATEST(P1.TEAM_SID, P2.TEAM_SID)) AS MATCHUP_ID,
            P1.TEAM_SID AS TEAM_1_SID,
            P2.TEAM_SID AS TEAM_2_SID,

            CASE WHEN P1.TOTAL_PTS > P2.TOTAL_PTS THEN 1 ELSE 0 END
            +
            CASE WHEN P1.TOTAL_REB > P2.TOTAL_REB THEN 1 ELSE 0 END
            +
            CASE WHEN P1.TOTAL_AST > P2.TOTAL_AST THEN 1 ELSE 0 END
            +
            CASE WHEN P1.TOTAL_STL > P2.TOTAL_STL THEN 1 ELSE 0 END
            +
            CASE WHEN P1.TOTAL_BLK > P2.TOTAL_BLK THEN 1 ELSE 0 END
            +
            CASE WHEN P1.TOTAL_THREE_PM > P2.TOTAL_THREE_PM THEN 1 ELSE 0 END
            +
            CASE WHEN P1.FG_PCT > P2.FG_PCT THEN 1 ELSE 0 END
            +
            CASE WHEN P1.FT_PCT > P2.FT_PCT THEN 1 ELSE 0 END
            +
            CASE WHEN P1.TOTAL_TOS < P2.TOTAL_TOS THEN 1 ELSE 0 END
            AS CATEGORIES_WON_BY_TEAM_1,

            CASE WHEN P2.TOTAL_PTS > P1.TOTAL_PTS THEN 1 ELSE 0 END
            +
            CASE WHEN P2.TOTAL_REB > P1.TOTAL_REB THEN 1 ELSE 0 END
            +
            CASE WHEN P2.TOTAL_AST > P1.TOTAL_AST THEN 1 ELSE 0 END
            +
            CASE WHEN P2.TOTAL_STL > P1.TOTAL_STL THEN 1 ELSE 0 END
            +
            CASE WHEN P2.TOTAL_BLK > P1.TOTAL_BLK THEN 1 ELSE 0 END
            +
            CASE WHEN P2.TOTAL_THREE_PM > P1.TOTAL_THREE_PM THEN 1 ELSE 0 END
            +
            CASE WHEN P2.FG_PCT > P1.FG_PCT THEN 1 ELSE 0 END
            +
            CASE WHEN P2.FT_PCT > P1.FT_PCT THEN 1 ELSE 0 END
            +
            CASE WHEN P2.TOTAL_TOS < P1.TOTAL_TOS THEN 1 ELSE 0 END
            AS CATEGORIES_WON_BY_TEAM_2,

            CASE WHEN CATEGORIES_WON_BY_TEAM_1 > CATEGORIES_WON_BY_TEAM_2 THEN 'W'
                    WHEN CATEGORIES_WON_BY_TEAM_1 < CATEGORIES_WON_BY_TEAM_2 THEN 'L'
                    ELSE 'T' END AS TEAM_1_RESULT,

            CURRENT_TIMESTAMP() AS LOAD_DT
        FROM DWH.WEEKLY_TEAM_TOTALS P1
        INNER JOIN DWH.WEEKLY_TEAM_TOTALS P2
            ON P1.LEAGUE_START_YEAR = P2.LEAGUE_START_YEAR
            AND P1.WEEK_NUMBER = P2.WEEK_NUMBER
        WHERE
            P1.TEAM_SID <> P2.TEAM_SID
            AND P1.LEAGUE_START_YEAR = ${current_season}
            AND P1.WEEK_NUMBER = ${current_week_number}
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
            RESULT_MSG = 'WEEKLY REFRESH PROCESS COMPLETED SUCCESSFULLY',
            END_DT = CURRENT_TIMESTAMP()
        WHERE AUDIT_SID = ${audit_sid};
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    stmt.execute();

    var sql_command = `
         CALL SYSTEM$SEND_EMAIL(
                'BASIC_EMAIL_INTEGRATION',
                'connorruff99@gmail.com',
                'SUCCESS: Zeta Ball Automation',
                'The weekly refresh process has completed successfully.'
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
                'FAILURE: Zeta Ball Automation',
                'The weekly refresh process has failed.'
        );
    `;
    var email_stmt = snowflake.createStatement({sqlText: sql_command});
    email_stmt.execute();

    // Catch and return error message
    return "Error: " + err.message;
}
$$;