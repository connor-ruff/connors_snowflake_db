CREATE OR REPLACE PROCEDURE STG.REFRESH_WEEKLY(WEEK_NUMBER_INPUT FLOAT)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
try {
   
    var WEEK_NUMBER = WEEK_NUMBER_INPUT;
    var sql_command;
    var stmt;
    
    sql_command = `
        BEGIN TRANSACTION;
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    stmt.execute();

    sql_command = `
        TRUNCATE TABLE STG.WEEKLY_STATS_TEAMS;
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    stmt.execute();

    sql_command = `
        COPY INTO STG.WEEKLY_STATS_TEAMS (
            WEEK_NUMBER,
            TEAM,
            TEAM_KEY,
            MANAGER_NICKNAME,
            MANAGER_EMAIL,
            ADDS,
            FULL_TEAM_JSON_OBJ,
            SOURCE_FILE,
            LOAD_DT
        )
        FROM (
            SELECT 
                ROUND(${WEEK_NUMBER}, 0) AS WEEK_NUMBER,
                $1:fantasy_content:team:name AS TEAM,
                $1:fantasy_content:team:team_key AS TEAM_KEY,
                $1:fantasy_content:team:managers:manager:nickname AS MANAGER_NICKNAME,
                $1:fantasy_content:team:managers:manager:email AS MANAGER_EMAIL,

                $1:fantasy_content:team:roster_adds:value AS ADDS,

                $1:fantasy_content:team AS FULL_TEAM_JSON_OBJ,

                METADATA$FILENAME AS SOURCE_FILE,
                CURRENT_TIMESTAMP() AS LOAD_DT

            FROM @STG.ZETA_BALL_STAGE/
            (FILE_FORMAT => 'STG.JSON_FF',
            PATTERN => '.*/team_stats/.*\.json$')
        );
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    stmt.execute();

    sql_command = `
        TRUNCATE TABLE STG.WEEKLY_STATS_PLAYERS;
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    stmt.execute();

    sql_command = `
        INSERT INTO STG.WEEKLY_STATS_PLAYERS (
            WEEK_NUMBER,
            TEAM,
            TEAM_KEY,
            PLAYER_KEY,
            PLAYER_NAME,
            PLAYER_TEAM,
            PLAYER_POSITION,
            PLAYER_STATS,
            LOAD_DT
        )
        SELECT
            WEEK_NUMBER,
            TEAM,
            TEAM_KEY,
            P.VALUE:player_key::VARCHAR AS PLAYER_KEY,
            P.VALUE:name:full AS PLAYER_NAME,
            P.VALUE:editorial_team_full_name::VARCHAR AS PLAYER_TEAM,
            P.VALUE:eligible_positions:position::ARRAY AS PLAYER_POSITION,
            P.VALUE:player_stats:stats AS PLAYER_STATS,
            CURRENT_TIMESTAMP() AS LOAD_DT
        FROM STG.WEEKLY_STATS_TEAMS S, 
        LATERAL FLATTEN (input => S.FULL_TEAM_JSON_OBJ:roster:players:player) AS P
        ;
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    stmt.execute();

    sql_command = `
        TRUNCATE TABLE STG.WEEKLY_STATS_PLAYER_STATS;
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    stmt.execute();

    sql_command = `
        INSERT INTO STG.WEEKLY_STATS_PLAYER_STATS (
            WEEK_NUMBER,
            TEAM,
            TEAM_KEY,
            PLAYER_KEY,
            PLAYER_NAME,
            PLAYER_TEAM,
            PLAYER_POSITION,
            STAT_ID,
            STAT_VALUE,
            LOAD_DT
        )
        SELECT 
            S.WEEK_NUMBER,
            TEAM, 
            TEAM_KEY,
            PLAYER_KEY,
            PLAYER_NAME,
            PLAYER_TEAM,
            PLAYER_POSITION,
            P.VALUE:stat_id::INT AS STAT_ID,
            REPLACE(P.VALUE:value::VARCHAR, '-', '0') AS STAT_VALUE,
            CURRENT_TIMESTAMP() AS LOAD_DT
        FROM STG.WEEKLY_STATS_PLAYERS S,
        LATERAL FLATTEN(INPUT => S.PLAYER_STATS:stat) P
        ;
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    stmt.execute();

    sql_command = `
        INSERT INTO DWH.WEEKLY_STATS (
            WEEK_NUMBER,
            TEAM_KEY,
            TEAM,
            PLAYER_KEY,
            PLAYER_NAME,
            PLAYER_TEAM,
            POSITIONS_STARTED,
            
            PTS,
            REB,
            AST,
            BLK,
            STL,
            THREE_PM,
            FGM,
            FGA,
            FTM,
            FTA,
            TOS,

            LOAD_DT
        )
        SELECT
            S.WEEK_NUMBER,
            S.TEAM_KEY,
            S.TEAM,
            S.PLAYER_KEY,
            S.PLAYER_NAME,
            S.PLAYER_TEAM,
            S.PLAYER_POSITION,
            MAX(CASE WHEN S.STAT_ID = 12 THEN S.STAT_VALUE ELSE NULL END)::INT AS PTS,
            MAX(CASE WHEN S.STAT_ID = 15 THEN S.STAT_VALUE ELSE NULL END)::INT AS REB,
            MAX(CASE WHEN S.STAT_ID = 16 THEN S.STAT_VALUE ELSE NULL END)::INT AS AST,
            MAX(CASE WHEN S.STAT_ID = 18 THEN S.STAT_VALUE ELSE NULL END)::INT AS BLK,
            MAX(CASE WHEN S.STAT_ID = 17 THEN S.STAT_VALUE ELSE NULL END)::INT AS STL,
            MAX(CASE WHEN S.STAT_ID = 10 THEN S.STAT_VALUE ELSE NULL END)::INT AS THREE_PM,

            SPLIT(
                MAX(CASE WHEN S.STAT_ID = 9004003 THEN S.STAT_VALUE ELSE NULL END),
                '/'
            )[0]::INT AS FGM,

            SPLIT(
                MAX(CASE WHEN S.STAT_ID = 9004003 THEN S.STAT_VALUE ELSE NULL END),
                '/'
            )[1]::INT AS FGA,


            SPLIT(
                MAX(CASE WHEN S.STAT_ID = 9007006 THEN S.STAT_VALUE ELSE NULL END),
                '/'
            )[0]::INT AS FTM,

                SPLIT(
                MAX(CASE WHEN S.STAT_ID = 9007006 THEN S.STAT_VALUE  ELSE NULL END),
                '/'
            )[1]::INT AS FTA,

            MAX(CASE WHEN S.STAT_ID = 19 THEN S.STAT_VALUE ELSE NULL END)::INT AS TOS,

            CURRENT_TIMESTAMP() AS LOAD_DT
        FROM STG.WEEKLY_STATS_PLAYER_STATS S
        GROUP BY 
            S.WEEK_NUMBER,
            S.TEAM_KEY,
            S.TEAM,
            S.PLAYER_KEY,
            S.PLAYER_NAME,
            S.PLAYER_TEAM,
            S.PLAYER_POSITION
        ;
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    stmt.execute();

    sql_command = `
        INSERT INTO DWH.WEEKLY_TEAM_TOTALS
        (
            WEEK_NUMBER,
            TEAM_KEY, 
            TEAM,
            TOTAL_PTS,
            TOTAL_REB,
            TOTAL_AST,
            TOTAL_BLK,
            TOTAL_STL,
            TOTAL_THREE_PM,
            TOTAL_FGM,
            TOTAL_FGA,
            FG_PCT,
            TOTAL_FTM,
            TOTAL_FTA,
            FT_PCT,
            TOTAL_TOS,
            LOAD_DT
        )
        SELECT 
            WEEK_NUMBER,
            TEAM_KEY, 
            TEAM,
            SUM(PTS) AS TOTAL_PTS,
            SUM(REB) AS TOTAL_REB,
            SUM(AST) AS TOTAL_AST,
            SUM(BLK) AS TOTAL_BLK,
            SUM(STL) AS TOTAL_STL,
            SUM(THREE_PM) AS TOTAL_THREE_PM,
            SUM(FGM) AS TOTAL_FGM,
            SUM(FGA) AS TOTAL_FGA,
            ROUND(TOTAL_FGM / NULLIF(TOTAL_FGA, 0), 6) AS FG_PCT,
            SUM(FTM) AS TOTAL_FTM,
            SUM(FTA) AS TOTAL_FTA,
            ROUND(TOTAL_FTM / NULLIF(TOTAL_FTA, 0), 6) AS FT_PCT,
            SUM(TOS) AS TOTAL_TOS,
            CURRENT_TIMESTAMP() AS LOAD_DT
        FROM DWH.WEEKLY_STATS
        GROUP BY 
            WEEK_NUMBER,
            TEAM_KEY, 
            TEAM
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    stmt.execute();

    sql_command = `
        COMMIT;
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    stmt.execute();

    
    // Return success message or result
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