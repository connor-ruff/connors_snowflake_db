CREATE OR REPLACE PROCEDURE STG.REFRESH_WEEKLY()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
try {

    // Get current week number
    sql_command = `
        SELECT 
        DATEDIFF('WEEK', TO_DATE('2025-10-20'), CURRENT_DATE()) AS WEEK_DIFF
    ;`;
    var stmt = snowflake.createStatement({sqlText: sql_command});
    var result_set = stmt.execute();
    result_set.next();
    var WEEK_NUMBER = result_set.getColumnValue('WEEK_DIFF');

    // Call external function
    var team_key_list = [
        "466.l.6036.t.8",
        "466.l.6036.t.7",
        "466.l.6036.t.14",
        "466.l.6036.t.2",
        "466.l.6036.t.5",
        "466.l.6036.t.6",
        "466.l.6036.t.11",
        "466.l.6036.t.13",
        "466.l.6036.t.12",
        "466.l.6036.t.1",
        "466.l.6036.t.10",
        "466.l.6036.t.4",
        "466.l.6036.t.15",
        "466.l.6036.t.9",
        "466.l.6036.t.18",
        "466.l.6036.t.16",
        "466.l.6036.t.3",
        "466.l.6036.t.17",
    ];

    for (var i = 0; i < team_key_list.length; i++) {
        var team_key = team_key_list[i];

        var lambda_sql = `
            SELECT STG.ZETA_BALL_REFRESH_LAMBDA_TRIGGER('${team_key}', ${WEEK_NUMBER})
            AS RESPONSE;
        `;
        var lambda_stmt = snowflake.createStatement({sqlText: lambda_sql});
        var lambda_result_set = lambda_stmt.execute();
        // Optionally process the result if needed
        lambda_result_set.next();
        var response = lambda_result_set.getColumnValue('RESPONSE');

        if (response !== 'SUCCESS') {
            throw new Error('Lambda function failed for team_key: ' + team_key + ' with response: ' + response);
        }
    }

    // round to int
    var sql_command;
    var stmt;
    
    sql_command = `
        BEGIN TRANSACTION;
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    stmt.execute();

    sql_command = `
        TRUNCATE TABLE STG.WEEKLY_TEAM_MATCHUPS_LVL1;
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    stmt.execute();

    sql_command = `
        COPY INTO STG.WEEKLY_TEAM_MATCHUPS_LVL1 (
            WEEK_NUMBER,
            TEAM_KEY, 
            TEAM_NAME,
            FULL_OBJECT,
            MATCHUPS_ARRAY,
            SOURCE_FILE_NAME,
            LOAD_DT
        )
        FROM (
            SELECT 
                ROUND(${WEEK_NUMBER}, 0) AS WEEK_NUMBER,
                
                $1:fantasy_content:team:team_key AS TEAM_KEY,
                $1:fantasy_content:team:name AS TEAM_NAME,

                $1 AS FULL_OBJECT,
                $1:fantasy_content:team:matchups:matchup AS MATCHUPS_ARRAY,

                METADATA$FILENAME AS SOURCE_FILE_NAME,
                CURRENT_TIMESTAMP() AS LOAD_DT

            FROM @STG.ZETA_BALL_STAGE/
            (FILE_FORMAT => 'STG.JSON_FF',
            PATTERN => '.*week_${WEEK_NUMBER}/team_matchups/.*\.json$')
        );
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    stmt.execute();



    sql_command = `
        TRUNCATE TABLE STG.WEEKLY_TEAM_MATCHUPS_LVL2;
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    stmt.execute();

    sql_command = `
        INSERT INTO STG.WEEKLY_TEAM_MATCHUPS_LVL2 (
            WEEK_NUMBER,
            TEAM_KEY,
            TEAM_NAME,
            FULL_MATCHUP_OBJECT,
            WEEK_START_DATE,
            WEEK_END_DATE,
            SOURCE_FILE_NAME,
            LOAD_DT
        )
        SELECT
            S.WEEK_NUMBER,
            S.TEAM_KEY,
            S.TEAM_NAME,

            P.VALUE AS FULL_MATCHUP_OBJECT,
            P.VALUE:week_start::DATE AS WEEK_START_DATE,
            P.VALUE:week_end::DATE AS WEEK_END_DATE,

            S.SOURCE_FILE_NAME,
            S.LOAD_DT AS LOAD_DT
        FROM STG.WEEKLY_TEAM_MATCHUPS_LVL1 S, 
        LATERAL FLATTEN (input => S.MATCHUPS_ARRAY) AS P
        WHERE S.WEEK_NUMBER = P.VALUE:week::INT
        ;
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    stmt.execute();

    sql_command = `
        TRUNCATE TABLE STG.WEEKLY_TEAM_MATCHUPS_LVL3;
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    stmt.execute();

    sql_command = `
        INSERT INTO STG.WEEKLY_TEAM_MATCHUPS_LVL3 (
            WEEK_NUMBER,
            TEAM_KEY,
            TEAM_NAME,
            WEEK_START_DATE,
            WEEK_END_DATE,
            MATCHUP_OBJECT,
            SOURCE_FILE_NAME,
            LOAD_DT
        )
        SELECT 
            S.WEEK_NUMBER,
            S.TEAM_KEY,
            S.TEAM_NAME,
            S.WEEK_START_DATE,
            S.WEEK_END_DATE,

            P.VALUE AS MATCHUP_OBJECT,

            S.SOURCE_FILE_NAME,
            S.LOAD_DT
        FROM STG.WEEKLY_TEAM_MATCHUPS_LVL2 S,
        LATERAL FLATTEN(INPUT => S.FULL_MATCHUP_OBJECT:teams:team) P
        WHERE TRIM(P.VALUE:team_key::VARCHAR) = TRIM(S.TEAM_KEY)
        ;
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    stmt.execute();

    sql_command = `
        TRUNCATE TABLE STG.WEEKLY_TEAM_MATCHUP_STATS;
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    stmt.execute();

    sql_command = `
        INSERT INTO STG.WEEKLY_TEAM_MATCHUP_STATS (
            WEEK_NUMBER,
            TEAM_KEY,
            TEAM_NAME,
            WEEK_START_DATE,
            WEEK_END_DATE,
            STAT_ID,
            STAT_VALUE,
            SOURCE_FILE_NAME,
            LOAD_DT
        )
        SELECT 
            S.WEEK_NUMBER,
            S.TEAM_KEY,
            S.TEAM_NAME,
            S.WEEK_START_DATE,
            S.WEEK_END_DATE,

            P.VALUE:stat_id::INT AS STAT_ID,
            P.VALUE:value::VARCHAR AS STAT_VALUE,

            S.SOURCE_FILE_NAME,
            S.LOAD_DT
        FROM STG.WEEKLY_TEAM_MATCHUPS_LVL3 S,
        LATERAL FLATTEN (INPUT => S.MATCHUP_OBJECT:team_stats:stats:stat) P
        ;
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    stmt.execute();

    sql_command = `
        DELETE FROM DWH.WEEKLY_TEAM_TOTALS
        WHERE WEEK_NUMBER = ${WEEK_NUMBER}
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
            WEEK_NUMBER, TEAM_KEY, TEAM_NAME AS TEAM,

            LISTAGG(
                CASE WHEN STAT_ID = 12 THEN STAT_VALUE ELSE '' END
            )::NUMBER(38,0) AS TOTAL_PTS,
            LISTAGG(
                CASE WHEN STAT_ID = 15 THEN STAT_VALUE ELSE '' END
            )::NUMBER(38,0) AS TOTAL_REB,
            LISTAGG(
                CASE WHEN STAT_ID = 16 THEN STAT_VALUE ELSE '' END
            )::NUMBER(38,0) AS TOTAL_AST,
            LISTAGG(
                CASE WHEN STAT_ID = 18 THEN STAT_VALUE ELSE '' END
            )::NUMBER(38,0) AS TOTAL_BLK,
            LISTAGG(
                CASE WHEN STAT_ID = 17 THEN STAT_VALUE ELSE '' END
            )::NUMBER(38,0) AS TOTAL_STL,
            LISTAGG(
                CASE WHEN STAT_ID = 10 THEN STAT_VALUE ELSE '' END
            )::NUMBER(38,0) AS TOTAL_THREE_PM,

            SPLIT(
                LISTAGG(
                CASE WHEN STAT_ID = 9004003 THEN STAT_VALUE ELSE '' END
                )::VARCHAR,
                '/')[0]::NUMBER(38,0)
            AS TOTAL_FGM,

            SPLIT(
                LISTAGG(
                CASE WHEN STAT_ID = 9004003 THEN STAT_VALUE ELSE '' END
                )::VARCHAR,
                '/')[1]::NUMBER(38,0)
            AS TOTAL_FGA,

            ROUND(TOTAL_FGM::FLOAT / NULLIF(TOTAL_FGA::FLOAT, 0), 6) AS FG_PCT,

            SPLIT(
                LISTAGG(
                CASE WHEN STAT_ID = 9007006 THEN STAT_VALUE ELSE '' END
                )::VARCHAR,
                '/')[0]::NUMBER(38,0)
            AS TOTAL_FTM,

            SPLIT(
                LISTAGG(
                CASE WHEN STAT_ID = 9007006 THEN STAT_VALUE ELSE '' END
                )::VARCHAR,
                '/')[1]::NUMBER(38,0)
                AS TOTAL_FTA,

                ROUND(TOTAL_FTM::FLOAT / NULLIF(TOTAL_FTA::FLOAT, 0), 6) AS FT_PCT,

            LISTAGG(
                CASE WHEN STAT_ID = 19 THEN STAT_VALUE ELSE '' END
            )::NUMBER(38,0) AS TOTAL_TOS,

            CURRENT_TIMESTAMP() AS LOAD_DT

        FROM STG.WEEKLY_TEAM_MATCHUP_STATS
        GROUP BY 
            WEEK_NUMBER, TEAM_KEY, TEAM_NAME
        ;
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    stmt.execute();


    sql_command = `
        DELETE FROM DWH.MATCHUPS_CROSSJOIN
        WHERE WEEK_NUMBER = ${WEEK_NUMBER}
        ;
    `;
    stmt = snowflake.createStatement({sqlText: sql_command});
    stmt.execute();

    sql_command = `
        INSERT INTO DWH.MATCHUPS_CROSSJOIN
        (
            WEEK_NUMBER,
            MATCHUP_ID,
            TEAM_1,
            TEAM_1_KEY,
            TEAM_2,
            TEAM_2_KEY,
            CATEGORIES_WON_BY_TEAM_1,
            CATEGORIES_WON_BY_TEAM_2,
            TEAM_1_RESULT,
            LOAD_DT
        )
        SELECT 
            P1.WEEK_NUMBER,
            CONCAT(P1.WEEK_NUMBER::VARCHAR, '^', LEAST(P1.TEAM_KEY, P2.TEAM_KEY), '--', GREATEST(P1.TEAM_KEY, P2.TEAM_KEY) ) AS MATCHUP_ID,
            P1.TEAM AS TEAM_1, 
            P1.TEAM_KEY AS TEAM_1_KEY,
            P2.TEAM AS TEAM_2,
            P2.TEAM_KEY AS TEAM_2_KEY,
        
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
            ON P1.WEEK_NUMBER = P2.WEEK_NUMBER
        WHERE 
            P1.TEAM_KEY <> P2.TEAM_KEY
            AND P1.WEEK_NUMBER = ${WEEK_NUMBER}
        ORDER BY WEEK_NUMBER, TEAM_1, TEAM_2
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