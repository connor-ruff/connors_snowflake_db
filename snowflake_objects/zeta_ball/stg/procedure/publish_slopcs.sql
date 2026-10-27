CREATE OR REPLACE PROCEDURE STG.PUBLISH_SLOPCS()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
// Unloads the latest season's SlopCS standings to the public website bucket as
// data/latest.json (read by index.html) and data/season_{yr}/week_{n}.json (history).
// Everything written here is PUBLIC.
try {

    var sql_command = `
        SELECT LEAGUE_START_YEAR, MAX(THROUGH_WEEK) AS THROUGH_WEEK
        FROM DWH.SLOP_CS_RANKINGS_BY_WEEK
        WHERE LEAGUE_START_YEAR = (SELECT MAX(LEAGUE_START_YEAR) FROM DWH.SLOP_CS_RANKINGS_BY_WEEK)
        GROUP BY LEAGUE_START_YEAR;
    `;
    var stmt = snowflake.createStatement({sqlText: sql_command});
    var result_set = stmt.execute();
    if (!result_set.next()) {
        throw new Error('No SlopCS standings to publish');
    }
    var season = result_set.getColumnValue('LEAGUE_START_YEAR');
    var week = result_set.getColumnValue('THROUGH_WEEK');

    // One row, one JSON object, so each file is a single valid JSON document.
    // The SlopCS algorithm is proprietary: publish only rank, team, and score, never the inputs.
    // The score is scaled by 10000 (2 decimals, from the unrounded inputs) so it does not read as a percentage.
    var payload_query = `
        SELECT OBJECT_CONSTRUCT(
            'league_start_year', ${season},
            'through_week', ${week},
            'generated_at', TO_VARCHAR(CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()), 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
            'rankings', ARRAY_AGG(
                OBJECT_CONSTRUCT_KEEP_NULL(
                    'rank', C.SLOP_CS_RANK,
                    'team', C.TEAM,
                    'slopcs_score', ROUND((C.WINS + C.TIES * 0.5) / C.TOTAL_GAMES * 10000, 2)
                )
            ) WITHIN GROUP (ORDER BY C.SLOP_CS_RANK, C.TEAM)
        )
        FROM DWH.SLOP_CS_RANKINGS_BY_WEEK C
        WHERE C.LEAGUE_START_YEAR = ${season}
        AND C.THROUGH_WEEK = ${week}
    `;

    var targets = [
        `data/season_${season}/week_${week}.json`,
        `data/latest.json`
    ];

    for (var i = 0; i < targets.length; i++) {
        sql_command = `
            COPY INTO @STG.SLOPCS_SITE_STAGE/${targets[i]}
            FROM (${payload_query})
            FILE_FORMAT = (TYPE = JSON COMPRESSION = NONE)
            SINGLE = TRUE
            OVERWRITE = TRUE;
        `;
        stmt = snowflake.createStatement({sqlText: sql_command});
        stmt.execute();
    }

    return "SUCCESS";

} catch (err) {
    return "Error: " + err.message;
}
$$;
