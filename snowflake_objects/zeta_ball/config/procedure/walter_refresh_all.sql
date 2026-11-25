CREATE OR REPLACE PROCEDURE CONFIG.WALTER_REFRESH_ALL()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
// Master trigger for a full refresh. Runs each step in order and stops at the first failure
// (the child procs return "Error: ..." instead of raising, so the return value is checked).
var steps = [
    'CONFIG.WALTER_REFRESH_TEAMS',
    'CONFIG.WALTER_REFRESH_ROSTERS',
    'CONFIG.WALTER_REFRESH_WEEKLY_MATCHUPS',
    'CONFIG.PUBLISH_SLOPCS'
];

for (var i = 0; i < steps.length; i++) {
    var stmt = snowflake.createStatement({sqlText: `CALL ${steps[i]}()`});
    var result_set = stmt.execute();
    result_set.next();
    var response = result_set.getColumnValue(1);

    if (response !== 'SUCCESS') {
        throw new Error(steps[i] + ' failed: ' + response);
    }
}

return "SUCCESS";
$$;
