-- Central SQL executor for the TERR database.
-- Every other procedure should route its SQL through this so that error handling,
-- result shaping, and query-id capture are consistent in one place.
--
-- Returns a VARIANT of the shape:
-- {
--   "success":        TRUE | FALSE,
--   "message":        "human readable summary",
--   "query_id":       "01b2c3d4-...",
--   "sql":            "the statement that was run",
--   "rows_affected":  <int>   -- DML only, null otherwise
--   "row_count":      <int>   -- number of rows returned in "data"
--   "truncated":      TRUE | FALSE,   -- TRUE if the result set was larger than MAX_ROWS_IN
--   "columns":        ["COL_A", "COL_B"],
--   "data":           [ {"COL_A": 1, "COL_B": "x"}, ... ],
--   "error":          { "code": .., "state": .., "message": .., "stack": .. }  -- failure only
-- }

CREATE OR REPLACE PROCEDURE CONFIG.RUN_SQL_COMMAND(
    SQL_IN      VARCHAR,   -- statement to run; use ? placeholders for any user-supplied values
    BINDS_IN    ARRAY,     -- values for the ? placeholders (NULL if none)
    MAX_ROWS_IN FLOAT      -- cap on rows pulled back into "data" (NULL => 10000)
                           -- FLOAT, not INT: JavaScript procedures do not support NUMBER types
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    var MAX_ROWS = (MAX_ROWS_IN === null || MAX_ROWS_IN === undefined)
        ? 10000
        : Math.floor(MAX_ROWS_IN);

    var response = {
        success:       false,
        message:       null,
        query_id:      null,
        sql:           SQL_IN,
        rows_affected: null,
        row_count:     0,
        truncated:     false,
        columns:       [],
        data:          []
    };

    try {
        var stmt_options = { sqlText: SQL_IN };
        if (BINDS_IN !== null && BINDS_IN !== undefined && BINDS_IN.length > 0) {
            stmt_options.binds = BINDS_IN;
        }

        var stmt   = snowflake.createStatement(stmt_options);
        var result = stmt.execute();

        response.query_id = stmt.getQueryId();

        // Rows changed by an INSERT/UPDATE/DELETE/MERGE. Throws for statements
        // that aren't DML, so it is best-effort.
        try {
            response.rows_affected = stmt.getNumRowsAffected();
        } catch (e) {
            response.rows_affected = null;
        }

        // Column metadata (1-indexed in the Snowflake JS API).
        var col_count = stmt.getColumnCount();
        for (var c = 1; c <= col_count; c++) {
            response.columns.push(stmt.getColumnName(c));
        }

        // Materialize rows into an array of objects keyed by column name.
        while (result.next()) {
            if (response.data.length >= MAX_ROWS) {
                response.truncated = true;
                break;
            }

            var row = {};
            for (var i = 1; i <= col_count; i++) {
                var value = result.getColumnValue(i);

                // Dates/timestamps arrive as JS Date objects; stringify them so the
                // VARIANT holds a stable ISO string instead of an epoch-ish blob.
                if (value instanceof Date) {
                    value = value.toISOString();
                }
                row[stmt.getColumnName(i)] = value;
            }
            response.data.push(row);
        }

        response.row_count = response.data.length;
        response.success   = true;
        response.message   = response.truncated
            ? 'Statement executed successfully; results truncated at ' + MAX_ROWS + ' rows.'
            : 'Statement executed successfully.';

        return response;

    } catch (err) {
        response.success = false;
        response.message = 'ERROR: ' + err.message;
        response.error   = {
            code:    err.code,
            state:   err.state,
            message: err.message,
            stack:   err.stackTraceTxt
        };
        return response;
    }
$$
;


-- Convenience overload: no binds, default row cap.
CREATE OR REPLACE PROCEDURE CONFIG.RUN_SQL_COMMAND(
    SQL_IN VARCHAR
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    RESPONSE VARIANT;
BEGIN
    CALL CONFIG.RUN_SQL_COMMAND(:SQL_IN, NULL::ARRAY, NULL::FLOAT) INTO :RESPONSE;
    RETURN RESPONSE;
END
$$
;
