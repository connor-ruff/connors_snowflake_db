-- Snowflake Scripting can only RAISE exceptions with a fixed message, so this helper
-- lets a SQL procedure fail with a dynamically built message.
CREATE OR REPLACE PROCEDURE BOOKS.CONFIG.RAISE_ERROR(MESSAGE VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$
    throw MESSAGE;
$$;
