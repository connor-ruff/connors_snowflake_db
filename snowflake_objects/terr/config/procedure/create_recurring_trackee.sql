-- Creates a single DBO.TRACKEE row with TYPE = 'RECURRING'.
-- The type is fixed by this procedure, not passed in -- see the TRACKEE_TYPE constant below.
--
-- All SQL is routed through CONFIG.RUN_SQL_COMMAND so error handling and
-- result shaping stay consistent across the TERR procedures.
--
-- Returns a VARIANT:
--   { "success": TRUE,  "message": "...", "trackee_sid": 12, "trackee_type": "RECURRING" }
--   { "success": FALSE, "message": "...", "error": <run_sql_command response or null> }

CREATE OR REPLACE PROCEDURE CONFIG.CREATE_RECURRING_TRACKEE(
    NAME_IN        VARCHAR,
    NOTES_IN       VARCHAR,
    FREQUENCY_NUM  INT,
    FREQUENCY_UNIT VARCHAR
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    -- Treat as a constant -- do not reassign below.
    -- This procedure only ever creates TRACKEE rows of this type; the type is not
    -- a parameter. A one-off / non-recurring trackee needs its own procedure.
    TRACKEE_TYPE VARCHAR := 'RECURRING';

    RES      VARIANT;
    NEW_SID  INT;
    UNIT_VAR VARCHAR;
BEGIN

    -- ---------- input validation ----------
    IF (NAME_IN IS NULL OR TRIM(NAME_IN) = '') THEN
        RETURN OBJECT_CONSTRUCT(
            'success', FALSE,
            'message', 'NAME_IN is required.'
        );
    END IF;

    IF (FREQUENCY_NUM IS NULL OR FREQUENCY_NUM < 1) THEN
        RETURN OBJECT_CONSTRUCT(
            'success', FALSE,
            'message', 'FREQUENCY_NUM must be a positive integer.'
        );
    END IF;

    UNIT_VAR := UPPER(TRIM(COALESCE(FREQUENCY_UNIT, '')));

    IF (NOT ARRAY_CONTAINS(:UNIT_VAR::VARIANT,
                           ARRAY_CONSTRUCT('DAY', 'WEEK', 'MONTH', 'YEAR'))) THEN
        RETURN OBJECT_CONSTRUCT(
            'success', FALSE,
            'message', 'FREQUENCY_UNIT must be one of DAY, WEEK, MONTH, YEAR. Got: '
                       || COALESCE(FREQUENCY_UNIT, '<null>')
        );
    END IF;

    -- ---------- write ----------
    BEGIN TRANSACTION;

    -- Pull the SID up front so it can be returned to the caller.
    CALL CONFIG.RUN_SQL_COMMAND('SELECT DBO.TRACKEE_SID.NEXTVAL AS SID') INTO :RES;

    IF (NOT RES:success::BOOLEAN) THEN
        ROLLBACK;
        RETURN OBJECT_CONSTRUCT(
            'success', FALSE,
            'message', 'Failed to reserve a TRACKEE_SID.',
            'error',   RES
        );
    END IF;

    NEW_SID := RES:data[0]:SID::INT;

    CALL CONFIG.RUN_SQL_COMMAND(
        'INSERT INTO DBO.TRACKEE
            (TRACKEE_SID, TYPE, NAME, NOTES, FREQUENCY_NUM, FREQUENCY_UNIT,
             ACTIVE_FLAG, RECORD_CREATED_DT, RECORD_UPDATED_DT)
         SELECT ?, ?, ?, ?, ?, ?, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()',
        ARRAY_CONSTRUCT(:NEW_SID, :TRACKEE_TYPE, :NAME_IN, :NOTES_IN,
                        :FREQUENCY_NUM, :UNIT_VAR),
        NULL::FLOAT
    ) INTO :RES;

    IF (NOT RES:success::BOOLEAN) THEN
        ROLLBACK;
        RETURN OBJECT_CONSTRUCT(
            'success', FALSE,
            'message', 'Failed to insert TRACKEE "' || NAME_IN || '".',
            'error',   RES
        );
    END IF;

    COMMIT;

    RETURN OBJECT_CONSTRUCT(
        'success',      TRUE,
        'message',      'Created ' || TRACKEE_TYPE || ' trackee "' || NAME_IN || '" every '
                        || FREQUENCY_NUM || ' ' || UNIT_VAR || '(S).',
        'trackee_sid',  NEW_SID,
        'trackee_type', TRACKEE_TYPE
    );

EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;
        RETURN OBJECT_CONSTRUCT(
            'success', FALSE,
            'message', 'Unhandled error: ' || SQLERRM,
            'error',   OBJECT_CONSTRUCT('code', SQLCODE, 'state', SQLSTATE, 'message', SQLERRM)
        );
END
$$
;
