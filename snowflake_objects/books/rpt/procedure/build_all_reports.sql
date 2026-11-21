CREATE OR REPLACE PROCEDURE BOOKS.RPT.BUILD_ALL_REPORTS()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    c_checks        CURSOR FOR
        SELECT CHECK_NAME, DESCRIPTION, CHECK_SQL
        FROM   BOOKS.CONFIG.PRE_REPORT_REBUILD_CHECKS
        WHERE  IS_ACTIVE
        ORDER BY CHECK_ID;
    c_reports       CURSOR FOR
        SELECT SOURCE_VIEW_NAME, TARGET_TABLE_NAME
        FROM   BOOKS.CONFIG.REPORTING_TABLES
        WHERE  IS_ACTIVE
        ORDER BY TARGET_TABLE_NAME;
    v_pass          BOOLEAN;
    v_failed_count  INT DEFAULT 0;
    v_failed_list   VARCHAR DEFAULT '';
    v_inserted      INT DEFAULT 0;
    v_total         INT DEFAULT 0;
    v_sql           VARCHAR;
BEGIN

    -- 1) QC checks: run every check, collect failures, then fail once with the full list
    FOR chk IN c_checks DO
        v_pass := NULL;
        BEGIN
            v_sql := chk.CHECK_SQL;
            EXECUTE IMMEDIATE :v_sql;
            SELECT PASS_TEST INTO :v_pass FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

            -- anything other than TRUE (FALSE or NULL) is a failure
            IF (v_pass IS NULL OR NOT v_pass) THEN
                v_failed_count := v_failed_count + 1;
                v_failed_list  := v_failed_list || CHR(10) || '- ' || chk.CHECK_NAME || ': ' || chk.DESCRIPTION;
            END IF;
        EXCEPTION
            WHEN OTHER THEN
                v_failed_count := v_failed_count + 1;
                v_failed_list  := v_failed_list || CHR(10) || '- ' || chk.CHECK_NAME || ' (check SQL errored: ' || SQLERRM || ')';
        END;
    END FOR;

    IF (v_failed_count > 0) THEN
        CALL BOOKS.CONFIG.RAISE_ERROR('QC FAILED: ' || :v_failed_count::VARCHAR || ' check(s) failed. No reports were rebuilt.' || :v_failed_list);
    END IF;

    -- 2) Rebuild each reporting table from its source view
    FOR rpt IN c_reports DO
        v_sql := 'TRUNCATE TABLE BOOKS.' || rpt.TARGET_TABLE_NAME;
        EXECUTE IMMEDIATE :v_sql;

        v_sql := 'INSERT INTO BOOKS.' || rpt.TARGET_TABLE_NAME || ' SELECT * FROM BOOKS.' || rpt.SOURCE_VIEW_NAME;
        EXECUTE IMMEDIATE :v_sql;

        v_inserted := SQLROWCOUNT;
        v_total    := v_total + v_inserted;
    END FOR;

    RETURN 'SUCCESS: Inserted ' || :v_total::VARCHAR || ' total row(s) across all reporting tables.';
END;
$$;
