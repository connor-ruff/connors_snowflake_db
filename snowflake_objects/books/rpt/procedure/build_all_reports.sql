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
    c_groups        CURSOR FOR
        SELECT DISTINCT RUN_GROUP
        FROM   BOOKS.CONFIG.REPORTING_TABLES
        WHERE  IS_ACTIVE
        ORDER BY RUN_GROUP;
    rs_reports      RESULTSET;
    v_group         INT;
    v_src           VARCHAR;
    v_tgt           VARCHAR;
    v_pass          BOOLEAN;
    v_failed_count  INT DEFAULT 0;
    v_failed_list   VARCHAR DEFAULT '';
    v_tables        INT DEFAULT 0;
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

    -- 2) Rebuild reporting tables one run group at a time (ascending).
    --    Within a group, every table is rebuilt in parallel via async child jobs;
    --    AWAIT ALL blocks until the group is done (and raises if any child failed),
    --    so later groups never start on top of a failed or unfinished earlier group.
    FOR grp IN c_groups DO
        v_group := grp.RUN_GROUP;
        -- RESULTSET (not a cursor) so :v_group is bound at assignment time
        rs_reports := (
            SELECT SOURCE_VIEW_NAME, TARGET_TABLE_NAME
            FROM   BOOKS.CONFIG.REPORTING_TABLES
            WHERE  IS_ACTIVE
              AND  RUN_GROUP = :v_group
        );
        FOR rpt IN rs_reports DO
            -- record fields can't be bound directly inside ASYNC, so copy to plain variables first
            v_src := rpt.SOURCE_VIEW_NAME;
            v_tgt := rpt.TARGET_TABLE_NAME;
            ASYNC (CALL BOOKS.RPT.REBUILD_REPORT_TABLE(:v_src, :v_tgt));
            v_tables := v_tables + 1;
        END FOR;
        AWAIT ALL;
    END FOR;

    RETURN 'SUCCESS: Rebuilt ' || :v_tables::VARCHAR || ' reporting table(s).';
END;
$$;
