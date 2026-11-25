USE ROLE SYSADMIN;
CREATE DATABASE IF NOT EXISTS ZETA_BALL;

USE ROLE USERADMIN;
CREATE ROLE IF NOT EXISTS ZETA_BALL_ADMIN;
GRANT ROLE ZETA_BALL_ADMIN TO ROLE SYSADMIN;

USE ROLE SYSADMIN;

GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE ZETA_BALL_ADMIN;
GRANT USAGE ON DATABASE ZETA_BALL TO ROLE ZETA_BALL_ADMIN;
GRANT CREATE SCHEMA ON DATABASE ZETA_BALL TO ROLE ZETA_BALL_ADMIN;
GRANT USAGE ON INTEGRATION S3_FULL_INT TO ROLE ZETA_BALL_ADMIN;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE ZETA_BALL_ADMIN;

USE ROLE ZETA_BALL_ADMIN;
USE DATABASE ZETA_BALL;
CREATE SCHEMA IF NOT EXISTS CONFIG;
CREATE SCHEMA IF NOT EXISTS RAW;

USE ROLE ACCOUNTADMIN;
-- =============================================================================
-- grant_ownership.sql
-- Transfers ownership of ALL existing objects and ALL future objects of the
-- listed types, in the listed schemas, to a single role.
--
-- Run as a role that can transfer ownership (current owner of the objects,
-- or a role with MANAGE GRANTS, e.g. SECURITYADMIN / ACCOUNTADMIN).
-- Start with v_dry_run = TRUE to review the statements, then set it to FALSE.
-- =============================================================================

EXECUTE IMMEDIATE $$
DECLARE
  -- ---------------------------- CONFIG ------------------------------------
  v_role          VARCHAR DEFAULT 'ZETA_BALL_ADMIN';   -- role that will own everything
  v_database      VARCHAR DEFAULT 'ZETA_BALL';
  v_schemas       ARRAY   DEFAULT ARRAY_CONSTRUCT('CONFIG', 'RAW', 'DWH');
  v_object_types  ARRAY   DEFAULT ARRAY_CONSTRUCT('TABLE', 'VIEW', 'SEQUENCE', 'STAGE', 'FILE FORMAT', 'PROCEDURE', 'FUNCTION', 'TASK');
  
  -- What to do with existing privileges on current objects:
  -- 'COPY CURRENT GRANTS' keeps them, 'REVOKE CURRENT GRANTS' drops them.
  v_grant_option  VARCHAR DEFAULT 'REVOKE CURRENT GRANTS';
  v_dry_run       BOOLEAN DEFAULT FALSE;                  -- TRUE = only log, don't execute
  -- ------------------------------------------------------------------------

  v_modes         ARRAY   DEFAULT ARRAY_CONSTRUCT('ALL', 'FUTURE');
  v_schema        VARCHAR;
  v_type          VARCHAR;
  v_mode          VARCHAR;
  v_stmt          VARCHAR;
  v_log           VARCHAR DEFAULT '';
  n_schemas       INTEGER;
  n_types         INTEGER;
  n_modes         INTEGER;
BEGIN
  n_schemas := ARRAY_SIZE(v_schemas) - 1;
  n_types   := ARRAY_SIZE(v_object_types) - 1;
  n_modes   := ARRAY_SIZE(v_modes) - 1;

  FOR i IN 0 TO n_schemas DO
    v_schema := v_schemas[i]::VARCHAR;

    FOR j IN 0 TO n_types DO
      v_type := UPPER(TRIM(v_object_types[j]::VARCHAR));

      FOR k IN 0 TO n_modes DO
        v_mode := v_modes[k]::VARCHAR;

        -- Object type is pluralised by appending 'S' (TABLE -> TABLES, FILE FORMAT -> FILE FORMATS)
        v_stmt := 'GRANT OWNERSHIP ON ' || v_mode || ' ' || v_type || 'S IN SCHEMA '
               || v_database || '.' || v_schema
               || ' TO ROLE ' || v_role;

        -- COPY/REVOKE CURRENT GRANTS is only valid for existing objects, not FUTURE
        IF (v_mode = 'ALL') THEN
          v_stmt := v_stmt || ' ' || v_grant_option;
        END IF;

        IF (v_dry_run) THEN
          v_log := v_log || 'DRY : ' || v_stmt || CHAR(10);
        ELSE
          BEGIN
            EXECUTE IMMEDIATE :v_stmt;
            v_log := v_log || 'OK  : ' || v_stmt || CHAR(10);
          EXCEPTION
            WHEN OTHER THEN
              v_log := v_log || 'FAIL: ' || v_stmt || '  -- ' || SQLERRM || CHAR(10);
          END;
        END IF;

      END FOR;
    END FOR;
  END FOR;

  RETURN v_log;
END;
$$;