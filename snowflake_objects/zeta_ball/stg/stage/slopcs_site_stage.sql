-- Write target for the public SlopCS website bucket. Everything written here is PUBLICLY readable.
-- Requires s3://zeta-ball-slopcs in S3_FULL_INT's STORAGE_ALLOWED_LOCATIONS (see snowflake_admin/storage_integration/s3_full_int.sql).
CREATE STAGE IF NOT EXISTS STG.SLOPCS_SITE_STAGE
URL = 's3://zeta-ball-slopcs/'
STORAGE_INTEGRATION = S3_FULL_INT
;
