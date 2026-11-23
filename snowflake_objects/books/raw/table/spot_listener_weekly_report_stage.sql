CREATE OR REPLACE TABLE RAW.SPOT_LISTENER_WEEKLY_REPORT_STAGE (
    "Licensor ID"  VARCHAR,
    "Publisher" VARCHAR,
    "Audiobook URI" VARCHAR,
    "ISBN" VARCHAR,
    "Audiobook Title" VARCHAR,
    "Author(s)" VARCHAR,
    "Listener Country" VARCHAR,
    "Listener Gender" VARCHAR,
    "Listener Age Group" VARCHAR,
    "Total Minutes Listened" NUMBER(38,10),
    "Complete Listen Equivalents (Total Minutes / Length of Book)" NUMBER(38,10),

    DP_RUN_ID                 VARCHAR,
    DP_SOURCE_FILE_NAME       VARCHAR,
    DP_SOURCE_FILE_ROW_NUMBER INT,
    DP_LOAD_TIMESTAMP         TIMESTAMP_NTZ(9)
)
;