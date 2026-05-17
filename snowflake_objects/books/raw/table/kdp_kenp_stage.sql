CREATE OR REPLACE TABLE BOOKS.RAW.KDP_KENP_STAGE (
    "Date"           VARCHAR,
    "Title"          VARCHAR,
    "Author Name"    VARCHAR,
    "eBook ASIN"     VARCHAR,
    "Audiobook ASIN" VARCHAR,
    "Audible ASIN"   VARCHAR,
    "Marketplace"    VARCHAR,
    "KENP"           VARCHAR,
    DP_RUN_ID                 VARCHAR,
    DP_SOURCE_FILE_NAME       VARCHAR,
    DP_SOURCE_FILE_ROW_NUMBER INT,
    DP_LOAD_TIMESTAMP         TIMESTAMP_NTZ(9)
);
