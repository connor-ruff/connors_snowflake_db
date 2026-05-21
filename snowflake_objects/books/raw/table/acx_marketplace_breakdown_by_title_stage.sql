CREATE OR REPLACE TABLE BOOKS.RAW.ACX_MARKETPLACE_BREAKDOWN_BY_TITLE_STAGE (
    "Royalty Earner"        VARCHAR,
    "Product ID"            VARCHAR,
    "Author"                VARCHAR,
    "Title"                 VARCHAR,
    "Digital ISBN"          VARCHAR,
    "AU"                    VARCHAR,
    "CA"                    VARCHAR,
    "DE"                    VARCHAR,
    "IN"                    VARCHAR,
    "JP"                    VARCHAR,
    "UK"                    VARCHAR,
    "US"                    VARCHAR,
    "Total"                 VARCHAR,
    DP_RUN_ID               VARCHAR,
    DP_SOURCE_FILE_NAME     VARCHAR,
    DP_SOURCE_FILE_ROW_NUMBER INT,
    DP_LOAD_TIMESTAMP       TIMESTAMP_NTZ(9)
);
