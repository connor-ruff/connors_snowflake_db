CREATE OR REPLACE TABLE BOOKS.RAW.ACX_SUMMARY_STAGE (
    "Royalty Earner"             VARCHAR,
    "Amount Paid (Prior Period)" VARCHAR,
    "Balance (Brought Forward)"  VARCHAR,
    "Net Royalties Earned"       VARCHAR,
    "Adjustments"                VARCHAR,
    "Recoupments"                VARCHAR,
    "Net Royalties"              VARCHAR,
    "Bounties"                   VARCHAR,
    "Payment Due"                VARCHAR,
    DP_RUN_ID               VARCHAR,
    DP_SOURCE_FILE_NAME     VARCHAR,
    DP_SOURCE_FILE_ROW_NUMBER INT,
    DP_LOAD_TIMESTAMP       TIMESTAMP_NTZ(9)
);
