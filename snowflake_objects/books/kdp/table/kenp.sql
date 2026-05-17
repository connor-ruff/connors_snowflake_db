CREATE TABLE IF NOT EXISTS BOOKS.KDP.KENP (
    KENP_DATE                 DATE,
    TITLE                     VARCHAR(250),
    AUTHOR_NAME               VARCHAR(250),
    EBOOK_ASIN                VARCHAR(25),
    AUDIOBOOK_ASIN            VARCHAR(25),
    AUDIBLE_ASIN              VARCHAR(25),
    MARKETPLACE               VARCHAR(250),
    KENP                      NUMBER(38,0),
    DP_RUN_ID                 VARCHAR,
    DP_SOURCE_FILE_NAME       VARCHAR,
    DP_SOURCE_FILE_ROW_NUMBER INT,
    DP_LOAD_TIMESTAMP         TIMESTAMP_NTZ(9)
);
