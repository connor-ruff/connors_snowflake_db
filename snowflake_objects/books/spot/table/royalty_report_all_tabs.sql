CREATE TABLE IF NOT EXISTS BOOKS.SPOT.ROYALTY_REPORT_ALL_TABS (
    TITLE                     VARCHAR,
    SALES_TYPE                VARCHAR,
    ROYALTY_RATE              NUMBER(38,10),
    DISPLAY_NUMBER            VARCHAR,
    ISBN                      VARCHAR,
    PUBLISHER                 VARCHAR,
    PARTNER                   VARCHAR,
    PROMOTION                 VARCHAR,
    SALE_TERRITORY            VARCHAR,
    CURRENCY                  VARCHAR,
    DLP                       NUMBER(38,10),
    PRICE_TYPE                VARCHAR,
    SALES_QTY                 NUMBER(38,10),
    REVENUE                   NUMBER(38,10),
    ROYALTY_EARNED            NUMBER(38,10),
    LESS_DISTRIBUTION_FEE     NUMBER(38,10),
    EXCHANGE_RATE             NUMBER(38,10),
    ROYALTY_PAYABLE_CURRENCY  VARCHAR,
    ROYALTY_PAYABLE           NUMBER(38,10),

    DP_SOURCE_FILE_MONTH      DATE,      -- First Date of the month of the earnings file
    DP_RUN_ID                 VARCHAR,
    DP_SOURCE_FILE_NAME       VARCHAR,
    DP_SOURCE_FILE_ROW_NUMBER NUMBER(38,0),
    DP_LOAD_TIMESTAMP         TIMESTAMP_NTZ(9)
);
