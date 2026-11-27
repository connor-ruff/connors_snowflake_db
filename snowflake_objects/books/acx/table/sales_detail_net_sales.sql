CREATE TABLE IF NOT EXISTS BOOKS.ACX.SALES_DETAIL_NET_SALES (
    ROYALTY_EARNER            VARCHAR(250),
    PRODUCT_ID                VARCHAR(50),
    AUTHOR                    VARCHAR(250),
    TITLE                     VARCHAR(250),
    DIGITAL_ISBN              VARCHAR(50),
    PROVIDER_PRODUCT_ID       VARCHAR(50),
    TRANSACTION_TYPE          VARCHAR(50),
    MARKETPLACE               VARCHAR(10),
    PURCHASE_TYPE             VARCHAR(100),
    OFFER                     VARCHAR(50),
    ROYALTY_RULE              VARCHAR(100),
    ADDITIONAL_RULE_DETAILS   VARCHAR(250),
    CURRENCY                  VARCHAR(10),
    ROYALTY_RATE              NUMBER(38,10),
    PAYEE_SPLIT               NUMBER(38,2),
    NET_UNITS                 NUMBER(38,2),
    NET_SALES                 NUMBER(38,10),
    NET_ROYALTIES_EARNED      NUMBER(38,10),

    DP_SOURCE_FILE_MONTH      DATE,      -- First Date of the month of the earnings file
    DP_RUN_ID                 VARCHAR,
    DP_SOURCE_FILE_NAME       VARCHAR,
    DP_SOURCE_FILE_ROW_NUMBER NUMBER(38,0),
    DP_LOAD_TIMESTAMP         TIMESTAMP_NTZ(9)
);
