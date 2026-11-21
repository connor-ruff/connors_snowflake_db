CREATE TABLE IF NOT EXISTS BOOKS.ACX.ADJUSTMENTS (
    ROYALTY_EARNER             VARCHAR,
    ADJUSTMENT_TYPE            VARCHAR,
    ADJUSTMENT_COMMENTS        VARCHAR,
    ORDER_START_DATE           DATE,
    ORDER_END_DATE             DATE,
    PARENT_PRODUCT_ID          VARCHAR,
    TITLE                      VARCHAR,
    AUTHOR                     VARCHAR,
    DIGITAL_ISBN               VARCHAR,
    PRICE                      NUMBER(38,10),
    PRICE_CURRENCY             VARCHAR,
    PRICE_TYPE                 VARCHAR,
    MARKETPLACE                VARCHAR,
    PAYEE_SPLIT                NUMBER(38,10),
    ROYALTY_RATE               NUMBER(38,10),
    CONSUMPTION_VALUE          NUMBER(38,10),
    LISTENING_DURATION_MINUTES NUMBER(38,10),
    SALE_TYPE                  VARCHAR,
    NET_SALES                  NUMBER(38,10),
    QUANTITY                   NUMBER(38,10),
    AMOUNT_DUE                 NUMBER(38,10),

    DP_SOURCE_FILE_MONTH      DATE,      -- First Date of the month of the earnings file
    DP_RUN_ID                 VARCHAR,
    DP_SOURCE_FILE_NAME       VARCHAR,
    DP_SOURCE_FILE_ROW_NUMBER INT,
    DP_LOAD_TIMESTAMP         TIMESTAMP_NTZ(9)
);
