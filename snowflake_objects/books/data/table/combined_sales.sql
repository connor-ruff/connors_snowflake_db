CREATE TABLE IF NOT EXISTS BOOKS.DATA.COMBINED_SALES (
    ROYALTY_DATE DATE, 
    TITLE VARCHAR(250),
    AUTHOR_NAME VARCHAR(250),
    ASIN_ISBN VARCHAR(25),
    MARKETPLACE VARCHAR(250),
    ROYALTY_TYPE VARCHAR(250),
    TRANSACTION_TYPE VARCHAR(250),
    UNITS_SOLD NUMBER(38,2),
    UNITS_REFUNDED NUMBER(38,2),
    NET_UNITS_SOLD NUMBER(38,2),
    AVG_LIST_PRICE_WOUT_TAX NUMBER(38,2),
    AVG_OFFER_PRICE_WOUT_TAX NUMBER(38,2),
    AVG_DELIV_MANUFACT_COST NUMBER(38,2),
    ROYALTY NUMBER(38,2),
    CURRENCY VARCHAR(25),
    S3_SOURCE_FILE_NAME VARCHAR(250),
    LOAD_DT TIMESTAMP_NTZ(9)
);