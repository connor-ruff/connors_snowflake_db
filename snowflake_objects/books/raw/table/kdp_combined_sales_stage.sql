CREATE OR REPLACE TABLE BOOKS.RAW.KDP_COMBINED_SALES_STAGE (
    "Royalty Date"                      VARCHAR,
    "Title"                             VARCHAR,
    "Author Name"                       VARCHAR,
    "ASIN/ISBN"                         VARCHAR,
    "Marketplace"                       VARCHAR,
    "Royalty Type"                      VARCHAR,
    "Transaction Type"                  VARCHAR,
    "Units Sold"                        VARCHAR,
    "Units Refunded"                    VARCHAR,
    "Net Units Sold"                    VARCHAR,
    "Avg. List Price without tax"       VARCHAR,
    "Avg. Offer Price without tax"      VARCHAR,
    "Avg. Delivery/Manufacturing cost"  VARCHAR,
    "Royalty"                           VARCHAR,
    "Currency"                          VARCHAR,
    DP_RUN_ID                           VARCHAR,
    DP_SOURCE_FILE_NAME                 VARCHAR,
    DP_SOURCE_FILE_ROW_NUMBER           INT,
    DP_LOAD_TIMESTAMP                   TIMESTAMP_NTZ(9)
);


