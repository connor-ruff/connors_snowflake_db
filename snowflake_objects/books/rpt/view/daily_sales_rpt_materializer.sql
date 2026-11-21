-- Column order must match BOOKS.RPT.DAILY_SALES_REPORT exactly (loaded with INSERT ... SELECT *)
CREATE OR REPLACE VIEW BOOKS.RPT.DAILY_SALES_RPT_MATERIALIZER AS
SELECT
    BV.BOOK_SID                                                 AS BOOK_SID,
    BV.BOOK_VERSION_SID                                         AS BOOK_VERSION_SID,
    NVL(B.TITLE, 'UNKNOWN')                                     AS BOOK_TITLE,
    'KDP'                                                       AS SALE_SOURCE,
    BV.VERSION_TYPE                                             AS VERSION_TYPE,
    BV.VERSION_LANGUAGE                                         AS VERSION_LANGUAGE,
    CS.MARKETPLACE                                              AS MARKETPLACE,
    CS.ROYALTY_DATE                                             AS ROYALTY_DATE,
    SUM(FOREX.EXCHANGE_RATE * CS.ROYALTY)                       AS ROYALTY_IN_USD,
    SUM(NVL(CS.UNITS_SOLD, 0) - NVL(CS.UNITS_REFUNDED, 0))      AS NET_UNITS_SOLD,
    NULL                                                        AS PAGE_READS,
    CURRENT_TIMESTAMP()                                         AS LAST_MATERIALIZED_DT
FROM       BOOKS.KDP.COMBINED_SALES CS
LEFT JOIN  BOOKS.CONFIG.BOOK_VERSIONS BV  ON  BV.BOOK_VERSION_UNIQUE_ID = CS.ASIN_ISBN
LEFT JOIN  BOOKS.CONFIG.FOREX_RATES FOREX ON  FOREX.SOURCE_CURRENCY  = CS.CURRENCY
                                          AND FOREX.MONTH_BEGIN_DATE = DATE_TRUNC('MONTH', CS.ROYALTY_DATE)
LEFT JOIN  BOOKS.CONFIG.BOOKS B           ON  B.BOOK_SID = BV.BOOK_SID
GROUP BY ALL

UNION ALL

SELECT
    BV.BOOK_SID                                                 AS BOOK_SID,
    BV.BOOK_VERSION_SID                                         AS BOOK_VERSION_SID,
    B.TITLE                                                     AS BOOK_TITLE,
    'KDP - KENP'                                                AS SALE_SOURCE,
    BV.VERSION_TYPE                                             AS VERSION_TYPE,
    BV.VERSION_LANGUAGE                                         AS VERSION_LANGUAGE,
    K.MARKETPLACE                                               AS MARKETPLACE,
    K.KENP_DATE                                                 AS ROYALTY_DATE,
    SUM(K.KENP * KR.KENP_RATE * FR.EXCHANGE_RATE)               AS ROYALTY_IN_USD,
    NULL                                                        AS NET_UNITS_SOLD,
    SUM(K.KENP)                                                 AS PAGE_READS,
    CURRENT_TIMESTAMP()                                         AS LAST_MATERIALIZED_DT
    
FROM       BOOKS.KDP.KENP K
LEFT JOIN  BOOKS.CONFIG.MARKETPLACES M   ON  M.AMAZON_NAME = K.MARKETPLACE
LEFT JOIN  BOOKS.CONFIG.KENP_RATES KR    ON  KR.COUNTRY = M.COUNTRY
                                         AND KR.MONTH_BEGIN_DATE = DATE_TRUNC('MONTH', K.KENP_DATE)
LEFT JOIN  BOOKS.CONFIG.BOOK_VERSIONS BV ON  BV.BOOK_VERSION_UNIQUE_ID = K.EBOOK_ASIN
LEFT JOIN  BOOKS.CONFIG.BOOKS B          ON  B.BOOK_SID = BV.BOOK_SID
LEFT JOIN  BOOKS.CONFIG.FOREX_RATES FR   ON  FR.SOURCE_CURRENCY = M.CURRENCY
                                         AND FR.MONTH_BEGIN_DATE = DATE_TRUNC('MONTH', K.KENP_DATE)
GROUP BY ALL;
