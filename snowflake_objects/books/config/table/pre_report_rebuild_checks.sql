CREATE TABLE IF NOT EXISTS BOOKS.CONFIG.PRE_REPORT_REBUILD_CHECKS (
    CHECK_ID        INT,
    CHECK_NAME      VARCHAR,
    DESCRIPTION     VARCHAR,   -- what the check verifies
    WHY_IMPORTANT   VARCHAR,   -- what goes wrong in the reports if the check fails
    CHECK_SQL       VARCHAR,   -- must return exactly one row with one BOOLEAN column named PASS_TEST (TRUE = pass)
    IS_ACTIVE       BOOLEAN
);

TRUNCATE TABLE BOOKS.CONFIG.PRE_REPORT_REBUILD_CHECKS;

INSERT INTO BOOKS.CONFIG.PRE_REPORT_REBUILD_CHECKS
(CHECK_ID, CHECK_NAME, DESCRIPTION, WHY_IMPORTANT, CHECK_SQL, IS_ACTIVE)
VALUES

(
    1, 'KDP_COMBINED_SALES_BOOK_VERSIONS',
    'Every KDP.COMBINED_SALES row has a matching CONFIG.BOOK_VERSIONS entry (ASIN_ISBN = BOOK_VERSION_UNIQUE_ID).',
    'Unmatched rows would show up in reports with no book, version type or language.',
    $$SELECT COUNT(*) = 0 AS PASS_TEST
FROM BOOKS.KDP.COMBINED_SALES CS
WHERE NOT EXISTS (
    SELECT 1
    FROM BOOKS.CONFIG.BOOK_VERSIONS BV
    WHERE BV.BOOK_VERSION_UNIQUE_ID = CS.ASIN_ISBN
)$$,
    TRUE
),

(
    2, 'KDP_COMBINED_SALES_FOREX_RATES',
    'Every KDP.COMBINED_SALES row has a CONFIG.FOREX_RATES entry for its currency and royalty month.',
    'Without a rate, ROYALTY_IN_USD is NULL and revenue is silently understated.',
    $$SELECT COUNT(*) = 0 AS PASS_TEST
FROM BOOKS.KDP.COMBINED_SALES CS
WHERE NOT EXISTS (
    SELECT 1
    FROM BOOKS.CONFIG.FOREX_RATES FOREX
    WHERE FOREX.SOURCE_CURRENCY  = CS.CURRENCY
      AND FOREX.MONTH_BEGIN_DATE = DATE_TRUNC('MONTH', CS.ROYALTY_DATE)
)$$,
    TRUE
),

(
    3, 'KDP_KENP_BOOK_VERSIONS',
    'Every KDP.KENP row has a matching CONFIG.BOOK_VERSIONS entry (EBOOK_ASIN = BOOK_VERSION_UNIQUE_ID).',
    'Unmatched rows would show up in reports with no book, version type or language.',
    $$SELECT COUNT(*) = 0 AS PASS_TEST
FROM BOOKS.KDP.KENP K
WHERE NOT EXISTS (
    SELECT 1
    FROM BOOKS.CONFIG.BOOK_VERSIONS BV
    WHERE BV.BOOK_VERSION_UNIQUE_ID = K.EBOOK_ASIN
)$$,
    TRUE
),

(
    4, 'KDP_KENP_MARKETPLACES',
    'Every KDP.KENP row has a matching CONFIG.MARKETPLACES entry (MARKETPLACE = AMAZON_NAME).',
    'The marketplace supplies the country and currency used to look up KENP rates and forex rates; without it KENP revenue is NULL.',
    $$SELECT COUNT(*) = 0 AS PASS_TEST
FROM BOOKS.KDP.KENP K
WHERE NOT EXISTS (
    SELECT 1
    FROM BOOKS.CONFIG.MARKETPLACES M
    WHERE M.AMAZON_NAME = K.MARKETPLACE
)$$,
    TRUE
),

(
    5, 'KDP_KENP_KENP_RATES',
    'Every KDP.KENP row has a CONFIG.KENP_RATES entry for its marketplace country and KENP month.',
    'Without a KENP rate, page reads cannot be converted to royalty and KENP revenue is NULL.',
    $$SELECT COUNT(*) = 0 AS PASS_TEST
FROM BOOKS.KDP.KENP K
INNER JOIN BOOKS.CONFIG.MARKETPLACES M ON M.AMAZON_NAME = K.MARKETPLACE
WHERE NOT EXISTS (
    SELECT 1
    FROM BOOKS.CONFIG.KENP_RATES KR
    WHERE KR.COUNTRY          = M.COUNTRY
      AND KR.MONTH_BEGIN_DATE = DATE_TRUNC('MONTH', K.KENP_DATE)
)$$,
    TRUE
),

(
    6, 'KDP_KENP_FOREX_RATES',
    'Every KDP.KENP row has a CONFIG.FOREX_RATES entry for its marketplace currency and KENP month.',
    'Without a rate, KENP ROYALTY_IN_USD is NULL and revenue is silently understated.',
    $$SELECT COUNT(*) = 0 AS PASS_TEST
FROM BOOKS.KDP.KENP K
INNER JOIN BOOKS.CONFIG.MARKETPLACES M ON M.AMAZON_NAME = K.MARKETPLACE
WHERE NOT EXISTS (
    SELECT 1
    FROM BOOKS.CONFIG.FOREX_RATES FR
    WHERE FR.SOURCE_CURRENCY  = M.CURRENCY
      AND FR.MONTH_BEGIN_DATE = DATE_TRUNC('MONTH', K.KENP_DATE)
)$$,
    TRUE
);
