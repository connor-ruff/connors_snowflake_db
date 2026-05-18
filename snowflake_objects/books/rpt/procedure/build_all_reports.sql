CREATE OR REPLACE PROCEDURE BOOKS.RPT.BUILD_ALL_REPORTS()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    qc_cs_book_versions_fail   EXCEPTION (-20001, 'QC FAILED: COMBINED_SALES has record(s) with no matching BOOK_VERSIONS entry');
    qc_cs_forex_fail           EXCEPTION (-20002, 'QC FAILED: COMBINED_SALES has record(s) with no matching FOREX_RATES entry');
    qc_kenp_book_versions_fail EXCEPTION (-20003, 'QC FAILED: KENP has record(s) with no matching BOOK_VERSIONS entry');
    qc_kenp_marketplaces_fail  EXCEPTION (-20004, 'QC FAILED: KENP has record(s) with no matching MARKETPLACES entry');
    qc_kenp_kenp_rates_fail    EXCEPTION (-20005, 'QC FAILED: KENP has record(s) with no matching KENP_RATES entry');
    qc_kenp_forex_fail         EXCEPTION (-20006, 'QC FAILED: KENP has record(s) with no matching FOREX_RATES entry');
    v_qc_cs_book_versions      INT;
    v_qc_cs_forex              INT;
    v_qc_kenp_book_versions    INT;
    v_qc_kenp_marketplaces     INT;
    v_qc_kenp_kenp_rates       INT;
    v_qc_kenp_forex            INT;
    v_inserted                 INT;
BEGIN

    -- QC: COMBINED_SALES
    SELECT COUNT(*)
    INTO   :v_qc_cs_book_versions
    FROM   BOOKS.KDP.COMBINED_SALES CS
    WHERE  NOT EXISTS (
        SELECT 1
        FROM   BOOKS.CONFIG.BOOK_VERSIONS BV
        WHERE  BV.BOOK_VERSION_UNIQUE_ID = CS.ASIN_ISBN
    );

    IF (:v_qc_cs_book_versions > 0) THEN
        RAISE qc_cs_book_versions_fail;
    END IF;

    SELECT COUNT(*)
    INTO   :v_qc_cs_forex
    FROM   BOOKS.KDP.COMBINED_SALES CS
    WHERE  NOT EXISTS (
        SELECT 1
        FROM   BOOKS.CONFIG.FOREX_RATES FOREX
        WHERE  FOREX.SOURCE_CURRENCY  = CS.CURRENCY
          AND  FOREX.MONTH_BEGIN_DATE = DATE_TRUNC('MONTH', CS.ROYALTY_DATE)
    );

    IF (:v_qc_cs_forex > 0) THEN
        RAISE qc_cs_forex_fail;
    END IF;

    -- QC: KENP
    SELECT COUNT(*)
    INTO   :v_qc_kenp_book_versions
    FROM   BOOKS.KDP.KENP K
    WHERE  NOT EXISTS (
        SELECT 1
        FROM   BOOKS.CONFIG.BOOK_VERSIONS BV
        WHERE  BV.BOOK_VERSION_UNIQUE_ID = K.EBOOK_ASIN
    );

    IF (:v_qc_kenp_book_versions > 0) THEN
        RAISE qc_kenp_book_versions_fail;
    END IF;

    SELECT COUNT(*)
    INTO   :v_qc_kenp_marketplaces
    FROM   BOOKS.KDP.KENP K
    WHERE  NOT EXISTS (
        SELECT 1
        FROM   BOOKS.CONFIG.MARKETPLACES M
        WHERE  M.AMAZON_NAME = K.MARKETPLACE
    );

    IF (:v_qc_kenp_marketplaces > 0) THEN
        RAISE qc_kenp_marketplaces_fail;
    END IF;

    SELECT COUNT(*)
    INTO   :v_qc_kenp_kenp_rates
    FROM   BOOKS.KDP.KENP K
    INNER JOIN BOOKS.CONFIG.MARKETPLACES M ON M.AMAZON_NAME = K.MARKETPLACE
    WHERE  NOT EXISTS (
        SELECT 1
        FROM   BOOKS.CONFIG.KENP_RATES KR
        WHERE  KR.COUNTRY          = M.COUNTRY
          AND  KR.MONTH_BEGIN_DATE = DATE_TRUNC('MONTH', K.KENP_DATE)
    );

    IF (:v_qc_kenp_kenp_rates > 0) THEN
        RAISE qc_kenp_kenp_rates_fail;
    END IF;

    SELECT COUNT(*)
    INTO   :v_qc_kenp_forex
    FROM   BOOKS.KDP.KENP K
    INNER JOIN BOOKS.CONFIG.MARKETPLACES M ON M.AMAZON_NAME = K.MARKETPLACE
    WHERE  NOT EXISTS (
        SELECT 1
        FROM   BOOKS.CONFIG.FOREX_RATES FR
        WHERE  FR.SOURCE_CURRENCY  = M.CURRENCY
          AND  FR.MONTH_BEGIN_DATE = DATE_TRUNC('MONTH', K.KENP_DATE)
    );

    IF (:v_qc_kenp_forex > 0) THEN
        RAISE qc_kenp_forex_fail;
    END IF;

    TRUNCATE TABLE BOOKS.RPT.DAILY_SALES_REPORT;

    INSERT INTO BOOKS.RPT.DAILY_SALES_REPORT (
        BOOK_SID,
        BOOK_VERSION_SID,
        BOOK_TITLE,
        SALE_SOURCE,
        VERSION_TYPE,
        VERSION_LANGUAGE,
        MARKETPLACE,
        ROYALTY_DATE,
        ROYALTY_IN_USD,
        NET_UNITS_SOLD
    )
    SELECT
        BV.BOOK_SID,
        BV.BOOK_VERSION_SID,
        NVL(B.TITLE, 'UNKNOWN')                                    AS TITLE,
        'KDP'                                                        AS SALE_SOURCE,
        BV.VERSION_TYPE,
        BV.VERSION_LANGUAGE,
        CS.MARKETPLACE,
        CS.ROYALTY_DATE,
        SUM(FOREX.EXCHANGE_RATE * CS.ROYALTY)                      AS ROYALTY_IN_USD,
        SUM(NVL(CS.UNITS_SOLD, 0) - NVL(CS.UNITS_REFUNDED, 0))    AS NET_UNITS_SOLD
    FROM       BOOKS.KDP.COMBINED_SALES CS
    LEFT JOIN  BOOKS.CONFIG.BOOK_VERSIONS BV  ON  BV.BOOK_VERSION_UNIQUE_ID = CS.ASIN_ISBN
    LEFT JOIN  BOOKS.CONFIG.FOREX_RATES FOREX  ON  FOREX.SOURCE_CURRENCY = CS.CURRENCY
                                               AND FOREX.MONTH_BEGIN_DATE = DATE_TRUNC('MONTH', CS.ROYALTY_DATE)
    LEFT JOIN  BOOKS.CONFIG.BOOKS B            ON  B.BOOK_SID = BV.BOOK_SID
    GROUP BY ALL;

    v_inserted := SQLROWCOUNT;

    INSERT INTO BOOKS.RPT.DAILY_SALES_REPORT (
        BOOK_SID,
        BOOK_VERSION_SID,
        BOOK_TITLE,
        SALE_SOURCE,
        VERSION_TYPE,
        VERSION_LANGUAGE,
        MARKETPLACE,
        ROYALTY_DATE,
        ROYALTY_IN_USD,
        PAGE_READS
    )
    SELECT
        BV.BOOK_SID,
        BV.BOOK_VERSION_SID,
        B.TITLE                                              AS BOOK_TITLE,
        'KDP - KENP'                                         AS SALE_SOURCE,
        BV.VERSION_TYPE,
        BV.VERSION_LANGUAGE,
        K.MARKETPLACE,
        K.KENP_DATE                                          AS ROYALTY_DATE,
        SUM(K.KENP * KR.KENP_RATE * FR.EXCHANGE_RATE)       AS ROYALTY_IN_USD,
        SUM(K.KENP)                                          AS PAGE_READS
    FROM       BOOKS.KDP.KENP K
    LEFT JOIN  BOOKS.CONFIG.MARKETPLACES M   ON  M.AMAZON_NAME = K.MARKETPLACE
    LEFT JOIN  BOOKS.CONFIG.KENP_RATES KR    ON  KR.COUNTRY = M.COUNTRY
                                             AND KR.MONTH_BEGIN_DATE = DATE_TRUNC('MONTH', K.KENP_DATE)
    LEFT JOIN  BOOKS.CONFIG.BOOK_VERSIONS BV ON  BV.BOOK_VERSION_UNIQUE_ID = K.EBOOK_ASIN
    LEFT JOIN  BOOKS.CONFIG.BOOKS B          ON  B.BOOK_SID = BV.BOOK_SID
    LEFT JOIN  BOOKS.CONFIG.FOREX_RATES FR   ON  FR.SOURCE_CURRENCY = M.CURRENCY
                                             AND FR.MONTH_BEGIN_DATE = DATE_TRUNC('MONTH', K.KENP_DATE)
    GROUP BY ALL;

    v_inserted := v_inserted + SQLROWCOUNT;

    RETURN 'SUCCESS: Inserted ' || :v_inserted::VARCHAR || ' total row(s) into BOOKS.RPT.DAILY_SALES_REPORT.';
END;
$$;
