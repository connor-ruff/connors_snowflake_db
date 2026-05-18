CREATE OR REPLACE PROCEDURE BOOKS.RPT.BUILD_ALL_REPORTS()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    qc_book_versions_fail EXCEPTION (-20001, 'QC FAILED: COMBINED_SALES has record(s) with no matching BOOK_VERSIONS entry');
    qc_forex_fail         EXCEPTION (-20002, 'QC FAILED: COMBINED_SALES has record(s) with no matching FOREX_RATES entry');
    v_qc_book_versions    INT;
    v_qc_forex            INT;
    v_inserted            INT;
BEGIN

    SELECT COUNT(*)
    INTO   :v_qc_book_versions
    FROM   BOOKS.KDP.COMBINED_SALES CS
    WHERE  NOT EXISTS (
        SELECT 1
        FROM   BOOKS.CONFIG.BOOK_VERSIONS BV
        WHERE  BV.BOOK_VERSION_UNIQUE_ID = CS.ASIN_ISBN
    );

    IF (:v_qc_book_versions > 0) THEN
        RAISE qc_book_versions_fail;
    END IF;

    SELECT COUNT(*)
    INTO   :v_qc_forex
    FROM   BOOKS.KDP.COMBINED_SALES CS
    WHERE  NOT EXISTS (
        SELECT 1
        FROM   BOOKS.CONFIG.FOREX_RATES FOREX
        WHERE  FOREX.SOURCE_CURRENCY   = CS.CURRENCY
          AND  FOREX.MONTH_BEGIN_DATE  = DATE_TRUNC('MONTH', CS.ROYALTY_DATE)
    );

    IF (:v_qc_forex > 0) THEN
        RAISE qc_forex_fail;
    END IF;

    TRUNCATE TABLE BOOKS.RPT.DAILY_SALES_REPORT;

    INSERT INTO BOOKS.RPT.DAILY_SALES_REPORT (
        BOOK_SID,
        BOOK_VERSION_SID,
        BOOK_TITLE,
        VERSION_TYPE,
        VERSION_LANGUAGE,
        MARKETPLACE,
        ROYALTY_DATE,
        NET_UNITS_SOLD,
        ROYALTY_IN_USD
    )
    SELECT
        BV.BOOK_SID,
        BV.BOOK_VERSION_SID,
        NVL(B.TITLE, 'UNKNOWN')                                    AS TITLE,
        BV.VERSION_TYPE,
        BV.VERSION_LANGUAGE,
        CS.MARKETPLACE,
        CS.ROYALTY_DATE,
        SUM(NVL(CS.UNITS_SOLD, 0) - NVL(CS.UNITS_REFUNDED, 0))    AS NET_UNITS_SOLD,
        SUM(FOREX.EXCHANGE_RATE * CS.ROYALTY)                      AS ROYALTY_IN_USD
    FROM       BOOKS.KDP.COMBINED_SALES CS
    LEFT JOIN  BOOKS.CONFIG.BOOK_VERSIONS BV  ON  BV.BOOK_VERSION_UNIQUE_ID = CS.ASIN_ISBN
    LEFT JOIN  BOOKS.CONFIG.FOREX_RATES FOREX  ON  FOREX.SOURCE_CURRENCY = CS.CURRENCY
                                               AND FOREX.MONTH_BEGIN_DATE = DATE_TRUNC('MONTH', CS.ROYALTY_DATE)
    LEFT JOIN  BOOKS.CONFIG.BOOKS B            ON  B.BOOK_SID = BV.BOOK_SID
    GROUP BY ALL;

    v_inserted := SQLROWCOUNT;

    RETURN 'SUCCESS: Inserted ' || :v_inserted::VARCHAR || ' row(s) into BOOKS.RPT.DAILY_SALES_REPORT.';
END;
$$;
