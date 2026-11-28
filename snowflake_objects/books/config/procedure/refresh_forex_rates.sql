-- Refreshes BOOKS.CONFIG.FOREX_RATES with monthly average rates (USD per 1 unit) from BOOKS.CONFIG.FETCH_FOREX.
--   * Starts at the month after the latest IS_FINAL = TRUE month and runs through the current month.
--   * Each month's rate = average of the daily rates (every calendar day; through today for the current month).
--   * Existing rows for those months (all IS_FINAL = FALSE) are replaced; new months are added.
--   * Months before the current month are written as IS_FINAL = TRUE; the current month as FALSE.
--   * Currencies = the distinct non-USD SOURCE_CURRENCY values already in the table (USD is written as 1.0).
--   * Emails BASIC_EMAIL_INTEGRATION on success and on failure (failure is re-raised so the task run shows as failed).
--   * Rates are pulled into a temp table first, so an API failure leaves FOREX_RATES untouched.
--
-- Usage:
--   CALL BOOKS.CONFIG.REFRESH_FOREX_RATES();
CREATE OR REPLACE PROCEDURE BOOKS.CONFIG.REFRESH_FOREX_RATES()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    v_start DATE;
    v_ccys  ARRAY;
    v_msg   VARCHAR;
BEGIN
    v_start := (SELECT DATEADD(MONTH, 1, MAX(MONTH_BEGIN_DATE)) FROM BOOKS.CONFIG.FOREX_RATES WHERE IS_FINAL);
    v_ccys  := (SELECT ARRAY_AGG(DISTINCT SOURCE_CURRENCY) FROM BOOKS.CONFIG.FOREX_RATES WHERE SOURCE_CURRENCY <> 'USD');

    CREATE OR REPLACE TEMPORARY TABLE TMP_FOREX_NEW AS
    WITH days AS (
        SELECT DATEADD(DAY, ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1, :v_start) AS D
        FROM TABLE(GENERATOR(ROWCOUNT => 4000))
        QUALIFY D <= CURRENT_DATE()
    )
    SELECT DATE_TRUNC('MONTH', D)                                   AS MONTH_BEGIN_DATE,
           f.KEY::VARCHAR                                           AS SOURCE_CURRENCY,
           AVG(f.VALUE::FLOAT)                                      AS EXCHANGE_RATE,
           DATE_TRUNC('MONTH', D) < DATE_TRUNC('MONTH', CURRENT_DATE()) AS IS_FINAL
    FROM days, TABLE(FLATTEN(BOOKS.CONFIG.FETCH_FOREX(TO_VARCHAR(D), :v_ccys))) f
    GROUP BY 1, 2
    UNION ALL
    SELECT DISTINCT DATE_TRUNC('MONTH', D), 'USD', 1.0,
           DATE_TRUNC('MONTH', D) < DATE_TRUNC('MONTH', CURRENT_DATE())
    FROM days;

    BEGIN TRANSACTION;
    DELETE FROM BOOKS.CONFIG.FOREX_RATES WHERE MONTH_BEGIN_DATE >= :v_start;
    INSERT INTO BOOKS.CONFIG.FOREX_RATES (MONTH_BEGIN_DATE, SOURCE_CURRENCY, EXCHANGE_RATE, IS_FINAL)
        SELECT MONTH_BEGIN_DATE, SOURCE_CURRENCY, EXCHANGE_RATE, IS_FINAL FROM TMP_FOREX_NEW;
    COMMIT;

    v_msg := 'FOREX_RATES refreshed from ' || v_start::VARCHAR || ' (' || (SELECT COUNT(*) FROM TMP_FOREX_NEW) || ' rows)';
    CALL SYSTEM$SEND_EMAIL('BASIC_EMAIL_INTEGRATION', 'connorruff99@gmail.com', 'SUCCESS: Forex Rates Refresh', :v_msg);
    RETURN v_msg;

EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;
        v_msg := 'Forex rates refresh failed: ' || SQLERRM;
        CALL SYSTEM$SEND_EMAIL('BASIC_EMAIL_INTEGRATION', 'connorruff99@gmail.com', 'FAILURE: Forex Rates Refresh', :v_msg);
        RAISE;
END;
$$;
