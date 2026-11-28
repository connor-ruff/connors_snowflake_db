-- Fetches FX rates from frankfurter.dev for a date (or 'latest') and returns an OBJECT of
-- { currency_code : USD per 1 unit of that currency }, i.e. the same convention as CONFIG.FOREX_RATES.EXCHANGE_RATE.
--   * The API (base=USD) returns units of foreign currency per 1 USD, so each rate is inverted (1 / rate) here.
--   * Frankfurter returns the nearest prior business day's rates if the date is a weekend/holiday.
--   * Requires BOOKS.CONFIG.FRANKFURTER_API_RULE and FRANKFURTER_ACCESS_INTEGRATION.
--
-- Usage:
--   SELECT BOOKS.CONFIG.FETCH_FOREX('2026-11-26', ARRAY_CONSTRUCT('GBP','EUR'));
--   -- {"EUR": 1.15..., "GBP": 1.31...}
--   SELECT f.key AS SOURCE_CURRENCY, f.value::FLOAT AS EXCHANGE_RATE
--   FROM TABLE(FLATTEN(BOOKS.CONFIG.FETCH_FOREX('2026-10-31', ARRAY_CONSTRUCT('GBP','EUR')))) f;
CREATE OR REPLACE FUNCTION BOOKS.CONFIG.FETCH_FOREX(DATE_INPUT VARCHAR, CURRENCY_LIST ARRAY)
RETURNS OBJECT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('requests')
HANDLER = 'fetch_forex'
EXTERNAL_ACCESS_INTEGRATIONS = (FRANKFURTER_ACCESS_INTEGRATION)
AS
$$
import requests

API = "https://api.frankfurter.dev/v1/{d}?base=USD&symbols={s}"

def fetch_forex(date_input, currency_list):
    symbols = ",".join(currency_list)
    r = requests.get(API.format(d=date_input, s=symbols), timeout=30)
    r.raise_for_status()
    resp = r.json()
    return {c: 1.0 / resp["rates"][c] for c in currency_list}
$$;
