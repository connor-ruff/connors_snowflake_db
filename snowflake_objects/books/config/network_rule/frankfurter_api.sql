-- Egress rule allowing Snowflake Python UDFs/procs to reach the Frankfurter FX API.
CREATE NETWORK RULE IF NOT EXISTS BOOKS.CONFIG.FRANKFURTER_API_RULE
    MODE = EGRESS
    TYPE = HOST_PORT
    VALUE_LIST = ('api.frankfurter.dev:443');
