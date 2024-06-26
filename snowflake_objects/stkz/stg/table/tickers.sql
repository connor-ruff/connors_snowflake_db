CREATE OR REPLACE TABLE STKZ.STG.TICKERS (
	TICKER VARCHAR(25),
	PERIOD VARCHAR(1),
	RECORD_DATE DATE,
	RECORD_TIME VARCHAR(10),
	OPEN_PRICE NUMBER(38,4),
	HIGH_PRICE NUMBER(38,4),
	LOW_PRICE NUMBER(38,4),
	CLOSE_PRICE NUMBER(38,4),
	VOLUME NUMBER(38,10),
	OPENINT VARCHAR(25),
	SECURITY_TYPE VARCHAR(25),
	MARKET VARCHAR(25),
	SOURCE_FILE_NAME VARCHAR(100),
	LOAD_DT TIMESTAMP_NTZ(9)
);