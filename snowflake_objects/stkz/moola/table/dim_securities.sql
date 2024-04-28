CREATE TABLE IF NOT EXISTS STKZ.MOOLA.DIM_SECURITIES (
	TICKER VARCHAR(25),
	COMMON_NAME VARCHAR(200),
	SECURITY_TYPE VARCHAR(35),
	MARKET VARCHAR(25),
	LOAD_DT TIMESTAMP_NTZ(9),
	UPDATED_DT TIMESTAMP_NTZ(9)
);