CREATE TABLE IF NOT EXISTS STKZ.MOOLA.AUDIT (
	AUDIT_SID NUMBER(38,0),
	START_DT TIMESTAMP_NTZ(9),
	SUCCESS_FLAG BOOLEAN,
	END_DT TIMESTAMP_NTZ(9)
);