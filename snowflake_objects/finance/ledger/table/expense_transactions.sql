CREATE TABLE IF NOT EXISTS FINANCE.LEDGER.EXPENSE_TRANSACTIONS (
	TRANSACTION_SID NUMBER(38,0),
	TRANSACTION_DATE DATE,
	AMOUNT NUMBER(38,2),
	SELLER_SID NUMBER(38,0),
	SELLER_NAME VARCHAR(16777216),
	ITEM VARCHAR(16777216),
	CATEGORY_1 VARCHAR(16777216),
	CATEGORY_2 VARCHAR(16777216),
	CATEGORY_3 VARCHAR(16777216),
	NOTES VARCHAR(16777216),
	TAGS ARRAY,
	LOAD_DT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
	AUDIT_SID NUMBER(38,0)
);