CREATE FILE FORMAT IF NOT EXISTS BOOKS.STG.CSV_SKIP_HEADER_QUOTES
TYPE = 'CSV'
FIELD_DELIMITER = ','
RECORD_DELIMITER = '\n'
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
SKIP_HEADER= 1
;