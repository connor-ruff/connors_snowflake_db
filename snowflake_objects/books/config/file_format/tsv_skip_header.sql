-- Tab-delimited text files (e.g. IngramSpark reports, which are named ".xls" but are really tab-separated text).
-- WINDOWS1252: some files contain characters such as the trademark sign encoded as byte 0x99, which is not valid UTF-8.
-- Plain-ASCII files decode identically under this encoding.
CREATE FILE FORMAT IF NOT EXISTS BOOKS.CONFIG.TSV_SKIP_HEADER
TYPE = 'CSV'
FIELD_DELIMITER = '\t'
RECORD_DELIMITER = '\n'
FIELD_OPTIONALLY_ENCLOSED_BY = NONE
SKIP_HEADER = 1
ENCODING = 'WINDOWS1252'
;
