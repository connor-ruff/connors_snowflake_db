CREATE TABLE IF NOT EXISTS CONNOR.BIRD.YEAR_LISTS (
	YEAR NUMBER(38,0),
	SPECIES_CODE VARCHAR(31),
	SPECIES VARCHAR(255),
	FIRST_SIGHT_DATE DATE,
	FIRST_SIGHT_CITY VARCHAR(127),
	FIRST_SIGHT_STATE VARCHAR(3),
	FIRST_SIGHT_DETAILS VARCHAR(2047),
	WAS_LIFE_BIRD NUMBER(38,0)
);