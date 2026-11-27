CREATE TABLE IF NOT EXISTS BOOKS.CONFIG.KENP_RATES (
    MONTH_BEGIN_DATE DATE NOT NULL,
    COUNTRY VARCHAR,
    KENP_RATE FLOAT,
    IS_FINAL BOOLEAN NOT NULL,
    DP_LOAD_TIMESTAMP TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP()
);


TRUNCATE TABLE BOOKS.CONFIG.KENP_RATES;

INSERT INTO BOOKS.CONFIG.KENP_RATES
(MONTH_BEGIN_DATE, COUNTRY, KENP_RATE, IS_FINAL)
VALUES

-- Jan 2026
('2026-01-01', 'United States', 0.0041765, TRUE),
('2026-01-01', 'United Kingdom', 0.0028477, TRUE),
('2026-01-01', 'Canada', 0.0037036, TRUE),
('2026-01-01', 'Australia', 0.0031966, TRUE),
('2026-01-01', 'Germany', 0.0026471, TRUE),
('2026-01-01', 'India', 0.0742906, TRUE),
('2026-01-01', 'Spain', 0.0032562, TRUE),
('2026-01-01', 'Brazil', 0.0092937, TRUE),
('2026-01-01', 'Japan', 0.366771, TRUE),
('2026-01-01', 'France', 0.0029512, TRUE),
('2026-01-01', 'Italy', 0.0029102, TRUE),
('2026-01-01', 'Netherlands', NULL, FALSE), --- IGNORE ---
('2026-01-01', 'Mexico', 0.0728071, TRUE),

-- Feb 2026
('2026-02-01', 'United States', 0.00458, TRUE),
('2026-02-01', 'United Kingdom', 0.0029959, TRUE),
('2026-02-01', 'Canada', 0.0037805, TRUE),
('2026-02-01', 'Australia', 0.0036809, TRUE),
('2026-02-01', 'Germany', 0.0027964, TRUE),
('2026-02-01', 'India', 0.0806631, TRUE),
('2026-02-01', 'Spain', 0.0035626, TRUE),
('2026-02-01', 'Brazil', 0.0092772, TRUE),
('2026-02-01', 'Japan', 0.398513, TRUE),
('2026-02-01', 'France', 0.0031754, TRUE),
('2026-02-01', 'Italy', 0.0030696, TRUE),
('2026-02-01', 'Netherlands', NULL, FALSE), --- IGNORE ---
('2026-02-01', 'Mexico', 0.0891409, TRUE),

-- March 2026
('2026-03-01', 'United States', 0.0046937, TRUE),
('2026-03-01', 'United Kingdom', 0.0031749, TRUE),
('2026-03-01', 'Canada', 0.0039157, TRUE),
('2026-03-01', 'Australia', 0.0038345, TRUE),
('2026-03-01', 'Germany', 0.0029182, TRUE),
('2026-03-01', 'India', 0.0793337, TRUE),
('2026-03-01', 'Spain', 0.0038919, TRUE),
('2026-03-01', 'Brazil', 0.0099408, TRUE),
('2026-03-01', 'Japan', 0.419051, TRUE),
('2026-03-01', 'France', 0.0033144, TRUE),
('2026-03-01', 'Italy', 0.0031374, TRUE),
('2026-03-01', 'Netherlands', NULL, FALSE), --- IGNORE ---
('2026-03-01', 'Mexico', 0.102804, TRUE),

-- April 2026
('2026-04-01', 'United States', 0.0047667, TRUE),
('2026-04-01', 'United Kingdom', 0.0032, TRUE),
('2026-04-01', 'Canada', 0.004086, TRUE),
('2026-04-01', 'Australia', 0.003857, TRUE),
('2026-04-01', 'Germany', 0.0029238, TRUE),
('2026-04-01', 'India', 0.0724668, TRUE),
('2026-04-01', 'Spain', 0.0037699, TRUE),
('2026-04-01', 'Brazil', 0.0090807, TRUE),
('2026-04-01', 'Japan', 0.408767, TRUE),
('2026-04-01', 'France', 0.003207, TRUE),
('2026-04-01', 'Italy', 0.003235, TRUE),
('2026-04-01', 'Netherlands', NULL, TRUE), --- IGNORE ---
('2026-04-01', 'Mexico', 0.109424, TRUE),

-- May 2026
('2026-05-01', 'United States', 0.0048844, TRUE),
('2026-05-01', 'United Kingdom', 0.0031801, TRUE),
('2026-05-01', 'Canada', 0.0042006, TRUE),
('2026-05-01', 'Australia', 0.0039588, TRUE),
('2026-05-01', 'Germany', 0.0028145, TRUE),
('2026-05-01', 'India', 0.0714756, TRUE),
('2026-05-01', 'Spain', 0.0041609, TRUE),
('2026-05-01', 'Brazil', 0.0094846, TRUE),
('2026-05-01', 'Japan', 0.371228, TRUE),
('2026-05-01', 'France', 0.0031831, TRUE),
('2026-05-01', 'Italy', 0.0033162, TRUE),
('2026-05-01', 'Netherlands', NULL, TRUE), --- IGNORE ---
('2026-05-01', 'Mexico', 0.117994, TRUE),

('2026-06-01', 'United States', 0.0046951, TRUE),
('2026-06-01', 'United Kingdom', 0.0032282, TRUE),
('2026-06-01', 'Canada', 0.0043289, TRUE),
('2026-06-01', 'Australia', 0.0039587, TRUE),
('2026-06-01', 'Germany', 0.0028565, TRUE),
('2026-06-01', 'India', 0.0722053, TRUE),
('2026-06-01', 'Spain', 0.0039928, TRUE),
('2026-06-01', 'Brazil', 0.0098042, TRUE),
('2026-06-01', 'Japan', 0.372447, TRUE),
('2026-06-01', 'France', 0.0033716, TRUE),
('2026-06-01', 'Italy', 0.0031399, TRUE),
('2026-06-01', 'Netherlands', NULL, TRUE),
('2026-06-01', 'Mexico', 0.119378, TRUE),

('2026-07-01', 'United States', 0.0047976, TRUE),
('2026-07-01', 'United Kingdom', 0.0032611, TRUE),
('2026-07-01', 'Canada', 0.0042101, TRUE),
('2026-07-01', 'Australia', 0.0040077, TRUE),
('2026-07-01', 'Germany', 0.0029096, TRUE),
('2026-07-01', 'India', 0.0713342, TRUE),
('2026-07-01', 'Spain', 0.0039585, TRUE),
('2026-07-01', 'Brazil', 0.0091204, TRUE),
('2026-07-01', 'Japan', 0.363709, TRUE),
('2026-07-01', 'France', 0.0032704, TRUE),
('2026-07-01', 'Italy', 0.0029453, TRUE),
('2026-07-01', 'Netherlands', NULL, TRUE),
('2026-07-01', 'Mexico', 0.124126, TRUE),

('2026-08-01', 'United States', 0.0048641, TRUE),
('2026-08-01', 'United Kingdom', 0.0031453, TRUE),
('2026-08-01', 'Canada', 0.0042233, TRUE),
('2026-08-01', 'Australia', 0.0040872, TRUE),
('2026-08-01', 'Germany', 0.0027444, TRUE),
('2026-08-01', 'India', 0.0689586, TRUE),
('2026-08-01', 'Spain', 0.0035105, TRUE),
('2026-08-01', 'Brazil', 0.0095651, TRUE),
('2026-08-01', 'Japan', 0.300412, TRUE),
('2026-08-01', 'France', 0.00305, TRUE),
('2026-08-01', 'Italy', 0.0029101, TRUE),
('2026-08-01', 'Netherlands', NULL, TRUE),
('2026-08-01', 'Mexico', 0.134444, TRUE),


('2026-09-01', 'United States', 0.0053128, TRUE),
('2026-09-01', 'United Kingdom', 0.003527, TRUE),
('2026-09-01', 'Canada', 0.0046666, TRUE),
('2026-09-01', 'Australia', 0.0042509, TRUE),
('2026-09-01', 'Germany', 0.0030127, TRUE),
('2026-09-01', 'India', 0.0731963, TRUE),
('2026-09-01', 'Spain', 0.0037827, TRUE),
('2026-09-01', 'Brazil', 0.0103038, TRUE),
('2026-09-01', 'Japan', 0.326796, TRUE),
('2026-09-01', 'France', 0.0035534, TRUE),
('2026-09-01', 'Italy', 0.003126, TRUE),
('2026-09-01', 'Netherlands', NULL, TRUE),
('2026-09-01', 'Mexico', 0.137183, TRUE),

('2026-10-01', 'United States', 0.0055634, TRUE),
('2026-10-01', 'United Kingdom', 0.0036366, TRUE),
('2026-10-01', 'Canada', 0.0048224, TRUE),
('2026-10-01', 'Australia', 0.0043571, TRUE),
('2026-10-01', 'Germany', 0.0031581, TRUE),
('2026-10-01', 'India', 0.0734217, TRUE),
('2026-10-01', 'Spain', NULL, TRUE),
('2026-10-01', 'Brazil', 0.0106667, TRUE),
('2026-10-01', 'Japan', 0.349953, TRUE),
('2026-10-01', 'France', 0.0036293, TRUE),
('2026-10-01', 'Italy', 0.0033621, TRUE),
('2026-10-01', 'Netherlands', NULL, TRUE),
('2026-10-01', 'Mexico', NULL, TRUE),

('2026-11-01', 'United States', 0.0055634, FALSE),
('2026-11-01', 'United Kingdom', 0.0036366, FALSE),
('2026-11-01', 'Canada', 0.0048224, FALSE),
('2026-11-01', 'Australia', 0.0043571, FALSE),
('2026-11-01', 'Germany', 0.0031581, FALSE),
('2026-11-01', 'India', 0.0734217, FALSE),
('2026-11-01', 'Spain', NULL, FALSE),
('2026-11-01', 'Brazil', 0.0106667, FALSE),
('2026-11-01', 'Japan', 0.349953, FALSE),
('2026-11-01', 'France', 0.0036293, FALSE),
('2026-11-01', 'Italy', 0.0033621, FALSE),
('2026-11-01', 'Netherlands', NULL, FALSE),
('2026-11-01', 'Mexico', NULL, FALSE)

-- https://readerlinks.com/kenp_rates/index.php

;