CREATE TABLE IF NOT EXISTS BOOKS.CONFIG.KENP_RATES (
    MONTH_BEGIN_DATE DATE NOT NULL,
    COUNTRY VARCHAR,
    KENP_RATE FLOAT,
    IS_FINAL BOOLEAN NOT NULL
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
('2026-04-01', 'United States', 0.0046937, FALSE),
('2026-04-01', 'United Kingdom', 0.0031749, FALSE),
('2026-04-01', 'Canada', 0.0039157, FALSE),
('2026-04-01', 'Australia', 0.0038345, FALSE),
('2026-04-01', 'Germany', 0.0029182, FALSE),
('2026-04-01', 'India', 0.0793337, FALSE),
('2026-04-01', 'Spain', 0.0038919, FALSE),
('2026-04-01', 'Brazil', 0.0099408, FALSE),
('2026-04-01', 'Japan', 0.419051, FALSE),
('2026-04-01', 'France', 0.0033144, FALSE),
('2026-04-01', 'Italy', 0.0031374, FALSE),
('2026-04-01', 'Netherlands', NULL, FALSE), --- IGNORE ---
('2026-04-01', 'Mexico', 0.102804, FALSE),

-- May 2026
('2026-05-01', 'United States', 0.0046937, FALSE),
('2026-05-01', 'United Kingdom', 0.0031749, FALSE),
('2026-05-01', 'Canada', 0.0039157, FALSE),
('2026-05-01', 'Australia', 0.0038345, FALSE),
('2026-05-01', 'Germany', 0.0029182, FALSE),
('2026-05-01', 'India', 0.0793337, FALSE),
('2026-05-01', 'Spain', 0.0038919, FALSE),
('2026-05-01', 'Brazil', 0.0099408, FALSE),
('2026-05-01', 'Japan', 0.419051, FALSE),
('2026-05-01', 'France', 0.0033144, FALSE),
('2026-05-01', 'Italy', 0.0031374, FALSE),
('2026-05-01', 'Netherlands', NULL, FALSE), --- IGNORE ---
('2026-05-01', 'Mexico', 0.102804, FALSE)
;