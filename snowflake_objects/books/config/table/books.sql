CREATE TABLE IF NOT EXISTS BOOKS.CONFIG.BOOKS (
    BOOK_SID INT NOT NULL UNIQUE,
    TITLE VARCHAR,
    SERIES VARCHAR,
    SERIES_NO INT,
    IS_SET BOOLEAN
);


TRUNCATE TABLE BOOKS.CONFIG.BOOKS;

INSERT INTO BOOKS.CONFIG.BOOKS
(BOOK_SID, TITLE, SERIES, SERIES_NO, IS_SET)
VALUES


-- 10 Victoria
(1,  'The Numbers Killer', 'Victoria', 1, FALSE),
(2,  'Pretty Little Girls', 'Victoria', 2, FALSE),
(3,  'When They Find Us', 'Victoria', 3, FALSE),
(4,  'Ripple of Doubt', 'Victoria', 4, FALSE),
(5,  'The Groom Went Missing', 'Victoria', 5, FALSE),
(6,  'Vanished on Vacation', 'Victoria', 6, FALSE),
(7,  'The Atonement Murders', 'Victoria', 7, FALSE),
(8,  'The Ones They Buried', 'Victoria', 8, FALSE),
(9,  'The Bad Neighbor', 'Victoria', 9, FALSE),
(10, 'Lies in the Snow', 'Victoria', 10, FALSE),


-- 3 Brooke Walton
(11, 'Everett', 'Brooke', 1, FALSE),
(12, 'Rothaker', 'Brooke', 2, FALSE),
(13, 'The Intern', 'Brooke', 3, FALSE),

-- 3 FBI & CDC Thriller Series
(14, 'Only Wrong Once', 'FBI & CDC', 1, FALSE),
(15, 'Only One Cure', 'FBI & CDC', 2, FALSE),
(16, 'Only One Wave', 'FBI & CDC', 3, FALSE),

-- 3 Standalones
(17, 'When She Escaped', NULL, NULL, FALSE),
(18, 'Lauren''s Secret: Full-Out', NULL, NULL, FALSE),
(19, 'The Last Seat: A Thriller', NULL, NULL, FALSE),

-- Sets
(20, 'Brooke Walton Box Set: Books 1-3', 'Brooke', NULL, TRUE),
(21, 'Agent Victoria Heslin Series: Books 1-3', 'Victoria', NULL, TRUE),
(22, 'FBI & CDC Thriller Series Books 1-3', 'FBI & CDC', NULL, TRUE),
(23, 'Everett Series: Books 1-2', 'Brooke', NULL, TRUE)
;


