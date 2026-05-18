CREATE TABLE IF NOT EXISTS BOOKS.CONFIG.MARKETPLACES (
    AMAZON_NAME VARCHAR NOT NULL,
    COUNTRY VARCHAR NOT NULL,
    CURRENCY VARCHAR NOT NULL
);

TRUNCATE TABLE BOOKS.CONFIG.MARKETPLACES;


INSERT INTO BOOKS.CONFIG.MARKETPLACES (AMAZON_NAME, COUNTRY, CURRENCY)
VALUES 
('Amazon.se', 'Sweden', 'SEK'),
('Amazon.co.uk', 'United Kingdom', 'GBP'),
('Amazon.ca', 'Canada', 'CAD'),
('Amazon.com', 'United States', 'USD'),
('Amazon.com.au', 'Australia', 'AUD'),
('Amazon.fr', 'France', 'EUR'),
('Amazon.it', 'Italy', 'EUR'),
('Amazon.de', 'Germany', 'EUR'),
('Amazon.es', 'Spain', 'EUR'),
('Amazon.co.jp', 'Japan', 'JPY'),
('Amazon.com.be', 'Belgium', 'EUR'),
('Amazon.nl', 'Netherlands', 'EUR'),
('Amazon.com.mx', 'Mexico', 'MXN'),
('Amazon.ie', 'Ireland', 'EUR'),
('Amazon.in', 'India', 'INR'),
('Amazon.com.br', 'Brazil', 'BRL')
;

