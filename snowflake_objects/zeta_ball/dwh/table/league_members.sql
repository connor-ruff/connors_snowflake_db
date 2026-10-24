CREATE TABLE IF NOT EXISTS DWH.LEAGUE_MEMBERS(
    LEAGUE_MEMBER_SID NUMBER(38,0) UNIQUE NOT NULL,
    MANAGER VARCHAR,
    LOAD_DT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(9)
);

TRUNCATE TABLE DWH.LEAGUE_MEMBERS; 

INSERT INTO DWH.LEAGUE_MEMBERS (LEAGUE_MEMBER_SID, MANAGER)
VALUES 
(1, 'Dylan Anderson'),
(2, 'Daquan Ross'),
(3, 'Walter Robson'),
(4, 'Danny Casteneda'),
(5, 'Jordan Smith'),
(6, 'Tommy Crooks'),
(7, 'Glenn Fiocca'),
(8, 'Luke Sisung'),
(9, 'Jackson Dooley'),
(10, 'Zach Holland'),
(11, 'Conor Neal'),
(12, 'Charles Langenfeld'),
(13, 'Connor Ruff'),
(14, 'Chris Hovard'),
(15, 'Cooper Derudder'),
(16, 'Michael McCarthy'),
(17, 'Riley Kennedy'),
(18, 'Matt Heubner')
;
