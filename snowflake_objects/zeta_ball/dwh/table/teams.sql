CREATE TABLE IF NOT EXISTS DWH.TEAMS(
    TEAM_SID NUMBER(38,0) UNIQUE NOT NULL DEFAULT DWH.TEAM_SEQ.NEXTVAL,
    LEAGUE_START_YEAR NUMBER(38,0) NOT NULL,
    LEAGUE_MEMBER_SID NUMBER(38,0) NOT NULL,
    YAHOO_TEAM_ID NUMBER(38,0) NOT NULL,
    YAHOO_TEAM_NAME VARCHAR,  -- Set manually initially; to be updated regularly with refreshed data from Yahoo
    LAST_UPDATE_DT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(9),
    FIRST_LOAD_DT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(9)
);

TRUNCATE TABLE DWH.TEAMS; 

INSERT INTO DWH.TEAMS (LEAGUE_START_YEAR, LEAGUE_MEMBER_SID, YAHOO_TEAM_ID)
VALUES 
(2025, 1, 1), -- Dylan
(2025, 2, 2), -- Quan
(2025, 3, 3), -- Walter
(2025, 4, 4), -- Danny
(2025, 5, 5), -- Jordan
(2025, 6, 6), -- Tommy
(2025, 7, 7), -- Glenn
(2025, 8, 8), -- Luke
(2025, 9, 9), -- Jackson
(2025, 10, 10), -- Zach
(2025, 11, 11), -- Conor
(2025, 12, 12), -- Charles
(2025, 13, 13), -- Connor
(2025, 14, 14), -- Chris
(2025, 15, 15), -- Cooper
(2025, 16, 16), -- Michael
(2025, 17, 17), -- Riley
(2025, 18, 18), -- Matt

-- 2026 yahoo team IDs are the same as 2025
(2026, 1, 1), -- Dylan
(2026, 2, 2), -- Quan
(2026, 3, 3), -- Walter
(2026, 4, 4), -- Danny
(2026, 5, 5), -- Jordan
(2026, 6, 6), -- Tommy
(2026, 7, 7), -- Glenn
(2026, 8, 8), -- Luke
(2026, 9, 9), -- Jackson
(2026, 10, 10), -- Zach
(2026, 11, 11), -- Conor
(2026, 12, 12), -- Charles
(2026, 13, 13), -- Connor
(2026, 14, 14), -- Chris
(2026, 15, 15), -- Cooper
(2026, 16, 16), -- Michael
(2026, 17, 17), -- Riley
(2026, 18, 18) -- Matt
;




