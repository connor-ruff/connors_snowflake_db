CREATE TABLE IF NOT EXISTS DWH.TEAMS(
    TEAM_SID NUMBER(38,0) DEFAULT DWH.TEAMS_SEQ.NEXTVAL,
    TEAM_KEY VARCHAR,
    TEAM_NAME VARCHAR,
    MANAGER VARCHAR,
    LOAD_DT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(9)
);

TRUNCATE TABLE DWH.TEAMS; 
INSERT INTO DWH.TEAMS (TEAM_KEY, TEAM_NAME, MANAGER)
VALUES 
($$466.l.6036.t.8$$, $$Jer Bears$$, 'Luke Sisung'),
($$466.l.6036.t.7$$, $$Italian Stallion$$, 'Glenn Fiocca'),
($$466.l.6036.t.14$$, $$Franklin Fleetwood$$, 'Chris Hovard'),
($$466.l.6036.t.2$$, $$1of1$$, 'Daquan Ross'),
($$466.l.6036.t.5$$, $$Foe Nem$$, 'Jordan Smith'),
($$466.l.6036.t.6$$, $$Hibernating Hoopers$$, 'Tommy Crooks'),
($$466.l.6036.t.11$$, $$Neal Down & Sugg My Cock$$, 'Conor Neal'),
($$466.l.6036.t.13$$, $$Slop Slop$$, 'Connor Ruff'),
($$466.l.6036.t.12$$, $$Net Yurishing$$, 'Charles Langenfeld'),
($$466.l.6036.t.1$$, $$Tommy Dating Nikki$$, 'Dylan Anderson'),
($$466.l.6036.t.10$$, $$Midnight Riders$$, 'Zach Holland'),
($$466.l.6036.t.4$$, $$Don Juan Add$$, 'Danny Casteneda'),
($$466.l.6036.t.15$$, $$Locked in$$, 'Cooper Derudder'),
($$466.l.6036.t.9$$, $$Mexican Mamis$$, 'Jackson Dooley'),
($$466.l.6036.t.18$$, $$Thots and prayers$$, 'Matt Heubner'),
($$466.l.6036.t.16$$, $$The Playbook$$, 'Michael McCarthy'),
($$466.l.6036.t.3$$, $$A Tinderella Story$$, 'Walter Robson'),
($$466.l.6036.t.17$$, $$The Slumpy Boyzzz$$, 'Riley Kennedy')
;

SELECT * FROM DWH.TEAMS;