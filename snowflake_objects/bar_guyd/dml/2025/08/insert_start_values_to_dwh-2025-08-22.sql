INSERT INTO DWH.SPECIAL_TYPE (
    NAME, 
    RECORD_CREATE_DT,
    CREATE_AUDIT_ID,
    RECORD_UPDATE_DT,
    UPDATE_AUDIT_ID
) VALUES 
    ('$ Amount', CURRENT_TIMESTAMP(), -1, CURRENT_TIMESTAMP(), -1),
    ('% Discount', CURRENT_TIMESTAMP(), -1, CURRENT_TIMESTAMP(), -1),
    ('$ Discount', CURRENT_TIMESTAMP(), -1, CURRENT_TIMESTAMP(), -1),
    ('Unlimited', CURRENT_TIMESTAMP(), -1, CURRENT_TIMESTAMP(), -1)
;


INSERT INTO DWH.ITEM_CLASSIFICATION (
    NAME, 
    RECORD_CREATE_DT,
    CREATE_AUDIT_ID,
    RECORD_UPDATE_DT,
    UPDATE_AUDIT_ID
) VALUES 
    ('Food', CURRENT_TIMESTAMP(), -1, CURRENT_TIMESTAMP(), -1),
    ('Drink', CURRENT_TIMESTAMP(), -1, CURRENT_TIMESTAMP(), -1)
;


INSERT INTO DWH.BUSINESS (
    NAME, 
    VIBE_TYPE, 
    SECTOR, 
    ADDRESS_1, 
    CITY,
    STATE, 
    STATE_CD, 
    ZIP_CD, 
    LATITUDE,
    LONGITUDE,
    TIME_ZONE,
    WEBSITE,
    PHONE_NUM,
    EMAIL,


    IS_ACTIVE,
    RECORD_CREATE_DT,
    CREATE_AUDIT_ID,
    RECORD_UPDATE_DT,
    UPDATE_AUDIT_ID

)
VALUES
($$The Shannon$$, $$Pub$$, $$Lower$$, $$106 First St$$, $$Hoboken$$, $$New Jersey$$, $$NJ$$, $$07030$$, 40.737636, -74.031347, $$US- East$$, $$https://theshannonhoboken.com$$, $$(201)-566-7229$$, $$theshannonhoboken@gmail.com$$, TRUE, CURRENT_TIMESTAMP(), -1, CURRENT_TIMESTAMP(), -1),
($$Green Rock Tap & Grill$$, $$Pub$$, $$Lower$$, $$70 Hudson St$$, $$Hoboken$$, $$New Jersey$$, $$NJ$$, $$07030$$, 40.736141, -74.030314, $$US- East$$, $$https://www.greenrockhoboken.com$$, NULL, $$greenrockhoboken@gmail.com$$, TRUE, CURRENT_TIMESTAMP(), -1, CURRENT_TIMESTAMP(), -1),
($$The Pig and Parrot$$, $$Pub$$, $$Lower$$, $$77 Hudson St$$, $$Hoboken$$, $$New Jersey$$, $$NJ$$, $$07030$$, 40.736082, -74.029886, $$US- East$$, $$https://www.thepigandparrot.com$$, $$(201) 683-7369$$, NULL, TRUE, CURRENT_TIMESTAMP(), -1, CURRENT_TIMESTAMP(), -1),
($$Texas Arizona$$, $$Sports Bar$$, $$Lower$$, $$76 River St$$, $$Hoboken$$, $$New Jersey$$, $$NJ$$, $$07030$$, 40.736042, -74.029321, $$US- East$$, $$https://www.texasarizona.com$$, $$(201)-386-5600$$, NULL, TRUE, CURRENT_TIMESTAMP(), -1, CURRENT_TIMESTAMP(), -1),
($$River Street Garage$$, $$Bar$$, $$Lower$$, $$80 River St$$, $$Hoboken$$, $$New Jersey$$, $$NJ$$, $$07030$$, 40.736239, -74.029267, $$US- East$$, $$https://www.riverstreetgarage.com$$, $$(201)-420-7070$$, NULL, TRUE, CURRENT_TIMESTAMP(), -1, CURRENT_TIMESTAMP(), -1),
($$McSwiggans Pub$$, $$Pub$$, $$Lower$$, $$110 First St$$, $$Hoboken$$, $$New Jersey$$, $$NJ$$, $$07030$$, 40.7377, -74.031561, $$US- East$$, NULL, NULL, NULL, TRUE, CURRENT_TIMESTAMP(), -1, CURRENT_TIMESTAMP(), -1),
($$Mike's Wild Moose Saloon$$, $$Sports Bar$$, $$Lower$$, $$30 Newark St$$, $$Hoboken$$, $$New Jersey$$, $$NJ$$, $$07030$$, 40.736682, -74.029128, $$US- East$$, $$https://www.wildmoosesaloon.com$$, $$(201)-744-5117$$, $$info@wildmoosesaloon.com$$, TRUE, CURRENT_TIMESTAMP(), -1, CURRENT_TIMESTAMP(), -1),
($$The Ferryman Hoboken$$, $$Pub$$, $$Lower$$, $$94 Bloomfield St$$, $$Hoboken$$, $$New Jersey$$, $$NJ$$, $$07030$$, 40.737529, -74.031893, $$US- East$$, $$https://www.theferrymanhoboken.com$$, $$(201)-420-9222$$, $$theferrymanon1st@gmail.com$$, TRUE, CURRENT_TIMESTAMP(), -1, CURRENT_TIMESTAMP(), -1),
($$Mulligans Pub$$, $$Pub$$, $$Lower$$, $$159 First St$$, $$Hoboken$$, $$New Jersey$$, $$NJ$$, $$07030$$, 40.737613, -74.03226, $$US- East$$, $$https://mulligansonfirst.net$$, $$(201)-876-4101$$, $$paul@mulligansonfirst.net$$, TRUE, CURRENT_TIMESTAMP(), -1, CURRENT_TIMESTAMP(), -1),
($$Dear Maud$$, $$Cocktail$$, $$Lower$$, $$205 First St$$, $$Hoboken$$, $$New Jersey$$, $$NJ$$, $$07030$$, 40.737721, -74.033051, $$US- East$$, $$https://www.dearmaud.com$$, $$(201)-792-2337$$, $$info@dearmaud.com$$, TRUE, CURRENT_TIMESTAMP(), -1, CURRENT_TIMESTAMP(), -1),
($$Wicked Wolf Tavern$$, $$Sports Bar$$, $$Lower$$, $$120 Frank Sinatra Dr$$, $$Hoboken$$, $$New Jersey$$, $$NJ$$, $$07030$$, 40.738026, -74.027651, $$US- East$$, $$https://www.wickedwolftavern.com$$, $$(201)-659-7500$$, NULL, TRUE, CURRENT_TIMESTAMP(), -1, CURRENT_TIMESTAMP(), -1),
($$Madd Hatter Bar & Restaurant$$, $$Sports Bar$$, $$Lower$$, $$221 Washington St$$, $$Hoboken$$, $$New Jersey$$, $$NJ$$, $$07030$$, 40.739369, -74.030126, $$US- East$$, $$https://maddhatternj.com$$, $$(201)-850-1281$$, $$maddhatterhoboken@gmail.com$$, TRUE, CURRENT_TIMESTAMP(), -1, CURRENT_TIMESTAMP(), -1),
($$Psycho Mikes$$, $$Bar$$, $$Lower$$, $$125 Washington St$$, $$Hoboken$$, $$New Jersey$$, $$NJ$$, $$07030$$, 40.738235, -74.030451, $$US- East$$, $$https://www.psychomikestherapybar.com$$, $$(201)-792-1900$$, $$macgroup56@gmail.com$$, TRUE, CURRENT_TIMESTAMP(), -1, CURRENT_TIMESTAMP(), -1),
($$Black Bear Bar & Grill$$, $$Bar & Grill$$, $$Lower$$, $$205 Washington St$$, $$Hoboken$$, $$New Jersey$$, $$NJ$$, $$07030$$, 40.738957, -74.030212, $$US- East$$, $$https://blackbearhoboken.com$$, $$(201)-656-5511$$, $$info@blackbearhoboken.com$$, TRUE, CURRENT_TIMESTAMP(), -1, CURRENT_TIMESTAMP(), -1),
($$Louise & Jerry's$$, $$Pub$$, $$Lower$$, $$329 Washington St$$, $$Hoboken$$, $$New Jersey$$, $$NJ$$, $$07030$$, 40.740911, -74.02965, $$US- East$$, $$https://www.louiseandjerrys.com$$, $$(201)-656-9698$$, $$louiseandjerrystavern@gmail.com$$, TRUE, CURRENT_TIMESTAMP(), -1, CURRENT_TIMESTAMP(), -1),
($$Cork City$$, $$Pub$$, $$Lower$$, $$239 Bloomfield St$$, $$Hoboken$$, $$New Jersey$$, $$NJ$$, $$07030$$, 40.740013, -74.030834, $$US- East$$, NULL, NULL, NULL, TRUE, CURRENT_TIMESTAMP(), -1, CURRENT_TIMESTAMP(), -1),
($$8th Street Tavern$$, $$Bar & Grill$$, $$Mid$$, $$800 Washington St$$, $$Hoboken$$, $$New Jersey$$, $$NJ$$, $$07030$$, 40.746152, -74.028446, $$US- East$$, $$https://new8thstreet.com$$, NULL, NULL, TRUE, CURRENT_TIMESTAMP(), -1, CURRENT_TIMESTAMP(), -1),
($$10th & Willow Bar & Grill$$, $$Bar & Grill$$, $$Inner$$, $$935 Willow Ave$$, $$Hoboken$$, $$New Jersey$$, $$NJ$$, $$07030$$, 40.749082, -74.03103, $$US- East$$, $$https://10thandwillow.com$$, $$(201)-653-2358$$, $$tenthandwillow@gmail.com$$, TRUE, CURRENT_TIMESTAMP(), -1, CURRENT_TIMESTAMP(), -1),
($$Willie McBrides$$, $$Pub$$, $$Inner$$, $$616 Grand 6th St$$, $$Hoboken$$, $$New Jersey$$, $$NJ$$, $$07030$$, 40.745066, -74.034483, $$US- East$$, $$https://hoboken.williemcbrides.com$$, $$(201) 610-1522$$, $$willies999@aol.com$$, TRUE, CURRENT_TIMESTAMP(), -1, CURRENT_TIMESTAMP(), -1),
($$The Madison Bar & Grill$$, $$Bar & Grill$$, $$Upper$$, $$1316 Washington St$$, $$Hoboken$$, $$New Jersey$$, $$NJ$$, $$07030$$, 40.753115, -74.026369, $$US- East$$, $$https://madisonbarandgrill.com$$, $$(201)-386-0300$$, $$management@madisonbarandgrill.com$$, TRUE, CURRENT_TIMESTAMP(), -1, CURRENT_TIMESTAMP(), -1)
;


CREATE OR REPLACE TEMP TABLE T 
(
    CLASSIFICATION VARCHAR(255),
    NAME VARCHAR(255),
    ITEM_TAG_ARRAY VARCHAR
);

INSERT INTO T 
VALUES 
($$Food$$, $$Wings$$, NULL),
($$Food$$, $$Tacos$$, NULL),
($$Food$$, $$Flatbread$$, NULL),
($$Food$$, $$Appetizers$$, $$Broad Deal$$),
($$Food$$, $$Pizza$$, NULL),
($$Food$$, $$French Fries$$, NULL),
($$Drink$$, $$Bud Light$$, $$Beer, Light Beer, Domestic Beer$$),
($$Drink$$, $$Miller Lite$$, $$Beer, Light Beer, Domestic Beer$$),
($$Drink$$, $$Coors Light$$, $$Beer, Light Beer, Domestic Beer$$),
($$Drink$$, $$Corona Light$$, $$Beer, Light Beer, Domestic Beer$$),
($$Drink$$, $$Busch Light$$, $$Beer, Light Beer, Domestic Beer$$),
($$Drink$$, $$Stella Artois$$, $$Beer$$),
($$Drink$$, $$Kona Big Wave$$, $$Beer$$),
($$Drink$$, $$Modelo$$, $$Beer$$),
($$Drink$$, $$Glass of Wine$$, $$Wine, Broad Deal$$),
($$Drink$$, $$Los Sundays Seltzer$$, $$Seltzer$$),
($$Drink$$, $$Margarita$$, $$Cocktail$$),
($$Drink$$, $$Jager Bomb$$, $$Cocktail$$),
($$Drink$$, $$Mixed Drinks$$, $$Broad Deal$$),
($$Drink$$, $$Draft Beers$$, $$Beer, Broad Deal$$),
($$Drink$$, $$Beer Pitcher$$, $$Beer, Broad Deal$$),
($$Drink$$, $$Beer Bucket$$, $$Beer, Broad Deal$$),
($$Drink$$, $$Green Tea Shot$$, $$Liquor, Shot$$),
($$Drink$$, $$Espresso Martini$$, $$Cocktail$$),
($$Drink$$, $$White Claw Bucket$$, $$Seltzer$$),
($$Drink$$, $$White Claw$$, $$Seltzer$$),
($$Drink$$, $$Mimosa$$, $$Cocktail$$),
($$Drink$$, $$Bloody Mary$$, $$Cocktail$$),
($$Drink$$, $$Martini$$, $$Cocktail$$)
;

INSERT INTO DWH.ITEM (
    ITEM_CLASSIFICATION_ID,
    NAME, 
    IS_CUSTOM, 
    ITEM_TAG_ARRAY,
    RECORD_CREATE_DT,
    CREATE_AUDIT_ID,
    RECORD_UPDATE_DT,
    UPDATE_AUDIT_ID
)
SELECT 
    CASE WHEN CLASSIFICATION = 'Food' THEN 1
         WHEN CLASSIFICATION = 'Drink' THEN 2
         ELSE NULL END AS ITEM_CLASSIFICATION_ID,
    NAME,
    FALSE AS IS_CUSTOM,
    CASE WHEN ITEM_TAG_ARRAY IS NULL THEN NULL 
        ELSE SPLIT(ITEM_TAG_ARRAY, ', ')::ARRAY END AS ITEM_TAG_ARRAY,

    CURRENT_TIMESTAMP() AS RECORD_CREATE_DT,
    -1 AS CREATE_AUDIT_ID,
    CURRENT_TIMESTAMP() AS RECORD_UPDATE_DT,
    -1 AS UPDATE_AUDIT_ID
FROM T
;



