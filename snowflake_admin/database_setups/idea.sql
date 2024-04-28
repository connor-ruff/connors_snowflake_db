--- CREATE NEW ROLE 
USE ROLE USERADMIN;
CREATE ROLE IDEA_ADMIN;
GRANT ROLE IDEA_ADMIN TO ROLE SYSADMIN;

-- CREATE NEW DATABASE, AND GIVE OWNERSHIP TO IDEA_ADMIN
USE ROLE SYSADMIN;
CREATE DATABASE IDEA;
GRANT OWNERSHIP ON DATABASE IDEA TO ROLE IDEA_ADMIN;
GRANT USAGE ON DATABASE IDEA TO ROLE IDEA_ADMIN;

-- CREATE SCHEMA WITH NEW ADMIN ROLE
USE ROLE IDEA_ADMIN;
CREATE SCHEMA IDEA.CORE;
SHOW GRANTS ON SCHEMA CORE;

-- CREATE A DEVELOPER ROLE
USE ROLE USERADMIN;
CREATE ROLE IDEA_DEV;
GRANT ROLE IDEA_DEV TO ROLE SYSADMIN;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE IDEA_DEV;

DROP USER LAURA;
-- CREATE ROLE FOR LAURA
CREATE USER IF NOT EXISTS LAURA
LOGIN_NAME = 'laurapesanello'
EMAIL = 'laurapesanello@gmail.com'
PASSWORD = 'tmp'
MUST_CHANGE_PASSWORD = TRUE
DEFAULT_ROLE = 'IDEA_DEV';

GRANT ROLE IDEA_DEV TO USER LAURA;

----


