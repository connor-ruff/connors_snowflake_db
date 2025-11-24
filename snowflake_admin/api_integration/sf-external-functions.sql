USE ROLE ACCOUNTADMIN;


CREATE OR REPLACE API INTEGRATION AWS_LAMBDA_EXTERNAL_FUNCTIONS
  api_provider = aws_api_gateway
  api_aws_role_arn = 'arn:aws:iam::676058464455:role/sf-external-function-role'
  api_allowed_prefixes = ('https://1dmrv6cveg.execute-api.us-east-2.amazonaws.com/prod/lambda-zeta-ball-refresh')
  enabled = true;

DESC INTEGRATION AWS_LAMBDA_EXTERNAL_FUNCTIONS;
-- API_AWS_IAM_USER_ARN: arn:aws:iam::735350675005:user/4cw40000-s
-- API_AWS_EXTERNAL_ID: EI90710_SFCRole=2_WUwYWI+8yBA0hoSuomsElk7Adpo=

GRANT USAGE ON INTEGRATION AWS_LAMBDA_EXTERNAL_FUNCTIONS TO ROLE ZETA_BALL_ADMIN;