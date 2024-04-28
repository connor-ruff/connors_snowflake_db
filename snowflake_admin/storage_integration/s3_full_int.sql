CREATE STORAGE INTEGRATION IF NOT EXISTS S3_FULL_INT 
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = 'S3'
ENABLED = TRUE 
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::676058464455:role/s3-read-access-for-aws-accounts'
STORAGE_ALLOWED_LOCATIONS = ('s3://connors-big-money-data-bucket', 
                        's3://connors-misc-blob-for-blobs')
;