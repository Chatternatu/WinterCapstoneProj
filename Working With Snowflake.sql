create or replace warehouse Team2_wh 
warehouse_size = xsmall;
create or replace database Team2_db; 

create or replace schema Team2_schema;

create or replace table TEAM2_DB.TEAM2_SCHEMA.EPISODES_OF_CARE
( 
	HOSPITAL_EPISODES_OF_CARE_BY_DRG_FACT_KEY string NOT NULL ,
	HOSPITAL_DIM_KEY     string  NOT NULL ,
	YEAR_KEY             string  NULL ,
	DRG                  string  NULL ,
	COUNT_OF_EPISODES    string  NULL ,
	AVERAGE_AMOUNT_PAID_BY_MEDICARE_FOR_HOME_HEALTH string NULL ,
	AVERAGE_AMOUNT_PAID_BY_MEDICARE_FOR_HOSPICE string  NULL ,
	AVERAGE_AMOUNT_PAID_BY_MEDICARE_FOR_HOSPITAL string NULL ,
	AVERAGE_AMOUNT_CHARGED_BY_FACILITY_FOR_REHAB string  NULL ,
	AVERAGE_AMOUNT_PAID_BY_MEDICARE_FOR_SNF string NULL ,
	AVERAGE_AMOUNT_PAID_BY_MEDICARE_FOR_SUBSEQUENT_HOSPITALIZATION string  NULL ,
	AVERAGE_LENGTH_OF_STAY_FOR_HOME_HEALTH string  NULL ,
	AVERAGE_LENGTH_OF_STAY_FOR_HOSPICE string NULL ,
	AVERAGE_LENGTH_OF_STAY_FOR_HOSPITAL string  NULL ,
	AVERAGE_LENGTH_OF_STAY_FOR_REHAB string  NULL ,
	AVERAGE_LENGTH_OF_STAY_FOR_SNF string NULL ,
	AVERAGE_LENGTH_OF_STAY_FOR_SUBSEQUENT_HOSPITALIZATION string NULL 
);

SELECT* from TEAM2_DB.TEAM2_SCHEMA.EPISODES_OF_CARE;

CREATE STORAGE INTEGRATION Team2_integration
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::724772059319:role/Team2projectrole'
STORAGE_ALLOWED_LOCATIONS = ('s3://episodecare'); 

desc integration Team2_integration;
----Create the file format to match the files in the S3 Bucket using the template below---
CREATE OR REPLACE FILE FORMAT csv_format_team2  -- N/B <- Replace file format (csv,xls…)
TYPE = CSV
SKIP_HEADER = 1; 

--Creating the stage
CREATE OR REPLACE STAGE s3_stage_team2    -- Choose stage name
STORAGE_INTEGRATION = Team2_integration    -- Replace with previously created sto. int. (1.)
url = 's3://episodecare/Episode_of_care.csv'       		      --Replace with File Url from AWS S3 Bucket
FILE_FORMAT = csv_format_team2;                     --The file format name you just created up

--List the file visibility in your s3 bucket

List @s3_stage_team2;
----Create Ingestion Pipeline to ingest the file from S3 into the Snowflake table----
COPY INTO TEAM2_DB.TEAM2_SCHEMA.EPISODES_OF_CARE		--Replace file/table name from S3		
FROM @s3_stage_team2;			-- Replace with created stage name

-- Upon runing into error while copying into your created table, include the code below, to ignore special charaters in our file
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' FIELD_DELIMETER = ',' RECORD_DELIMITER = '\n')
ON_ERROR = 'CONTINUE';

Select * from TEAM2_DB.TEAM2_SCHEMA.EPISODES_OF_CARE;

----Create Snowflake Task Utility to automate Data Ingestion----
CREATE OR REPLACE Task Episode_of_Care_ingestion_Task		-- Replace task name
WAREHOUSE = Team2_wh				--Replace warehouse name
SCHEDULE = '5 minutes'
AS
COPY INTO TEAM2_DB.TEAM2_SCHEMA.EPISODES_OF_CARE				--Replace table name
FROM @s3_stage_team2;

Alter task Episode_of_Care_ingestion_Task resume;
show tasks;
alter task Episode_of_Care_ingestion_Task suspend;

DESCRIBE TASK Episode_of_care_ingestion_Task;
show tasks;




