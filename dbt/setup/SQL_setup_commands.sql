-- SQL setup commands for Snowflake

CREATE WAREHOUSE ABS_BUILDING_APPROVALS_WAREHOUSE
  WITH WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;

CREATE DATABASE ABS_BUILDING_APPROVALS_DATABASE;
CREATE SCHEMA ABS_BUILDING_APPROVALS_DATABASE.ABS_BUILDING_APPROVALS;

CREATE TABLE ABS_BUILDING_APPROVALS_DATABASE.ABS_BUILDING_APPROVALS.SILVER_ABS_BUILDING_APPROVALS (
    time_period         VARCHAR,
    obs_value           FLOAT,
    obs_status          VARCHAR,
    obs_comment         VARCHAR,
    dataflow_code       VARCHAR,
    dataflow_label      VARCHAR,
    measure_code        VARCHAR,
    measure_label       VARCHAR,
    sector_code         VARCHAR,
    sector_label        VARCHAR,
    work_type_code      VARCHAR,
    work_type_label     VARCHAR,
    building_type_code  VARCHAR,
    building_type_label VARCHAR,
    region_type_code    VARCHAR,
    region_type_label   VARCHAR,
    region_code         VARCHAR,
    region_label        VARCHAR,
    frequency_code      VARCHAR,
    frequency_label     VARCHAR,
    unit_measure_code   VARCHAR,
    unit_measure_label  VARCHAR,
    unit_mult_code      VARCHAR,
    unit_mult_label     VARCHAR,
    region_file         VARCHAR,
    ingestion_date      DATE
);

CREATE STAGE ABS_BUILDING_APPROVALS_DATABASE.S3_SILVER_STAGE
  URL = 's3://johnq-data-lake-dev/abs-building-approvals/silver/'
  CREDENTIALS = (
    AWS_ACCESS_KEY_ID = ''
    AWS_SECRET_ACCESS_KEY = ''
  )
  FILE_FORMAT = (
    TYPE = 'PARQUET'
  );

COPY INTO ABS_BUILDING_APPROVALS_DATABASE.ABS_BUILDING_APPROVALS.SILVER_ABS_BUILDING_APPROVALS
FROM @ABS_BUILDING_APPROVALS_DATABASE.ABS_BUILDING_APPROVALS.S3_SILVER_STAGE
FILE_FORMAT = (TYPE = 'PARQUET')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*\parquet';