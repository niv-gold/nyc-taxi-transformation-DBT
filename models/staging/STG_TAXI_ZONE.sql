-- This Model name was aliased in the "_schema.yml" file to match the DB convention.
-- Table name is "TAXI_ZONE_RAW"

{{config(materialized='view')}}
WITH source_data AS (
    SELECT
        raw_line,
        src_file_name,
        src_row_number,
        ingested_at,
        'Snowflake.taxi_zone' as RECORD_SOURCE 
    FROM {{ source('RAW', 'CSV_LANDING_RAW') }}
    WHERE src_file_name ILIKE '%taxi_zone_lookup%' 
),
parsed_data AS (
    SELECT
        TRY_TO_NUMBER(REGEXP_SUBSTR(raw_line, '^[^,]+')) AS LOCATION_ID,
        TRIM(REGEXP_SUBSTR(raw_line, '"([^"]*)"', 1, 1, 'e', 1)) AS BOROUGH,
        TRIM(REGEXP_SUBSTR(raw_line, '"([^"]*)"', 1, 2, 'e', 1)) AS ZONE,
        TRIM(REGEXP_SUBSTR(raw_line, '"([^"]*)"', 1, 3, 'e', 1)) AS SERVICE_ZONE,
        SRC_FILE_NAME,
        SRC_ROW_NUMBER,
        INGESTED_AT AS INGEST_TS,
        RECORD_SOURCE
    FROM source_data
    WHERE regexp_substr(raw_line, '^[^,]+') <> 'LocationID'
),
history_data AS (
    SELECT
        LOCATION_ID,
        BOROUGH,
        ZONE,
        SERVICE_ZONE,
        SRC_FILE_NAME,
        SRC_ROW_NUMBER,
        INGEST_TS,        
        RECORD_SOURCE
    FROM parsed_data
),
default_record AS (
    SELECT
        {{ unknown_int() }} AS LOCATION_ID,
        {{ unknown_str() }} AS BOROUGH,
        {{ unknown_str() }} AS ZONE,
        {{ unknown_str() }} AS SERVICE_ZONE,
        {{ unknown_system() }} AS SRC_FILE_NAME,
        {{ unknown_int() }} AS SRC_ROW_NUMBER,
        {{ unknown_date() }} AS INGEST_TS,        
        {{ unknown_system() }} AS RECORD_SOURCE
),
union_data AS (
    SELECT * FROM history_data
    UNION ALL
    SELECT * FROM default_record
),
hashed_col AS (
    SELECT
        *,
        {{ dbt_utils.generate_surrogate_key(['LOCATION_ID',
        'BOROUGH', 'ZONE', 'SERVICE_ZONE']) }} AS HDIFF
    FROM union_data
)
SELECT *
FROM hashed_col
order by LOCATION_ID