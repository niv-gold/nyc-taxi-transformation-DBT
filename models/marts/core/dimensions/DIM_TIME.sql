{{config(materialized='table')}}
WITH data_snapshot AS (
    SELECT 
         DATE_KEY
        ,DATE_DAY
        ,YEAR_NUMBER
        ,QUARTER_NUMBER
        ,MONTH_NUMBER
        ,MONTH_NAME
        ,MONTH_ABBREV
        ,DAY_OF_MONTH
        ,DAY_OF_WEEK
        ,DAY_NAME 
        ,WEEK_NUMBER 
        ,IS_WEEKEND 
        ,IS_MONTH_START 
        ,IS_MONTH_END 
        ,IS_QUARTER_START 
        ,IS_QUARTER_END 
        ,IS_YEAR_START 
        ,IS_YEAR_END 
        ,INGEST_TS
        ,RECORD_SOURCE
        ,LOAD_TS
        ,HDIFF
        ,DBT_SCD_ID
        ,DBT_UPDATED_AT
        ,DBT_VALID_FROM
        ,DBT_VALID_TO
    FROM {{ ref('SNSH_DIM_TIME') }}
    WHERE DBT_VALID_TO IS NULL
),
final AS(
    SELECT
         * EXCLUDE (RECORD_SOURCE, HDIFF, DBT_SCD_ID, DBT_UPDATED_AT, DBT_VALID_FROM, DBT_VALID_TO)
    FROM data_snapshot
)

SELECT
    *
FROM final