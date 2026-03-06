{{ config(materialized='view') }}

WITH stg_dim_time AS (
    SELECT
         DATE_KEY
        ,DATE_DAY
        ,YEAR AS YEAR_NUMBER
        ,QUARTER AS QUARTER_NUMBER
        ,MONTH AS MONTH_NUMBER
        ,MONTH_NAME
        ,MONTH_ABBREV
        ,DAY AS DAY_OF_MONTH
        ,DAY_OF_WEEK
        ,DAY_NAME 
        ,WEEK_OF_YEAR AS WEEK_NUMBER 
        ,IS_WEEKEND 
        ,IS_MONTH_START 
        ,IS_MONTH_END 
        ,IS_QUARTER_START 
        ,IS_QUARTER_END 
        ,IS_YEAR_START 
        ,IS_YEAR_END 
        ,LOAD_TS AS INGEST_TS 
        ,'seeds.dim_time_1980_2099' AS RECORD_SOURCE
    FROM {{source('RAW_SEEDS','DIM_TIME')}}
),
default_record AS (
    SELECT
        {{ unknown_int() }} AS DATE_KEY
        ,{{ unknown_date() }} AS DATE_DAY
        ,{{ unknown_int() }} AS YEAR_NUMBER
        ,{{ unknown_int() }} AS QUARTER_NUMBER
        ,{{ unknown_int() }} AS MONTH_NUMBER
        ,{{ unknown_str() }} AS MONTH_NAME
        ,{{ unknown_str() }} AS MONTH_ABBREV
        ,{{ unknown_int() }} AS DAY_OF_MONTH
        ,{{ unknown_int() }} AS DAY_OF_WEEK
        ,{{ unknown_str() }} AS DAY_NAME
        ,{{ unknown_int() }} AS WEEK_NUMBER
        ,{{ unknown_bool() }} AS IS_WEEKEND
        ,{{ unknown_bool() }} AS IS_MONTH_START
        ,{{ unknown_bool() }} AS IS_MONTH_END
        ,{{ unknown_bool() }} AS IS_QUARTER_START
        ,{{ unknown_bool() }} AS IS_QUARTER_END
        ,{{ unknown_bool() }} AS IS_YEAR_START
        ,{{ unknown_bool() }} AS IS_YEAR_END
        ,{{ unknown_date() }} AS INGEST_TS
        ,{{ unknown_system() }} AS RECORD_SOURCE
),
unioned AS (
    SELECT * FROM stg_dim_time
    UNION ALL
    SELECT * FROM default_record
),
hashed_col AS (
    SELECT     
        *        
        ,{{ dbt_utils.generate_surrogate_key(['DATE_KEY','DATE_DAY','YEAR_NUMBER','QUARTER_NUMBER','MONTH_NUMBER','MONTH_NAME','MONTH_ABBREV','DAY_OF_MONTH','DAY_OF_WEEK','DAY_NAME','WEEK_NUMBER'])}} AS HDIFF
    FROM unioned
)

SELECT *
FROM hashed_col
order by 1