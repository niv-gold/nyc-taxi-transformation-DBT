{{ config(materialized='table') }}

WITH stg_dim_time AS (
    SELECT
         DATE_KEY
        ,DATE_DAY
        ,YEAR
        ,QUARTER
        ,MONTH
        ,MONTH_NAME
        ,MONTH_ABBREV
        ,DAY
        ,DAY_OF_WEEK
        ,DAY_NAME
        ,WEEK_OF_YEAR
        ,IS_WEEKEND
        ,IS_MONTH_START
        ,IS_MONTH_END
        ,IS_QUARTER_START
        ,IS_QUARTER_END
        ,IS_YEAR_START
        ,IS_YEAR_END
        ,LOAD_TS        
    FROM {{source('seeds','dim_time')}}
)

SELECT 
    *
    ,'{{ run_started_at }}' AS LOAD_TS_UTC
    ,'seed.dim_time' as RECORD_SOURCE
    ,dbt_utils.surrogate_key(
        [DATE_DAY, YEAR, QUARTER, MONTH, MONTH_NAME, MONTH_ABBREV, DAY, DAY_OF_WEEK, DAY_NAME, WEEK_OF_YEAR, IS_WEEKEND, IS_MONTH_START, IS_MONTH_END, IS_QUARTER_START, IS_QUARTER_END, IS_YEAR_START, IS_YEAR_END]
    ) AS HDIFF
FROM stg_dim_time