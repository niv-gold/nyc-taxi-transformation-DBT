{{ config(materialized='ephemeral') }}

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
    FROM {{source('seeds','dim_time')}}
),
hashed AS (
    SELECT     
        *
        ,'seeds.dim_time_1980_2099' AS RECORD_SOURCE
        ,CAST('{{ run_started_at }}' AS TIMESTAMP) AS LOAD_TS
        ,{{ dbt_utils.generate_surrogate_key(['DATE_KEY','DATE_DAY','YEAR_NUMBER','QUARTER_NUMBER','MONTH_NUMBER','MONTH_NAME','MONTH_ABBREV','DAY_OF_MONTH','DAY_OF_WEEK','DAY_NAME','WEEK_NUMBER'])}} AS HDIFF
    FROM stg_dim_time
),
default_record AS (
    SELECT
        -1 AS DATE_KEY
        ,'1900-01-01' AS DATE_DAY
        ,-1 AS YEAR_NUMBER
        ,-1 AS QUARTER_NUMBER
        ,-1 AS MONTH_NUMBER
        ,'missing' AS MONTH_NAME
        ,'missing' AS MONTH_ABBREV
        ,-1 AS DAY_OF_MONTH
        ,-1 AS DAY_OF_WEEK
        ,'missing' AS DAY_NAME
        ,-1 AS WEEK_NUMBER
        ,FALSE AS IS_WEEKEND
        ,FALSE AS IS_MONTH_START
        ,FALSE AS IS_MONTH_END
        ,FALSE AS IS_QUARTER_START
        ,FALSE AS IS_QUARTER_END
        ,FALSE AS IS_YEAR_START
        ,FALSE AS IS_YEAR_END
        ,CAST('1900-01-01' AS TIMESTAMP) AS INGEST_TS
        ,'System.DefaultKey' AS RECORD_SOURCE
        ,CAST('{{ run_started_at }}' AS TIMESTAMP) AS LOAD_TS
        ,{{ dbt_utils.generate_surrogate_key(['DATE_KEY','DATE_DAY','YEAR_NUMBER','QUARTER_NUMBER','MONTH_NUMBER','MONTH_NAME','MONTH_ABBREV','DAY_OF_MONTH','DAY_OF_WEEK','DAY_NAME','WEEK_NUMBER'])}} AS HDIFF
),
unioned AS (
    SELECT * FROM hashed
    UNION ALL
    SELECT * FROM default_record
)

SELECT *
FROM unioned 
order by 1