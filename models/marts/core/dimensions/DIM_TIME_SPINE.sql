{{ config(materialized='table') }}
WITH date_spine AS (
    {{ dbt_utils.date_spine(
        start_date = "'1980-01-01'",
        end_date   = "'2099-12-31'",
        datepart   = "day"
    ) }}
),

final AS (
    SELECT
        /* Surrogate numeric key YYYYMMDD */
        TO_NUMBER(TO_CHAR(date_day, 'YYYYMMDD'))                AS date_key,
        /* Base date */
        CAST(date_day AS DATE)                                  AS date_day,
        /* Calendar attributes */
        CAST(EXTRACT(YEAR FROM date_day) AS NUMBER(4,0))        AS year_number,
        CAST(EXTRACT(QUARTER FROM date_day) AS NUMBER(1,0))     AS quarter_number,
        CAST(EXTRACT(MONTH FROM date_day) AS NUMBER(2,0))       AS month_number,
        /* Month naming */
        TO_CHAR(date_day, 'MMMM')                                AS month_name,
        TO_CHAR(date_day, 'MON')                                 AS month_abbrev,
        /* Day attributes */
        CAST(EXTRACT(DAY FROM date_day) AS NUMBER(2,0))          AS day_of_month,
        CAST(DAYOFWEEK(date_day) AS NUMBER(1,0))                 AS day_of_week,
        TO_CHAR(date_day, 'DAY')                                 AS day_name,
        /* Week */
        CAST(WEEK(date_day) AS NUMBER(2,0))                      AS week_number,
        /* Boolean flags */
        CASE WHEN DAYOFWEEK(date_day) IN (1,7) 
             THEN TRUE ELSE FALSE END                            AS is_weekend,
        CASE WHEN date_day = DATE_TRUNC('MONTH', date_day)
             THEN TRUE ELSE FALSE END                            AS is_month_start,
        CASE WHEN date_day = LAST_DAY(date_day)
             THEN TRUE ELSE FALSE END                            AS is_month_end,
        CASE WHEN date_day = DATE_TRUNC('QUARTER', date_day)
             THEN TRUE ELSE FALSE END                            AS is_quarter_start,
        CASE WHEN date_day = LAST_DAY(date_day, 'QUARTER')
             THEN TRUE ELSE FALSE END                            AS is_quarter_end,
        CASE WHEN date_day = DATE_TRUNC('YEAR', date_day)
             THEN TRUE ELSE FALSE END                            AS is_year_start,
        CASE WHEN date_day = LAST_DAY(date_day, 'YEAR')
             THEN TRUE ELSE FALSE END                            AS is_year_end,
        /* Metadata */
        CURRENT_TIMESTAMP()                                      AS load_ts
    FROM date_spine
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
        ,{{ unknown_date() }} AS load_ts
),
union_data AS (
    SELECT * FROM final
    UNION ALL
    SELECT * FROM default_record
)
SELECT * FROM union_data