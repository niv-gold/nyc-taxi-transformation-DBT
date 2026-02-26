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
        CAST(EXTRACT(DAY FROM date_day) AS NUMBER(2,0))          AS day_number,
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
)
SELECT * FROM final