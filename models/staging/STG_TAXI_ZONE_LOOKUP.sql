WITH src AS (
  SELECT
    raw_line,
    src_file_name,
    src_row_number,
    ingested_at
  FROM nyc_taxi_dev.RAW.csv_landing_raw
  WHERE src_file_name ILIKE '%taxi_zone_lookup%'
),

-- Parse quoted CSV: LocationID,"Borough","Zone","service_zone"
parsed AS (
  SELECT
    TRY_TO_NUMBER(REGEXP_SUBSTR(raw_line, '^[^,]+')) AS location_id,

    REGEXP_SUBSTR(raw_line, '"([^"]*)"', 1, 1, 'e', 1) AS borough,
    REGEXP_SUBSTR(raw_line, '"([^"]*)"', 1, 2, 'e', 1) AS zone,
    REGEXP_SUBSTR(raw_line, '"([^"]*)"', 1, 3, 'e', 1) AS service_zone,

    src_file_name,
    src_row_number,
    ingested_at,
    CAST('{{ run_started_at }}' AS TIMESTAMP) AS LOAD_TS
  FROM src
)

SELECT *
FROM parsed