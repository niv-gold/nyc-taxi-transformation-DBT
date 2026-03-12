{% macro save_history_last_version(
    input_rel,
    key_column,
    diff_column,
    ingest_ts_column = 'LOAD_TS',
    input_filter_expr = 'true',
    history_filter_expr = 'true',
    high_watermark_column = none,
    high_watermark_test = '>=',
    order_by_expr = none
) -%}

{{config(
    materialized = 'incremental',
    unique_key = key_column,
    incremental_strategy = 'merge',
)}}

WITH

{%- if is_incremental() %}
current_from_history as (
    {{current_from_history(
        history_rel = this,
        key_column = key_column,
        selection_expr = key_column ~ ', ' ~ diff_column,
        ingest_ts_column = ingest_ts_column,
        history_filter_expr = history_filter_expr
    ) }}
),

load_new_keys_from_input as (
    SELECT i.*
    FROM 
        {{input_rel}} as i
        LEFT OUTER JOIN current_from_history as h 
        ON i.{{key_column}} = h.{{key_column}}
    WHERE {{input_filter_expr}}
        AND h.{{key_column}} is null
        {%- if high_watermark_column %}
            AND case when (select max({{high_watermark_column}}) from {{this}}) is null then true
                else i.{{high_watermark_column}} {{high_watermark_test}} (select max({{high_watermark_column}}) from {{ this }})
            end
        {%- endif %}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY i.{{key_column}} ORDER BY i.{{ingest_ts_column}} DESC) = 1
),

load_exist_keys_from_input as (
    SELECT i.hskey
    FROM 
        {{input_rel}} as i
        LEFT OUTER JOIN current_from_history as h 
        ON i.{{key_column}} = h.{{key_column}}
    WHERE NOT (
                (h.{{diff_column}} = i.{{diff_column}})
                OR (h.{{diff_column}} is null AND i.{{diff_column}} is null)
            )
        and {{input_filter_expr}}
        {%- if high_watermark_column %}
            and case when (select max({{high_watermark_column}}) from {{this}}) is null then true
                else i.{{high_watermark_column}} {{high_watermark_test}} (select max({{high_watermark_column}}) from {{ this }})
            end
        {%- endif %}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY i.{{key_column}} ORDER BY i.{{ingest_ts_column}} DESC) = 1
),

load_last_version_from_input as (
    SELECT * FROM load_new_keys_from_input
    UNION ALL
    SELECT * FROM load_exist_keys_from_input
)

{%- else %}
load_last_version_from_input as (
    SELECT *
    FROM {{input_rel}}
    WHERE {{input_filter_expr}}
)
{%- endif %}

SELECT * FROM load_last_version_from_input
{%- if order_by_expr %}
ORDER BY {{order_by_expr}}
{%- endif %}

{%- endmacro %}