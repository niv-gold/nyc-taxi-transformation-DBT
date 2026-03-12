{% macro current_from_history(
    history_rel,
    key_column,
    selection_expr = '*',
    ingest_ts_column = 'INGEST_TS',
    history_filter_expr = 'true',
    qualify_function = 'row_number'
) -%}

SELECT {{selection_expr}}
FROM {{history_rel}}
WHERE {{history_filter_expr}}
QUALIFY {{qualify_function}}() OVER( PARTITION BY {{key_column}} ORDER BY {{ingest_ts_column}} desc) = 1

{%- endmacro %}