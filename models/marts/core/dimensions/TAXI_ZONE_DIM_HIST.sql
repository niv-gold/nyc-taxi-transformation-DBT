{{
    config(
        materialized         = 'incremental',
        unique_key           = 'location_sk',
        incremental_strategy = 'merge',
        merge_update_columns = ['valid_to', 'is_current']
    )
}}

{{ scd4_dimension(
    source_ref   = ref('TAXI_ZONE_STG'),
    natural_key  = 'location_id',
    tracked_cols = ['borough', 'zone', 'service_zone'],
    extra_cols   = ['src_file_name', 'record_source'],
    table_type   = 'history'
) }}