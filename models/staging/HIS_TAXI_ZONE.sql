{{ save_history(
    input_rel =  ref('STG_TAXI_ZONE'),
    key_column = 'TAXI_ZONE_ID',
    diff_column = 'HDIFF',
    load_ts_column = 'LOAD_TS',
    history_filter_expr = 'true',
    input_filter_expr = 'true',
    high_watermark_column = 'LOAD_TS',
    high_watermark_test = '>=',
    order_by_expr = 'LOAD_TS'
) }}