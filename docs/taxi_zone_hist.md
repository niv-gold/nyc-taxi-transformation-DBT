{% docs taxi_zone_hist %}
# TAXI_ZONE_HIST Model Documentation

This document describes `models/staging/TAXI_ZONE_HIST.sql`.

## Purpose

`TAXI_ZONE_HIST` is the history target table for taxi zone records.
The model delegates transformation and merge behavior to the `save_history` macro.

## Model SQL

File: `models/staging/TAXI_ZONE_HIST.sql`

- Macro used: `save_history`
- Source relation: `ref('TAXI_ZONE_STG')`
- Business key: `LOCATION_ID`
- Change hash column: `HDIFF`
- Load timestamp column: `LOAD_TS`
- High watermark column: `INGEST_TS`
- High watermark comparison: `>=`
- Order by expression: `INGEST_TS`

## Runtime Behavior

On incremental runs:

- New keys are inserted.
- Existing keys are updated only when `HDIFF` changes.
- Merge condition is key-based (`LOCATION_ID`).
- Merge update guard is hash-diff based (`HDIFF`).
- Source is de-duplicated to latest row per key using `load_ts_column`.

On non-incremental runs:

- Source rows are loaded through macro logic without incremental filtering.
- If target is dropped or rebuilt, table is recreated from source.

## Related Files

- `macros/scd_scripts/save_history.sql`
- `macros/system/snowflake__get_merge_sql.sql`
- `models/staging/STG_TAXI_ZONE.sql`
- `docs/save_history.md`

## Validation Notes

Manual Snowflake checks reported as OK for:

- New key insert
- Rebuild after dropping target table
- Reload after truncating target table
- Update on same key with changed monitored fields
- No update on same key with unchanged data
{% enddocs %}