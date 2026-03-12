-- macros/scd_scripts/scd4_dimension.sql
{% macro scd4_dimension(
    source_ref,
    natural_key,
    tracked_cols,
    extra_cols=[],
    table_type='current'
) %}

{#
    Generic SCD4 macro — "History Table" pattern.
    Produces TWO separate models from the same staging source by setting table_type.

    Parameters:
    -----------
    source_ref   : ref() or source() call for the stage table
                   e.g. ref('TAXI_ZONE_STG')

    natural_key  : string — the business/natural key column
                   e.g. 'location_id'

    tracked_cols : list — columns for change detection
                   e.g. ['borough', 'zone', 'service_zone']

    extra_cols   : list (optional) — metadata columns to pass through, not tracked
                   NOTE: do NOT include 'ingest_ts' — it is always handled by the macro
                   e.g. ['src_file_name', 'record_source']

    table_type   : 'current' (default) or 'history'
                   Controls which half of the SCD4 pair this model builds.

    Required columns in the stage table:
    -------------------------------------
    - <natural_key>  : business key
    - <tracked_cols> : business columns to track for changes
    - hdiff          : pre-computed hash diff (change detection)
    - ingest_ts      : timestamp of ingestion

    SCD4 pattern — two models, one macro:
    --------------------------------------
    table_type='current'  →  DIM_<entity>
      - One row per natural key, always the latest values
      - No valid_from / valid_to — history is not the current table's concern
      - SK is stable: derived from natural_key only
      - On change: row is updated in-place (full merge/upsert)
      - effective_from tracks when the current version became active

    table_type='history'  →  DIM_<entity>_HIST
      - All versions ever seen, including superseded ones
      - valid_from / valid_to / is_current — identical logic to SCD2 single_vers
      - SK includes ingest_ts to make each version unique
      - Only valid_to and is_current are updated on merge (preserve history)

    Model configs to use:
    ---------------------
    -- current table
    {{ config(
        materialized         = 'incremental',
        unique_key           = '<dim_prefix>_nk',
        incremental_strategy = 'merge'
    ) }}

    -- history table
    {{ config(
        materialized          = 'incremental',
        unique_key            = '<dim_prefix>_sk',
        incremental_strategy  = 'merge',
        merge_update_columns  = ['valid_to', 'is_current']
    ) }}
#}

{%- set dim_prefix = natural_key | replace('_id', '') -%}

{%- set safe_extra_cols = [] -%}
{%- for col in extra_cols -%}
    {%- if col | lower != 'ingest_ts' -%}
        {%- do safe_extra_cols.append(col) -%}
    {%- endif -%}
{%- endfor -%}

{%- if table_type == 'current' -%}
    {%- set sk_cols = [natural_key] -%}                              {#-- stable: derived from NK only    --#}
{%- elif table_type == 'history' -%}
    {%- set sk_cols = [natural_key] + tracked_cols + ['ingest_ts'] -%} {#-- unique per version             --#}
{%- else -%}
    {{ exceptions.raise_compiler_error(
        "scd4_dimension: table_type must be 'current' or 'history', got: '" ~ table_type ~ "'"
    ) }}
{%- endif -%}

-- ============================================================
-- CURRENT TABLE  (table_type = 'current')
-- One row per NK, always the latest. History lives elsewhere.
-- ============================================================
{% if table_type == 'current' %}

{% if is_incremental() %}

with source as (
    select * from {{ source_ref }}
),

-- Deduplicate: keep only the latest version per NK in this batch
latest as (
    select
        {{ dbt_utils.generate_surrogate_key(sk_cols) }}  as {{ dim_prefix }}_sk,
        {{ natural_key }}                                 as {{ dim_prefix }}_nk,
        {% for col in tracked_cols %}
        {{ col }},
        {% endfor %}
        {% for col in safe_extra_cols %}
        {{ col }},
        {% endfor %}
        ingest_ts,
        hdiff                                             as _row_hash
    from source
    qualify row_number() over (
        partition by {{ natural_key }}
        order by ingest_ts desc     -- latest version wins
    ) = 1
),

current_dim as (
    select * from {{ this }}        -- every row in the current table is already the live version
),

-- Case 1: brand new natural keys never seen before
new_records as (
    select
        l.{{ dim_prefix }}_sk,
        l.{{ dim_prefix }}_nk,
        {% for col in tracked_cols %}
        l.{{ col }},
        {% endfor %}
        {% for col in safe_extra_cols %}
        l.{{ col }},
        {% endfor %}
        l._row_hash,
        l.ingest_ts                  as effective_from    -- when this version became active
    from latest l
    left join current_dim cd on l.{{ dim_prefix }}_nk = cd.{{ dim_prefix }}_nk
    where cd.{{ dim_prefix }}_nk is null
),

-- Case 2: existing keys with a detected change — full in-place update
changed_records as (
    select
        cd.{{ dim_prefix }}_sk,                          -- SK is stable (NK-based, never changes)
        cd.{{ dim_prefix }}_nk,
        {% for col in tracked_cols %}
        l.{{ col }},                                     -- new current value replaces old
        {% endfor %}
        {% for col in safe_extra_cols %}
        l.{{ col }},
        {% endfor %}
        l._row_hash,
        l.ingest_ts                  as effective_from    -- updated to new version's ingest_ts
    from latest l
    inner join current_dim cd on l.{{ dim_prefix }}_nk = cd.{{ dim_prefix }}_nk
    where l._row_hash != cd._row_hash
),

final as (
    select * from new_records
    union all
    select * from changed_records
)

select * from final

{% else %}

-- First run / full refresh: latest version per NK only
with source as (
    select * from {{ source_ref }}
)

select
    {{ dbt_utils.generate_surrogate_key(sk_cols) }}  as {{ dim_prefix }}_sk,
    {{ natural_key }}                                 as {{ dim_prefix }}_nk,
    {% for col in tracked_cols %}
    {{ col }},
    {% endfor %}
    {% for col in safe_extra_cols %}
    {{ col }},
    {% endfor %}
    hdiff                        as _row_hash,
    ingest_ts                    as effective_from
from source
qualify row_number() over (
    partition by {{ natural_key }}
    order by ingest_ts desc
) = 1

{% endif %}

-- ============================================================
-- HISTORY TABLE  (table_type = 'history')
-- All versions ever seen, with valid_from / valid_to / is_current.
-- Logic is identical to scd2_dimension_single_vers.
-- ============================================================
{% elif table_type == 'history' %}

{% if is_incremental() %}

with source as (
    select * from {{ source_ref }}
),

-- Deduplicate: keep only the latest version per NK in this batch
hashed as (
    select
        {{ dbt_utils.generate_surrogate_key(sk_cols) }}  as {{ dim_prefix }}_sk,
        {{ natural_key }}                                 as {{ dim_prefix }}_nk,
        {% for col in tracked_cols %}
        {{ col }},
        {% endfor %}
        {% for col in safe_extra_cols %}
        {{ col }},
        {% endfor %}
        ingest_ts,
        hdiff                                             as _row_hash
    from source
    qualify row_number() over (
        partition by {{ natural_key }}
        order by ingest_ts desc     -- latest version wins
    ) = 1
),

current_dim as (
    select * from {{ this }}
    where is_current = true         -- only compare against the live version
),

-- Brand new natural keys never seen before
new_records as (
    select
        h.{{ dim_prefix }}_sk,
        h.{{ dim_prefix }}_nk,
        {% for col in tracked_cols %}
        h.{{ col }},
        {% endfor %}
        {% for col in safe_extra_cols %}
        h.{{ col }},
        {% endfor %}
        h._row_hash,
        h.ingest_ts                      as valid_from,
        cast('2999-12-31' as timestamp)  as valid_to,
        true                             as is_current
    from hashed h
    left join current_dim cd on h.{{ dim_prefix }}_nk = cd.{{ dim_prefix }}_nk
    where cd.{{ dim_prefix }}_nk is null
),

-- Existing keys whose latest batch version differs from current history row
new_versions as (
    select
        h.{{ dim_prefix }}_sk,
        h.{{ dim_prefix }}_nk,
        {% for col in tracked_cols %}
        h.{{ col }},
        {% endfor %}
        {% for col in safe_extra_cols %}
        h.{{ col }},
        {% endfor %}
        h._row_hash,
        h.ingest_ts                      as valid_from,
        cast('2999-12-31' as timestamp)  as valid_to,
        true                             as is_current
    from hashed h
    inner join current_dim cd on h.{{ dim_prefix }}_nk = cd.{{ dim_prefix }}_nk
    where h._row_hash != cd._row_hash
),

-- Close out the previous current row for changed keys
expired_records as (
    select
        cd.{{ dim_prefix }}_sk,
        cd.{{ dim_prefix }}_nk,
        {% for col in tracked_cols %}
        cd.{{ col }},
        {% endfor %}
        {% for col in safe_extra_cols %}
        cd.{{ col }},
        {% endfor %}
        cd._row_hash,
        cd.valid_from,
        h.ingest_ts                      as valid_to,    -- closed at latest batch ingest_ts
        false                            as is_current
    from current_dim cd
    inner join hashed h on h.{{ dim_prefix }}_nk = cd.{{ dim_prefix }}_nk
    where h._row_hash != cd._row_hash
),

final as (
    select * from new_records
    union all
    select * from new_versions
    union all
    select * from expired_records
)

select * from final

{% else %}

-- First run / full refresh: deduplicate and load latest version per key
with source as (
    select * from {{ source_ref }}
)

select
    {{ dbt_utils.generate_surrogate_key(sk_cols) }}  as {{ dim_prefix }}_sk,
    {{ natural_key }}                                 as {{ dim_prefix }}_nk,
    {% for col in tracked_cols %}
    {{ col }},
    {% endfor %}
    {% for col in safe_extra_cols %}
    {{ col }},
    {% endfor %}
    hdiff                            as _row_hash,
    ingest_ts                        as valid_from,
    cast('2999-12-31' as timestamp)  as valid_to,
    true                             as is_current
from source
qualify row_number() over (
    partition by {{ natural_key }}
    order by ingest_ts desc
) = 1

{% endif %}

{% endif %}

{% endmacro %}