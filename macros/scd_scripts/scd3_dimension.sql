-- macros/scd_scripts/scd3_dimension.sql
{% macro scd3_dimension(
    source_ref,
    natural_key,
    tracked_cols,
    extra_cols=[]
) %}

{#
    Generic SCD3 macro for building a Type 3 dimension from a stage table.

    Parameters:
    -----------
    source_ref   : ref() or source() call for the stage table
                   e.g. ref('TAXI_ZONE_STG')

    natural_key  : string — the business/natural key column
                   e.g. 'location_id'

    tracked_cols : list — columns to track; each gets a current and prev_ version
                   e.g. ['borough', 'zone', 'service_zone']

    extra_cols   : list (optional) — metadata columns to pass through, not tracked
                   NOTE: do NOT include 'ingest_ts' — it is always handled by the macro
                   e.g. ['src_file_name', 'record_source']

    Required columns in the stage table:
    -------------------------------------
    - <natural_key>  : business key
    - <tracked_cols> : business columns to track for changes
    - hdiff          : pre-computed hash diff (change detection)
    - ingest_ts      : timestamp of ingestion

    Output columns:
    ---------------
    - <dim_prefix>_sk         : surrogate key (stable — derived from natural key only)
    - <dim_prefix>_nk         : natural key
    - <col>                   : current value  } for each tracked_col
    - prev_<col>              : previous value }
    - effective_from          : when current version became active (ingest_ts)
    - prev_effective_from     : when previous version was active (null for new records)
    - _row_hash               : hash diff of current version
    - <extra_cols>            : passed through unchanged

    Type 3 behavior:
    ----------------
    - ONE row per natural key — no new rows are inserted on change (unlike SCD2)
    - On change: current values shift to prev_*, incoming values become current
    - Only ONE level of history is preserved (current + previous)
    - If multiple versions arrive in the same batch, only the LATEST is applied;
      intermediate versions are not preserved (use SCD2 multi_vers for that)

    Model config to use:
    --------------------
    {{
        config(
            materialized        = 'incremental',
            unique_key          = '<dim_prefix>_nk',
            incremental_strategy= 'merge'
        )
    }}
    -- No merge_update_columns restriction: all columns are updated on change
#}

{%- set sk_cols    = [natural_key] -%}
{%- set dim_prefix = natural_key | replace('_id', '') -%}

{%- set safe_extra_cols = [] -%}
{%- for col in extra_cols -%}
    {%- if col | lower != 'ingest_ts' -%}
        {%- do safe_extra_cols.append(col) -%}
    {%- endif -%}
{%- endfor -%}

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
    select * from {{ this }}
),

-- Case 1: brand new natural keys never seen before
new_records as (
    select
        l.{{ dim_prefix }}_sk,
        l.{{ dim_prefix }}_nk,
        {% for col in tracked_cols %}
        l.{{ col }},                                     -- current value
        null                         as prev_{{ col }},  -- no previous yet
        {% endfor %}
        {% for col in safe_extra_cols %}
        l.{{ col }},
        {% endfor %}
        l._row_hash,
        l.ingest_ts                  as effective_from,
        cast(null as timestamp_ntz)  as prev_effective_from
    from latest l
    left join current_dim cd on l.{{ dim_prefix }}_nk = cd.{{ dim_prefix }}_nk
    where cd.{{ dim_prefix }}_nk is null
),

-- Case 2: existing keys with a detected change — rotate current → previous
changed_records as (
    select
        cd.{{ dim_prefix }}_sk,                          -- SK is stable (NK-based, never changes)
        cd.{{ dim_prefix }}_nk,
        {% for col in tracked_cols %}
        l.{{ col }},                                     -- new current value
        cd.{{ col }}             as prev_{{ col }},      -- old current value → previous
        {% endfor %}
        {% for col in safe_extra_cols %}
        l.{{ col }},
        {% endfor %}
        l._row_hash,
        l.ingest_ts              as effective_from,      -- new effective date
        cd.effective_from        as prev_effective_from  -- old effective date preserved
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

-- First run / full refresh: latest version per NK, prev_ columns initialised as null
with source as (
    select * from {{ source_ref }}
)

select
    {{ dbt_utils.generate_surrogate_key(sk_cols) }}  as {{ dim_prefix }}_sk,
    {{ natural_key }}                                 as {{ dim_prefix }}_nk,
    {% for col in tracked_cols %}
    {{ col }},
    null                         as prev_{{ col }},
    {% endfor %}
    {% for col in safe_extra_cols %}
    {{ col }},
    {% endfor %}
    hdiff                        as _row_hash,
    ingest_ts                    as effective_from,
    cast(null as timestamp_ntz)  as prev_effective_from
from source
qualify row_number() over (
    partition by {{ natural_key }}
    order by ingest_ts desc
) = 1

{% endif %}

{% endmacro %}