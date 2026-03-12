{% macro scd2_dimension_single_vers(
    source_ref,
    natural_key,
    tracked_cols,
    extra_cols=[]
) %}

{#
    Generic SCD2 macro for building a dimension from a stage table.

    Parameters:
    -----------
    source_ref   : ref() or source() call for the stage table
                   e.g. ref('TAXI_ZONE_STG')

    natural_key  : string — the business/natural key column
                   e.g. 'location_id'

    tracked_cols : list — columns for change detection (also included in SK along with ingest_ts)
                   e.g. ['borough', 'zone', 'service_zone']

    extra_cols   : list (optional) — metadata columns to pass through, not tracked
                   NOTE: do NOT include 'ingest_ts' — it is always handled by the macro
                   e.g. ['src_file_name', 'record_source']

    Required columns in the stage table:
    -------------------------------------
    - <natural_key>  : business key
    - <tracked_cols> : business columns to track for changes
    - hdiff          : pre-computed hash diff (change detection)
    - ingest_ts      : timestamp used as valid_from / valid_to (never pass in extra_cols)

    Deduplication behavior:
    -----------------------
    If multiple versions of the same natural key arrive in the same batch
    (different ingest_ts), only the LATEST version (max ingest_ts) is kept.
    Intermediate versions are ignored entirely.
#}

{%- set sk_cols = [natural_key] + tracked_cols + ['ingest_ts'] -%}
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

-- Deduplicate: keep only the latest version per natural key in this batch
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
        order by ingest_ts desc         -- latest version wins
    ) = 1
),

current_dim as (
    select * from {{ this }}
    where is_current = true
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
        h.ingest_ts                  as valid_from,
        cast('2999-12-31' as timestamp)  as valid_to,
        true                         as is_current
    from hashed h
    left join current_dim cd on h.{{ dim_prefix }}_nk = cd.{{ dim_prefix }}_nk
    where cd.{{ dim_prefix }}_nk is null
),

-- Existing keys whose latest batch version differs from current dim
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
        h.ingest_ts                  as valid_from,
        cast('2999-12-31' as timestamp)  as valid_to,
        true                         as is_current
    from hashed h
    inner join current_dim cd on h.{{ dim_prefix }}_nk = cd.{{ dim_prefix }}_nk
    where h._row_hash != cd._row_hash
),

-- Close out the old version of changed records
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
        h.ingest_ts                  as valid_to,       -- closed at latest batch ingest_ts
        false                        as is_current
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
    hdiff                        as _row_hash,
    ingest_ts                    as valid_from,
    cast('2999-12-31' as timestamp)  as valid_to,
    true                         as is_current
from source
qualify row_number() over (
    partition by {{ natural_key }}
    order by ingest_ts desc
) = 1

{% endif %}

{% endmacro %}