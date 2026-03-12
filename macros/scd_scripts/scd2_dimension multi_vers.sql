-- macros/scd2_dimension_multi_vers.sql
{% macro scd2_dimension_multi_vers(
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
    - ingest_ts      : timestamp of ingestion — drives valid_from and valid_to

    valid_from / valid_to conventions:
    -----------------------------------
    - valid_from = current version's ingest_ts
    - valid_to   = next version's ingest_ts     (consecutive, no gaps)
    - valid_to   = '2999-12-31'                 when is_current = true (far future sentinel)

    Multi-version behavior:
    -----------------------
    If multiple versions of the same natural key arrive in the same batch:
      - ALL versions are preserved as historical rows
      - Each version's valid_from  = its own ingest_ts
      - Each version's valid_to    = the next version's ingest_ts
      - Only the LATEST version    gets is_current = true + valid_to = '2999-12-31'
      - Intermediate versions      get is_current = false + valid_to = next ingest_ts
      - The existing current dim row is closed at the earliest batch ingest_ts
#}

{%- set sk_cols = [natural_key] + tracked_cols + ['ingest_ts'] -%}
{%- set dim_prefix = natural_key | replace('_id', '') -%}
{%- set high_date = "cast('2999-12-31' as timestamp_ntz)" -%}

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

versioned as (
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
        hdiff                                             as _row_hash,
        row_number() over (
            partition by {{ natural_key }}
            order by ingest_ts asc                        -- asc: oldest first for lead() to look forward
        )                                                 as _version_rank,
        lead(ingest_ts) over (
            partition by {{ natural_key }}
            order by ingest_ts asc                        -- lead() = next (newer) version's ingest_ts
        )                                                 as _next_ingest_ts,
        max(ingest_ts) over (
            partition by {{ natural_key }}
        )                                                 as _max_ingest_ts  -- identifies latest version
    from source
),

-- Latest version per natural key → is_current = true
latest_versions as (
    select * from versioned
    where ingest_ts = _max_ingest_ts
),

-- Intermediate versions in this batch → closed historical rows
intermediate_versions as (
    select * from versioned
    where ingest_ts != _max_ingest_ts
),

current_dim as (
    select * from {{ this }}
    where is_current = true
),

-- Case 1: brand new natural keys — latest version inserted as current
new_records as (
    select
        lv.{{ dim_prefix }}_sk,
        lv.{{ dim_prefix }}_nk,
        {% for col in tracked_cols %}
        lv.{{ col }},
        {% endfor %}
        {% for col in safe_extra_cols %}
        lv.{{ col }},
        {% endfor %}
        lv._row_hash,
        lv.ingest_ts                 as valid_from,       -- this version's ingest_ts
        {{ high_date }}              as valid_to,         -- current: far future sentinel
        true                         as is_current
    from latest_versions lv
    left join current_dim cd on lv.{{ dim_prefix }}_nk = cd.{{ dim_prefix }}_nk
    where cd.{{ dim_prefix }}_nk is null
),

-- Case 2: changed existing keys — latest batch version becomes new current row
new_versions as (
    select
        lv.{{ dim_prefix }}_sk,
        lv.{{ dim_prefix }}_nk,
        {% for col in tracked_cols %}
        lv.{{ col }},
        {% endfor %}
        {% for col in safe_extra_cols %}
        lv.{{ col }},
        {% endfor %}
        lv._row_hash,
        lv.ingest_ts                 as valid_from,       -- this version's ingest_ts
        {{ high_date }}              as valid_to,         -- current: far future sentinel
        true                         as is_current
    from latest_versions lv
    inner join current_dim cd on lv.{{ dim_prefix }}_nk = cd.{{ dim_prefix }}_nk
    where lv._row_hash != cd._row_hash
),

-- Case 3: close the existing dim row for changed keys
-- valid_from = unchanged (original valid_from preserved)
-- valid_to   = earliest ingest_ts in this batch → keeps timeline consecutive
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
        cd.valid_from,                                    -- preserve original valid_from
        min(v.ingest_ts)             as valid_to,         -- closed at earliest batch ingest_ts
        false                        as is_current
    from current_dim cd
    inner join versioned v          on v.{{ dim_prefix }}_nk  = cd.{{ dim_prefix }}_nk
    inner join latest_versions lv   on lv.{{ dim_prefix }}_nk = cd.{{ dim_prefix }}_nk
    where lv._row_hash != cd._row_hash
    group by
        cd.{{ dim_prefix }}_sk,
        cd.{{ dim_prefix }}_nk,
        {% for col in tracked_cols %}
        cd.{{ col }},
        {% endfor %}
        {% for col in safe_extra_cols %}
        cd.{{ col }},
        {% endfor %}
        cd._row_hash,
        cd.valid_from
),

-- Case 4: intermediate batch versions — fully closed historical rows
-- valid_from = this version's ingest_ts
-- valid_to   = next version's ingest_ts (lead) — consecutive, no gaps
batch_intermediate_records as (
    select
        iv.{{ dim_prefix }}_sk,
        iv.{{ dim_prefix }}_nk,
        {% for col in tracked_cols %}
        iv.{{ col }},
        {% endfor %}
        {% for col in safe_extra_cols %}
        iv.{{ col }},
        {% endfor %}
        iv._row_hash,
        iv.ingest_ts                 as valid_from,       -- this version's ingest_ts
        iv._next_ingest_ts           as valid_to,         -- next version's ingest_ts
        false                        as is_current
    from intermediate_versions iv
),

final as (
    select * from new_records
    union all
    select * from new_versions
    union all
    select * from expired_records
    union all
    select * from batch_intermediate_records
)

select * from final

{% else %}

-- First run / full refresh
-- valid_from = this version's ingest_ts
-- valid_to   = next version's ingest_ts (lead), or '2999-12-31' for the latest
with source as (
    select * from {{ source_ref }}
),

versioned as (
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
        hdiff                                             as _row_hash,
        lead(ingest_ts) over (
            partition by {{ natural_key }}
            order by ingest_ts asc                        -- lead() looks at next newer version
        )                                                 as _next_ingest_ts,
        max(ingest_ts) over (
            partition by {{ natural_key }}
        )                                                 as _max_ingest_ts
    from source
)

select
    {{ dim_prefix }}_sk,
    {{ dim_prefix }}_nk,
    {% for col in tracked_cols %}
    {{ col }},
    {% endfor %}
    {% for col in safe_extra_cols %}
    {{ col }},
    {% endfor %}
    _row_hash,
    ingest_ts                                             as valid_from,   -- this version's ingest_ts
    case
        when ingest_ts = _max_ingest_ts then {{ high_date }}              -- latest: sentinel
        else _next_ingest_ts                                              -- historical: next ingest_ts
    end                                                   as valid_to,
    case
        when ingest_ts = _max_ingest_ts then true
        else false
    end                                                   as is_current
from versioned

{% endif %}

{% endmacro %}