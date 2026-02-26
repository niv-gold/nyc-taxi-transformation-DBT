{% snapshot SNSH_DIM_TIME %}
    {{
        config(
          target_schema='snapshots',
          unique_key='DATE_KEY',
          strategy='check',
          check_cols=['HDIFF'],
          invalidate_hard_deletes=True
        )
    }}
    
    SELECT *
    FROM {{ ref('STG_DIM_TIME') }}
{% endsnapshot %}