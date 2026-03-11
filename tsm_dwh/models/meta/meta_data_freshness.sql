{{
    config(
        materialized='view',
        tags=['meta']
    )
}}

WITH mart_sizes AS (
    SELECT relname AS model_name, n_live_tup AS row_count
    FROM pg_stat_user_tables
    WHERE schemaname = current_schema()
      AND relname IN (
          'mart_fact_journal',
          'mart_fact_workload',
          'mart_fact_norm_workload',
          'mart_res_downtimes'
      )
)

SELECT
    'mart_fact_journal' AS model_name,
    MAX(_dbt_loaded_at) AS last_updated_at,
    (SELECT row_count FROM mart_sizes WHERE model_name = 'mart_fact_journal') AS row_count,
    NOW() AS checked_at
FROM {{ ref('mart_fact_journal') }}

UNION ALL

SELECT
    'mart_fact_workload',
    MAX(_dbt_loaded_at),
    (SELECT row_count FROM mart_sizes WHERE model_name = 'mart_fact_workload'),
    NOW()
FROM {{ ref('mart_fact_workload') }}

UNION ALL

SELECT
    'mart_fact_norm_workload',
    MAX(_dbt_loaded_at),
    (SELECT row_count FROM mart_sizes WHERE model_name = 'mart_fact_norm_workload'),
    NOW()
FROM {{ ref('mart_fact_norm_workload') }}

UNION ALL

SELECT
    'mart_res_downtimes',
    MAX(_dbt_loaded_at),
    (SELECT row_count FROM mart_sizes WHERE model_name = 'mart_res_downtimes'),
    NOW()
FROM {{ ref('mart_res_downtimes') }}
