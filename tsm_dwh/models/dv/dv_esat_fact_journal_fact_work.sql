-- depends_on: {{ ref('dv_lnk_fact_journal_fact_work') }}

{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        tags=['esat', 'жуфвр_1с']
    )
}}


{% if is_incremental() %}
-- aктивные записи
WITH active_esat as(
    SELECT
        t.hk_dv_lnk_fact_journal_fact_work,
        t.loadts,
        t.start_date,
        l.hk_dv_hub_fact_journal,
        l.hk_dv_hub_fact_work
    FROM 
        (SELECT 
            e.*,
            dense_rank() over(PARTITION BY hk_dv_lnk_fact_journal_fact_work ORDER BY loadts desc) AS rnk
        FROM {{ this }} e) t
    JOIN {{ ref('dv_lnk_fact_journal_fact_work') }} l ON t.hk_dv_lnk_fact_journal_fact_work = l.hk_dv_lnk_fact_journal_fact_work
    WHERE rnk = 1 AND end_date IS NULL
)

-- stage + активные записи
, stage AS (
    SELECT 
        hk_dv_lnk_fact_journal_fact_work,
        hk_dv_hub_fact_journal,
        hk_dv_hub_fact_work,
        loadts,
        start_date,
        'active esat' AS select_source
        
    FROM active_esat a
    
    UNION ALL
    
    SELECT 
        hk_dv_lnk_fact_journal_fact_work,
        hk_dv_hub_fact_journal,
        hk_dv_hub_fact_work,
        loadts,
        loadts AS start_date,
        'stage' AS select_source
        
    FROM {{ ref('stg_1c_works') }} stg
    CROSS JOIN (SELECT max(loadts) FROM active_esat) m (max_loadts)
    WHERE loadts > max_loadts
)

-- агрегированные в одной строке зависимые ключи по каждому bk
, stage_agg AS (
    SELECT 
        hk_dv_hub_fact_journal,
        loadts,
        string_agg(hk_dv_hub_fact_work::varchar, ',' ORDER BY hk_dv_hub_fact_work) AS dep_keys,
        select_source
    FROM stage
    GROUP BY hk_dv_hub_fact_journal, loadts, select_source
)

-- сравнение зависимых ключей между партиями вставки
, compared_stage_agg AS (
  SELECT
        hk_dv_hub_fact_journal,
        loadts,
        dep_keys,
        lead(dep_keys) OVER(PARTITION BY hk_dv_hub_fact_journal order BY loadts) AS lead_dep_keys,
        lead(loadts) OVER(PARTITION BY hk_dv_hub_fact_journal order BY loadts) AS end_date,
        lag_dep_keys,
        select_source
  FROM (
      SELECT
          hk_dv_hub_fact_journal,
          loadts,
          dep_keys,
          lag(dep_keys) OVER(PARTITION BY hk_dv_hub_fact_journal order BY loadts) AS lag_dep_keys,
          select_source
      FROM stage_agg s) s
  WHERE dep_keys <> lag_dep_keys OR lag_dep_keys IS null
)

-- фильтрация
, parts_for_insert AS (
    SELECT DISTINCT hk_dv_hub_fact_journal, loadts, end_date
    FROM compared_stage_agg
    WHERE 
        dep_keys <> lead_dep_keys 
        OR (end_date IS NULL AND select_source <> 'active esat')
)
    
SELECT 
    s.hk_dv_lnk_fact_journal_fact_work,
    s.loadts,
    s.start_date,
    p.end_date
FROM stage s
JOIN parts_for_insert p
    ON s.hk_dv_hub_fact_journal = p.hk_dv_hub_fact_journal
    AND s.loadts = p.loadts


{% else %}


with stage as (
    SELECT 
        hk_dv_lnk_fact_journal_fact_work,
        hk_dv_hub_fact_journal,
        hk_dv_hub_fact_work,
        loadts,
        loadts AS start_date
        
    FROM {{ ref('stg_1c_works') }} stg

)

-- агрегированные в одной строке зависимые ключи по каждому bk
, stage_agg AS (
    SELECT 
        hk_dv_hub_fact_journal,
        loadts,
        string_agg(hk_dv_hub_fact_work::varchar, ',' ORDER BY hk_dv_hub_fact_work) AS dep_keys
    FROM stage
    GROUP BY hk_dv_hub_fact_journal, loadts
)

-- сравнение зависимых ключей между партиями вставки
, compared_stage_agg AS (
  SELECT
        hk_dv_hub_fact_journal,
        loadts,
        dep_keys,
        lead(dep_keys) OVER(PARTITION BY hk_dv_hub_fact_journal order BY loadts) AS lead_dep_keys,
        lead(loadts) OVER(PARTITION BY hk_dv_hub_fact_journal order BY loadts) AS end_date,
        lag_dep_keys
  FROM (
      SELECT
          hk_dv_hub_fact_journal,
          loadts,
          dep_keys,
          lag(dep_keys) OVER(PARTITION BY hk_dv_hub_fact_journal order BY loadts) AS lag_dep_keys
      FROM stage_agg s) s
  WHERE dep_keys <> lag_dep_keys OR lag_dep_keys IS null
)

-- фильтрация
, parts_for_insert AS (
    SELECT DISTINCT hk_dv_hub_fact_journal, loadts, end_date
    FROM compared_stage_agg
    WHERE 
        dep_keys <> lead_dep_keys 
        OR end_date IS NULL
)
    
SELECT 
    s.hk_dv_lnk_fact_journal_fact_work,
    s.loadts,
    s.start_date,
    p.end_date
FROM stage s
JOIN parts_for_insert p
    ON s.hk_dv_hub_fact_journal = p.hk_dv_hub_fact_journal
    AND s.loadts = p.loadts
{% endif %}
