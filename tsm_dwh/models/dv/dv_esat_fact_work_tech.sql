-- depends_on: {{ ref('dv_lnk_fact_work_tech') }}

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
        t.hk_dv_lnk_fact_work_tech,
        t.loadts,
        t.start_date,
        l.hk_dv_hub_fact_work,
        l.hk_dv_hub_tech
    FROM 
        (SELECT 
            e.*,
            dense_rank() over(PARTITION BY hk_dv_lnk_fact_work_tech ORDER BY loadts desc) AS rnk
        FROM {{ this }} e) t
    JOIN {{ ref('dv_lnk_fact_work_tech') }} l ON t.hk_dv_lnk_fact_work_tech = l.hk_dv_lnk_fact_work_tech
    WHERE rnk = 1 AND end_date IS NULL
)

-- stage + активные записи
, stage AS (
    SELECT 
        hk_dv_lnk_fact_work_tech,
        hk_dv_hub_fact_work,
        hk_dv_hub_tech,
        loadts,
        start_date,
        'active esat' AS select_source
        
    FROM active_esat a
    
    UNION ALL
    
    SELECT 
        hk_dv_lnk_fact_work_tech,
        hk_dv_hub_fact_work,
        hk_dv_hub_tech,
        loadts,
        loadts AS start_date,
        'stage' AS select_source
        
    FROM {{ ref('stg_1c_tech') }} stg
    CROSS JOIN (SELECT max(loadts) FROM active_esat) m (max_loadts)
    WHERE loadts > max_loadts
)

-- агрегированные в одной строке зависимые ключи по каждому bk
, stage_agg AS (
    SELECT 
        hk_dv_hub_fact_work,
        loadts,
        string_agg(hk_dv_hub_tech::varchar, ',' ORDER BY hk_dv_hub_tech) AS dep_keys,
        select_source
    FROM stage
    GROUP BY hk_dv_hub_fact_work, loadts, select_source
)

-- сравнение зависимых ключей между партиями вставки
, compared_stage_agg AS (
  SELECT
        hk_dv_hub_fact_work,
        loadts,
        dep_keys,
        lead(dep_keys) OVER(PARTITION BY hk_dv_hub_fact_work order BY loadts) AS lead_dep_keys,
        lead(loadts) OVER(PARTITION BY hk_dv_hub_fact_work order BY loadts) AS end_date,
        lag_dep_keys,
        select_source
  FROM (
      SELECT
          hk_dv_hub_fact_work,
          loadts,
          dep_keys,
          lag(dep_keys) OVER(PARTITION BY hk_dv_hub_fact_work order BY loadts) AS lag_dep_keys,
          select_source
      FROM stage_agg s) s
  WHERE dep_keys <> lag_dep_keys OR lag_dep_keys IS null
)

-- фильтрация
, parts_for_insert AS (
    SELECT DISTINCT hk_dv_hub_fact_work, loadts, end_date
    FROM compared_stage_agg
    WHERE 
        dep_keys <> lead_dep_keys 
        OR (end_date IS NULL AND select_source <> 'active esat')
)
    
SELECT 
    s.hk_dv_lnk_fact_work_tech,
    s.loadts,
    s.start_date,
    p.end_date
FROM stage s
JOIN parts_for_insert p
    ON s.hk_dv_hub_fact_work = p.hk_dv_hub_fact_work
    AND s.loadts = p.loadts


{% else %}


with stage as (
    SELECT 
        hk_dv_lnk_fact_work_tech,
        hk_dv_hub_fact_work,
        hk_dv_hub_tech,
        loadts,
        loadts AS start_date
        
    FROM {{ ref('stg_1c_tech') }} stg

)

-- агрегированные в одной строке зависимые ключи по каждому bk
, stage_agg AS (
    SELECT 
        hk_dv_hub_fact_work,
        loadts,
        string_agg(hk_dv_hub_tech::varchar, ',' ORDER BY hk_dv_hub_tech) AS dep_keys
    FROM stage
    GROUP BY hk_dv_hub_fact_work, loadts
)

-- сравнение зависимых ключей между партиями вставки
, compared_stage_agg AS (
  SELECT
        hk_dv_hub_fact_work,
        loadts,
        dep_keys,
        lead(dep_keys) OVER(PARTITION BY hk_dv_hub_fact_work order BY loadts) AS lead_dep_keys,
        lead(loadts) OVER(PARTITION BY hk_dv_hub_fact_work order BY loadts) AS end_date,
        lag_dep_keys
  FROM (
      SELECT
          hk_dv_hub_fact_work,
          loadts,
          dep_keys,
          lag(dep_keys) OVER(PARTITION BY hk_dv_hub_fact_work order BY loadts) AS lag_dep_keys
      FROM stage_agg s) s
  WHERE dep_keys <> lag_dep_keys OR lag_dep_keys IS null
)

-- фильтрация
, parts_for_insert AS (
    SELECT DISTINCT hk_dv_hub_fact_work, loadts, end_date
    FROM compared_stage_agg
    WHERE 
        dep_keys <> lead_dep_keys 
        OR end_date IS NULL
)
    
SELECT 
    s.hk_dv_lnk_fact_work_tech,
    s.loadts,
    s.start_date,
    p.end_date
FROM stage s
JOIN parts_for_insert p
    ON s.hk_dv_hub_fact_work = p.hk_dv_hub_fact_work
    AND s.loadts = p.loadts
{% endif %}
