{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        tags=['sat', 'жуфвр_1с']
    )
}}


{% if is_incremental() %}
    WITH active_sat AS (
    SELECT DISTINCT ON (hk_dv_hub_fact_work) hk_dv_hub_fact_work, hdiff_dv_sat_fact_work, loadts
    FROM {{this}}
    ORDER BY hk_dv_hub_fact_work, loadts DESC)

    , max_load AS (
    SELECT max(loadts) AS max_loadts
    FROM {{this}}
    )
    , stage_cte AS (

        SELECT
        s.hk_dv_hub_fact_work,
        s.hdiff_dv_sat_fact_work,
        s.loadts,
        s.recsource,
        lag(s.hdiff_dv_sat_fact_work) over(PARTITION BY s.hk_dv_hub_fact_work ORDER BY s.loadts) AS lag_hdiff,
        a.loadts AS sat_loadts,
        a.hdiff_dv_sat_fact_work AS sat_hdiff,
        идентификатор,
	объем_работы,
	примечание,
	кв,
	пометка_удаления,
	тип_spider,
	структура_работ_value,
	структура_работ_codе,
	структура_работ_name,
	видработ_value,
	видработ_codе,
	видработ_name

    FROM
        (SELECT
            s.hk_dv_hub_fact_work,
            s.hdiff_dv_sat_fact_work,
            s.loadts,
            s.recsource,
            идентификатор,
	объем_работы,
	примечание,
	кв,
	пометка_удаления,
	тип_spider,
	структура_работ_value,
	структура_работ_codе,
	структура_работ_name,
	видработ_value,
	видработ_codе,
	видработ_name
        FROM {{ ref('stg_1c_works') }} s
        CROSS JOIN max_load ml
        WHERE s.loadts > ml.max_loadts
        ) s
    LEFT JOIN active_sat a ON a.hk_dv_hub_fact_work = s.hk_dv_hub_fact_work)

    SELECT DISTINCT
        hk_dv_hub_fact_work,
        recsource,
        loadts,
        hdiff_dv_sat_fact_work,
        идентификатор,
	объем_работы,
	примечание,
	кв,
	пометка_удаления,
	тип_spider,
	структура_работ_value,
	структура_работ_codе,
	структура_работ_name,
	видработ_value,
	видработ_codе,
	видработ_name
    FROM stage_cte
    WHERE 
        hdiff_dv_sat_fact_work <> lag_hdiff 
            OR (hdiff_dv_sat_fact_work <> sat_hdiff AND lag_hdiff IS NULL) 
            OR sat_hdiff IS NULL
    ORDER BY loadts

{% else %}
    WITH stage_cte AS (
        SELECT
        s.hk_dv_hub_fact_work,
        s.hdiff_dv_sat_fact_work,
        s.loadts,
        s.recsource,
        lag(s.hdiff_dv_sat_fact_work) over(PARTITION BY s.hk_dv_hub_fact_work ORDER BY s.loadts) AS lag_hdiff,
        идентификатор,
	объем_работы,
	примечание,
	кв,
	пометка_удаления,
	тип_spider,
	структура_работ_value,
	структура_работ_codе,
	структура_работ_name,
	видработ_value,
	видработ_codе,
	видработ_name

    FROM
        (SELECT
            s.hk_dv_hub_fact_work,
            s.hdiff_dv_sat_fact_work,
            s.loadts,
            s.recsource,
            идентификатор,
	объем_работы,
	примечание,
	кв,
	пометка_удаления,
	тип_spider,
	структура_работ_value,
	структура_работ_codе,
	структура_работ_name,
	видработ_value,
	видработ_codе,
	видработ_name
        FROM {{ ref('stg_1c_works') }} s
        ) s
    )

    SELECT DISTINCT
        hk_dv_hub_fact_work,
        recsource,
        loadts,
        hdiff_dv_sat_fact_work,
        идентификатор,
	объем_работы,
	примечание,
	кв,
	пометка_удаления,
	тип_spider,
	структура_работ_value,
	структура_работ_codе,
	структура_работ_name,
	видработ_value,
	видработ_codе,
	видработ_name
    FROM stage_cte
    WHERE 
        hdiff_dv_sat_fact_work <> lag_hdiff 
        OR lag_hdiff IS NULL
    ORDER BY loadts
{% endif %}
