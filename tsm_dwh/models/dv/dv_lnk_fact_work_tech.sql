{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        tags=['link', 'жуфвр_1с']
    )
}}

SELECT distinct on (hk_dv_hub_fact_work,
	hk_dv_hub_tech)
        hk_dv_lnk_fact_work_tech,
        hkcode,
        recsource,
        loadts,
        hk_dv_hub_fact_work,
	hk_dv_hub_tech
FROM {{ ref('stg_1c_tech') }} stg

{% if is_incremental() %}
where not exists (
    select 1
    from {{this}} target
    where 
        target.hk_dv_lnk_fact_work_tech = stg.hk_dv_lnk_fact_work_tech 
)
{% endif %}
