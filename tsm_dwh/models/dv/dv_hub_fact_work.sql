{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        tags=['dev', 'hub', 'жуфвр_1с']
    )
}}

SELECT distinct on (bk_работа_uuid)
        hk_dv_hub_fact_work,
        hkcode,
        recsource,
        loadts,
        bk_работа_uuid
FROM {{ ref('stg_1c_works') }} stg

{% if is_incremental() %}
where not exists (
    select 1
    from {{this}} target
    where 
        target.hk_dv_hub_fact_work = stg.hk_dv_hub_fact_work 
)
{% endif %}
        
