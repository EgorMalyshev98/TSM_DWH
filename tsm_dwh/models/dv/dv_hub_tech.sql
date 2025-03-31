{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        tags=['dev', 'hub', 'жуфвр_1с']
    )
}}

SELECT distinct on (bk_ресурс_uuid)
        hk_dv_hub_tech,
        hkcode,
        recsource,
        loadts,
        bk_ресурс_uuid
FROM {{ ref('stg_1c_tech') }} stg

{% if is_incremental() %}
where not exists (
    select 1
    from {{this}} target
    where 
        target.hk_dv_hub_tech = stg.hk_dv_hub_tech 
)
{% endif %}
        
