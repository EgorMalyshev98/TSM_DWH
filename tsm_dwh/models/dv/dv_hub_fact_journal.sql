{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        tags=['hub', 'жуфвр_1с']
    )
}}

SELECT distinct on (bk_жуфвр_uuid)
        hk_dv_hub_fact_journal,
        hkcode,
        recsource,
        loadts,
        bk_жуфвр_uuid
FROM {{ ref('stg_1c_journal') }} stg

{% if is_incremental() %}
where not exists (
    select 1
    from {{this}} target
    where 
        target.hk_dv_hub_fact_journal = stg.hk_dv_hub_fact_journal 
)
{% endif %}
        
