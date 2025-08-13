{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        tags=['hub', 'ПУ_xl_поставки_материалов']
    )
}}

SELECT distinct on (bk_факт_дата)
        hk_dv_hub_date,
        hkcode,
        recsource,
        loadts,
        bk_факт_дата
FROM {{ ref('stg_pu_mat_supply') }} stg

{% if is_incremental() %}
where not exists (
    select 1
    from {{this}} target
    where 
        target.hk_dv_hub_date = stg.hk_dv_hub_date 
)
{% endif %}
        
