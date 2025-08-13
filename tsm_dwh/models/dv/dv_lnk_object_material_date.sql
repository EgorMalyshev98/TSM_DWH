{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        tags=['link', 'ПУ_xl_поставки_материалов']
    )
}}

SELECT distinct on (hk_dv_hub_date,
	hk_dv_hub_material,
	hk_dv_hub_object)
        hk_dv_lnk_object_material_date,
        hkcode,
        recsource,
        loadts,
        hk_dv_hub_date,
	hk_dv_hub_material,
	hk_dv_hub_object
FROM {{ ref('stg_pu_mat_supply') }} stg

{% if is_incremental() %}
where not exists (
    select 1
    from {{this}} target
    where 
        target.hk_dv_lnk_object_material_date = stg.hk_dv_lnk_object_material_date 
)
{% endif %}
