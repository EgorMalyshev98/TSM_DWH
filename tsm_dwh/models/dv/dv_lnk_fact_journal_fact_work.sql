{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        tags=['link', 'жуфвр_1с']
    )
}}

SELECT distinct on (hk_dv_hub_fact_journal,
	hk_dv_hub_fact_work)
        hk_dv_lnk_fact_journal_fact_work,
        hkcode,
        recsource,
        loadts,
        hk_dv_hub_fact_journal,
	hk_dv_hub_fact_work
FROM {{ ref('stg_1c_works') }} stg

{% if is_incremental() %}
where not exists (
    select 1
    from {{this}} target
    where 
        target.hk_dv_lnk_fact_journal_fact_work = stg.hk_dv_lnk_fact_journal_fact_work 
)
{% endif %}
