SELECT DISTINCT
    hk_dv_hub_fact_journal,
    hkcode,
    recsource,
    loadts,
    bk_жуфвр_uuid
FROM {{ ref('stg_1c_journal') }}
