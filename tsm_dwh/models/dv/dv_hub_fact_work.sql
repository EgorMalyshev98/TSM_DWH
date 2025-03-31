SELECT DISTINCT
    hk_dv_hub_fact_work,
    hkcode,
    recsource,
    loadts,
    bk_работа_uuid
FROM {{ ref('stg_1c_works') }}
