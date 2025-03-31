SELECT DISTINCT
    hk_dv_hub_tech,
    hkcode,
    recsource,
    loadts,
    bk_гар_номер
FROM {{ ref('stg_1c_tech') }}
